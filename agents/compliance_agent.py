"""
Compliance Agent — validates KYC + AML rules and emits ComplianceRulePassed events.
Trigger: Both CreditAnalysisCompleted AND FraudScreeningCompleted exist for the app.
         (Ensures credit has run first so state machine transitions are valid.)
"""
import asyncio
import random
from agents.base import BaseAgent
from src.commands.handlers import (
    handle_request_compliance,
    handle_record_compliance_check,
)
from src.aggregates.compliance_record import ComplianceRecordAggregate


RULES = [
    ("KYC-RULE-01", {"id_verified": True, "document": "passport"}),
    ("AML-RULE-01", {"sanctions_clear": True, "pep_clear": True}),
]
REGULATION_VERSION = "v3.2"


class ComplianceAgent(BaseAgent):
    def __init__(self, store, log_sink, target_app_id=None):
        super().__init__(store, "compliance-agent-01", "v1.0",
                         poll_interval=0.7, color="cyan", log_sink=log_sink,
                         target_app_id=target_app_id)

    async def _poll(self):
        for app_id in await self._apps_with_event("CreditAnalysisCompleted"):
            # Wait for fraud to finish too (both feed the decision engine)
            if not await self._has_event(f"loan-{app_id}", "FraudScreeningCompleted"):
                continue
            # Skip if compliance already done
            if await self._has_event(f"loan-{app_id}", "ComplianceRulePassed"):
                continue
            await self._process(app_id)

    async def _process(self, app_id: str):
        self._log(f"[dim]→[/dim] Starting KYC/AML checks for [bold]{app_id}[/bold] ...")

        # Request compliance (initialises the compliance stream)
        compliance = await ComplianceRecordAggregate.load(self.store, app_id)
        if not compliance.is_requested:
            await self._retry(
                handle_request_compliance,
                self.store, app_id,
                [r[0] for r in RULES],
                REGULATION_VERSION,
            )

        # Record each rule with a short processing delay
        for rule_id, evidence in RULES:
            # Skip if already recorded
            async with self.store.transaction() as conn:
                exists = await conn.fetchrow(
                    "SELECT 1 FROM events "
                    "WHERE stream_id = $1 AND event_type = 'ComplianceRulePassed' "
                    "AND payload->>'rule_id' = $2 LIMIT 1",
                    f"compliance-{app_id}", rule_id,
                )
            if exists:
                continue

            await asyncio.sleep(random.uniform(0.4, 0.8))

            result = await self._retry(
                handle_record_compliance_check,
                self.store, app_id, rule_id, REGULATION_VERSION,
                True, evidence,
            )
            if result:
                self._log(
                    f"[green]✓[/green] [bold]ComplianceRulePassed[/bold] — {rule_id}"
                )
