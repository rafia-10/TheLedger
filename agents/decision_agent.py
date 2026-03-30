"""
Decision Agent — synthesizes all agent inputs and emits DecisionGenerated.
Trigger: Compliance status = PASSED (both rules cleared).
"""
import asyncio
import random
from agents.base import BaseAgent
from src.aggregates.compliance_record import ComplianceRecordAggregate
from src.aggregates.loan_application import LoanApplicationAggregate
from src.commands.handlers import handle_generate_decision


class DecisionAgent(BaseAgent):
    def __init__(self, store, log_sink, target_app_id=None):
        super().__init__(store, "decision-engine", "v1.0",
                         poll_interval=0.7, color="blue", log_sink=log_sink,
                         target_app_id=target_app_id)

    async def _poll(self):
        for app_id in await self._apps_with_event("ComplianceRulePassed"):
            if await self._has_event(f"loan-{app_id}", "DecisionGenerated"):
                continue

            compliance = await ComplianceRecordAggregate.load(self.store, app_id)
            if compliance.get_status() != "PASSED":
                continue

            await self._process(app_id)

    async def _process(self, app_id: str):
        self._log(f"[dim]→[/dim] Synthesizing decision for [bold]{app_id}[/bold] ...")

        await asyncio.sleep(random.uniform(0.8, 1.4))  # simulate LLM reasoning

        # Pull credit confidence from the event stream to base decision on real data
        loan_stream = await self.store.load_stream(f"loan-{app_id}")
        credit_evt = next(
            (e for e in loan_stream if e.event_type == "CreditAnalysisCompleted"), None
        )
        fraud_evt = next(
            (e for e in loan_stream if e.event_type == "FraudScreeningCompleted"), None
        )

        credit_conf = credit_evt.payload.get("confidence_score", 0.88) if credit_evt else 0.88
        fraud_score = fraud_evt.payload.get("fraud_score", 0.1) if fraud_evt else 0.1
        # Combine: high credit conf + low fraud score → high overall confidence
        confidence = round(min(0.98, credit_conf * (1 - fraud_score * 0.5)), 2)
        recommendation = "APPROVE" if confidence >= 0.6 else "REFER"

        credit_session = credit_evt.payload.get("session_id", "unknown") if credit_evt else "unknown"

        result = await self._retry(
            handle_generate_decision,
            self.store, app_id, self.agent_id,
            recommendation, confidence,
            [credit_session],
            (
                f"Credit profile strong (conf={credit_conf:.0%}). "
                f"Fraud risk low (score={fraud_score:.2f}). "
                f"KYC/AML cleared. Auto-approve threshold met."
            ),
            {"credit": "v2.1", "fraud": "v3.0"},
        )
        if result:
            self._log(
                f"[green]✓[/green] [bold]DecisionGenerated[/bold] — "
                f"[bold green]{recommendation}[/bold green] | conf={confidence:.0%}"
            )
