"""
Credit Agent — analyzes credit risk and emits CreditAnalysisCompleted.
Trigger: ApplicationSubmitted exists for an app.
"""
import asyncio
import random
from agents.base import BaseAgent
from src.aggregates.agent_session import AgentSessionAggregate
from src.commands.handlers import (
    handle_start_agent_session,
    handle_credit_analysis_completed,
)


class CreditAgent(BaseAgent):
    def __init__(self, store, log_sink, target_app_id=None):
        super().__init__(store, "credit-agent-01", "v2.1",
                         poll_interval=0.6, color="magenta", log_sink=log_sink,
                         target_app_id=target_app_id)

    async def _poll(self):
        for app_id in await self._apps_with_event("ApplicationSubmitted"):
            if await self._has_event(f"loan-{app_id}", "CreditAnalysisCompleted"):
                continue
            await self._process(app_id)

    async def _process(self, app_id: str):
        session_id = f"s-{app_id[:8]}-cr"
        self._log(f"[dim]→[/dim] Analyzing credit risk for [bold]{app_id}[/bold] ...")

        # Boot agent session (Gas Town)
        session = await AgentSessionAggregate.load(self.store, self.agent_id, session_id)
        if not session.context_loaded:
            await self._retry(
                handle_start_agent_session,
                self.store, self.agent_id, session_id,
                "VectorDB-KYC", 0, 512, self.model_version, budget=20000,
            )

        await asyncio.sleep(random.uniform(0.9, 1.7))  # simulate credit bureau pull

        # Read requested amount from the ApplicationSubmitted event
        async with self.store.transaction() as conn:
            row = await conn.fetchrow(
                "SELECT payload->>'requested_amount_usd' AS amount "
                "FROM events WHERE stream_id = $1 AND event_type = 'ApplicationSubmitted' LIMIT 1",
                f"loan-{app_id}",
            )
        requested_amount = float(row["amount"]) if row and row["amount"] else 250_000.0

        fico = random.randint(690, 780)
        dti = round(random.uniform(0.22, 0.35), 2)
        confidence = round(random.uniform(0.83, 0.96), 2)
        risk_tier = "TIER_A" if fico >= 720 else "TIER_B"
        duration_ms = random.randint(110, 220)

        result = await self._retry(
            handle_credit_analysis_completed,
            self.store, app_id, self.agent_id, session_id,
            self.model_version, confidence, risk_tier,
            requested_amount, duration_ms,
            {"fico": fico, "dti": dti, "income_usd": random.randint(90000, 150000)},
        )
        if result:
            self._log(
                f"[green]✓[/green] [bold]CreditAnalysisCompleted[/bold] — "
                f"{risk_tier} | conf={confidence:.0%} | FICO={fico} | DTI={dti}"
            )
