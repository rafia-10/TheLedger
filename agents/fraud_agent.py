"""
Fraud Agent — screens for fraud signals and emits FraudScreeningCompleted.
Trigger: ApplicationSubmitted exists for an app.
Runs concurrently with the Credit Agent.
"""
import asyncio
import random
from agents.base import BaseAgent
from src.aggregates.agent_session import AgentSessionAggregate
from src.commands.handlers import (
    handle_start_agent_session,
    handle_fraud_screening_completed,
)


class FraudAgent(BaseAgent):
    def __init__(self, store, log_sink, target_app_id=None):
        super().__init__(store, "fraud-agent-01", "v3.0",
                         poll_interval=0.6, color="yellow", log_sink=log_sink,
                         target_app_id=target_app_id)

    async def _poll(self):
        for app_id in await self._apps_with_event("ApplicationSubmitted"):
            if await self._has_event(f"loan-{app_id}", "FraudScreeningCompleted"):
                continue
            await self._process(app_id)

    async def _process(self, app_id: str):
        session_id = f"s-{app_id[:8]}-fr"
        self._log(f"[dim]→[/dim] Running fraud screen for [bold]{app_id}[/bold] ...")

        session = await AgentSessionAggregate.load(self.store, self.agent_id, session_id)
        if not session.context_loaded:
            await self._retry(
                handle_start_agent_session,
                self.store, self.agent_id, session_id,
                "RiskGraph-v5", 0, 380, self.model_version, budget=20000,
            )

        await asyncio.sleep(random.uniform(0.6, 1.2))  # simulate ML inference

        fraud_score = round(random.uniform(0.03, 0.13), 3)
        anomaly_flags = []
        if fraud_score > 0.10:
            anomaly_flags.append("velocity_check_marginal")

        result = await self._retry(
            handle_fraud_screening_completed,
            self.store, app_id, self.agent_id, session_id,
            self.model_version, fraud_score, anomaly_flags,
            {"ip": "192.168.1.1", "device": "desktop", "velocity": "normal"},
        )
        if result:
            flag_str = f"flags={anomaly_flags}" if anomaly_flags else "no flags"
            self._log(
                f"[green]✓[/green] [bold]FraudScreeningCompleted[/bold] — "
                f"score={fraud_score:.3f} | {flag_str}"
            )
