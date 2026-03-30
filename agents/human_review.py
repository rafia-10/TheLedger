"""
Human Review Agent — simulates a senior underwriter reviewing the AI decision.
Trigger: DecisionGenerated exists for an app.
Emits HumanReviewCompleted, then ApplicationApproved / ApplicationDeclined.
"""
import asyncio
import random
from datetime import datetime, timedelta
from agents.base import BaseAgent
from src.aggregates.loan_application import LoanApplicationAggregate
from src.commands.handlers import handle_record_human_review
from src.models.events import BaseEvent, ApplicationApproved, ApplicationDeclined


REVIEWERS = ["senior-underwriter-sarah", "manager-chen", "underwriter-patel"]


class HumanReviewAgent(BaseAgent):
    def __init__(self, store, log_sink, target_app_id=None):
        super().__init__(store, "human-reviewer", "human",
                         poll_interval=0.8, color="pink1", log_sink=log_sink,
                         target_app_id=target_app_id)

    async def _poll(self):
        for app_id in await self._apps_with_event("DecisionGenerated"):
            if await self._has_event(f"loan-{app_id}", "HumanReviewCompleted"):
                continue
            await self._process(app_id)

    async def _process(self, app_id: str):
        reviewer = random.choice(REVIEWERS)
        self._log(f"[dim]→[/dim] [bold]{reviewer}[/bold] reviewing [bold]{app_id}[/bold] ...")

        await asyncio.sleep(random.uniform(1.2, 2.0))  # humans take longer

        # Read the AI decision from the stream
        loan_stream = await self.store.load_stream(f"loan-{app_id}")
        decision_evt = next(
            (e for e in loan_stream if e.event_type == "DecisionGenerated"), None
        )
        ai_decision = decision_evt.payload.get("recommendation", "APPROVE") if decision_evt else "APPROVE"

        # Human rarely overrides the AI (10% chance for demo variety)
        override = random.random() < 0.10
        if override:
            final = "APPROVE" if ai_decision == "DECLINE" else "APPROVE"
            reason = "Manual review: additional context supports approval."
        else:
            final = ai_decision
            reason = None

        result = await self._retry(
            handle_record_human_review,
            self.store, app_id, reviewer, override, final, reason,
        )
        if result is None:
            return

        override_str = f" [override → {final}]" if override else " [confirmed AI]"
        self._log(
            f"[green]✓[/green] [bold]HumanReviewCompleted[/bold] — "
            f"{reviewer}{override_str}"
        )

        # Append the terminal outcome event
        await self._append_outcome(app_id, final, reviewer)

    async def _append_outcome(self, app_id: str, final_decision: str, reviewer: str):
        app = await LoanApplicationAggregate.load(self.store, app_id)
        if final_decision == "APPROVE":
            event = BaseEvent(
                event_type="ApplicationApproved",
                payload=ApplicationApproved(
                    application_id=app_id,
                    approved_amount_usd=app.requested_amount,
                    interest_rate=0.0625,
                    conditions=["Fixed-rate 30yr", "No prepayment penalty"],
                    approved_by=reviewer,
                    effective_date=datetime.now() + timedelta(days=30),
                ).model_dump(mode="json"),
            )
            outcome_label = f"[bold green]ApplicationApproved[/bold green] — ${app.requested_amount:,.0f} @ 6.25%"
        else:
            event = BaseEvent(
                event_type="ApplicationDeclined",
                payload=ApplicationDeclined(
                    application_id=app_id,
                    decline_reasons=["Risk profile does not meet current criteria."],
                    declined_by=reviewer,
                    adverse_action_notice_required=True,
                ).model_dump(mode="json"),
            )
            outcome_label = "[bold red]ApplicationDeclined[/bold red]"

        await self._retry(
            self.store.append,
            f"loan-{app_id}", [event], app.version,
        )
        self._log(f"[green]✓[/green] {outcome_label}")
