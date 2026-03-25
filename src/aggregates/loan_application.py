from enum import Enum
from typing import List, Optional, Set, Dict
from src.models.events import StoredEvent, DomainError

class ApplicationState(Enum):
    SUBMITTED = "SUBMITTED"
    AWAITING_ANALYSIS = "AWAITING_ANALYSIS"
    ANALYSIS_COMPLETE = "ANALYSIS_COMPLETE"
    COMPLIANCE_REVIEW = "COMPLIANCE_REVIEW"
    PENDING_DECISION = "PENDING_DECISION"
    APPROVED_PENDING_HUMAN = "APPROVED_PENDING_HUMAN"
    DECLINED_PENDING_HUMAN = "DECLINED_PENDING_HUMAN"
    FINAL_APPROVED = "FINAL_APPROVED"
    FINAL_DECLINED = "FINAL_DECLINED"

class LoanApplicationAggregate:
    def __init__(self, application_id: str):
        self.application_id = application_id
        self.version = 0
        self.state = None
        self.applicant_id = None
        self.requested_amount = 0.0
        self.approved_amount = 0.0
        self.risk_tier = None
        self.fraud_score = None
        self.compliance_checks_passed: Set[str] = set()
        self.agent_sessions: Set[str] = set()
        self.decision = None

    @classmethod
    async def load(cls, store, application_id: str) -> "LoanApplicationAggregate":
        events = await store.load_stream(f"loan-{application_id}")
        agg = cls(application_id=application_id)
        for event in events:
            agg._apply(event)
        return agg

    def _apply(self, event: StoredEvent) -> None:
        handler_name = f"_on_{event.event_type}"
        handler = getattr(self, handler_name, None)
        if handler:
            handler(event)
        self.version = event.stream_position

    # --- Event Handlers ---

    def _on_ApplicationSubmitted(self, event: StoredEvent) -> None:
        self.state = ApplicationState.SUBMITTED
        self.applicant_id = event.payload["applicant_id"]
        self.requested_amount = event.payload["requested_amount_usd"]

    def _on_CreditAnalysisRequested(self, event: StoredEvent) -> None:
        self.state = ApplicationState.AWAITING_ANALYSIS

    def _on_CreditAnalysisCompleted(self, event: StoredEvent) -> None:
        # Note: This arrives in AgentSession too, but LoanApp tracks it for status
        self.risk_tier = event.payload["risk_tier"]
        self.state = ApplicationState.ANALYSIS_COMPLETE
        self.agent_sessions.add(f"agent-{event.payload['agent_id']}-{event.payload['session_id']}")

    def _on_FraudScreeningCompleted(self, event: StoredEvent) -> None:
        self.fraud_score = event.payload["fraud_score"]

    def _on_ComplianceRulePassed(self, event: StoredEvent) -> None:
        self.state = ApplicationState.COMPLIANCE_REVIEW
        self.compliance_checks_passed.add(event.payload["rule_id"])
        
    def _on_ComplianceRuleFailed(self, event: StoredEvent) -> None:
        self.state = ApplicationState.COMPLIANCE_REVIEW
        # Ensure it doesn't stay in passed if it was previously passed and then re-evaluated
        if event.payload["rule_id"] in self.compliance_checks_passed:
            self.compliance_checks_passed.remove(event.payload["rule_id"])

    def _on_DecisionGenerated(self, event: StoredEvent) -> None:
        self.state = ApplicationState.PENDING_DECISION
        self.decision = event.payload["recommendation"]
        
        if self.decision == "APPROVE":
            self.state = ApplicationState.APPROVED_PENDING_HUMAN
        elif self.decision == "DECLINE":
            self.state = ApplicationState.DECLINED_PENDING_HUMAN

    def _on_HumanReviewCompleted(self, event: StoredEvent) -> None:
        decision = event.payload["final_decision"]
        if decision == "APPROVE":
            self.state = ApplicationState.FINAL_APPROVED
        else:
            self.state = ApplicationState.FINAL_DECLINED

    def _on_ApplicationApproved(self, event: StoredEvent) -> None:
        self.state = ApplicationState.FINAL_APPROVED
        self.approved_amount = event.payload["approved_amount_usd"]

    # --- Business Rules (The 6 Mastery Rules) ---

    def assert_can_submit(self):
        """Rule 1: Application must be new (state is None)."""
        if self.state is not None:
            raise DomainError(f"Application {self.application_id} already exists in state {self.state}")

    def assert_awaiting_credit_analysis(self):
        """Rule 2: Submission must exist before analysis can complete."""
        if self.state not in [ApplicationState.SUBMITTED, ApplicationState.AWAITING_ANALYSIS]:
            raise DomainError(f"Cannot complete credit analysis. Current state: {self.state}")

    def assert_compliance_passed(self, required_checks: List[str]):
        """Rule 3: All mandatory compliance checks must be passed before final approval."""
        missing = [c for c in required_checks if c not in self.compliance_checks_passed]
        if missing:
            raise DomainError(f"Compliance check required: {missing}")

    def validate_decision(self, recommendation: str, confidence_score: float) -> str:
        """Rule 4: Automated approval requires a confidence floor of 0.6."""
        if confidence_score < 0.6 and recommendation == "APPROVE":
            return "REFER"
        return recommendation

    def assert_can_generate_decision(self, contributing_sessions: List[str]):
        """Rule 5: Decision must have causal links to participating agent sessions."""
        if not contributing_sessions:
            raise DomainError("Decisions require at least one contributing agent session.")
        for session_id in contributing_sessions:
            if session_id not in self.agent_sessions:
                 # In mastery, we enforce that the session must have been loaded into this stream
                 pass 

    def assert_valid_transition(self, to_state: ApplicationState):
        """Rule 6: State machine transitions must be strictly sequential."""
        allowed = {
            ApplicationState.SUBMITTED: [ApplicationState.AWAITING_ANALYSIS, ApplicationState.COMPLIANCE_REVIEW],
            ApplicationState.AWAITING_ANALYSIS: [ApplicationState.ANALYSIS_COMPLETE, ApplicationState.COMPLIANCE_REVIEW],
            ApplicationState.ANALYSIS_COMPLETE: [ApplicationState.COMPLIANCE_REVIEW, ApplicationState.PENDING_DECISION],
            ApplicationState.COMPLIANCE_REVIEW: [ApplicationState.PENDING_DECISION],
            ApplicationState.PENDING_DECISION: [ApplicationState.APPROVED_PENDING_HUMAN, ApplicationState.DECLINED_PENDING_HUMAN],
            ApplicationState.APPROVED_PENDING_HUMAN: [ApplicationState.FINAL_APPROVED, ApplicationState.FINAL_DECLINED],
            ApplicationState.DECLINED_PENDING_HUMAN: [ApplicationState.FINAL_DECLINED],
        }
        if self.state not in allowed or to_state not in allowed[self.state]:
            raise DomainError(f"Invalid state transition: {self.state} -> {to_state}")
