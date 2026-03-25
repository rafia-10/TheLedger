import pytest
import asyncio
from datetime import datetime
from src.models.events import StoredEvent, DomainError
from src.aggregates.loan_application import LoanApplicationAggregate, ApplicationState
from src.aggregates.agent_session import AgentSessionAggregate
from src.aggregates.compliance_record import ComplianceRecordAggregate

def create_event(event_type: str, payload: dict, pos: int = 1) -> StoredEvent:
    import uuid
    return StoredEvent(
        event_id=uuid.uuid4(),
        stream_id="test",
        stream_position=pos,
        global_position=pos,
        event_type=event_type,
        event_version=1,
        payload=payload,
        metadata={},
        recorded_at=datetime.now()
    )

def test_rule_1_can_submit():
    app = LoanApplicationAggregate("app-123")
    app.assert_can_submit() # Should pass
    
    app._apply(create_event("ApplicationSubmitted", {"applicant_id": "u1", "requested_amount_usd": 1000}))
    with pytest.raises(DomainError, match="already exists"):
        app.assert_can_submit()

def test_rule_2_awaiting_analysis():
    app = LoanApplicationAggregate("app-123")
    with pytest.raises(DomainError, match="Cannot complete credit analysis"):
        app.assert_awaiting_credit_analysis()
        
    app._apply(create_event("ApplicationSubmitted", {"applicant_id": "u1", "requested_amount_usd": 1000}))
    app.assert_awaiting_credit_analysis() # Should pass

def test_rule_3_compliance_dependency():
    app = LoanApplicationAggregate("app-123")
    # Rule 3 says we need required checks
    with pytest.raises(DomainError, match="Compliance check required"):
        app.assert_compliance_passed(["KYC"])
        
    app._apply(create_event("ComplianceRulePassed", {"rule_id": "KYC"}))
    app.assert_compliance_passed(["KYC"]) # Should pass

def test_rule_4_confidence_floor():
    app = LoanApplicationAggregate("app-123")
    # Low confidence -> REFER
    assert app.validate_decision("APPROVE", 0.5) == "REFER"
    # High confidence -> APPROVE
    assert app.validate_decision("APPROVE", 0.7) == "APPROVE"
    # Decline remains Decline regardless of confidence
    assert app.validate_decision("DECLINE", 0.9) == "DECLINE"

def test_rule_5_causal_sessions():
    app = LoanApplicationAggregate("app-123")
    with pytest.raises(DomainError, match="Decisions require at least one"):
        app.assert_can_generate_decision([])

def test_rule_6_state_machine():
    app = LoanApplicationAggregate("app-123")
    app._apply(create_event("ApplicationSubmitted", {"applicant_id": "u1", "requested_amount_usd": 1000}))
    
    # Valid transition
    app.assert_valid_transition(ApplicationState.AWAITING_ANALYSIS)
    
    # Invalid transition (skipping steps to FINAL_APPROVED)
    with pytest.raises(DomainError, match="Invalid state transition"):
        app.assert_valid_transition(ApplicationState.FINAL_APPROVED)

def test_rule_7_token_budget():
    session = AgentSessionAggregate("a1", "s1")
    session._apply(create_event("AgentContextLoaded", {"model_version": "v1", "token_budget": 50}))
    
    # Within budget
    session.tokens_used = 40
    session.assert_within_token_budget()
    
    # Exceeded
    session.tokens_used = 60
    with pytest.raises(DomainError, match="Token budget exceeded"):
        session.assert_within_token_budget()
    assert session.health_status == "NEEDS_RECONCILIATION"
