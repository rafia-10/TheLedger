import hashlib
import json
from datetime import datetime
from typing import Dict, Any, Optional, List
from pydantic import BaseModel

from src.event_store import EventStore
from src.models.events import (
    ApplicationSubmitted,
    CreditAnalysisCompleted,
    AgentContextLoaded,
    FraudScreeningCompleted,
    DecisionGenerated,
    HumanReviewCompleted,
    ComplianceRulePassed,
    ComplianceRuleFailed,
    ComplianceCheckRequested,
    BaseEvent,
    DomainError
)
from src.aggregates.loan_application import LoanApplicationAggregate, ApplicationState
from src.aggregates.agent_session import AgentSessionAggregate
from src.aggregates.compliance_record import ComplianceRecordAggregate
from src.aggregates.audit_ledger import AuditLedgerAggregate

def hash_inputs(data: Dict[str, Any]) -> str:
    """Deterministic hash of input data for auditability."""
    encoded = json.dumps(data, sort_keys=True).encode()
    return hashlib.sha256(encoded).hexdigest()

async def handle_submit_application(
    store: EventStore,
    application_id: str,
    applicant_id: str,
    amount: float,
    purpose: str,
    channel: str
) -> int:
    # 1. Load aggregate
    app = await LoanApplicationAggregate.load(store, application_id)
    
    # 2. Validate
    app.assert_can_submit()
    
    # 3. Determine new events
    new_events = [
        BaseEvent(
            event_type="ApplicationSubmitted",
            payload=ApplicationSubmitted(
                application_id=application_id,
                applicant_id=applicant_id,
                requested_amount_usd=amount,
                loan_purpose=purpose,
                submission_channel=channel,
                submitted_at=datetime.now()
            ).model_dump(mode="json")
        )
    ]
    
    # 4. Append
    return await store.append(
        stream_id=f"loan-{application_id}",
        events=new_events,
        expected_version=-1 # New stream
    )

async def handle_credit_analysis_completed(
    store: EventStore,
    application_id: str,
    agent_id: str,
    session_id: str,
    model_version: str,
    confidence_score: float,
    risk_tier: str,
    recommended_limit: float,
    duration_ms: int,
    input_data: Dict[str, Any],
    correlation_id: Optional[str] = None
) -> int:
    # 1. Reconstruct current aggregate state
    app = await LoanApplicationAggregate.load(store, application_id)
    agent = await AgentSessionAggregate.load(store, agent_id, session_id)

    # 2. Validate — business rules checked BEFORE any state change
    app.assert_awaiting_credit_analysis()
    agent.assert_context_loaded() # Gas Town pattern
    agent.assert_model_version_current(model_version)

    # 3. Determine new events
    analysis_payload = CreditAnalysisCompleted(
        application_id=application_id,
        agent_id=agent_id,
        session_id=session_id,
        model_version=model_version,
        confidence_score=confidence_score,
        risk_tier=risk_tier,
        recommended_limit_usd=recommended_limit,
        analysis_duration_ms=duration_ms,
        input_data_hash=hash_inputs(input_data)
    ).model_dump(mode="json")
    
    new_events = [
        BaseEvent(event_type="CreditAnalysisCompleted", payload=analysis_payload)
    ]

    # 4. Append atomically to both (Mastery: Session History Tracking)
    # loan-{app_id} for domain state
    await store.append(
        stream_id=f"loan-{application_id}",
        events=new_events,
        expected_version=app.version,
        correlation_id=correlation_id
    )
    # agent-{agent_id}-{session_id} for session history / token billing
    return await store.append(
        stream_id=f"agent-{agent_id}-{session_id}",
        events=new_events,
        expected_version=agent.version,
        correlation_id=correlation_id
    )

async def handle_fraud_screening_completed(
    store: EventStore,
    application_id: str,
    agent_id: str,
    session_id: str,
    model_version: str,
    fraud_score: float,
    anomaly_flags: List[str],
    input_data: Dict[str, Any],
    correlation_id: Optional[str] = None
) -> int:
    app = await LoanApplicationAggregate.load(store, application_id)
    agent = await AgentSessionAggregate.load(store, agent_id, session_id)

    agent.assert_context_loaded()
    agent.assert_model_version_current(model_version)

    # 3. Determine
    fraud_payload = FraudScreeningCompleted(
        application_id=application_id,
        agent_id=agent_id,
        fraud_score=fraud_score,
        anomaly_flags=anomaly_flags,
        screening_model_version=model_version,
        input_data_hash=hash_inputs(input_data)
    ).model_dump(mode="json")
    
    new_events = [
        BaseEvent(event_type="FraudScreeningCompleted", payload=fraud_payload)
    ]

    # 4. Append to both
    await store.append(
        stream_id=f"loan-{application_id}",
        events=new_events,
        expected_version=app.version,
        correlation_id=correlation_id
    )
    return await store.append(
        stream_id=f"agent-{agent_id}-{session_id}",
        events=new_events,
        expected_version=agent.version,
        correlation_id=correlation_id
    )

async def handle_request_compliance(
    store: EventStore,
    application_id: str,
    checks_required: List[str],
    regulation_version: str = "v1",
    correlation_id: Optional[str] = None
) -> int:
    # 1. Load
    compliance = await ComplianceRecordAggregate.load(store, application_id)
    compliance.assert_can_request()
    
    # 2. Determine
    new_events = [
        BaseEvent(
            event_type="ComplianceCheckRequested",
            payload=ComplianceCheckRequested(
                application_id=application_id,
                regulation_set_version=regulation_version,
                checks_required=checks_required
            ).model_dump(mode="json")
        )
    ]
    
    # 3. Append
    return await store.append(
        stream_id=f"compliance-{application_id}",
        events=new_events,
        expected_version=compliance.version,
        correlation_id=correlation_id
    )

async def handle_record_compliance_check(
    store: EventStore,
    application_id: str,
    rule_id: str,
    rule_version: str,
    passed: bool,
    evidence: Dict[str, Any],
    failure_reason: Optional[str] = None,
    remediation: Optional[str] = None,
    correlation_id: Optional[str] = None
) -> int:
    # 1. Load both
    compliance = await ComplianceRecordAggregate.load(store, application_id)
    app = await LoanApplicationAggregate.load(store, application_id)
    
    # 2. Validate
    # (Any specific compliance rules?)

    # 3. Determine
    if passed:
        event_type = "ComplianceRulePassed"
        payload = ComplianceRulePassed(
            application_id=application_id,
            rule_id=rule_id,
            rule_version=rule_version,
            evaluation_timestamp=datetime.now(),
            evidence_hash=hash_inputs(evidence)
        ).model_dump(mode="json")
    else:
        event_type = "ComplianceRuleFailed"
        payload = ComplianceRuleFailed(
            application_id=application_id,
            rule_id=rule_id,
            rule_version=rule_version,
            failure_reason=failure_reason or "Unknown",
            remediation_required=remediation or "None"
        ).model_dump(mode="json")

    # 4. Append to both (Mastery: Domain visibility)
    # compliance-{app_id} for secondary audit view
    await store.append(
        stream_id=f"compliance-{application_id}",
        events=[BaseEvent(event_type=event_type, payload=payload)],
        expected_version=compliance.version,
        correlation_id=correlation_id
    )
    # loan-{app_id} for domain state visibility (Rule 3)
    return await store.append(
        stream_id=f"loan-{application_id}",
        events=[BaseEvent(event_type=event_type, payload=payload)],
        expected_version=app.version,
        correlation_id=correlation_id
    )

async def handle_generate_decision(
    store: EventStore,
    application_id: str,
    orchestrator_agent_id: str,
    recommendation: str,
    confidence_score: float,
    contributing_sessions: List[str],
    basis_summary: str,
    model_versions: Dict[str, str],
    correlation_id: Optional[str] = None,
    expected_version: Optional[int] = None
) -> int:
    # 1. Load
    app = await LoanApplicationAggregate.load(store, application_id)
    compliance = await ComplianceRecordAggregate.load(store, application_id)
    
    # 2. Validate (The 6 rules)
    app.assert_valid_transition(ApplicationState.PENDING_DECISION)
    app.assert_can_generate_decision(contributing_sessions)
    
    # Rule 3: Compliance Status check
    if compliance.get_status() != "PASSED":
        raise DomainError(f"Cannot generate decision. Compliance status: {compliance.get_status()}")
    
    # Rule 4: Confidence floor enforcement
    final_rec = app.validate_decision(recommendation, confidence_score)
    
    # 3. Determine
    new_events = [
        BaseEvent(
            event_type="DecisionGenerated",
            payload=DecisionGenerated(
                application_id=application_id,
                orchestrator_agent_id=orchestrator_agent_id,
                recommendation=final_rec,
                confidence_score=confidence_score,
                contributing_agent_sessions=contributing_sessions,
                decision_basis_summary=basis_summary,
                model_versions=model_versions
            ).model_dump(mode="json")
        )
    ]
    
    # 4. Append
    return await store.append(
        stream_id=f"loan-{application_id}",
        events=new_events,
        expected_version=expected_version if expected_version is not None else app.version,
        correlation_id=correlation_id
    )

async def handle_record_human_review(
    store: EventStore,
    application_id: str,
    reviewer_id: str,
    override: bool,
    final_decision: str,
    override_reason: Optional[str] = None,
    correlation_id: Optional[str] = None
) -> int:
    app = await LoanApplicationAggregate.load(store, application_id)
    
    if override and not override_reason:
        raise DomainError("Override reason required for human override.")
        
    new_events = [
        BaseEvent(
            event_type="HumanReviewCompleted",
            payload=HumanReviewCompleted(
                application_id=application_id,
                reviewer_id=reviewer_id,
                override=override,
                final_decision=final_decision,
                override_reason=override_reason
            ).model_dump(mode="json")
        )
    ]
    
    return await store.append(
        stream_id=f"loan-{application_id}",
        events=new_events,
        expected_version=app.version,
        correlation_id=correlation_id
    )

async def handle_start_agent_session(
    store: EventStore,
    agent_id: str,
    session_id: str,
    context_source: str,
    replay_pos: int,
    tokens: int,
    model_version: str,
    summary: Optional[str] = None,
    budget: int = 20000
) -> int:
    # 1. Load (Check for existing session)
    session = await AgentSessionAggregate.load(store, agent_id, session_id)
    if session.context_loaded:
        raise DomainError(f"Session {session_id} already initialized.")

    # 2. Validate (e.g., agent authorization check could go here)

    # 3. Determine
    new_events = [
        BaseEvent(
            event_type="AgentContextLoaded",
            payload=AgentContextLoaded(
                agent_id=agent_id,
                session_id=session_id,
                context_source=context_source,
                event_replay_from_position=replay_pos,
                context_token_count=tokens,
                model_version=model_version,
                context_text_summary=summary,
                token_budget=budget,
                health_status="HEALTHY"
            ).model_dump(mode="json")
        )
    ]
    
    # 4. Append
    return await store.append(
        stream_id=f"agent-{agent_id}-{session_id}",
        events=new_events,
        expected_version=-1 # Enforce new session
    )
