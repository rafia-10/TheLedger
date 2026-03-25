from datetime import datetime
from typing import Any, Dict, List, Optional
from uuid import UUID, uuid4
from pydantic import BaseModel, Field, ConfigDict

class BaseEvent(BaseModel):
    """Base class for all domain events."""
    model_config = ConfigDict(frozen=True)
    
    event_type: str
    event_version: int = 1
    payload: Dict[str, Any]
    metadata: Dict[str, Any] = Field(default_factory=dict)

class StoredEvent(BaseModel):
    """Wrapper for an event as it exists in the store."""
    model_config = ConfigDict(frozen=True)
    
    event_id: UUID
    stream_id: str
    stream_position: int
    global_position: int
    event_type: str
    event_version: int
    payload: Dict[str, Any]
    metadata: Dict[str, Any]
    recorded_at: datetime

    def with_payload(self, new_payload: Dict[str, Any], version: int) -> "StoredEvent":
        """Used for upcasting."""
        return self.model_copy(update={"payload": new_payload, "event_version": version})

class StreamMetadata(BaseModel):
    """Metadata for an entire event stream."""
    stream_id: str
    aggregate_type: str
    current_version: int
    created_at: datetime
    archived_at: Optional[datetime] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)

class OptimisticConcurrencyError(Exception):
    """Raised when the expected stream version does not match the actual version."""
    def __init__(self, stream_id: str, expected: int, actual: int):
        self.stream_id = stream_id
        self.expected = expected
        self.actual = actual
        super().__init__(
            f"Concurrency conflict on stream '{stream_id}': "
            f"expected version {expected}, but found {actual}."
        )

class DomainError(Exception):
    """Base class for business rule violations."""
    pass

# --- Loan Application Events ---

class ApplicationSubmitted(BaseModel):
    application_id: str
    applicant_id: str
    requested_amount_usd: float
    loan_purpose: str
    submission_channel: str
    submitted_at: datetime

class CreditAnalysisRequested(BaseModel):
    application_id: str
    assigned_agent_id: str
    requested_at: datetime
    priority: str

class DecisionGenerated(BaseModel):
    application_id: str
    orchestrator_agent_id: str
    recommendation: str # APPROVE/DECLINE/REFER
    confidence_score: float
    contributing_agent_sessions: List[str]
    decision_basis_summary: str
    model_versions: Dict[str, str]

class HumanReviewCompleted(BaseModel):
    application_id: str
    reviewer_id: str
    override: bool
    final_decision: str
    override_reason: Optional[str] = None

class ApplicationApproved(BaseModel):
    application_id: str
    approved_amount_usd: float
    interest_rate: float
    conditions: List[str] = Field(default_factory=list)
    approved_by: str
    effective_date: datetime

class ApplicationDeclined(BaseModel):
    application_id: str
    decline_reasons: List[str]
    declined_by: str
    adverse_action_notice_required: bool

# --- Agent Session Events ---

class AgentContextLoaded(BaseModel):
    agent_id: str
    session_id: str
    context_source: str
    event_replay_from_position: int
    context_token_count: int
    model_version: str
    context_text_summary: Optional[str] = None
    token_budget: int = 10000
    health_status: str = "HEALTHY" # HEALTHY, DEGRADED, NEEDS_RECONCILIATION

class CreditAnalysisCompleted(BaseModel):
    application_id: str
    agent_id: str
    session_id: str
    model_version: str
    confidence_score: Optional[float]
    risk_tier: str
    recommended_limit_usd: float
    analysis_duration_ms: int
    input_data_hash: str
    regulatory_basis: Optional[str] = None # Added for upcasting v2

class FraudScreeningCompleted(BaseModel):
    application_id: str
    agent_id: str
    fraud_score: float
    anomaly_flags: List[str]
    screening_model_version: str
    input_data_hash: str

# --- Compliance Events ---

class ComplianceCheckRequested(BaseModel):
    application_id: str
    regulation_set_version: str
    checks_required: List[str]

class ComplianceRulePassed(BaseModel):
    application_id: str
    rule_id: str
    rule_version: str
    evaluation_timestamp: datetime
    evidence_hash: str

class ComplianceRuleFailed(BaseModel):
    application_id: str
    rule_id: str
    rule_version: str
    failure_reason: str
    remediation_required: str

# --- Audit Events ---

class AuditIntegrityCheckRun(BaseModel):
    entity_id: str
    check_timestamp: datetime
    events_verified_count: int
    integrity_hash: str
    previous_hash: Optional[str] = None
