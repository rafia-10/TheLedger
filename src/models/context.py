from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field
from datetime import datetime

class DecisionSummary(BaseModel):
    type: str
    application_id: str
    risk_tier: Optional[str] = None
    confidence: Optional[float] = None
    fraud_score: Optional[float] = None

class AgentContext(BaseModel):
    agent_id: str
    session_id: str
    stream_id: str
    context_loaded: bool = False
    model_version: Optional[str] = None
    decisions_made: List[DecisionSummary] = Field(default_factory=list)
    unfinished_work: bool = False
    total_events: int = 0
    last_position: int = 0
    token_budget: int = 20000
    tokens_used: int = 0
    health_status: str = "UNKNOWN"
    context_text_summary: Optional[str] = None
    pending_work: List[str] = Field(default_factory=list)
    needs_reconciliation: bool = False

    def is_healthy(self) -> bool:
        return self.health_status == "HEALTHY" and not self.needs_reconciliation
