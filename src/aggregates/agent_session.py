from typing import Optional, Dict, Any
from src.models.events import StoredEvent, DomainError

class AgentSessionAggregate:
    def __init__(self, agent_id: str, session_id: str):
        self.agent_id = agent_id
        self.session_id = session_id
        self.version = 0
        self.context_loaded = False
        self.model_version = None
        self.decisions_count = 0
        self.last_input_hash = None
        self.token_budget = 0
        self.tokens_used = 0
        self.health_status = "UNKNOWN"
        self.pending_work: List[str] = []

    @classmethod
    async def load(cls, store, agent_id: str, session_id: str) -> "AgentSessionAggregate":
        events = await store.load_stream(f"agent-{agent_id}-{session_id}")
        agg = cls(agent_id=agent_id, session_id=session_id)
        for event in events:
            agg._apply(event)
        return agg

    def _apply(self, event: StoredEvent) -> None:
        handler = getattr(self, f"_on_{event.event_type}", None)
        if handler:
            handler(event)
        self.version = event.stream_position

    # --- Event Handlers ---

    def _on_AgentContextLoaded(self, event: StoredEvent) -> None:
        self.context_loaded = True
        self.model_version = event.payload["model_version"]
        self.token_budget = event.payload.get("token_budget", 10000)
        self.tokens_used = event.payload.get("context_token_count", 0)
        self.health_status = event.payload.get("health_status", "HEALTHY")

    def _on_CreditAnalysisCompleted(self, event: StoredEvent) -> None:
        self.decisions_count += 1
        self.last_input_hash = event.payload["input_data_hash"]
        # Increment tokens used realistically
        self.tokens_used += event.payload.get("analysis_duration_ms", 100) // 10

    def _on_FraudScreeningCompleted(self, event: StoredEvent) -> None:
        self.decisions_count += 1
        self.last_input_hash = event.payload["input_data_hash"]

    # --- Business Rules ---

    def assert_context_loaded(self):
        """Rule 3: (Gas Town) Context must be loaded before actions."""
        if not self.context_loaded:
            raise DomainError(
                f"Agent {self.agent_id} session {self.session_id} has no context loaded."
            )

    def assert_model_version_current(self, version: str):
        """Rule 4: Model version must match the session lock."""
        if self.model_version and self.model_version != version:
             raise DomainError(
                 f"Model version mismatch: {self.model_version} vs {version}."
             )

    def assert_within_token_budget(self):
        """Rule 7: Session must not exceed its allocated token budget."""
        if self.tokens_used > self.token_budget:
            self.health_status = "NEEDS_RECONCILIATION"
            raise DomainError(f"Token budget exceeded: {self.tokens_used}/{self.token_budget}")
