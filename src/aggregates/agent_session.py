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

    def _on_CreditAnalysisCompleted(self, event: StoredEvent) -> None:
        self.decisions_count += 1
        self.last_input_hash = event.payload["input_data_hash"]

    def _on_FraudScreeningCompleted(self, event: StoredEvent) -> None:
        self.decisions_count += 1
        self.last_input_hash = event.payload["input_data_hash"]

    # --- Business Rules ---

    def assert_context_loaded(self):
        """
        Gas Town: An AgentSession aggregate MUST have an AgentContextLoaded event 
        as its first event before any decision event can be appended.
        """
        if not self.context_loaded:
            raise DomainError(
                f"Agent {self.agent_id} session {self.session_id} has no context loaded. "
                "The persistent ledger pattern requires context before actions."
            )

    def assert_model_version_current(self, version: str):
        if self.model_version and self.model_version != version:
             raise DomainError(
                 f"Model version mismatch. Session locked to {self.model_version}, "
                 f"but command requested {version}."
             )
