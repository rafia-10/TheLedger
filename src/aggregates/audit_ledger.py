from typing import Optional
from src.models.events import StoredEvent, DomainError

class AuditLedgerAggregate:
    def __init__(self, entity_id: str = "global-ledger"):
        self.entity_id = entity_id
        self.version = 0
        self.last_verified_position = 0
        self.last_integrity_hash: Optional[str] = None
        self.integrity_maintained = True

    @classmethod
    async def load(cls, store, entity_id: str = "global-ledger") -> "AuditLedgerAggregate":
        events = await store.load_stream(f"audit-{entity_id}")
        agg = cls(entity_id=entity_id)
        for event in events:
            agg._apply(event)
        return agg

    def _apply(self, event: StoredEvent) -> None:
        if event.event_type == "AuditIntegrityCheckRun":
            self.last_verified_position = event.payload["events_verified_count"]
            self.last_integrity_hash = event.payload["integrity_hash"]
        self.version = event.stream_position

    # --- Business Rules ---

    def assert_integrity_maintained(self):
        if not self.integrity_maintained:
            raise DomainError("Ledger integrity has been compromised. All operations suspended.")

    def validate_new_check(self, events_count: int, new_hash: str):
        if events_count < self.last_verified_position:
            raise DomainError("Cannot run integrity check backward in time.")
        # In a real system, we'd verify the hash chain here
        # For the aggregate, we just enforce the causal link
