from typing import Dict, List, Set, Optional
from src.models.events import StoredEvent, DomainError

class ComplianceRecordAggregate:
    def __init__(self, application_id: str):
        self.application_id = application_id
        self.version = 0
        self.required_rules: Set[str] = set()
        self.passed_rules: Set[str] = set()
        self.failed_rules: Dict[str, str] = {} # rule_id -> failure_reason
        self.is_requested = False

    @classmethod
    async def load(cls, store, application_id: str) -> "ComplianceRecordAggregate":
        events = await store.load_stream(f"compliance-{application_id}")
        agg = cls(application_id=application_id)
        for event in events:
            agg._apply(event)
        return agg

    def _apply(self, event: StoredEvent) -> None:
        handler = getattr(self, f"_on_{event.event_type}", None)
        if handler:
            handler(event)
        self.version = event.stream_position

    # --- Event Handlers ---

    def _on_ComplianceCheckRequested(self, event: StoredEvent) -> None:
        self.is_requested = True
        self.required_rules.update(event.payload["checks_required"])

    def _on_ComplianceRulePassed(self, event: StoredEvent) -> None:
        rule_id = event.payload["rule_id"]
        self.passed_rules.add(rule_id)
        if rule_id in self.failed_rules:
            del self.failed_rules[rule_id]

    def _on_ComplianceRuleFailed(self, event: StoredEvent) -> None:
        rule_id = event.payload["rule_id"]
        self.failed_rules[rule_id] = event.payload["failure_reason"]
        if rule_id in self.passed_rules:
            self.passed_rules.remove(rule_id)

    # --- Business Rules ---

    def assert_can_request(self):
        if self.is_requested:
            raise DomainError(f"Compliance check already requested for {self.application_id}")

    def assert_all_passed(self, checks: List[str]):
        missing = [c for c in checks if c not in self.passed_rules]
        if missing:
            raise DomainError(f"Missing mandatory compliance checks: {missing}")
        
        failures = [c for c in checks if c in self.failed_rules]
        if failures:
            raise DomainError(f"Mandatory compliance checks failed: {failures}")

    def get_status(self) -> str:
        if not self.is_requested: return "NOT_STARTED"
        if self.failed_rules: return "FAILED"
        if self.passed_rules >= self.required_rules: return "PASSED"
        return "IN_PROGRESS"
