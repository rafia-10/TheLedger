"""
Upcaster Registry — Auto-transform old event schemas to new versions at read time.

The UpcasterRegistry holds a chain of transformations keyed by (event_type, from_version).
When an event is loaded, the registry continuously applies upcasters until no more are found.

CRITICAL INVARIANT: The raw database row is NEVER mutated.
    DB stores v1 → load_stream returns v2 → DB still contains v1.
"""
from typing import Dict, Callable, Any
from datetime import datetime
from src.models.events import StoredEvent


class UpcasterRegistry:
    def __init__(self):
        # Maps (event_type, from_version) → upcaster_func
        self._upcasters: Dict[tuple[str, int], Callable[[Dict[str, Any]], Dict[str, Any]]] = {}

    def register(self, event_type: str, from_version: int):
        """Decorator to register an upcaster."""
        def decorator(func: Callable[[Dict[str, Any]], Dict[str, Any]]):
            self._upcasters[(event_type, from_version)] = func
            return func
        return decorator

    def upcast(self, event: StoredEvent) -> StoredEvent:
        """Continuously upcast an event until no more upcasters are found."""
        current_payload = event.payload.copy()
        current_version = event.event_version
        
        # Mastery: Provide recorded_at in metadata for upcaster inference
        effective_metadata = event.metadata.copy()
        effective_metadata["recorded_at"] = event.recorded_at

        # logger.debug(f"Attempting to upcast {event.event_type} v{current_version}")
        while True:
            upcaster = self._upcasters.get((event.event_type, current_version))
            if not upcaster:
                break

            # print(f"DEBUG: Upcasting {event.event_type} from {current_version}")
            current_payload = upcaster(current_payload, effective_metadata)
            current_version += 1

        if current_version != event.event_version:
            # print(f"DEBUG: Event {event.event_id} upcast to v{current_version}")
            return event.with_payload(current_payload, current_version)
        return event


# ─── Global Registry with Production Upcasters ───────────────────────────────

registry = UpcasterRegistry()

def infer_model_from_date(recorded_at: datetime) -> str:
    # Rule: Events before 2026-01-01 are model-v1-legacy
    # Handle both naive and aware datetimes
    compare_date = datetime(2026, 1, 1)
    if recorded_at.tzinfo:
        from datetime import timezone
        compare_date = compare_date.replace(tzinfo=timezone.utc)
        
    if recorded_at < compare_date:
        return "model-v1-legacy"
    return "model-v2-production"

def infer_reg_from_date(recorded_at: datetime) -> str:
    compare_date = datetime(2026, 1, 1)
    if recorded_at.tzinfo:
        from datetime import timezone
        compare_date = compare_date.replace(tzinfo=timezone.utc)

    if recorded_at < compare_date:
        return "REG-2025-BASE"
    return "REG-2026-UPDATED"

@registry.register("CreditAnalysisCompleted", 1)
def credit_analysis_v1_to_v2(payload: Dict[str, Any], metadata: Dict[str, Any]) -> Dict[str, Any]:
    """
    CreditAnalysisCompleted v1 → v2
    Uses timestamp-based inference for consistency.
    """
    payload = payload.copy()
    recorded_at = metadata.get("recorded_at", datetime.now())
    if isinstance(recorded_at, str):
        recorded_at = datetime.fromisoformat(recorded_at)

    payload["model_version"] = payload.get("model_version", infer_model_from_date(recorded_at))
    payload["regulatory_basis"] = payload.get("regulatory_basis", infer_reg_from_date(recorded_at))
    
    # v2 also normalizes confidence_score to 0-1 range if it was 0-100
    if payload.get("confidence_score") and payload["confidence_score"] > 1:
        payload["confidence_score"] = payload["confidence_score"] / 100
    return payload


@registry.register("DecisionGenerated", 1)
def decision_generated_v1_to_v2(payload: Dict[str, Any], metadata: Dict[str, Any]) -> Dict[str, Any]:
    """
    DecisionGenerated v1 → v2
    v1 lacked `risk_score_breakdown`. Add default structure.
    """
    payload = payload.copy()
    if "risk_score_breakdown" not in payload:
        payload["risk_score_breakdown"] = {
            "credit_weight": 0.4,
            "fraud_weight": 0.3,
            "compliance_weight": 0.3,
            "composite_score": payload.get("confidence_score", 0.0),
        }
    return payload
