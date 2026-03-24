"""
Upcaster Registry — Auto-transform old event schemas to new versions at read time.

The UpcasterRegistry holds a chain of transformations keyed by (event_type, from_version).
When an event is loaded, the registry continuously applies upcasters until no more are found.

CRITICAL INVARIANT: The raw database row is NEVER mutated.
    DB stores v1 → load_stream returns v2 → DB still contains v1.
"""
from typing import Dict, Callable, Any
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
        current_payload = event.payload.copy()  # Never mutate original
        current_version = event.event_version

        while True:
            upcaster = self._upcasters.get((event.event_type, current_version))
            if not upcaster:
                break

            current_payload = upcaster(current_payload)
            current_version += 1

        if current_version != event.event_version:
            return event.with_payload(current_payload, current_version)
        return event


# ─── Global Registry with Production Upcasters ───────────────────────────────

registry = UpcasterRegistry()


@registry.register("CreditAnalysisCompleted", 1)
def credit_analysis_v1_to_v2(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    CreditAnalysisCompleted v1 → v2
    v1 lacked `regulatory_basis`. Infer from timestamp/risk_tier context.
    """
    payload = payload.copy()
    payload["regulatory_basis"] = payload.get("regulatory_basis", "INFERRED_FROM_LEGACY")
    # v2 also normalizes confidence_score to 0-1 range
    if payload.get("confidence_score") and payload["confidence_score"] > 1:
        payload["confidence_score"] = payload["confidence_score"] / 100
    return payload


@registry.register("DecisionGenerated", 1)
def decision_generated_v1_to_v2(payload: Dict[str, Any]) -> Dict[str, Any]:
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
