"""
What-If Simulator — Replay events with modifications and compare outcomes.

Allows you to:
1. Take an existing event stream
2. Apply modifications (insert, remove, alter events)
3. Replay through projections in-memory
4. Compare the original vs modified outcomes
"""
import json
import copy
from typing import List, Dict, Any, Optional, Callable
from datetime import datetime

from src.models.events import StoredEvent, BaseEvent
from src.projections.daemon import Projection
from src.projections.application_summary import ApplicationSummaryProjection
from src.projections.agent_performance import AgentPerformanceProjection
from src.projections.compliance_audit import ComplianceAuditProjection
from src.event_store import EventStore


class InMemoryProjectionState:
    """Captures projection state in-memory for comparison (no DB writes)."""

    def __init__(self):
        self.application_summaries: Dict[str, Dict[str, Any]] = {}
        self.agent_metrics: Dict[str, Dict[str, Any]] = {}
        self.compliance_records: List[Dict[str, Any]] = []
        self.events_processed: int = 0


class WhatIfSimulator:
    """
    Replay events with modifications and compare outcomes.

    Usage:
        sim = WhatIfSimulator(store)

        # Load original events
        original = await sim.load_stream("loan-app-001")

        # Apply modifications
        modified = sim.modify_events(original, [
            {"action": "alter", "position": 2, "changes": {"confidence_score": 0.4}},
            {"action": "remove", "position": 5},
        ])

        # Compare outcomes
        result = sim.compare(original, modified)
    """

    def __init__(self, store: EventStore):
        self._store = store

    async def load_stream(self, stream_id: str) -> List[StoredEvent]:
        """Load all events from a stream."""
        return await self._store.load_stream(stream_id)

    def modify_events(
        self,
        events: List[StoredEvent],
        modifications: List[Dict[str, Any]],
    ) -> List[StoredEvent]:
        """
        Apply modifications to a list of events (non-destructive copy).

        Modification format:
            {"action": "alter", "position": 2, "changes": {"field": "value"}}
            {"action": "remove", "position": 3}
            {"action": "insert", "after_position": 1, "event": BaseEvent(...)}
        """
        result = list(events)  # shallow copy

        # Sort by position descending to avoid index shifting issues
        mods_sorted = sorted(modifications, key=lambda m: m.get("position", m.get("after_position", 0)), reverse=True)

        for mod in mods_sorted:
            action = mod["action"]

            if action == "alter":
                pos = mod["position"]
                idx = pos - 1  # convert 1-indexed to 0-indexed
                if 0 <= idx < len(result):
                    original = result[idx]
                    new_payload = copy.deepcopy(original.payload)
                    new_payload.update(mod["changes"])
                    result[idx] = original.with_payload(new_payload, original.event_version)

            elif action == "remove":
                pos = mod["position"]
                idx = pos - 1
                if 0 <= idx < len(result):
                    result.pop(idx)

            elif action == "insert":
                after_pos = mod.get("after_position", len(result))
                event_data = mod["event"]
                # Create a StoredEvent-like object for projection processing
                from uuid import uuid4
                fake_stored = StoredEvent(
                    event_id=uuid4(),
                    stream_id=events[0].stream_id if events else "what-if",
                    stream_position=after_pos + 1,
                    global_position=999999,  # Synthetic
                    event_type=event_data.event_type,
                    event_version=event_data.event_version,
                    payload=event_data.payload,
                    metadata=event_data.metadata,
                    recorded_at=datetime.now(),
                )
                result.insert(after_pos, fake_stored)

        return result

    def replay_through_aggregate(
        self, events: List[StoredEvent]
    ) -> Dict[str, Any]:
        """Replay events through the loan application aggregate to get final state."""
        from src.aggregates.loan_application import LoanApplicationAggregate

        if not events:
            return {"state": None, "version": 0}

        # Extract application_id from first event
        app_id = events[0].payload.get(
            "application_id",
            events[0].stream_id.replace("loan-", ""),
        )
        agg = LoanApplicationAggregate(app_id)

        for event in events:
            agg._apply(event)

        return {
            "state": agg.state.value if agg.state else None,
            "version": agg.version,
            "risk_tier": agg.risk_tier,
            "fraud_score": agg.fraud_score,
            "decision": agg.decision,
            "compliance_checks": list(agg.compliance_checks_passed),
        }

    def compare(
        self,
        original_events: List[StoredEvent],
        modified_events: List[StoredEvent],
    ) -> Dict[str, Any]:
        """
        Compare original vs modified event streams through the aggregate.

        Returns a structured diff with:
            - original_outcome
            - modified_outcome
            - differences (field-by-field diff)
        """
        original_state = self.replay_through_aggregate(original_events)
        modified_state = self.replay_through_aggregate(modified_events)

        # Compute field-by-field differences
        differences = {}
        all_keys = set(list(original_state.keys()) + list(modified_state.keys()))
        for key in all_keys:
            orig_val = original_state.get(key)
            mod_val = modified_state.get(key)
            if orig_val != mod_val:
                differences[key] = {
                    "original": orig_val,
                    "modified": mod_val,
                }

        return {
            "original_outcome": original_state,
            "modified_outcome": modified_state,
            "differences": differences,
            "original_event_count": len(original_events),
            "modified_event_count": len(modified_events),
            "has_divergence": len(differences) > 0,
        }
