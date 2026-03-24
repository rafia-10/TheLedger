"""
Gas Town — Agent Memory Reconstruction.

The "Gas Town" pattern ensures agents must load their context before acting.
This module provides utilities to:
1. Replay an agent's session from the event stream.
2. Detect unfinished work (sessions that started but didn't complete).
3. Return a structured context dictionary for agent resumption.
"""
from typing import Dict, Any, List, Optional
from src.event_store import EventStore
from src.models.events import StoredEvent


async def reconstruct_agent_context(
    store: EventStore,
    agent_id: str,
    session_id: str,
) -> Dict[str, Any]:
    """
    Replay an agent session's events and reconstruct its working context.

    Returns a dict with:
        - agent_id, session_id
        - context_loaded: bool
        - model_version: str or None
        - decisions_made: list of decision summaries
        - unfinished_work: bool (context loaded but no decisions completed)
        - total_events: int
        - last_position: int (for incremental replay)
        - events: list of raw event dicts
    """
    stream_id = f"agent-{agent_id}-{session_id}"
    events: List[StoredEvent] = await store.load_stream(stream_id)

    context: Dict[str, Any] = {
        "agent_id": agent_id,
        "session_id": session_id,
        "stream_id": stream_id,
        "context_loaded": False,
        "model_version": None,
        "decisions_made": [],
        "unfinished_work": False,
        "total_events": len(events),
        "last_position": events[-1].stream_position if events else 0,
        "events": [],
    }

    for event in events:
        context["events"].append({
            "event_type": event.event_type,
            "stream_position": event.stream_position,
            "payload": event.payload,
            "recorded_at": event.recorded_at.isoformat() if event.recorded_at else None,
        })

        if event.event_type == "AgentContextLoaded":
            context["context_loaded"] = True
            context["model_version"] = event.payload.get("model_version")

        elif event.event_type == "CreditAnalysisCompleted":
            context["decisions_made"].append({
                "type": "CreditAnalysis",
                "application_id": event.payload.get("application_id"),
                "risk_tier": event.payload.get("risk_tier"),
                "confidence": event.payload.get("confidence_score"),
            })

        elif event.event_type == "FraudScreeningCompleted":
            context["decisions_made"].append({
                "type": "FraudScreening",
                "application_id": event.payload.get("application_id"),
                "fraud_score": event.payload.get("fraud_score"),
            })

    # Detect unfinished work: context loaded but no decisions completed
    if context["context_loaded"] and len(context["decisions_made"]) == 0:
        context["unfinished_work"] = True

    return context


async def find_unfinished_sessions(
    store: EventStore,
    agent_id: str,
) -> List[Dict[str, Any]]:
    """
    Scan all streams for an agent and identify sessions with unfinished work.
    Returns a list of session contexts where work was started but not completed.
    """
    # Query event_streams for all agent sessions
    async with store.transaction() as conn:
        rows = await conn.fetch(
            "SELECT stream_id FROM event_streams WHERE stream_id LIKE $1",
            f"agent-{agent_id}-%"
        )

    unfinished = []
    for row in rows:
        stream_id = row["stream_id"]
        # Extract session_id from stream_id: "agent-{agent_id}-{session_id}"
        parts = stream_id.split("-", 2)
        if len(parts) >= 3:
            session_id = parts[2]  # everything after "agent-{agent_id}-"
        else:
            continue

        ctx = await reconstruct_agent_context(store, agent_id, session_id)
        if ctx["unfinished_work"]:
            unfinished.append(ctx)

    return unfinished
