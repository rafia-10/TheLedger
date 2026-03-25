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
from src.models.context import AgentContext, DecisionSummary


async def reconstruct_agent_context(
    store: EventStore,
    agent_id: str,
    session_id: str,
) -> AgentContext:
    """
    Replay an agent session's events and reconstruct its working context.
    Matches the Mastery spec for token budgeting and health tracking.
    """
    stream_id = f"agent-{agent_id}-{session_id}"
    events: List[StoredEvent] = await store.load_stream(stream_id)

    ctx = AgentContext(
        agent_id=agent_id,
        session_id=session_id,
        stream_id=stream_id,
        total_events=len(events),
        last_position=events[-1].stream_position if events else 0
    )

    for event in events:
        if event.event_type == "AgentContextLoaded":
            ctx.context_loaded = True
            ctx.model_version = event.payload.get("model_version")
            ctx.token_budget = event.payload.get("token_budget", 20000)
            ctx.tokens_used = event.payload.get("context_token_count", 0)
            ctx.health_status = event.payload.get("health_status", "HEALTHY")
            ctx.context_text_summary = event.payload.get("context_text_summary")

        elif event.event_type == "CreditAnalysisCompleted":
            ctx.decisions_made.append(DecisionSummary(
                type="CreditAnalysis",
                application_id=event.payload.get("application_id"),
                risk_tier=event.payload.get("risk_tier"),
                confidence=event.payload.get("confidence_score"),
            ))
            # Track token usage (heuristic)
            ctx.tokens_used += event.payload.get("analysis_duration_ms", 100) // 10

        elif event.event_type == "FraudScreeningCompleted":
            ctx.decisions_made.append(DecisionSummary(
                type="FraudScreening",
                application_id=event.payload.get("application_id"),
                fraud_score=event.payload.get("fraud_score"),
            ))

    # Business Logic: Token Budget Enforcement
    if ctx.tokens_used > ctx.token_budget:
        ctx.needs_reconciliation = True
        ctx.health_status = "NEEDS_RECONCILIATION"

    # Detect unfinished work: context loaded but no decisions completed
    if ctx.context_loaded and len(ctx.decisions_made) == 0:
        ctx.unfinished_work = True
        ctx.pending_work.append("Complete initial analysis")

    return ctx


async def find_unfinished_sessions(
    store: EventStore,
    agent_id: str,
) -> List[AgentContext]:
    """
    Scan all streams for an agent and identify sessions with unfinished work or reconciliation needs.
    """
    # Query event_streams for all agent sessions
    async with store.transaction() as conn:
        rows = await conn.fetch(
            "SELECT stream_id FROM event_streams WHERE stream_id LIKE $1",
            f"agent-{agent_id}-%"
        )

    unfinished = []
    prefix = f"agent-{agent_id}-"
    for row in rows:
        stream_id = row["stream_id"]
        if not stream_id.startswith(prefix):
            continue
            
        session_id = stream_id[len(prefix):]
        if not session_id:
            continue

        ctx = await reconstruct_agent_context(store, agent_id, session_id)
        if ctx.unfinished_work or ctx.needs_reconciliation:
            unfinished.append(ctx)

    return unfinished
