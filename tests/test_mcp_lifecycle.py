"""
MCP Integration Test — Full lifecycle using command handlers only.

Simulates: submit → analysis → fraud → compliance → decision → review
All using the same handler functions that the MCP tools call.
"""
import pytest
import asyncio
from datetime import datetime
from src.event_store import EventStore
from src.commands.handlers import (
    handle_submit_application,
    handle_credit_analysis_completed,
    handle_fraud_screening_completed,
    handle_record_compliance_check,
    handle_generate_decision,
    handle_record_human_review,
    handle_start_agent_session,
)
from src.projections.daemon import ProjectionDaemon
from src.projections.application_summary import ApplicationSummaryProjection
from src.projections.agent_performance import AgentPerformanceProjection
from src.projections.compliance_audit import ComplianceAuditProjection
from src.integrity.audit_chain import AuditChain
from src.gas_town import reconstruct_agent_context
from src.models.events import DomainError


@pytest.mark.asyncio
async def test_full_lifecycle_via_handlers(event_store: EventStore):
    """
    Complete loan lifecycle driven entirely through command handlers
    (the same functions the MCP tools wrap):
        submit → start_session → credit_analysis → fraud → compliance → decision → review
    """
    app_id = "mcp-test-app-001"
    agent_id = "test-agent"
    session_id = "test-session"

    # ── Step 1: Submit Application ────────────────────────────────────────────
    v = await handle_submit_application(
        event_store, app_id, "user-42", 50000.0, "Auto Loan", "MCP"
    )
    assert v == 1

    # ── Step 2: Start Agent Session (Gas Town) ────────────────────────────────
    v = await handle_start_agent_session(
        event_store, agent_id, session_id, "VectorDB", 0, 500, "v3.0"
    )
    assert v == 1

    # ── Step 3: Credit Analysis ───────────────────────────────────────────────
    v = await handle_credit_analysis_completed(
        event_store, app_id, agent_id, session_id, "v3.0",
        0.92, "TIER_A", 50000.0, 100, {"fico": 720}
    )
    assert v == 2  # stream loan-{app_id} now at version 2

    # ── Step 4: Fraud Screening ───────────────────────────────────────────────
    v = await handle_fraud_screening_completed(
        event_store, app_id, agent_id, session_id, "v3.0",
        0.05, [], {"ip": "10.0.0.1"}
    )
    assert v == 3

    # ── Step 5: Compliance Check ──────────────────────────────────────────────
    v = await handle_record_compliance_check(
        event_store, app_id, "KYC-01", "v2", True, {"id_verified": True}
    )
    assert v == 4

    # ── Step 6: AI Decision ───────────────────────────────────────────────────
    v = await handle_generate_decision(
        event_store, app_id, "orchestrator-1", "APPROVE", 0.88,
        [session_id], "All checks passed", {"agent": "v3.0"}
    )
    assert v == 5

    # ── Step 7: Human Review ──────────────────────────────────────────────────
    v = await handle_record_human_review(
        event_store, app_id, "reviewer-alice", False, "APPROVED"
    )
    assert v == 6

    # ── Verify: Stream has exactly 6 events ───────────────────────────────────
    events = await event_store.load_stream(f"loan-{app_id}")
    assert len(events) == 6
    event_types = [e.event_type for e in events]
    assert event_types == [
        "ApplicationSubmitted",
        "CreditAnalysisCompleted",
        "FraudScreeningCompleted",
        "ComplianceRulePassed",
        "DecisionGenerated",
        "HumanReviewCompleted",
    ]

    # ── Verify: Integrity chain is intact ─────────────────────────────────────
    assert await AuditChain.verify_chain(event_store, f"loan-{app_id}") is True

    # ── Verify: Projections update correctly ──────────────────────────────────
    daemon = ProjectionDaemon(event_store, [
        ApplicationSummaryProjection(),
        AgentPerformanceProjection(),
        ComplianceAuditProjection(),
    ])
    await daemon._process_batch()

    async with event_store.transaction() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM application_summary WHERE application_id = $1", app_id
        )
        assert row is not None
        assert row["applicant_id"] == "user-42"
        assert row["requested_amount_usd"] == 50000.0

    # ── Verify: Agent context reconstruction (Gas Town) ───────────────────────
    ctx = await reconstruct_agent_context(event_store, agent_id, session_id)
    assert ctx["context_loaded"] is True
    assert ctx["model_version"] == "v3.0"
    assert len(ctx["decisions_made"]) >= 1  # at least credit analysis


@pytest.mark.asyncio
async def test_confidence_floor_enforcement(event_store: EventStore):
    """Confidence < 0.6 must auto-REFER regardless of recommendation."""
    app_id = "refer-test-app"
    agent_id = "refer-agent"
    session_id = "refer-session"

    await handle_submit_application(event_store, app_id, "user-1", 10000.0, "Test", "MCP")
    await handle_start_agent_session(event_store, agent_id, session_id, "DB", 0, 100, "v1.0")
    await handle_credit_analysis_completed(
        event_store, app_id, agent_id, session_id, "v1.0",
        0.5, "TIER_C", 5000.0, 50, {"data": "test"}
    )
    # Decision with low confidence should auto-REFER
    v = await handle_generate_decision(
        event_store, app_id, "orch-1", "APPROVE", 0.45,
        [session_id], "Low confidence", {"agent": "v1.0"}
    )

    events = await event_store.load_stream(f"loan-{app_id}")
    decision_event = [e for e in events if e.event_type == "DecisionGenerated"][0]
    assert decision_event.payload["recommendation"] == "REFER"


@pytest.mark.asyncio
async def test_gas_town_context_required(event_store: EventStore):
    """Agent must load context before performing analysis (Gas Town pattern)."""
    app_id = "gastown-test"
    agent_id = "lazy-agent"
    session_id = "no-context-session"

    await handle_submit_application(event_store, app_id, "user-1", 10000.0, "Test", "MCP")

    # Try to do analysis without starting session → should fail
    with pytest.raises(DomainError, match="no context loaded"):
        await handle_credit_analysis_completed(
            event_store, app_id, agent_id, session_id, "v1.0",
            0.9, "TIER_A", 10000.0, 100, {"data": "test"}
        )


@pytest.mark.asyncio
async def test_model_version_lock(event_store: EventStore):
    """Session locked to initial model version — mismatched version must fail."""
    app_id = "version-lock-test"
    agent_id = "locked-agent"
    session_id = "locked-session"

    await handle_submit_application(event_store, app_id, "user-1", 10000.0, "Test", "MCP")
    await handle_start_agent_session(event_store, agent_id, session_id, "DB", 0, 100, "v1.0")

    # Try analysis with different model version → should fail
    with pytest.raises(DomainError, match="Model version mismatch"):
        await handle_credit_analysis_completed(
            event_store, app_id, agent_id, session_id, "v2.0",
            0.9, "TIER_A", 10000.0, 100, {"data": "test"}
        )


# --- Direct execution support ---
if __name__ == "__main__":
    import sys
    sys.exit(pytest.main([__file__, "-v", "--tb=short"]))
