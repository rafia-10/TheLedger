import pytest
import asyncio
from datetime import datetime, timedelta
from src.event_store import EventStore
from src.models.events import StoredEvent, BaseEvent
from src.projections.daemon import ProjectionDaemon
from src.projections.application_summary import ApplicationSummaryProjection
from src.projections.agent_performance import AgentPerformanceProjection
from src.projections.compliance_audit import ComplianceAuditProjection
from src.commands.handlers import handle_submit_application, handle_start_agent_session, handle_credit_analysis_completed

@pytest.mark.asyncio
async def test_projection_lifecycle(event_store: EventStore):
    # 1. Setup Projections and Daemon
    summary = ApplicationSummaryProjection()
    performance = AgentPerformanceProjection()
    compliance = ComplianceAuditProjection()
    
    daemon = ProjectionDaemon(event_store, [summary, performance, compliance])
    
    # 2. Append events via handlers
    app_id = "test-app-001"
    agent_id = "agent-x"
    session_id = "sess-y"
    
    await handle_submit_application(event_store, app_id, "user-1", 10000.0, "Business", "Web")
    await handle_start_agent_session(event_store, agent_id, session_id, "Memory", 0, 1000, "v1.0")
    
    await handle_credit_analysis_completed(
        event_store, app_id, agent_id, session_id, "v1.0", 0.95, "LOW", 5000.0, 150, {"financials": "ok"}
    )
    
    # 3. Process events through daemon
    processed = await daemon._process_batch()
    assert processed >= 3
    
    # 4. Verify ApplicationSummary
    async with event_store.transaction() as conn:
        row = await conn.fetchrow("SELECT * FROM application_summary WHERE application_id = $1", app_id)
        assert row["state"] == "ANALYSIS_COMPLETE"
        assert row["requested_amount_usd"] == 10000.0
        assert row["risk_tier"] == "LOW"
    
    # 5. Verify AgentPerformance
    async with event_store.transaction() as conn:
        perf = await conn.fetchrow("SELECT * FROM agent_performance WHERE agent_id = $1", agent_id)
        assert perf["analyses_completed"] == 1
        assert perf["avg_duration_ms"] == 150
        
    # 6. Verify ComplianceAuditView (Temporal Query)
    # We add a fake compliance event via BaseEvent
    await event_store.append(
        f"loan-{app_id}",
        [
            BaseEvent(
                event_type="ComplianceRulePassed",
                payload={
                    "application_id": app_id, 
                    "rule_id": "KYC", 
                    "rule_version": "1.0", 
                    "evidence_hash": "abc"
                }
            )
        ],
        expected_version=2
    )
    
    await daemon._process_batch()
    
    # Query temporal
    now = datetime.now()
    results = await compliance.get_compliance_at(event_store, app_id, now)
    assert len(results) == 1
    assert results[0]["rule_id"] == "KYC"
    
    # Query for the past (before compliance event)
    past = now - timedelta(minutes=1)
    results_past = await compliance.get_compliance_at(event_store, app_id, past)
    assert len(results_past) == 0

# --- Direct execution support ---
if __name__ == "__main__":
    import sys
    sys.exit(pytest.main([__file__, "-v", "--tb=short"]))
