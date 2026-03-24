"""
MCP Server — The Ledger's regulatory and agent interface.

Write Tools (8):
    submit_application, record_credit_analysis, record_fraud_screening,
    record_compliance_check, generate_decision, record_human_review,
    start_agent_session, run_integrity_check

Read Resources (5):
    application summary, compliance view, audit trail, agent performance, health/lag
"""
import logging
import os
from typing import List, Optional, Dict, Any
from datetime import datetime

from dotenv import load_dotenv
load_dotenv()

from mcp.server.fastmcp import FastMCP
from src.event_store import EventStore
from src.models.events import BaseEvent
from src.commands.handlers import (
    handle_submit_application,
    handle_credit_analysis_completed,
    handle_fraud_screening_completed,
    handle_record_compliance_check,
    handle_generate_decision,
    handle_record_human_review,
    handle_start_agent_session,
)
from src.integrity.audit_chain import AuditChain
from src.projections.daemon import ProjectionDaemon
from src.projections.application_summary import ApplicationSummaryProjection
from src.projections.agent_performance import AgentPerformanceProjection
from src.projections.compliance_audit import ComplianceAuditProjection

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize FastMCP server
mcp = FastMCP("The Ledger")

# Database configuration — Cloud only (Supabase Transaction Pooler)
CLOUD_DSN = "postgresql://postgres.dxscaeckamkplshxqkae:supabase1224@aws-0-eu-central-1.pooler.supabase.com:6543/postgres?sslmode=require"
DSN = os.getenv("DATABASE_URL", CLOUD_DSN)
store = EventStore(DSN)

# Projection daemon (lazy-started)
_daemon: Optional[ProjectionDaemon] = None


def _get_daemon() -> ProjectionDaemon:
    global _daemon
    if _daemon is None:
        _daemon = ProjectionDaemon(store, [
            ApplicationSummaryProjection(),
            AgentPerformanceProjection(),
            ComplianceAuditProjection(),
        ])
    return _daemon


# ═══════════════════════════════════════════════════════════════════════════════
# WRITE TOOLS (8)
# ═══════════════════════════════════════════════════════════════════════════════

@mcp.tool()
async def submit_application(
    application_id: str,
    applicant_id: str,
    amount: float,
    purpose: str,
    channel: str = "MCP",
) -> Dict[str, Any]:
    """Submit a new loan application. Creates a new event stream."""
    await store.connect()
    version = await handle_submit_application(
        store, application_id, applicant_id, amount, purpose, channel
    )
    return {
        "status": "SUCCESS",
        "application_id": application_id,
        "stream_version": version,
        "message": f"Application {application_id} submitted for ${amount:,.2f}",
    }


@mcp.tool()
async def record_credit_analysis(
    application_id: str,
    agent_id: str,
    session_id: str,
    model_version: str,
    confidence_score: float,
    risk_tier: str,
    recommended_limit: float,
    duration_ms: int,
    input_data: Dict[str, Any],
) -> Dict[str, Any]:
    """Record a completed credit analysis by an AI agent."""
    await store.connect()
    version = await handle_credit_analysis_completed(
        store, application_id, agent_id, session_id, model_version,
        confidence_score, risk_tier, recommended_limit, duration_ms, input_data,
    )
    return {
        "status": "SUCCESS",
        "application_id": application_id,
        "risk_tier": risk_tier,
        "stream_version": version,
    }


@mcp.tool()
async def record_fraud_screening(
    application_id: str,
    agent_id: str,
    session_id: str,
    model_version: str,
    fraud_score: float,
    anomaly_flags: List[str],
    input_data: Dict[str, Any],
) -> Dict[str, Any]:
    """Record a completed fraud screening by an AI agent."""
    await store.connect()
    version = await handle_fraud_screening_completed(
        store, application_id, agent_id, session_id, model_version,
        fraud_score, anomaly_flags, input_data,
    )
    return {
        "status": "SUCCESS",
        "application_id": application_id,
        "fraud_score": fraud_score,
        "stream_version": version,
    }


@mcp.tool()
async def record_compliance_check(
    application_id: str,
    rule_id: str,
    rule_version: str,
    passed: bool,
    evidence: Dict[str, Any],
    failure_reason: Optional[str] = None,
    remediation: Optional[str] = None,
) -> Dict[str, Any]:
    """Record a compliance check result (pass or fail)."""
    await store.connect()
    version = await handle_record_compliance_check(
        store, application_id, rule_id, rule_version, passed, evidence,
        failure_reason, remediation,
    )
    return {
        "status": "SUCCESS",
        "application_id": application_id,
        "rule_id": rule_id,
        "result": "PASSED" if passed else "FAILED",
        "stream_version": version,
    }


@mcp.tool()
async def generate_decision(
    application_id: str,
    orchestrator_agent_id: str,
    recommendation: str,
    confidence_score: float,
    contributing_sessions: List[str],
    basis_summary: str,
    model_versions: Dict[str, str],
) -> Dict[str, Any]:
    """Generate an AI decision (APPROVE/DECLINE/REFER). Confidence < 0.6 auto-REFERs."""
    await store.connect()
    version = await handle_generate_decision(
        store, application_id, orchestrator_agent_id, recommendation,
        confidence_score, contributing_sessions, basis_summary, model_versions,
    )
    return {
        "status": "SUCCESS",
        "application_id": application_id,
        "recommendation": recommendation,
        "confidence_score": confidence_score,
        "stream_version": version,
    }


@mcp.tool()
async def record_human_review(
    application_id: str,
    reviewer_id: str,
    override: bool,
    final_decision: str,
    override_reason: Optional[str] = None,
) -> Dict[str, Any]:
    """Record a human reviewer's final decision."""
    await store.connect()
    version = await handle_record_human_review(
        store, application_id, reviewer_id, override, final_decision, override_reason,
    )
    return {
        "status": "SUCCESS",
        "application_id": application_id,
        "final_decision": final_decision,
        "override": override,
        "stream_version": version,
    }


@mcp.tool()
async def start_agent_session(
    agent_id: str,
    session_id: str,
    context_source: str,
    replay_position: int,
    token_count: int,
    model_version: str,
) -> Dict[str, Any]:
    """Start a new agent session (Gas Town pattern — context before action)."""
    await store.connect()
    version = await handle_start_agent_session(
        store, agent_id, session_id, context_source,
        replay_position, token_count, model_version,
    )
    return {
        "status": "SUCCESS",
        "agent_id": agent_id,
        "session_id": session_id,
        "model_version": model_version,
        "stream_version": version,
    }


@mcp.tool()
async def run_integrity_check(stream_id: str) -> Dict[str, Any]:
    """Run a cryptographic integrity check on an event stream to detect tampering."""
    await store.connect()
    return await AuditChain.run_integrity_check(store, stream_id)


# ═══════════════════════════════════════════════════════════════════════════════
# READ RESOURCES (5)
# ═══════════════════════════════════════════════════════════════════════════════

@mcp.resource("projections://application-summary/{application_id}")
async def get_application_summary(application_id: str) -> str:
    """Get the read-optimized summary for a loan application (from projections, not replay)."""
    await store.connect()
    async with store.transaction() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM application_summary WHERE application_id = $1",
            application_id,
        )
        if not row:
            return f"Application {application_id} not found."
        return str(dict(row))


@mcp.resource("projections://compliance-view/{application_id}")
async def get_compliance_view(application_id: str) -> str:
    """Get current compliance status for an application (from projections)."""
    await store.connect()
    async with store.transaction() as conn:
        rows = await conn.fetch(
            """SELECT DISTINCT ON (rule_id) *
               FROM compliance_audit
               WHERE application_id = $1
               ORDER BY rule_id, global_position DESC""",
            application_id,
        )
        if not rows:
            return f"No compliance data for {application_id}."
        return str([dict(r) for r in rows])


@mcp.resource("projections://audit-trail/{stream_id}")
async def get_audit_trail(stream_id: str) -> str:
    """Get the full event audit trail for a stream."""
    await store.connect()
    events = await store.load_stream(stream_id)
    return str([e.model_dump(mode="json") for e in events])


@mcp.resource("projections://agent-performance/{agent_id}")
async def get_agent_performance(agent_id: str) -> str:
    """Get performance metrics for an AI agent across all model versions (from projections)."""
    await store.connect()
    async with store.transaction() as conn:
        rows = await conn.fetch(
            "SELECT * FROM agent_performance WHERE agent_id = $1",
            agent_id,
        )
        if not rows:
            return f"Agent {agent_id} not found."
        return str([dict(r) for r in rows])


@mcp.resource("health://lag")
async def get_health_lag() -> str:
    """Get projection lag metrics. SLO: ApplicationSummary < 500ms, Compliance < 2s."""
    await store.connect()
    daemon = _get_daemon()
    lag = await daemon.get_lag()
    return str({
        "lag_events": lag,
        "slo": {
            "ApplicationSummary": "< 500ms",
            "ComplianceAuditView": "< 2s",
        },
        "checked_at": datetime.now().isoformat(),
    })


if __name__ == "__main__":
    mcp.run()
