import asyncio
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()
import json
from datetime import datetime, timedelta
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.live import Live
from rich.layout import Layout
from rich import box

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
from src.upcasting.registry import UpcasterRegistry
from src.models.events import BaseEvent, StoredEvent

# Database configuration — Cloud only (Supabase Transaction Pooler)
CLOUD_DSN = "postgresql://postgres.dxscaeckamkplshxqkae:supabase1224@aws-0-eu-central-1.pooler.supabase.com:6543/postgres?sslmode=require"
DSN = os.getenv("DATABASE_URL", CLOUD_DSN)
console = Console()

async def run_demo():
    store = EventStore(DSN)
    await store.connect()
    
    # Clean up DB for a fresh demo
    async with store.transaction() as conn:
        await conn.execute("TRUNCATE events CASCADE")
        await conn.execute("TRUNCATE event_streams CASCADE")
        await conn.execute("TRUNCATE projection_checkpoints CASCADE")
        await conn.execute("TRUNCATE application_summary CASCADE")
        await conn.execute("TRUNCATE agent_performance CASCADE")
        await conn.execute("TRUNCATE compliance_audit CASCADE")

    console.print(Panel("[bold green]The Ledger: Full Lifecycle Showroom[/bold green]", subtitle="Starting Workflow Demo"))

    app_id = "demo-app-888"
    applicant_id = "user-99"

    # --- PHASE 1: SUBMISSION ---
    console.print("\n[bold cyan]PHASE 1: Loan Submission[/bold cyan]")
    await handle_submit_application(store, app_id, applicant_id, 150000.0, "Home Improvement", "Web")
    console.print(f"✅ Application {app_id} submitted for $150,000.")

    # --- PHASE 2: PARALLEL AI CHECKS ---
    console.print("\n[bold cyan]PHASE 2: Parallel AI Agent Analysis[/bold cyan]")
    
    # Start projection daemon in background to live-update
    daemon = ProjectionDaemon(store, [
        ApplicationSummaryProjection(),
        AgentPerformanceProjection(),
        ComplianceAuditProjection()
    ])
    daemon_task = asyncio.create_task(daemon.run_forever(poll_interval_ms=100))

    # Gas Town: Start sessions first
    await handle_start_agent_session(store, "credit-agent-01", "session-c1", "VectorDB-KYC", 0, 450, "v1.1")
    await handle_start_agent_session(store, "fraud-agent-01", "session-f1", "RiskGraph-v4", 0, 320, "v2.0")

    # Credit Check (Agent v1.1)
    await handle_credit_analysis_completed(
        store, app_id, "credit-agent-01", "session-c1", "v1.1", 0.85, "TIER_A", 150000.0, 120, {"fico": 680}
    )
    console.print("🤖 [italic]Credit Agent:[/italic] Analysis complete. Score 680, Confidence 0.85.")

    # Fraud Screening (Agent v2.0)
    await handle_fraud_screening_completed(
        store, app_id, "fraud-agent-01", "session-f1", "v2.0", 0.12, [], {"ip": "127.0.0.1"}
    )
    console.print("🤖 [italic]Fraud Agent:[/italic] Screening complete. Risk 0.12, Confidence 0.92.")

    # Compliance Check (Rule R-101)
    await handle_record_compliance_check(
        store, app_id, "KYC-01", "v1", True, {"id_verified": True}
    )
    console.print("🛡️ [italic]Compliance Bot:[/italic] KYC check PASSED.")

    # --- PHASE 3: DECISION ---
    console.print("\n[bold cyan]PHASE 3: AI decision & Human Review[/bold cyan]")
    await handle_generate_decision(
        store, app_id, "decision-engine", "APPROVE", 0.88, ["session-c1", "session-f1"], 
        "Matches all risk criteria.", {"credit": "v1.1", "fraud": "v2.0"}
    )
    console.print("⚖️ [italic]Decision Engine:[/italic] Approved for full amount.")

    await handle_record_human_review(store, app_id, "reviewer-bob", False, "APPROVED", "Checks out, solid profile.")
    console.print("👤 [italic]Human Reviewer (Bob):[/italic] Final approval signed off.")

    # Give projections a moment to catch up
    await asyncio.sleep(0.5)

    # --- PHASE 4: DASHBOARD SHOWROOM ---
    console.print("\n[bold cyan]PHASE 4: Live Projection Dashboard (Read Side)[/bold cyan]")
    
    async with store.transaction() as conn:
        summary = await conn.fetchrow("SELECT * FROM application_summary WHERE application_id = $1", app_id)
        performance = await conn.fetch("SELECT * FROM agent_performance")
        
    table = Table(title="Application Summary", box=box.ROUNDED)
    table.add_column("App ID", style="magenta")
    table.add_column("State", style="green")
    table.add_column("Amount", style="yellow")
    table.add_column("Compliance", style="blue")
    table.add_column("Decision", style="cyan")
    
    table.add_row(
        summary["application_id"],
        summary["state"],
        f"${summary['requested_amount_usd']:,.2f}",
        summary["compliance_status"],
        summary["decision"]
    )
    console.print(table)

    perf_table = Table(title="Agent Performance Metrics", box=box.ROUNDED)
    perf_table.add_column("Agent", style="dim")
    perf_table.add_column("Model", style="dim")
    perf_table.add_column("Tasks", justify="right")
    perf_table.add_column("Avg Confidence", justify="right")
    
    for row in performance:
        perf_table.add_row(
            row["agent_id"],
            row["model_version"],
            str(row["analyses_completed"] + row["decisions_generated"]),
            f"{row['avg_confidence_score']:.2f}"
        )
    console.print(perf_table)

    # --- PHASE 5: CONCURRENCY CONFLICT ---
    console.print("\n[bold cyan]PHASE 5: Optimistic Concurrency Conflict[/bold cyan]")
    console.print("[dim]Simulating two agents trying to decide on the same version...[/dim]")
    
    stream_id = f"loan-{app_id}"
    version = await store.stream_version(stream_id)
    # Both read version N. Agent A wins, Agent B loses.
    try:
        # Agent A
        await handle_generate_decision(
            store, app_id, "agent-a", "APPROVE", 0.9, ["session-c1", "session-f1"], 
            "I'm first", {"credit": "v1.1", "fraud": "v2.0"}, 
            expected_version=version
        )
        # Agent B (should fail)
        await handle_generate_decision(
            store, app_id, "agent-b", "DECLINE", 0.9, ["session-c1", "session-f1"], 
            "I'm late", {"credit": "v1.1", "fraud": "v2.0"}, 
            expected_version=version
        )
    except Exception as e:
        console.print(f"❌ [bold red]Conflict Detected:[/bold red] {e}")

    # --- PHASE 6: UPCASTING & REPLAY ---
    console.print("\n[bold cyan]PHASE 6: Event Replay & Schema Upcasting[/bold cyan]")
    
    registry = UpcasterRegistry()
    @registry.register("ApplicationSubmitted", 1)
    def upcast_v1_to_v2(payload):
        payload["upcast_demo"] = "Legacy Migrated"
        return payload
    
    store._upcaster = registry
    events = await store.load_stream(stream_id)
    if events:
        console.print(f"Replayed {len(events)} events. First event upcast state: {events[0].payload.get('upcast_demo')}")
    else:
        console.print(f"[yellow]Warning: No events found for replay on {stream_id}. Check previous phases.[/yellow]")

    daemon.stop()
    await store.disconnect()
    console.print("\n[bold green]--- Demo Complete ---[/bold green]")

if __name__ == "__main__":
    asyncio.run(run_demo())
