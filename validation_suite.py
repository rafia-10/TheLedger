import asyncio
import os
import json
import time
import uuid
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
from dotenv import load_dotenv
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.live import Live
from rich.progress import Progress

from src.event_store import EventStore
from src.models.events import BaseEvent, CreditAnalysisCompleted, OptimisticConcurrencyError, StoredEvent
from src.gas_town import reconstruct_agent_context
from src.what_if import WhatIfSimulator
from src.projections.compliance_audit import ComplianceAuditProjection
from src.projections.application_summary import ApplicationSummaryProjection
from src.projections.agent_performance import AgentPerformanceProjection
from src.projections.daemon import ProjectionDaemon
from src.upcasting.registry import registry as upcaster_registry
from src.aggregates.loan_application import LoanApplicationAggregate, ApplicationState
from src.commands.handlers import (
    handle_submit_application,
    handle_start_agent_session,
    handle_credit_analysis_completed,
    handle_record_compliance_check,
    handle_generate_decision
)

load_dotenv()

console = Console()

async def get_store():
    dsn = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/eventledger")
    store = EventStore(dsn, upcaster=upcaster_registry)
    await store.connect()
    return store

async def step_1_the_week_standard(store: EventStore, application_id: str = "demo-app-001"):
    """
    Step 1 — The Week Standard: Run "Show me the complete decision history of application ID X" end-to-end.
    Show full event stream, all agent actions, compliance checks, human review, causal links, and cryptographic integrity verification.
    """
    console.print(Panel(f"[bold blue]Step 1: The Week Standard[/]\nApplication ID: {application_id}", expand=False))
    
    start_time = time.time()
    
    # 1. Load the stream
    stream_id = f"loan-{application_id}"
    events = await store.load_stream(stream_id)
    
    if not events:
        console.print(f"[yellow]No events found for {stream_id}. Appending real domain events via handlers...[/]")
        
        # Phase 1: Real Submission
        await handle_submit_application(store, application_id, "applicant-hardened", 50000.0, "Business Expansion", "API")
        
        # Phase 2: Real Agent Session & Analysis
        agent_id = "credit-agent-007"
        session_id = str(uuid.uuid4())[:8]
        await handle_start_agent_session(store, agent_id, session_id, "VectorDB-KYC", 0, 450, "v1.1")
        
        await handle_credit_analysis_completed(
            store, application_id, agent_id, session_id, "v1.1", 0.88, "TIER_A", 50000.0, 120, {"fico": 720}
        )
        
        # Phase 3: Compliance
        await handle_record_compliance_check(
            store, application_id, "KYC-01", "v1", True, {"id_verified": True}
        )
        
        # Phase 4: Decision
        await handle_generate_decision(
            store, application_id, "decision-engine", "APPROVE", 0.92, [f"{agent_id}-{session_id}"], 
            "Matches all risk criteria.", {"credit": "v1.1"}
        )
        
        events = await store.load_stream(stream_id)

    # 2. Display the Event Table
    table = Table(title=f"Event Stream: {stream_id}")
    table.add_column("Pos", justify="right", style="cyan")
    table.add_column("Event Type", style="magenta")
    table.add_column("Timestamp", style="green")
    table.add_column("Correlation ID", style="dim")
    table.add_column("Integrity Hash", style="dim", max_width=12)

    for e in events:
        table.add_row(
            str(e.stream_position),
            e.event_type,
            e.recorded_at.strftime("%H:%M:%S.%f") if e.recorded_at else "N/A",
            e.metadata.get("correlation_id", "N/A"),
            e.metadata.get("_hash", "N/A")[:10] + "..."
        )
    
    console.print(table)

    # 3. Cryptographic Verification
    with console.status("[bold green]Verifying cryptographic integrity chain..."):
        is_valid = await store.verify_stream_integrity(stream_id)
        
    if is_valid:
        console.print("[bold green]✅ Cryptographic Integrity Verified: All hashes chain correctly.[/]")
    else:
        console.print("[bold red]❌ Integrity Violation Detected: Hash chain broken![/]")

    end_time = time.time()
    duration = end_time - start_time
    console.print(f"\n[bold]Total Execution Time:[/] {duration:.2f} seconds")
    
    if duration < 60:
        console.print("[bold green]Status: PASS (Under 60s constraint)[/]")
    else:
        console.print("[bold red]Status: FAIL (Exceeded 60s constraint)[/]")

async def step_2_concurrency_under_pressure(store: EventStore):
    """
    Step 2 — Concurrency Under Pressure: Run the double-decision test live.
    Show two agents colliding on the same stream, one succeeding, one receiving OptimisticConcurrencyError and retrying.
    """
    console.print(Panel("[bold blue]Step 2: Concurrency Under Pressure[/]", expand=False))
    
    app_id = f"concurrency-app-{int(time.time())}"
    
    # 1. Setup: Real Submission
    await handle_submit_application(store, app_id, "applicant-occ", 10000.0, "Testing", "CLI")
    app = await LoanApplicationAggregate.load(store, app_id)
    
    console.print(f"Stream 'loan-{app_id}' initialized at version: {app.version}")
    
    # 2. Parallel Decisions: Two agents trying to approve same app
    results = []

    async def worker(agent_name, rec):
        console.print(f"[dim]Agent {agent_name} attempting to generate decision at version {app.version}...[/]")
        try:
            new_v = await handle_generate_decision(
                store, app_id, agent_name, rec, 0.95, [], "Verified via occ.", {"model": "v1"},
                expected_version=app.version
            )
            results.append(f"Agent {agent_name}: SUCCESS (New Version: {new_v})")
            return True
        except OptimisticConcurrencyError as e:
            results.append(f"Agent {agent_name}: [bold red]FAILURE (Error: {e})[/]")
            return False

    # Run concurrently
    console.print("Launching concurrent appends...")
    await asyncio.gather(
        worker("Agent-Alpha", "APPROVE"),
        worker("Agent-Beta", "DECLINE")
    )

    for r in results:
        console.print(r)

    # 3. Retry logic demonstration
    console.print("\n[bold yellow]Demonstrating Automatic Retry for the failed agent...[/]")
    app_retry = await LoanApplicationAggregate.load(store, app_id)
    console.print(f"Retrying at current version: {app_retry.version}")
    await handle_generate_decision(
        store, app_id, "Agent-Beta (Retry)", "DECLINE", 0.95, [], "Retrying after conflict.", {"model": "v1"}
    )
    
    final_version = (await LoanApplicationAggregate.load(store, app_id)).version
    console.print(f"Final Stream Version: {final_version}")

async def step_3_temporal_compliance_query(store: EventStore):
    """
    Step 3 — Temporal Compliance Query: Query ledger://applications/{id}/compliance?as_of={timestamp} for a past point in time.
    Show the compliance state as it existed at that moment, distinct from the current state.
    """
    console.print(Panel("[bold blue]Step 3: Temporal Compliance Query[/]", expand=False))
    
    app_id = f"temporal-app-{int(time.time())}"
    projection = ComplianceAuditProjection()
    daemon = ProjectionDaemon(store, [projection])
    
    # 1. State at T1: KYC Passed
    t1_time = datetime.now()
    await handle_record_compliance_check(store, app_id, "KYC", "v1", True, {"id": "verified"})
    
    # 2. State at T2: AML Failed (later)
    await asyncio.sleep(1) # Ensure timestamp difference
    t2_time = datetime.now()
    await handle_record_compliance_check(store, app_id, "AML", "v1", False, {"risk": "high"}, failure_reason="Listed")
    
    # 3. Process into projection
    await daemon.run_once() # Run real loop once
    
    # 4. Query T1
    console.print(f"Querying state at T1: {t1_time.isoformat()}")
    res_t1 = await projection.get_compliance_at(store, app_id, t1_time)
    console.print(f"Results at T1: {[r['rule_id'] for r in res_t1]}")
    
    # 5. Query T2 (Current)
    console.print(f"Querying state at T2 (Present): {t2_time.isoformat()}")
    res_t2 = await projection.get_compliance_at(store, app_id, datetime.now())
    console.print(f"Results at T2: {[(r['rule_id'], r['status']) for r in res_t2]}")

async def step_4_upcasting_and_immutability(store: EventStore):
    """
    Step 4 — Upcasting & Immutability: Load a v1 event through the store, show it arrives as v2.
    Query the raw database row and show the stored payload is unchanged.
    """
    console.print(Panel("[bold blue]Step 4: Upcasting & Immutability[/]", expand=False))
    
    app_id = f"upcast-app-{int(time.time())}"
    stream_id = f"loan-{app_id}"
    
    # 1. Start with a real submission
    await handle_submit_application(store, app_id, "upcast-user", 25000, "Refactor", "Web")
    
    # 2. Inject a v1 event manually into this real stream
    async with store.transaction() as conn:
        await conn.execute(
            """
            INSERT INTO events (stream_id, stream_position, event_type, event_version, payload, metadata)
            VALUES ($1, 2, 'CreditAnalysisCompleted', 1, '{"application_id": "test", "confidence_score": 95}', '{}')
            """,
            stream_id
        )
        await conn.execute(
            "UPDATE event_streams SET current_version = 2 WHERE stream_id = $1",
            stream_id
        )

    # 2. Load through EventStore (triggers upcasting)
    events = await store.load_stream(stream_id)
    # The upcast event should be the second one (pos 2)
    upcast_event = events[1]
    
    console.print(f"Loaded Event Version: [bold green]{upcast_event.event_version}[/]")
    console.print(f"Upcast Payload: {upcast_event.payload}")
    
    # 3. Query DB Raw (inspect pos 2)
    async with store.transaction() as conn:
        raw = await conn.fetchrow("SELECT event_version, payload FROM events WHERE stream_id = $1 AND stream_position = 2", stream_id)
        
    console.print(f"Raw DB Version: [bold yellow]{raw['event_version']}[/]")
    console.print(f"Raw DB Payload: {raw['payload']}")
    
    if upcast_event.event_version == 2 and raw['event_version'] == 1:
        console.print("[bold green]✅ Success: Event upcast in memory, DB remains immutable.[/]")

async def step_5_gas_town_recovery(store: EventStore):
    """
    Step 5 — Gas Town Recovery: Start an agent session, append several events, simulate a crash (kill the process).
    Call reconstruct_agent_context() and show the agent can resume with correct state.
    """
    console.print(Panel("[bold blue]Step 5: Gas Town Recovery[/]", expand=False))
    
    agent_id = "agent-delta"
    session_id = f"sess-{int(time.time())}"
    app_id = f"gas-app-{int(time.time())}"
    stream_id = f"agent-{agent_id}-{session_id}"
    
    # 0. Background setup: real app must exist
    await handle_submit_application(store, app_id, "gas-applicant", 10000.0, "Recovery", "Agent")

    # 1. Start session via handler
    console.print("Agent starting work...")
    await handle_start_agent_session(store, agent_id, session_id, "LocalCache", 0, 150, "v2.0")
    
    # 2. Partial work via handler
    await handle_credit_analysis_completed(
        store, app_id, agent_id, session_id, "v2.0", 0.95, "TIER_A", 10000.0, 45, {"data": "x"}
    )
    
    console.print("[bold red]SIMULATING CRASH... Agent process killed.[/]")
    
    # 3. Recovery
    console.print("[bold green]RESTARTING... Reconstructing context from Ledger.[/]")
    context = await reconstruct_agent_context(store, agent_id, session_id)
    
    console.print(f"Recovered Agent ID: {context['agent_id']}")
    console.print(f"Recovered Model Version: {context['model_version']}")
    console.print(f"Decisions Found: {len(context['decisions_made'])}")
    console.print(f"Wait, was there unfinished work? [bold cyan]{context['unfinished_work']}[/]")

async def step_6_what_if_counterfactual(store: EventStore):
    """
    Step 6 — What-If Counterfactual (Bonus): Run a what-if scenario substituting a HIGH risk tier for MEDIUM.
    Show the cascading effect on the final decision through business rule enforcement.
    """
    console.print(Panel("[bold blue]Step 6: What-If Counterfactual[/]", expand=False))
    
    app_id = f"what-if-{int(time.time())}"
    stream_id = f"loan-{app_id}"
    session_id = f"s-{int(time.time())}"
    
    # 1. Create original stream via real handlers
    console.print("Original Scenario: Risk Tier = MEDIUM")
    await handle_submit_application(store, app_id, "potential-customer", 12000, "Home", "Mobile")
    
    # Gas Town enforcement: Agent must start session
    await handle_start_agent_session(store, "risk-agent", session_id, "Legacy-System", 0, 100, "v1.0")
    
    await handle_credit_analysis_completed(
        store, app_id, "risk-agent", session_id, "v1.0", 0.7, "MEDIUM", 12000, 30, {"f": 650}
    )
    
    # 2. Load the real events we just created
    original = await store.load_stream(stream_id)
    
    sim = WhatIfSimulator(store)
    
    # 3. Modify: Change MEDIUM to HIGH in the second event
    modified = sim.modify_events(original, [
        {"action": "alter", "position": 2, "changes": {"risk_tier": "HIGH"}}
    ])
    
    console.print("Modified Scenario: Risk Tier = HIGH")
    
    # 4. Compare
    comparison = sim.compare(original, modified)
    
    console.print("\n[bold]Outcome Comparison:[/]")
    diffs = comparison["differences"]
    for field, vals in diffs.items():
        console.print(f"Field [magenta]{field}[/]: {vals['original']} -> [bold red]{vals['modified']}[/]")
    
    if comparison["has_divergence"]:
        console.print("\n[bold green]✅ Counterfactual Divergence Verified.[/]")
    
    if comparison["has_divergence"]:
        console.print("\n[bold green]✅ Counterfactual Divergence Verified.[/]")

async def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--step", type=int, choices=[1, 2, 3, 4, 5, 6], help="Run a specific step")
    parser.add_argument("--all", action="store_true", help="Run all steps")
    args = parser.parse_args()

    store = await get_store()
    
    try:
        if args.all or args.step == 1:
            await step_1_the_week_standard(store)
            console.print("\n" + "="*50 + "\n")
            
        if args.all or args.step == 2:
            await step_2_concurrency_under_pressure(store)
            console.print("\n" + "="*50 + "\n")
            
        if args.all or args.step == 3:
            await step_3_temporal_compliance_query(store)
            console.print("\n" + "="*50 + "\n")

        if args.all or args.step == 4:
            await step_4_upcasting_and_immutability(store)
            console.print("\n" + "="*50 + "\n")

        if args.all or args.step == 5:
            await step_5_gas_town_recovery(store)
            console.print("\n" + "="*50 + "\n")

        if args.all or args.step == 6:
            await step_6_what_if_counterfactual(store)
            console.print("\n" + "="*50 + "\n")

    finally:
        await store.disconnect()

if __name__ == "__main__":
    asyncio.run(main())
