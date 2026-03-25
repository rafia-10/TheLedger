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
from src.projections.daemon import ProjectionDaemon
from src.upcasting.registry import registry as upcaster_registry

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
        console.print(f"[yellow]No events found for {stream_id}. Appending demo events...[/]")
        demo_events = [
            BaseEvent(event_type="ApplicationSubmitted", payload={"applicant_id": application_id, "requested_amount_usd": 50000}),
            BaseEvent(event_type="AgentContextLoaded", payload={"agent_id": "agent-007", "session_id": "sess-01"}),
            BaseEvent(event_type="CreditAnalysisCompleted", payload={"application_id": application_id, "risk_tier": "MEDIUM", "confidence_score": 0.85, "agent_id": "agent-007", "session_id": "sess-01"}),
            BaseEvent(event_type="ComplianceRulePassed", payload={"application_id": application_id, "rule_id": "KYC", "rule_version": "1.0", "evidence_hash": "hash_abc"}),
            BaseEvent(event_type="DecisionGenerated", payload={"application_id": application_id, "decision": "APPROVED", "amount": 50000}),
        ]
        await store.append(stream_id, demo_events, expected_version=-1)
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
    
    stream_id = f"concurrency-test-{int(time.time())}"
    
    # Setup initial state
    await store.append(stream_id, [BaseEvent(event_type="TestStarted", payload={})], expected_version=-1)
    current_version = await store.stream_version(stream_id)
    console.print(f"Stream initialized at version: {current_version}")

    event_a = BaseEvent(event_type="AgentAction", payload={"agent": "A"})
    event_b = BaseEvent(event_type="AgentAction", payload={"agent": "B"})

    results = []

    async def worker(name, event, version):
        console.print(f"[dim]Agent {name} attempting to append at version {version}...[/]")
        try:
            new_v = await store.append(stream_id, [event], expected_version=version)
            results.append(f"Agent {name}: SUCCESS (New Version: {new_v})")
            return True
        except OptimisticConcurrencyError as e:
            results.append(f"Agent {name}: [bold red]FAILURE (Error: {e})[/]")
            return False

    # Run concurrently
    console.print("Launching concurrent appends...")
    await asyncio.gather(
        worker("A", event_a, current_version),
        worker("B", event_b, current_version)
    )

    for r in results:
        console.print(r)

    # Retry logic demonstration
    console.print("\n[bold yellow]Demonstrating Automatic Retry for the failed agent...[/]")
    retry_version = await store.stream_version(stream_id)
    console.print(f"Retrying at current version: {retry_version}")
    await worker("B (Retry)", event_b, retry_version)
    
    final_version = await store.stream_version(stream_id)
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
    await store.append(f"loan-{app_id}", [
        BaseEvent(event_type="ComplianceRulePassed", payload={"application_id": app_id, "rule_id": "KYC", "rule_version": "1.0", "evidence_hash": "h1"})
    ], expected_version=-1)
    
    # 2. State at T2: AML Failed (later)
    await asyncio.sleep(1) # Ensure timestamp difference
    t2_time = datetime.now()
    await store.append(f"loan-{app_id}", [
        BaseEvent(event_type="ComplianceRuleFailed", payload={"application_id": app_id, "rule_id": "AML", "rule_version": "1.0", "evidence_hash": "h2", "failure_reason": "High risk flag"})
    ], expected_version=1)
    
    # 3. Process into projection
    await daemon._process_batch()
    
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
    
    stream_id = f"upcast-test-{int(time.time())}"
    
    # 1. Manually insert v1 event (bypassing normal appends for demo)
    async with store.transaction() as conn:
        await conn.execute(
            """
            INSERT INTO events (stream_id, stream_position, event_type, event_version, payload, metadata)
            VALUES ($1, 1, 'CreditAnalysisCompleted', 1, '{"application_id": "test", "confidence_score": 95}', '{}')
            """,
            stream_id
        )
        await conn.execute(
            "INSERT INTO event_streams (stream_id, aggregate_type, current_version) VALUES ($1, 'loan', 1)",
            stream_id
        )

    # 2. Load through EventStore (triggers upcasting)
    events = await store.load_stream(stream_id)
    upcast_event = events[0]
    
    console.print(f"Loaded Event Version: [bold green]{upcast_event.event_version}[/]")
    console.print(f"Upcast Payload: {upcast_event.payload}")
    
    # 3. Query DB Raw
    async with store.transaction() as conn:
        raw = await conn.fetchrow("SELECT event_version, payload FROM events WHERE stream_id = $1", stream_id)
        
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
    stream_id = f"agent-{agent_id}-{session_id}"
    
    # 1. Start session
    console.print("Agent starting work...")
    await store.append(stream_id, [
        BaseEvent(event_type="AgentContextLoaded", payload={"agent_id": agent_id, "model_version": "gpt-4-v2"})
    ], expected_version=-1)
    
    # 2. Partial work
    await store.append(stream_id, [
        BaseEvent(event_type="CreditAnalysisCompleted", payload={"application_id": "app-999", "risk_tier": "LOW", "confidence_score": 0.99})
    ], expected_version=1)
    
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
    
    app_id = "what-if-demo"
    stream_id = f"loan-{app_id}"
    
    # Setup: Medium risk app
    events = [
        BaseEvent(event_type="ApplicationSubmitted", payload={"applicant_id": "applicant-123", "requested_amount_usd": 10000}),
        BaseEvent(event_type="CreditAnalysisCompleted", payload={"application_id": app_id, "risk_tier": "MEDIUM", "confidence_score": 0.7, "agent_id": "agent-007", "session_id": "sess-01"}),
    ]
    # Small hack: we need to make sure the aggregate logic handles this. 
    # The aggregate typically decides based on rules. 
    # If risk is HIGH, maybe it declines.
    
    sim = WhatIfSimulator(store)
    
    # 1. Create original stream
    console.print("Original Scenario: Risk Tier = MEDIUM")
    # We don't necessarily need to persist it if we have the events list
    # But let's build a fake "StoredEvent" list
    original = []
    for i, e in enumerate(events):
        original.append(StoredEvent(
            event_id=uuid.uuid4(), stream_id=stream_id, stream_position=i+1, global_position=i+1,
            event_type=e.event_type, event_version=e.event_version, payload=e.payload, metadata={}, recorded_at=datetime.now()
        ))
    
    # 2. Modify: Change MEDIUM to HIGH at position 2
    modified = sim.modify_events(original, [
        {"action": "alter", "position": 2, "changes": {"risk_tier": "HIGH"}}
    ])
    
    console.print("Modified Scenario: Risk Tier = HIGH")
    
    # 3. Compare
    comparison = sim.compare(original, modified)
    
    console.print("\n[bold]Outcome Comparison:[/]")
    diffs = comparison["differences"]
    for field, vals in diffs.items():
        console.print(f"Field [magenta]{field}[/]: {vals['original']} -> [bold red]{vals['modified']}[/]")
    
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
