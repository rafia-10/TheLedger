#!/usr/bin/env python3
"""
Supervisor Demo — EventLedger Loan Application Lifecycle

Real autonomous agents process a live application.
Events flow:  Postgres outbox → Kafka → WebSocket → Dashboard

Before running:
  Terminal 1:  docker-compose up -d
  Terminal 2:  uvicorn src.api.main:app --reload
  Browser:     http://localhost:8000
  Terminal 3:  .venv/bin/python supervisor_demo.py
"""
import asyncio
import json
import os
import random
import socket
import subprocess
from datetime import datetime
from typing import Dict, List, Set

import logging
from dotenv import load_dotenv
load_dotenv()

# Silence noisy aiokafka internals (coordinator retries, etc.)
logging.getLogger("aiokafka").setLevel(logging.CRITICAL)
logging.getLogger("kafka").setLevel(logging.CRITICAL)

from rich.columns import Columns
from rich.console import Console
from rich.live import Live
from rich.panel import Panel
from rich.rule import Rule
from rich.table import Table
from rich.text import Text
from rich import box

from src.event_store import EventStore
from src.commands.handlers import handle_submit_application
from agents.credit_agent import CreditAgent
from agents.fraud_agent import FraudAgent
from agents.compliance_agent import ComplianceAgent
from agents.decision_agent import DecisionAgent
from agents.human_review import HumanReviewAgent

# ─── APPLICANT PROFILES ───────────────────────────────────────────────────────
# Each run picks a different profile so the database has diverse, real-looking data.

PROFILES = [
    ("alice-chen",    "Alice Chen",     340_000, "Home Purchase",       "Web Portal"),
    ("bob-torres",    "Bob Torres",      85_000, "Home Renovation",     "Mobile App"),
    ("carol-james",   "Carol James",    420_000, "Commercial Property", "Branch Office"),
    ("david-park",    "David Park",     175_000, "Business Expansion",  "Web Portal"),
    ("emma-wilson",   "Emma Wilson",     52_000, "Auto Loan",           "Mobile App"),
    ("frank-ahmed",   "Frank Ahmed",     28_000, "Education Loan",      "Web Portal"),
    ("grace-liu",     "Grace Liu",      295_000, "Home Purchase",       "Referral"),
    ("henry-smith",   "Henry Smith",     66_000, "Debt Consolidation",  "Mobile App"),
    ("isabel-okafor", "Isabel Okafor",  510_000, "Commercial Property", "Branch Office"),
    ("james-murphy",  "James Murphy",   130_000, "Home Renovation",     "Web Portal"),
]

# ─── CONFIG ───────────────────────────────────────────────────────────────────

DSN = os.getenv("DATABASE_URL", "postgresql:///eventledger")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = "ledger-events"

console = Console()

EVENT_COLORS: Dict[str, str] = {
    "ApplicationSubmitted":    "blue",
    "CreditAnalysisCompleted": "magenta",
    "FraudScreeningCompleted": "yellow",
    "ComplianceRulePassed":    "cyan",
    "ComplianceRuleFailed":    "red",
    "DecisionGenerated":       "bold white",
    "HumanReviewCompleted":    "pink1",
    "ApplicationApproved":     "bold green",
    "ApplicationDeclined":     "bold red",
}

TERMINAL_EVENTS = {"ApplicationApproved", "ApplicationDeclined"}

# ─── KAFKA ────────────────────────────────────────────────────────────────────

def _kafka_reachable() -> bool:
    try:
        with socket.create_connection(("localhost", 9092), timeout=1):
            return True
    except OSError:
        return False


def ensure_kafka():
    if _kafka_reachable():
        console.print("  [dim]Kafka[/dim]  [green]already running[/green] on localhost:9092")
        return True
    console.print("  [dim]Kafka[/dim]  [yellow]not detected — running docker-compose up -d ...[/yellow]")
    subprocess.run(["docker-compose", "up", "-d"], capture_output=True)
    for _ in range(12):
        import time; time.sleep(1)
        if _kafka_reachable():
            console.print("  [dim]Kafka[/dim]  [green]ready[/green]")
            return True
    console.print("  [yellow]Kafka unavailable — dashboard WebSocket still works via in-memory bus.[/yellow]")
    return False


kafka_msgs: List[dict] = []
_kafka_stop = asyncio.Event()


async def run_kafka_consumer():
    """Subscribe to ledger-events and populate kafka_msgs list."""
    if not _kafka_reachable():
        return
    try:
        from aiokafka import AIOKafkaConsumer
        consumer = AIOKafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP,
            auto_offset_reset="latest",
            group_id=f"demo-{int(datetime.now().timestamp())}",
        )
        await consumer.start()
        try:
            while not _kafka_stop.is_set():
                try:
                    msg = await asyncio.wait_for(consumer.getone(), timeout=0.3)
                    kafka_msgs.append(json.loads(msg.value))
                except asyncio.TimeoutError:
                    pass
        finally:
            await consumer.stop()
    except Exception:
        pass  # Kafka not available — silently skip


# ─── DISPLAY ──────────────────────────────────────────────────────────────────

def _event_detail(event_type: str, payload: dict) -> str:
    d = {
        "ApplicationSubmitted":    f"${payload.get('requested_amount_usd',0):,.0f}  ·  {payload.get('loan_purpose','')}  ·  {payload.get('submission_channel','')}",
        "CreditAnalysisCompleted": f"{payload.get('risk_tier','')}  ·  conf={payload.get('confidence_score',0):.0%}  ·  agent={payload.get('agent_id','')}",
        "FraudScreeningCompleted": f"score={payload.get('fraud_score',0):.3f}  ·  flags={payload.get('anomaly_flags',[])}",
        "ComplianceRulePassed":    f"{payload.get('rule_id','')}  ·  {payload.get('rule_version','')}",
        "ComplianceRuleFailed":    f"{payload.get('rule_id','')}  ·  {payload.get('failure_reason','')}",
        "DecisionGenerated":       f"{payload.get('recommendation','')}  ·  conf={payload.get('confidence_score',0):.0%}  ·  engine={payload.get('orchestrator_agent_id','')}",
        "HumanReviewCompleted":    f"{payload.get('final_decision','')}  ·  {payload.get('reviewer_id','')}  ·  override={payload.get('override',False)}",
        "ApplicationApproved":     f"${payload.get('approved_amount_usd',0):,.0f}  ·  {payload.get('interest_rate',0):.2%}  ·  by {payload.get('approved_by','')}",
        "ApplicationDeclined":     f"reasons={payload.get('decline_reasons',[])}",
    }
    return d.get(event_type, str(payload)[:80])


def make_ledger_panel(events, app_id: str) -> Panel:
    t = Table(box=box.SIMPLE, show_header=True, expand=True)
    t.add_column("#", width=3, style="dim", justify="right")
    t.add_column("Event Type", min_width=28)
    t.add_column("Key Info", style="white")
    t.add_column("Time", style="dim", width=12)
    for e in events:
        color = EVENT_COLORS.get(e.event_type, "white")
        t.add_row(
            str(e.stream_position),
            f"[{color}]{e.event_type}[/{color}]",
            _event_detail(e.event_type, e.payload),
            e.recorded_at.strftime("%H:%M:%S"),
        )
    return Panel(
        t,
        title=f"[bold]Loan Ledger[/bold]  [dim]{app_id}  ({len(events)} events)[/dim]",
        border_style="blue",
    )


def make_kafka_panel() -> Panel:
    lines: List[str] = []
    for m in kafka_msgs[-14:]:
        et = m.get("event_type", "?")
        color = EVENT_COLORS.get(et, "white")
        ts = (m.get("published_at") or "")[:19].replace("T", " ")
        lines.append(f"[dim]{ts}[/dim]  [{color}]{et}[/{color}]")
    body = "\n".join(lines) if lines else "[dim]Waiting for Kafka messages...[/dim]"
    title = (
        f"[bold]Kafka Stream[/bold]  [dim]{KAFKA_TOPIC}  "
        f"({len(kafka_msgs)} msgs)[/dim]"
    )
    return Panel(body, title=title, border_style="dim cyan", width=52)


def make_live_display(events, app_id: str):
    return Columns([make_kafka_panel(), make_ledger_panel(events, app_id)], expand=True)


# ─── STEP HELPERS ─────────────────────────────────────────────────────────────

def pause(prompt: str = "  [dim]Press Enter to continue[/dim]"):
    console.print()
    console.print(prompt, end="")
    input()
    console.print()


async def wait_showing_live(
    store: EventStore,
    app_id: str,
    required: Set[str],
) -> list:
    """
    Show a live Kafka + Ledger panel until all `required` event types
    are present in the loan stream, then return the events.
    """
    with Live(console=console, refresh_per_second=3, vertical_overflow="visible") as live:
        while True:
            events = await store.load_stream(f"loan-{app_id}")
            live.update(make_live_display(events, app_id))
            found = {e.event_type for e in events}
            if required.issubset(found):
                await asyncio.sleep(0.5)
                events = await store.load_stream(f"loan-{app_id}")
                live.update(make_live_display(events, app_id))
                return events
            await asyncio.sleep(0.35)

# ─── MAIN ─────────────────────────────────────────────────────────────────────

async def run():
    # Pick a random applicant profile
    profile = random.choice(PROFILES)
    applicant_id, display_name, amount, purpose, channel = profile
    # Build a readable, unique App ID from the name + time
    surname = display_name.split()[-1].upper()[:5]
    APP_ID = f"LOAN-{surname}-{datetime.now().strftime('%H%M%S')}"

    store = EventStore(DSN)
    await store.connect()

    # Clean up any leftover streams for this exact ID (shouldn't exist, but safety)
    streams = [f"loan-{APP_ID}", f"compliance-{APP_ID}"]
    async with store.transaction() as conn:
        await conn.execute(
            "DELETE FROM outbox WHERE event_id IN "
            "(SELECT event_id FROM events WHERE stream_id = ANY($1))", streams
        )
        await conn.execute("DELETE FROM events WHERE stream_id = ANY($1)", streams)
        await conn.execute("DELETE FROM event_streams WHERE stream_id = ANY($1)", streams)
        await conn.execute("DELETE FROM application_summary WHERE application_id = $1", APP_ID)
        await conn.execute("DELETE FROM compliance_audit WHERE application_id = $1", APP_ID)

    # ── Title ─────────────────────────────────────────────────────────────────
    console.print()
    console.print(Panel.fit(
        "[bold white]EventLedger[/bold white]  ·  Supervisor Walkthrough\n"
        "[dim]Immutable Event-Sourced Loan Processing Pipeline[/dim]\n\n"
        f"  [cyan]Applicant:[/cyan]  [bold]{display_name}[/bold]\n"
        f"  [cyan]Amount:   [/cyan]  [bold]${amount:,.0f}[/bold]\n"
        f"  [cyan]Purpose:  [/cyan]  [bold]{purpose}[/bold]\n"
        f"  [cyan]App ID:   [/cyan]  [bold]{APP_ID}[/bold]",
        border_style="bold green", padding=(1, 4),
    ))
    console.print()
    ensure_kafka()
    console.print(
        "  [bold]Dashboard:[/bold]  "
        "[link=http://localhost:8000]http://localhost:8000[/link]  "
        "[dim](open in browser — events appear live via WebSocket)[/dim]"
    )

    # Start Kafka consumer (background)
    kafka_task = asyncio.create_task(run_kafka_consumer())

    # Start all agents (background) — scoped to this app only
    log_sink: List[str] = []
    agents = [
        CreditAgent(store, log_sink, target_app_id=APP_ID),
        FraudAgent(store, log_sink, target_app_id=APP_ID),
        ComplianceAgent(store, log_sink, target_app_id=APP_ID),
        DecisionAgent(store, log_sink, target_app_id=APP_ID),
        HumanReviewAgent(store, log_sink, target_app_id=APP_ID),
    ]
    agent_tasks = [asyncio.create_task(a.run()) for a in agents]

    pause()

    # ══════════════════════════════════════════════════════════════════════════
    # STEP 1 — User Applies
    # ══════════════════════════════════════════════════════════════════════════
    console.print(Rule("[bold white] STEP 1 [/bold white]  User Applies", style="bold blue"))
    console.print(
        f"  [dim]A new loan application arrives:[/dim]  "
        f"[bold]{display_name}[/bold]  ·  [cyan]${amount:,.0f}[/cyan]  ·  {purpose}  ·  {channel}"
    )

    await handle_submit_application(store, APP_ID, applicant_id, amount, purpose, channel)

    events = await store.load_stream(f"loan-{APP_ID}")
    console.print(make_ledger_panel(events, APP_ID))
    console.print(f"  [green]✓[/green]  [bold]ApplicationSubmitted[/bold] written to ledger.  "
                  f"Dashboard at [link=http://localhost:8000]http://localhost:8000[/link] will show it shortly.")

    pause()

    # ══════════════════════════════════════════════════════════════════════════
    # STEP 2 — Credit & Fraud Agents
    # ══════════════════════════════════════════════════════════════════════════
    console.print(Rule("[bold white] STEP 2 [/bold white]  Agents Kick In", style="bold blue"))
    console.print(
        "  [magenta]credit-agent-01[/magenta] and [yellow]fraud-agent-01[/yellow] "
        "detected the new application and are now processing it in parallel.\n"
        "  Watch the [cyan]Kafka Stream[/cyan] panel — events arrive there as agents emit them."
    )
    console.print()

    events = await wait_showing_live(
        store, APP_ID,
        {"CreditAnalysisCompleted", "FraudScreeningCompleted"},
    )
    console.print(f"  [green]✓[/green]  Credit analysis + fraud screening complete — {len(events)} events in ledger.")

    pause()

    # ══════════════════════════════════════════════════════════════════════════
    # STEP 3 — Compliance Agent
    # ══════════════════════════════════════════════════════════════════════════
    console.print(Rule("[bold white] STEP 3 [/bold white]  Compliance Validation", style="bold blue"))
    console.print(
        "  [cyan]compliance-agent-01[/cyan] is now running KYC and AML checks.\n"
        "  Each rule emits a separate [cyan]ComplianceRulePassed[/cyan] event."
    )
    console.print()

    # Wait for both KYC and AML rules
    events = await wait_showing_live(
        store, APP_ID,
        {"ComplianceRulePassed"},
    )
    while True:
        rule_ids = {e.payload.get("rule_id") for e in events if e.event_type == "ComplianceRulePassed"}
        if {"KYC-RULE-01", "AML-RULE-01"}.issubset(rule_ids):
            break
        await asyncio.sleep(0.4)
        events = await store.load_stream(f"loan-{APP_ID}")

    console.print(f"  [green]✓[/green]  KYC + AML both cleared.  Compliance status: [bold green]PASSED[/bold green].")

    pause()

    # ══════════════════════════════════════════════════════════════════════════
    # STEP 4 — Decision Generated
    # ══════════════════════════════════════════════════════════════════════════
    console.print(Rule("[bold white] STEP 4 [/bold white]  Decision Generated", style="bold blue"))
    console.print(
        "  The [white]decision-engine[/white] synthesizes credit, fraud, and compliance results\n"
        "  into a single [bold]DecisionGenerated[/bold] event.  Confidence threshold: 60%."
    )
    console.print()

    events = await wait_showing_live(store, APP_ID, {"DecisionGenerated"})

    decision_evt = next(e for e in events if e.event_type == "DecisionGenerated")
    rec = decision_evt.payload.get("recommendation", "?")
    conf = decision_evt.payload.get("confidence_score", 0)
    color = "green" if rec == "APPROVE" else "red"
    console.print(
        f"  [green]✓[/green]  Decision: [{color}][bold]{rec}[/bold][/{color}]  "
        f"|  Confidence: [bold]{conf:.0%}[/bold]"
    )

    pause()

    # ══════════════════════════════════════════════════════════════════════════
    # STEP 5 — Human Review & Final State
    # ══════════════════════════════════════════════════════════════════════════
    console.print(Rule("[bold white] STEP 5 [/bold white]  Human Review + Final Decision", style="bold blue"))
    console.print(
        "  A senior underwriter reviews the AI decision.\n"
        "  On approval, [bold green]ApplicationApproved[/bold green] is written as the terminal event."
    )
    console.print()

    events = await wait_showing_live(store, APP_ID, TERMINAL_EVENTS)

    final_evt = next(e for e in events if e.event_type in TERMINAL_EVENTS)
    is_approved = final_evt.event_type == "ApplicationApproved"
    outcome_color = "bold green" if is_approved else "bold red"
    outcome_label = "FINAL_APPROVED" if is_approved else "FINAL_DECLINED"

    review_evt = next((e for e in events if e.event_type == "HumanReviewCompleted"), None)
    reviewer = review_evt.payload.get("reviewer_id", "?") if review_evt else "?"
    override = review_evt.payload.get("override", False) if review_evt else False
    override_str = "override" if override else "confirmed AI decision"

    summary = Table(box=box.DOUBLE_EDGE, padding=(0, 2))
    summary.add_column("Field", style="dim", min_width=20)
    summary.add_column("Value", style="bold")
    rows = [
        ("Application ID",   APP_ID),
        ("Applicant",        display_name),
        ("Amount",           f"${amount:,.0f}"),
        ("Purpose",          purpose),
        ("Final State",      f"[{outcome_color}]{outcome_label}[/{outcome_color}]"),
        ("Reviewer",         f"{reviewer}  ({override_str})"),
    ]
    if is_approved:
        rows.append(("Interest Rate", f"{final_evt.payload.get('interest_rate',0):.2%}  (fixed 30yr)"))
    for k, v in rows:
        summary.add_row(k, v)
    console.print(summary)

    pause()

    # ══════════════════════════════════════════════════════════════════════════
    # STEP 6 — Full Event Replay
    # ══════════════════════════════════════════════════════════════════════════
    console.print(Rule("[bold white] STEP 6 [/bold white]  Full Replay — Audit Trail", style="bold blue"))
    console.print(
        "  Every event is immutable and cryptographically chained.\n"
        "  Load any application ID and replay the exact journey the system took."
    )
    console.print()

    replay = Table(
        title=f"[bold]Complete Audit Trail[/bold]  ·  {APP_ID}  ·  {len(events)} events",
        box=box.ROUNDED, show_lines=True,
    )
    replay.add_column("#",          style="dim",       width=3,  justify="right")
    replay.add_column("Event Type", min_width=30)
    replay.add_column("Details",    style="white")
    replay.add_column("Hash Chain", style="dim green", width=18)
    replay.add_column("Time",       style="dim",       width=12)

    for e in events:
        color = EVENT_COLORS.get(e.event_type, "white")
        h = e.metadata.get("_hash", "")
        prev = e.metadata.get("_prev_hash", "")
        replay.add_row(
            str(e.stream_position),
            f"[{color}]{e.event_type}[/{color}]",
            _event_detail(e.event_type, e.payload),
            (h[:14] + "...") if h else "n/a",
            e.recorded_at.strftime("%H:%M:%S.%f")[:-3],
        )

    console.print(replay)

    # Show final Kafka stats
    console.print()
    console.print(Panel(
        f"[bold]Kafka Messages Received:[/bold]  [cyan]{len(kafka_msgs)}[/cyan]\n"
        + "\n".join(
            f"  [{EVENT_COLORS.get(m.get('event_type',''), 'white')}]{m.get('event_type','?')}[/{EVENT_COLORS.get(m.get('event_type',''), 'white')}]"
            for m in kafka_msgs
        ),
        title="[bold]Kafka Stream Summary[/bold]  [dim]ledger-events[/dim]",
        border_style="dim cyan",
    ))

    pause()

    # ── Teardown ──────────────────────────────────────────────────────────────
    _kafka_stop.set()
    for a in agents:
        a.stop()
    for t in [kafka_task] + agent_tasks:
        t.cancel()
    await asyncio.gather(*([kafka_task] + agent_tasks), return_exceptions=True)

    console.print(Panel.fit(
        "[bold green]Demo Complete[/bold green]\n\n"
        "[dim]"
        "· Append-only — no event is ever modified\n"
        "· Cryptographically chained — tampering is detectable\n"
        "· Fully replayable — reconstruct any point in time\n"
        f"· Search for [bold]{APP_ID}[/bold] in the dashboard"
        "[/dim]",
        border_style="bold green", padding=(1, 4),
    ))
    console.print()
    await store.disconnect()


if __name__ == "__main__":
    asyncio.run(run())
