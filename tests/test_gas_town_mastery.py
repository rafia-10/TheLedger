import asyncio
import os
import time
from dotenv import load_dotenv
from src.event_store import EventStore
from src.commands.handlers import (
    handle_start_agent_session, 
    handle_credit_analysis_completed,
    handle_submit_application
)
from src.gas_town import reconstruct_agent_context, find_unfinished_sessions
from src.upcasting.registry import registry as upcaster_registry
from rich.console import Console

load_dotenv()

async def test_gas_town_mastery():
    console = Console()
    dsn = os.getenv("DATABASE_URL", "postgresql:///eventledger")
    store = EventStore(dsn, upcaster=upcaster_registry)
    await store.connect()

    agent_id = "master-agent"
    
    # ─── 1. Test Unfinished Work ───
    session_1 = f"unfinished-{int(time.time())}"
    console.print(f"🎬 Starting session {session_1} but doing no work...")
    await handle_start_agent_session(store, agent_id, session_1, "test", 0, 100, "v1")
    
    ctx_1 = await reconstruct_agent_context(store, agent_id, session_1)
    console.print(f"🔍 Session 1 Unfinished: [bold cyan]{ctx_1.unfinished_work}[/]")
    assert ctx_1.unfinished_work is True
    assert "Complete initial analysis" in ctx_1.pending_work

    # ─── 2. Test Token Budget & Reconciliation ───
    session_2 = f"budget-violation-{int(time.time())}"
    budget = 100
    console.print(f"🎬 Starting session {session_2} with budget {budget}...")
    
    # REQUIRED: Must submit app first to satisfy state machine Rule 2
    app_id_base = f"budget-app-{int(time.time())}"
    await handle_start_agent_session(store, agent_id, session_2, "test", 0, 50, "v1", budget=budget)
    
    # Exceed budget: 3 analyses with duration 500ms each = 3 * 50 = 150 tokens + 50 initial = 200
    for i in range(3):
        app_id = f"{app_id_base}-{i}"
        await handle_submit_application(store, app_id, "u1", 1000, "Test", "Web")
        await handle_credit_analysis_completed(
            store, app_id, agent_id, session_2, "v1", 0.9, "Low", 5000, 500, {"data": "..."}
        )
    
    ctx_2 = await reconstruct_agent_context(store, agent_id, session_2)
    console.print(f"🔍 Session 2 Tokens Used: {ctx_2.tokens_used}/{ctx_2.token_budget}")
    console.print(f"🔍 Session 2 Status: [bold red]{ctx_2.health_status}[/]")
    
    assert ctx_2.tokens_used > ctx_2.token_budget
    assert ctx_2.needs_reconciliation is True
    assert ctx_2.health_status == "NEEDS_RECONCILIATION"

    # ─── 3. Test Summarization & Preservation ───
    console.print(f"🔍 Session 2 Summarized: {ctx_2.summarized_at_position is not None}")
    console.print(f"🔍 Session 2 Preserved Events: {ctx_2.preserved_events_count}")
    console.print(f"🔍 Session 2 Summary Content: [italic]{ctx_2.context_text_summary}[/]")
    
    assert ctx_2.summarized_at_position is not None
    assert ctx_2.preserved_events_count >= 3 # Last 3 are preserved
    assert "SUMMARY AT POS" in ctx_2.context_text_summary

    # ─── 4. Test find_unfinished_sessions ───
    unfinished = await find_unfinished_sessions(store, agent_id)
    console.print(f"📋 Found {len(unfinished)} sessions requiring attention for agent {agent_id}")
    
    # Assert we found both sessions
    session_ids = [c.session_id for c in unfinished]
    assert session_1 in session_ids
    assert session_2 in session_ids

    console.print("[bold green]✅ Gas Town Mastery Verified: Correctly detects budget violations and unfinished work.[/]")
    await store.disconnect()

if __name__ == "__main__":
    asyncio.run(test_gas_town_mastery())
