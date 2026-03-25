import asyncio
import os
import json
from datetime import datetime
from dotenv import load_dotenv
from src.event_store import EventStore
from src.upcasting.registry import registry as upcaster_registry
from rich.console import Console

load_dotenv()

async def test_immutability():
    console = Console()
    dsn = os.getenv("DATABASE_URL", "postgresql:///eventledger")
    store = EventStore(dsn, upcaster=upcaster_registry)
    await store.connect()

    stream_id = f"immutability-test-{int(time.time())}"
    
    # 1. Insert RAW v1 event (Legacy 2025 date)
    legacy_date = datetime(2025, 12, 15, 10, 0, 0)
    payload_v1 = {"application_id": "imm-001", "confidence_score": 95} # 0-100 scale in v1
    
    async with store.transaction() as conn:
        await conn.execute(
            """
            INSERT INTO events (stream_id, stream_position, event_type, event_version, payload, metadata, recorded_at)
            VALUES ($1, 1, 'CreditAnalysisCompleted', 1, $2, $3, $4)
            """,
            stream_id, json.dumps(payload_v1), "{}", legacy_date
        )
        # Ensure stream exists
        await conn.execute(
            "INSERT INTO event_streams (stream_id, current_version, aggregate_type) VALUES ($1, 1, 'LoanApplication')", 
            stream_id
        )

    console.print(f"📥 Inserted legacy v1 event into stream {stream_id} (Date: 2025)")

    # 2. Load through EventStore (triggers upcasting)
    events = await store.load_stream(stream_id)
    upcast_event = events[0]
    
    console.print(f"🧠 Memory Event Version: [bold green]{upcast_event.event_version}[/]")
    console.print(f"🧠 Memory Payload: {upcast_event.payload}")

    # 3. Query DB Raw
    async with store.transaction() as conn:
        raw = await conn.fetchrow("SELECT event_version, payload FROM events WHERE stream_id = $1", stream_id)
        
    console.print(f"💾 Raw DB Version: [bold yellow]{raw['event_version']}[/]")
    console.print(f"💾 Raw DB Payload: {raw['payload']}")

    # 4. Assertions
    assert upcast_event.event_version == 2, "Memory event should be v2"
    assert upcast_event.payload["model_version"] == "model-v1-legacy", "Should infer legacy model"
    assert upcast_event.payload["confidence_score"] == 0.95, "Should normalize confidence score"
    
    assert raw["event_version"] == 1, "DB event MUST remain v1"
    assert json.loads(raw["payload"])["confidence_score"] == 95, "DB payload MUST remain unchanged"

    console.print("[bold green]✅ Mastery Proof: Database is IMMUTABLE. Upcasting occurs strictly in-memory.[/]")
    await store.disconnect()

import time
if __name__ == "__main__":
    asyncio.run(test_immutability())
