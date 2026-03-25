import asyncio
import time
import os
import random
from datetime import datetime
from dotenv import load_dotenv
from src.event_store import EventStore
from src.commands.handlers import handle_submit_application
from src.projections.daemon import ProjectionDaemon
from src.projections.compliance_audit import ComplianceAuditProjection
from src.upcasting.registry import registry as upcaster_registry
from rich.console import Console

load_dotenv()

async def run_load_test():
    console = Console()
    dsn = os.getenv("DATABASE_URL", "postgresql:///eventledger")
    store = EventStore(dsn, upcaster=upcaster_registry)
    await store.connect()

    # 1. Setup Daemon
    projection = ComplianceAuditProjection()
    daemon = ProjectionDaemon(store, [projection])
    
    # 2. Stress Test: 50 Concurrent Handlers
    concurrency = 50
    console.print(f"🚀 Launching {concurrency} concurrent application submissions...")
    
    start_time = time.time()
    tasks = []
    for i in range(concurrency):
        app_id = f"slo-test-{i}-{int(time.time())}"
        tasks.append(handle_submit_application(store, app_id, f"user-{i}", 1000 + i, "Stress Test", "LoadBot"))
    
    await asyncio.gather(*tasks)
    end_time = time.time()
    
    console.print(f"✅ Appended {concurrency} events in {end_time - start_time:.2f}s")
    
    # 3. Measure Projection Lag
    console.print("⏱️ Measuring projection catch-up time...")
    daemon_start = time.time()
    
    # Process all events
    processed = 0
    while True:
        batch_size = await daemon.run_once()
        processed += batch_size
        if batch_size == 0:
            break
            
    daemon_end = time.time()
    total_lag = daemon_end - end_time
    # Goal: Total processing time for 50 events should be low
    avg_lag_per_event = (total_lag / concurrency) * 1000 # ms
    
    console.print(f"📊 Total Lag: {total_lag:.2f}s (Avg {avg_lag_per_event:.1f}ms per event)")
    
    # Assertions
    assert avg_lag_per_event < 100, f"SLO Violation: Average lag {avg_lag_per_event:.1f}ms exceeds 100ms threshold."
    console.print("[bold green]✅ Performance SLO Passed: Lag is within acceptable limits.[/]")

    # 4. Non-blocking Rebuild Test
    console.print("🔄 Testing non-blocking rebuild...")
    # Simulate a concurrent read during rebuild
    async def concurrent_read():
        await asyncio.sleep(0.01) # Start right after rebuild begins
        async with store.transaction() as conn:
            # This should not be blocked by the rebuild transaction if isolation is correct
            val = await conn.fetchval("SELECT count(*) FROM compliance_audit")
            return val

    rebuild_task = asyncio.create_task(projection.rebuild_from_scratch(store))
    read_task = asyncio.create_task(concurrent_read())
    
    res = await asyncio.gather(rebuild_task, read_task)
    console.print(f"✅ Rebuild complete. Concurrent read saw {res[1]} records.")
    
    await store.disconnect()

if __name__ == "__main__":
    asyncio.run(run_load_test())
