import asyncio
import os
from dotenv import load_dotenv
from rich.console import Console
from rich.table import Table
from src.event_store import EventStore
from src.upcasting.registry import registry as upcaster_registry

load_dotenv()

async def main():
    dsn = os.getenv("DATABASE_URL", "postgresql:///eventledger")
    store = EventStore(dsn, upcaster=upcaster_registry)
    await store.connect()
    console = Console()

    async with store.transaction() as conn:
        rows = await conn.fetch(
            "SELECT global_position, stream_id, event_type, recorded_at FROM events ORDER BY global_position DESC LIMIT 20"
        )

    if not rows:
        console.print("[yellow]The ledger is currently empty.[/]")
        return

    table = Table(title="Recent Events in Ledger")
    table.add_column("Global Pos", justify="right", style="cyan")
    table.add_column("Stream ID", style="magenta")
    table.add_column("Event Type", style="green")
    table.add_column("Recorded At", style="dim")

    for r in rows:
        table.add_row(
            str(r["global_position"]),
            r["stream_id"],
            r["event_type"],
            r["recorded_at"].strftime("%Y-%m-%d %H:%M:%S")
        )

    console.print(table)
    await store.disconnect()

if __name__ == "__main__":
    asyncio.run(main())
