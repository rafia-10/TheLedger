import os
import pytest
import asyncio
from dotenv import load_dotenv
from src.event_store import EventStore

load_dotenv()

# Cloud-only: Supabase Transaction Pooler
CLOUD_DSN = "postgresql://postgres.dxscaeckamkplshxqkae:supabase1224@aws-0-eu-central-1.pooler.supabase.com:6543/postgres?sslmode=require"
DB_URL = os.getenv("DATABASE_URL", CLOUD_DSN)

@pytest.fixture
async def event_store():
    store = EventStore(DB_URL)
    await store.connect()
    yield store
    await store.disconnect()

@pytest.fixture(autouse=True)
async def clean_db(event_store: EventStore):
    async with event_store.transaction() as conn:
        await conn.execute("TRUNCATE events CASCADE")
        await conn.execute("TRUNCATE event_streams CASCADE")
        await conn.execute("TRUNCATE outbox CASCADE")
        await conn.execute("TRUNCATE application_summary CASCADE")
        await conn.execute("TRUNCATE agent_performance CASCADE")
        await conn.execute("TRUNCATE compliance_audit CASCADE")
        await conn.execute("TRUNCATE projection_checkpoints CASCADE")
    yield
