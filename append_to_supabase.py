import asyncio
import os
from dotenv import load_dotenv
from src.event_store import EventStore
from src.models.events import BaseEvent

load_dotenv()

async def append_sample_events():
    # Priority to DATABASE_URL, then the provided Supabase string
    CLOUD_DSN = "postgresql://postgres:supabase1224@db.dxscaeckamkplshxqkae.supabase.co:5432/postgres?sslmode=require"
    DSN = os.getenv("DATABASE_URL", CLOUD_DSN)
    
    store = EventStore(DSN)
    await store.connect()
    
    stream_id = "demo-cloud-stream-001"
    events = [
        BaseEvent(event_type="ApplicationSubmitted", payload={"applicant": "Cloud Tester", "amount": 50000}),
        BaseEvent(event_type="CreditAnalysisStarted", payload={"agent_id": "cloud-agent-01"}),
        BaseEvent(event_type="CreditAnalysisCompleted", payload={"score": 750, "recommendation": "APPROVE"})
    ]
    
    print(f"🚀 Appending {len(events)} events to stream {stream_id} on {DSN.split('@')[1]}...")
    
    try:
        new_version = await store.append(stream_id, events, expected_version=0)
        print(f"✅ SUCCESS! Stream is now at version {new_version}")
    except Exception as e:
        print(f"❌ FAILURE: {e}")
    finally:
        await store.disconnect()

if __name__ == "__main__":
    asyncio.run(append_sample_events())
