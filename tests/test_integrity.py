import pytest
import hashlib
import json
from src.event_store import EventStore
from src.models.events import BaseEvent

@pytest.mark.asyncio
async def test_cryptographic_chain_integrity(event_store: EventStore):
    stream_id = "test-integrity-stream"
    
    # 1. Append valid events
    events = [
        BaseEvent(event_type="EventA", payload={"data": 1}),
        BaseEvent(event_type="EventB", payload={"data": 2}),
    ]
    await event_store.append(stream_id, events, expected_version=-1)
    
    # 2. Verify integrity
    assert await event_store.verify_stream_integrity(stream_id) is True
    
    # 3. Manually tamper with the database to break the chain
    async with event_store.transaction() as conn:
        # Change the payload of the first event
        await conn.execute(
            "UPDATE events SET payload = '{\"data\": 999}' WHERE stream_id = $1 AND stream_position = 1",
            stream_id
        )
        
    # 4. Verify integrity should now FAIL
    assert await event_store.verify_stream_integrity(stream_id) is False

# --- Direct execution support ---
if __name__ == "__main__":
    import sys
    sys.exit(pytest.main([__file__, "-v", "--tb=short"]))

@pytest.mark.asyncio
async def test_hash_chain_continuity(event_store: EventStore):
    stream_id = "test-chain-stream"
    
    # Append events in two separate calls
    await event_store.append(stream_id, [BaseEvent(event_type="E1", payload={})], expected_version=-1)
    await event_store.append(stream_id, [BaseEvent(event_type="E2", payload={})], expected_version=1)
    
    stored = await event_store.load_stream(stream_id)
    assert len(stored) == 2
    
    h1 = stored[0].metadata["_hash"]
    prev2 = stored[1].metadata["_prev_hash"]
    
    assert prev2 == h1
    assert h1 != "0" * 64
