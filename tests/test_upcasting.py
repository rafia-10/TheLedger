import pytest
import json
from src.event_store import EventStore
from src.upcasting.registry import UpcasterRegistry
from src.models.events import StoredEvent

@pytest.mark.asyncio
async def test_upcasting_migration(event_store: EventStore):
    registry = UpcasterRegistry()
    
    # 1. Register an upcaster: v1 -> v2 (adds 'migrated' field)
    @registry.register("TestEvent", 1)
    def v1_to_v2(payload):
        payload["migrated"] = True
        return payload
    
    # Inject registry into store
    event_store._upcaster = registry
    
    stream_id = "test-upcast-stream"
    
    # 2. Manually insert a v1 event
    async with event_store.transaction() as conn:
        await conn.execute(
            """
            INSERT INTO events (stream_id, stream_position, event_type, event_version, payload, metadata)
            VALUES ($1, 1, 'TestEvent', 1, '{"old_data": 123}', '{}')
            """,
            stream_id
        )
        # We also need the stream entry for load_stream to work if we check metadata
        await conn.execute(
            "INSERT INTO event_streams (stream_id, aggregate_type, current_version) VALUES ($1, 'test', 1)",
            stream_id
        )
        
    # 3. Load stream and verify upcasting
    events = await event_store.load_stream(stream_id)
    assert len(events) == 1
    
    event = events[0]
    assert event.event_version == 2
    assert event.payload["migrated"] is True
    assert event.payload["old_data"] == 123

# --- Direct execution support ---
if __name__ == "__main__":
    import sys
    sys.exit(pytest.main([__file__, "-v", "--tb=short"]))
