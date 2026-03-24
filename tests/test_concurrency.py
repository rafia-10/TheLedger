import pytest
import asyncio
from datetime import datetime
from uuid import uuid4
from src.event_store import EventStore
from src.models.events import BaseEvent, CreditAnalysisCompleted, OptimisticConcurrencyError

@pytest.mark.asyncio
async def test_optimistic_concurrency_double_decision(event_store: EventStore):
    """
    Two AI agents simultaneously attempt to append a CreditAnalysisCompleted event 
    to the same loan application stream. Both read the stream at version 3 
    and pass expected_version=3. One must succeed, one must fail.
    """
    stream_id = "loan-app-123"
    
    # 1. Setup: Stream is at version 3
    # Initial events to reach version 3
    initial_events = [
        BaseEvent(event_type="ApplicationSubmitted", payload={"id": 1}),
        BaseEvent(event_type="CreditAnalysisRequested", payload={"id": 1}),
        BaseEvent(event_type="AgentContextLoaded", payload={"id": 1}),
    ]
    await event_store.append(stream_id, initial_events, expected_version=-1)
    
    version = await event_store.stream_version(stream_id)
    assert version == 3
    
    # 2. Simulate two agents trying to append at once
    # They both think the current version is 3
    agent_a_event = BaseEvent(
        event_type="CreditAnalysisCompleted", 
        payload={"agent": "A", "score": 0.8}
    )
    agent_b_event = BaseEvent(
        event_type="CreditAnalysisCompleted", 
        payload={"agent": "B", "score": 0.9}
    )
    
    results = []
    
    async def try_append(event):
        try:
            new_v = await event_store.append(stream_id, [event], expected_version=3)
            results.append(("SUCCESS", new_v))
        except OptimisticConcurrencyError as e:
            results.append(("FAILURE", e))
            
    # 3. Run concurrently
    await asyncio.gather(
        try_append(agent_a_event),
        try_append(agent_b_event)
    )
    
    # 4. Assertions
    # (a) total events appended to the stream = 4 (not 5)
    final_version = await event_store.stream_version(stream_id)
    assert final_version == 4
    
    # (b) exactly one success, one failure
    successes = [r for r in results if r[0] == "SUCCESS"]
    failures = [r for r in results if r[0] == "FAILURE"]
    
    assert len(successes) == 1
    assert len(failures) == 1
    
    # (c) the winning task's event has stream_position=4
    assert successes[0][1] == 4
    
    # (d) the losing task's OptimisticConcurrencyError is raised
    assert isinstance(failures[0][1], OptimisticConcurrencyError)
    assert failures[0][1].expected == 3
    assert failures[0][1].actual == 4

# --- Direct execution support ---
if __name__ == "__main__":
    import sys
    sys.exit(pytest.main([__file__, "-v", "--tb=short"]))
