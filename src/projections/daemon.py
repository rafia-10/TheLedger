import asyncio
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime
from abc import ABC, abstractmethod
from src.event_store import EventStore
from src.models.events import StoredEvent

logger = logging.getLogger(__name__)

class Projection(ABC):
    @property
    @abstractmethod
    def name(self) -> str:
        pass

    @abstractmethod
    async def handle_event(self, conn, event: StoredEvent) -> None:
        """Process a single event within a transaction."""
        pass

class ProjectionDaemon:
    def __init__(self, store: EventStore, projections: List[Projection]):
        self._store = store
        self._projections = {p.name: p for p in projections}
        self._running = False
        self._batch_size = 500

    async def run_forever(self, poll_interval_ms: int = 100) -> None:
        self._running = True
        logger.info(f"Starting Projection Daemon with {len(self._projections)} projections.")
        
        while self._running:
            try:
                processed_count = await self._process_batch()
                if processed_count == 0:
                    await asyncio.sleep(poll_interval_ms / 1000)
            except Exception as e:
                logger.error(f"Error in Projection Daemon loop: {e}", exc_info=True)
                await asyncio.sleep(1) # Backoff on error

    def stop(self):
        self._running = False

    async def _process_batch(self) -> int:
        """
        1. Find the lowest checkpoint across all projections.
        2. Load events from that position.
        3. For each event, route to projections that haven't processed it yet.
        4. Update checkpoints.
        """
        async with self._store.transaction() as conn:
            # Get checkpoints
            checkpoints = await self._get_checkpoints(conn)
            if not checkpoints:
                # Initialize checkpoints if missing
                for name in self._projections:
                    if name not in checkpoints:
                        await conn.execute(
                            "INSERT INTO projection_checkpoints (projection_name, last_position) VALUES ($1, 0) ON CONFLICT DO NOTHING",
                            name
                        )
                checkpoints = await self._get_checkpoints(conn)
            
            min_pos = min(checkpoints.values()) if checkpoints else 0
            
            # Load batch
            rows = await conn.fetch(
                "SELECT * FROM events WHERE global_position > $1 ORDER BY global_position ASC LIMIT $2",
                min_pos, self._batch_size
            )
            
            if not rows:
                return 0

            events = [self._row_to_event(row) for row in rows]
            
            for event in events:
                for name, projection in self._projections.items():
                    if event.global_position > checkpoints[name]:
                        try:
                            await projection.handle_event(conn, event)
                            checkpoints[name] = event.global_position
                        except Exception as e:
                            logger.error(f"Projection {name} failed on event {event.event_id}: {e}")
                            # In production, we might skip or retry based on policy
                            # For now, we log and potentially block if we don't update checkpoint
                            # but the requirement says 'skip offending event' for fault tolerance
                            checkpoints[name] = event.global_position 

            # Update checkpoints in DB
            for name, pos in checkpoints.items():
                await conn.execute(
                    "UPDATE projection_checkpoints SET last_position = $1, updated_at = NOW() WHERE projection_name = $2",
                    pos, name
                )
                
            return len(events)

    async def _get_checkpoints(self, conn) -> Dict[str, int]:
        rows = await conn.fetch("SELECT projection_name, last_position FROM projection_checkpoints")
        return {row["projection_name"]: row["last_position"] for row in rows}

    async def get_lag(self) -> Dict[str, int]:
        """Expose lag metric: global_position - last_processed_position"""
        async with self._store.transaction() as conn:
            max_pos = await conn.fetchval("SELECT COALESCE(MAX(global_position), 0) FROM events")
            checkpoints = await self._get_checkpoints(conn)
            return {name: max_pos - pos for name, pos in checkpoints.items()}

    def _row_to_event(self, row) -> StoredEvent:
        import json
        return StoredEvent(
            event_id=row["event_id"],
            stream_id=row["stream_id"],
            stream_position=row["stream_position"],
            global_position=row["global_position"],
            event_type=row["event_type"],
            event_version=row["event_version"],
            payload=json.loads(row["payload"]),
            metadata=json.loads(row["metadata"]),
            recorded_at=row["recorded_at"]
        )
