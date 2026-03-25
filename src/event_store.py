import json
from contextlib import asynccontextmanager
from typing import AsyncIterator, List, Optional, Any, Dict
import asyncpg
import hashlib
from uuid import UUID

from src.models.events import (
    BaseEvent, 
    StoredEvent, 
    StreamMetadata, 
    OptimisticConcurrencyError,
    BaseModel
)

class EventStore:
    def __init__(self, dsn: str, upcaster: Optional['UpcasterRegistry'] = None):
        self._dsn = dsn
        self._pool: Optional[asyncpg.Pool] = None
        self._upcaster = upcaster
        self._upcaster_registry = None # To be injected in Phase 4

    async def connect(self):
        if not self._pool:
            try:
                self._pool = await asyncpg.create_pool(
                    self._dsn, 
                    min_size=1, 
                    max_size=10,
                    command_timeout=60
                )
            except Exception as e:
                # Provide more context for debugging
                raise ConnectionError(f"Failed to connect to Event Store at {self._dsn.split('@')[-1]}: {e}")

    async def disconnect(self):
        if self._pool:
            await self._pool.close()
            self._pool = None

    @asynccontextmanager
    async def transaction(self):
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                yield conn

    async def append(
        self,
        stream_id: str,
        events: List[BaseEvent],
        expected_version: int,
        correlation_id: Optional[str] = None,
        causation_id: Optional[str] = None,
    ) -> int:
        """
        Atomically appends events to stream_id.
        Raises OptimisticConcurrencyError if stream version != expected_version.
        Writes to outbox in same transaction.
        Enforces cryptographic hash chaining for audit integrity.
        """
        async with self.transaction() as conn:
            # 1. Check/Update stream version
            row = await conn.fetchrow(
                "SELECT current_version FROM event_streams WHERE stream_id = $1 FOR UPDATE",
                stream_id
            )
            
            current_version = row["current_version"] if row else 0
            
            if expected_version != -1 and current_version != expected_version:
                raise OptimisticConcurrencyError(stream_id, expected_version, current_version)
            
            if expected_version == -1 and row:
                 if current_version > 0:
                     raise OptimisticConcurrencyError(stream_id, -1, current_version)

            new_version = current_version
            
            # 2. Insert Events & Outbox entries
            last_hash = await conn.fetchval(
                "SELECT metadata->>'_hash' FROM events WHERE stream_id = $1 ORDER BY stream_position DESC LIMIT 1",
                stream_id
            ) or "0" * 64

            for event in events:
                new_version += 1
                
                # Prepare metadata with correlation/causation and integrity hash
                meta = event.metadata.copy() if event.metadata else {}
                if correlation_id: meta["correlation_id"] = correlation_id
                if causation_id: meta["causation_id"] = causation_id
                
                # Payload handling
                payload_data = event.payload
                if isinstance(payload_data, BaseModel):
                    payload_data = payload_data.model_dump(mode="json")
                
                payload_json = json.dumps(payload_data, sort_keys=True)
                meta_json_pre_hash = json.dumps(meta, sort_keys=True)
                
                # Cryptographic Chain: SHA256(prev_hash + type + payload + metadata)
                hasher = hashlib.sha256()
                hasher.update(last_hash.encode())
                hasher.update(event.event_type.encode())
                hasher.update(payload_json.encode())
                hasher.update(meta_json_pre_hash.encode())
                current_hash = hasher.hexdigest()
                
                meta["_prev_hash"] = last_hash
                meta["_hash"] = current_hash
                last_hash = current_hash

                # Insert Event
                event_id = await conn.fetchval(
                    """
                    INSERT INTO events (stream_id, stream_position, event_type, event_version, payload, metadata)
                    VALUES ($1, $2, $3, $4, $5, $6)
                    RETURNING event_id
                    """,
                    stream_id, new_version, event.event_type, event.event_version,
                    payload_json, json.dumps(meta)
                )

                # Insert into Outbox
                await conn.execute(
                    """
                    INSERT INTO outbox (event_id, event_type, payload, metadata)
                    VALUES ($1, $2, $3, $4)
                    """,
                    event_id, event.event_type, payload_json, json.dumps(meta)
                )

            # 3. Update or Insert Stream Metadata
            if row:
                await conn.execute(
                    "UPDATE event_streams SET current_version = $1, updated_at = NOW() WHERE stream_id = $2",
                    new_version, stream_id
                )
            else:
                aggregate_type = stream_id.split("-")[0]
                await conn.execute(
                    "INSERT INTO event_streams (stream_id, aggregate_type, current_version) VALUES ($1, $2, $3)",
                    stream_id, aggregate_type, new_version
                )

            return new_version

    async def load_agent_session(
        self,
        agent_id: str,
        session_id: str,
        from_position: int = 0
    ) -> List[StoredEvent]:
        """Gas Town: Load the persistent memory for an agent session."""
        return await self.load_stream(f"agent-{agent_id}-{session_id}", from_position=from_position)

    async def load_stream(
        self,
        stream_id: str,
        from_position: int = 0,
        to_position: Optional[int] = None,
    ) -> List[StoredEvent]:
        query = "SELECT * FROM events WHERE stream_id = $1 AND stream_position > $2"
        params = [stream_id, from_position]
        
        if to_position:
            query += " AND stream_position <= $3"
            params.append(to_position)
            
        query += " ORDER BY stream_position ASC"
        
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(query, *params)
            
        events = []
        for row in rows:
            event = self._row_to_event(row)
            if self._upcaster:
                event = self._upcaster.upcast(event)
            events.append(event)
            
        return events

    async def verify_stream_integrity(self, stream_id: str) -> bool:
        """
        Re-calculates hashes for the entire stream and compares with stored hashes.
        Returns True if the chain is valid.
        """
        # We must load RAW events (no upcasting) to verify integrity
        query = "SELECT * FROM events WHERE stream_id = $1 ORDER BY stream_position ASC"
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(query, stream_id)
            
        events = [self._row_to_event(row) for row in rows]
        
        if not events:
            return True
            
        last_hash = "0" * 64
        for event in events:
            # Re-calculate hash
            payload_json = json.dumps(event.payload, sort_keys=True)
            # Filter out _hash and _prev_hash from metadata for re-calculation
            meta_to_hash = {k: v for k, v in event.metadata.items() if k not in ["_hash", "_prev_hash"]}
            meta_json = json.dumps(meta_to_hash, sort_keys=True)
            
            hasher = hashlib.sha256()
            hasher.update(last_hash.encode())
            hasher.update(event.event_type.encode())
            hasher.update(payload_json.encode())
            hasher.update(meta_json.encode())
            current_hash = hasher.hexdigest()
            
            if event.metadata.get("_hash") != current_hash:
                return False
            if event.metadata.get("_prev_hash") != last_hash:
                return False
                
            last_hash = current_hash
            
        return True

    async def load_all(
        self,
        from_global_position: int = 0,
        event_types: Optional[List[str]] = None,
        batch_size: int = 500,
    ) -> AsyncIterator[StoredEvent]:
        query = "SELECT * FROM events WHERE global_position > $1"
        params = [from_global_position]
        
        if event_types:
            query += " AND event_type = ANY($2)"
            params.append(event_types)
            
        query += f" ORDER BY global_position ASC LIMIT {batch_size}"
        
        current_pos = from_global_position
        while True:
            async with self._pool.acquire() as conn:
                rows = await conn.fetch(query, *params)
            
            if not rows:
                break
                
            for row in rows:
                event = self._row_to_event(row)
                if self._upcaster:
                    event = self._upcaster.upcast(event)
                yield event
                current_pos = event.global_position
            
            if len(rows) < batch_size:
                break
                
            params[0] = current_pos

    async def stream_version(self, stream_id: str) -> int:
        async with self._pool.acquire() as conn:
            val = await conn.fetchval(
                "SELECT current_version FROM event_streams WHERE stream_id = $1",
                stream_id
            )
            return val if val is not None else 0

    async def archive_stream(self, stream_id: str) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute(
                "UPDATE event_streams SET archived_at = NOW() WHERE stream_id = $1",
                stream_id
            )

    async def get_stream_metadata(self, stream_id: str) -> Optional[StreamMetadata]:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM event_streams WHERE stream_id = $1",
                stream_id
            )
            if not row:
                return None
            return StreamMetadata(
                stream_id=row["stream_id"],
                aggregate_type=row["aggregate_type"],
                current_version=row["current_version"],
                created_at=row["created_at"],
                archived_at=row["archived_at"],
                metadata=json.loads(row["metadata"] or "{}")
            )

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
