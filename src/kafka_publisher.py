"""
Kafka Outbox Publisher — Reliable messaging via transactional outbox pattern.

Reads unpublished events from the outbox table and publishes to Kafka.
Falls back to an in-memory event bus when no Kafka broker is available.
"""
import asyncio
import json
import logging
from typing import Optional, List, Dict, Any, Callable
from datetime import datetime

logger = logging.getLogger(__name__)


class InMemoryEventBus:
    """Fallback event bus when Kafka is unavailable. Stores events in memory."""

    def __init__(self):
        self._subscribers: List[Callable] = []
        self._events: List[Dict[str, Any]] = []

    def subscribe(self, callback: Callable):
        self._subscribers.append(callback)

    async def publish(self, event: Dict[str, Any]):
        self._events.append(event)
        for subscriber in self._subscribers:
            try:
                await subscriber(event)
            except Exception as e:
                logger.error(f"Subscriber error: {e}")

    @property
    def events(self) -> List[Dict[str, Any]]:
        return self._events.copy()


class OutboxPublisher:
    """
    Reads the outbox table and publishes events to Kafka or in-memory bus.

    Usage:
        publisher = OutboxPublisher(store, kafka_bootstrap="localhost:9092")
        await publisher.start()  # runs forever, polling outbox
    """

    def __init__(
        self,
        store,
        kafka_bootstrap: Optional[str] = None,
        topic: str = "ledger-events",
        poll_interval_ms: int = 500,
    ):
        self._store = store
        self._kafka_bootstrap = kafka_bootstrap
        self._topic = topic
        self._poll_interval = poll_interval_ms / 1000
        self._running = False
        self._producer = None
        self._bus = InMemoryEventBus()  # Always available as fallback

    @property
    def event_bus(self) -> InMemoryEventBus:
        """Access the in-memory event bus for WebSocket streaming."""
        return self._bus

    async def _init_kafka(self):
        """Try to connect to Kafka. If unavailable, fall back to in-memory bus."""
        if not self._kafka_bootstrap:
            logger.info("No Kafka bootstrap server configured. Using in-memory event bus.")
            return

        try:
            from aiokafka import AIOKafkaProducer
            self._producer = AIOKafkaProducer(
                bootstrap_servers=self._kafka_bootstrap,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
            await self._producer.start()
            logger.info(f"Connected to Kafka at {self._kafka_bootstrap}")
        except ImportError:
            logger.warning("aiokafka not installed. Using in-memory event bus. pip install aiokafka to enable Kafka.")
            self._producer = None
        except Exception as e:
            logger.warning(f"Kafka connection failed: {e}. Using in-memory event bus.")
            self._producer = None

    async def start(self):
        """Start the outbox polling loop."""
        await self._init_kafka()
        self._running = True
        logger.info("OutboxPublisher started.")

        while self._running:
            try:
                count = await self._publish_batch()
                if count == 0:
                    await asyncio.sleep(self._poll_interval)
            except Exception as e:
                logger.error(f"OutboxPublisher error: {e}")
                await asyncio.sleep(1)

    def stop(self):
        self._running = False

    async def shutdown(self):
        self.stop()
        if self._producer:
            await self._producer.stop()

    async def _publish_batch(self, batch_size: int = 100) -> int:
        """Read unpublished outbox entries and publish them."""
        async with self._store.transaction() as conn:
            rows = await conn.fetch(
                """
                SELECT id, event_id, event_type, payload, metadata, created_at
                FROM outbox
                WHERE published_at IS NULL
                ORDER BY created_at ASC
                LIMIT $1
                """,
                batch_size,
            )

            if not rows:
                return 0

            for row in rows:
                event_msg = {
                    "event_id": str(row["event_id"]),
                    "event_type": row["event_type"],
                    "payload": json.loads(row["payload"]) if isinstance(row["payload"], str) else row["payload"],
                    "metadata": json.loads(row["metadata"]) if isinstance(row["metadata"], str) else row["metadata"],
                    "published_at": datetime.now().isoformat(),
                }

                # Publish to Kafka if available
                if self._producer:
                    await self._producer.send_and_wait(self._topic, event_msg)

                # Always publish to in-memory bus (for WebSocket streaming)
                await self._bus.publish(event_msg)

                # Mark as published
                await conn.execute(
                    "UPDATE outbox SET published_at = NOW(), attempts = attempts + 1 WHERE id = $1",
                    row["id"],
                )

            return len(rows)
