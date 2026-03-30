"""
Base class for all autonomous loan-processing agents.
Each agent polls the event store for trigger events, checks if it
has already handled the application, and processes new work with retry.
"""
import asyncio
from datetime import datetime
from typing import Callable, List
from src.models.events import OptimisticConcurrencyError, DomainError


class BaseAgent:
    def __init__(
        self,
        store,
        agent_id: str,
        model_version: str,
        poll_interval: float = 0.7,
        color: str = "white",
        log_sink: List[str] = None,
        target_app_id: str = None,
    ):
        self.store = store
        self.agent_id = agent_id
        self.model_version = model_version
        self.poll_interval = poll_interval
        self.color = color
        self._log_sink = log_sink if log_sink is not None else []
        self._running = False
        self._target = target_app_id  # if set, only process this one app

    def _log(self, msg: str):
        ts = datetime.now().strftime("%H:%M:%S")
        line = f"[dim]{ts}[/dim]  [{self.color}]{self.agent_id:<24}[/{self.color}]  {msg}"
        self._log_sink.append(line)

    async def run(self):
        self._running = True
        while self._running:
            try:
                await self._poll()
            except Exception:
                pass
            await asyncio.sleep(self.poll_interval)

    def stop(self):
        self._running = False

    async def _poll(self):
        raise NotImplementedError

    # ── Shared DB helpers ──────────────────────────────────────────────────────

    async def _apps_with_event(self, event_type: str) -> list:
        """Return app_ids that have a given event type (scoped to target if set)."""
        if self._target:
            # fast path: just check if THIS app has the trigger event
            exists = await self._has_event(f"loan-{self._target}", event_type)
            return [self._target] if exists else []

        async with self.store.transaction() as conn:
            rows = await conn.fetch(
                "SELECT DISTINCT payload->>'application_id' AS app_id "
                "FROM events "
                "WHERE event_type = $1 AND payload->>'application_id' IS NOT NULL",
                event_type,
            )
            return [r["app_id"] for r in rows if r["app_id"]]

    async def _has_event(self, stream_id: str, event_type: str) -> bool:
        """Check whether a stream contains at least one event of this type."""
        async with self.store.transaction() as conn:
            row = await conn.fetchrow(
                "SELECT 1 FROM events WHERE stream_id = $1 AND event_type = $2 LIMIT 1",
                stream_id,
                event_type,
            )
            return row is not None

    async def _retry(self, coro_fn: Callable, *args, attempts: int = 4, **kwargs):
        """
        Call coro_fn(*args, **kwargs), retrying with backoff on
        OptimisticConcurrencyError.  Swallows DomainErrors (already processed).
        """
        for i in range(attempts):
            try:
                return await coro_fn(*args, **kwargs)
            except OptimisticConcurrencyError:
                if i < attempts - 1:
                    await asyncio.sleep(0.15 * (i + 1))
            except DomainError:
                return None
        return None
