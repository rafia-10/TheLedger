"""
Cryptographic Audit Chain — SHA-256 hash linking for tamper detection.

Each event's hash is computed from:
    SHA256(prev_hash + event_type + payload_json + metadata_json)

This forms an immutable chain: if any event is altered, all subsequent hashes break.
"""
import hashlib
import json
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime
from src.event_store import EventStore
from src.models.events import StoredEvent


class AuditChain:
    """Verifies and manages the cryptographic integrity chain on event streams."""

    @staticmethod
    def hash_event(
        prev_hash: str,
        event_type: str,
        payload: Dict[str, Any],
        metadata: Dict[str, Any],
    ) -> str:
        """Compute SHA-256 hash for a single event in the chain."""
        payload_json = json.dumps(payload, sort_keys=True)
        # Strip internal hash fields before hashing metadata
        meta_clean = {k: v for k, v in metadata.items() if k not in ("_hash", "_prev_hash")}
        meta_json = json.dumps(meta_clean, sort_keys=True)

        hasher = hashlib.sha256()
        hasher.update(prev_hash.encode())
        hasher.update(event_type.encode())
        hasher.update(payload_json.encode())
        hasher.update(meta_json.encode())
        return hasher.hexdigest()

    @classmethod
    async def verify_chain(cls, store: EventStore, stream_id: str) -> bool:
        """
        Re-calculate every hash in the stream and compare against stored hashes.
        Returns True if the chain is intact, False if tampered.
        """
        events = await store.load_stream(stream_id)
        if not events:
            return True

        prev_hash = "0" * 64
        for event in events:
            expected_hash = cls.hash_event(
                prev_hash, event.event_type, event.payload, event.metadata
            )
            stored_hash = event.metadata.get("_hash")
            stored_prev = event.metadata.get("_prev_hash")

            if stored_hash != expected_hash:
                return False
            if stored_prev != prev_hash:
                return False

            prev_hash = expected_hash

        return True

    @classmethod
    async def detect_tampering(
        cls, store: EventStore, stream_id: str
    ) -> List[Dict[str, Any]]:
        """
        Scan a stream and return details of every tampered event.
        Returns an empty list if the chain is clean.
        """
        events = await store.load_stream(stream_id)
        if not events:
            return []

        tampered: List[Dict[str, Any]] = []
        prev_hash = "0" * 64

        for event in events:
            expected_hash = cls.hash_event(
                prev_hash, event.event_type, event.payload, event.metadata
            )
            stored_hash = event.metadata.get("_hash")

            if stored_hash != expected_hash:
                tampered.append({
                    "event_id": str(event.event_id),
                    "stream_position": event.stream_position,
                    "event_type": event.event_type,
                    "expected_hash": expected_hash,
                    "stored_hash": stored_hash,
                    "detected_at": datetime.now().isoformat(),
                })

            # Always advance with the *stored* hash so we can detect which
            # events are genuinely broken vs cascade-broken.
            prev_hash = stored_hash or expected_hash

        return tampered

    @classmethod
    async def run_integrity_check(
        cls, store: EventStore, stream_id: str
    ) -> Dict[str, Any]:
        """Run a full integrity check and return a structured report."""
        events = await store.load_stream(stream_id)
        is_valid = await cls.verify_chain(store, stream_id)
        tampered = await cls.detect_tampering(store, stream_id) if not is_valid else []

        return {
            "stream_id": stream_id,
            "events_verified": len(events),
            "integrity_status": "VALID" if is_valid else "COMPROMISED",
            "tampered_events": tampered,
            "checked_at": datetime.now().isoformat(),
        }
