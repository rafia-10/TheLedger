from typing import List, Optional, Dict
from datetime import datetime
from src.projections.daemon import Projection
from src.models.events import StoredEvent

class ComplianceAuditProjection(Projection):
    @property
    def name(self) -> str:
        return "ComplianceAuditView"

    async def handle_event(self, conn, event: StoredEvent) -> None:
        if event.event_type in ["ComplianceRulePassed", "ComplianceRuleFailed"]:
            payload = event.payload
            
            # We insert a new record for every event to maintain historical state
            # This is the "Temporal Table" pattern
            await conn.execute(
                """
                INSERT INTO compliance_audit (
                    application_id, rule_id, rule_version, status, failure_reason, 
                    evaluation_at, evidence_hash, global_position
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                """,
                payload["application_id"], payload["rule_id"], payload["rule_version"],
                "PASSED" if event.event_type == "ComplianceRulePassed" else "FAILED",
                payload.get("failure_reason"), event.recorded_at, 
                payload["evidence_hash"], event.global_position
            )

    async def get_compliance_at(self, store, application_id: str, timestamp: datetime) -> List[Dict]:
        """Regulatory Time-Travel: Query state as it existed at timestamp."""
        # 1. Find the latest global_position at or before timestamp
        async with store.transaction() as conn:
            max_pos = await conn.fetchval(
                "SELECT COALESCE(MAX(global_position), 0) FROM events WHERE recorded_at <= $1",
                timestamp
            )
            
            # 2. Query only the LATEST rule evaluation for each rule_id up to that position
            rows = await conn.fetch(
                """
                SELECT DISTINCT ON (rule_id) *
                FROM compliance_audit
                WHERE application_id = $1 AND global_position <= $2
                ORDER BY rule_id, global_position DESC
                """,
                application_id, max_pos
            )
            return [dict(r) for r in rows]

    async def rebuild_from_scratch(self, store):
        """Truncate projection table and replay all events from position 0."""
        async with store.transaction() as conn:
            await conn.execute("TRUNCATE compliance_audit")
            await conn.execute(
                "UPDATE projection_checkpoints SET last_position = 0 WHERE projection_name = $1",
                self.name
            )
        # Note: The ProjectionDaemon will pick up from 0 on the next pass
