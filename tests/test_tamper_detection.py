import asyncio
import os
import pytest
import hashlib
import json
from datetime import datetime
from src.event_store import EventStore
from src.models.events import BaseEvent, ApplicationSubmitted
from src.integrity.audit_chain import AuditChain
from src.commands.handlers import handle_submit_application

DSN = os.getenv("DATABASE_URL", "postgresql:///eventledger")

@pytest.mark.asyncio
async def test_tamper_detection_full_flow():
    store = EventStore(DSN)
    await store.connect()
    
    app_id = f"tamper-app-{int(datetime.now().timestamp())}"
    
    # 1. Append valid events (Submit + Human Review simulation)
    await handle_submit_application(store, app_id, "user-1", 1000, "Tamper Test", "CLI")
    
    # 2. Verify integrity (Should be VALID)
    report = await AuditChain.run_integrity_check(store, f"loan-{app_id}")
    assert report["integrity_status"] == "VALID"
    assert report["events_verified"] >= 1
    
    # 3. MANUALLY CORRUPT A PAYLOAD IN THE DB
    async with store.transaction() as conn:
        # Change the amount in the payload without updating the hash
        await conn.execute(
            "UPDATE events SET payload = payload || '{\"requested_amount_usd\": 999999}'::jsonb WHERE stream_id = $1",
            f"loan-{app_id}"
        )
    
    # 4. Verify integrity (Should be COMPROMISED)
    report_compromised = await AuditChain.run_integrity_check(store, f"loan-{app_id}")
    assert report_compromised["integrity_status"] == "COMPROMISED"
    assert len(report_compromised["tampered_events"]) > 0
    
    print(f"\n✅ Tamper Detection Verified for {app_id}")
    print(f"❌ Detected: {report_compromised['tampered_events'][0]['event_type']} was tampered.")

if __name__ == "__main__":
    asyncio.run(test_tamper_detection_full_flow())
