import asyncio
import os
import random
import uuid
from datetime import datetime, timedelta
from dotenv import load_dotenv
from src.event_store import EventStore
from src.commands.handlers import (
    handle_submit_application,
    handle_start_agent_session,
    handle_credit_analysis_completed,
    handle_record_compliance_check,
    handle_generate_decision,
    handle_record_human_review
)
from src.upcasting.registry import registry as upcaster_registry

load_dotenv()

async def populate_data():
    dsn = os.getenv("DATABASE_URL", "postgresql:///eventledger")
    store = EventStore(dsn, upcaster=upcaster_registry)
    await store.connect()

    applicants = [
        ("Alice Smith", 50000, "Home Improvement"),
        ("Bob Johnson", 12000, "Debt Consolidation"),
        ("Charlie Brown", 100000, "Business Startup"),
        ("Diana Prince", 5000, "Medical Expenses"),
        ("Eve Adams", 25000, "Education"),
        ("Frank Miller", 40000, "Vehicle Purchase")
    ]

    agents = ["agent-alpha", "agent-beta", "agent-gamma"]

    print(f"🚀 Populating ledger with {len(applicants)} real-world application lifecycles...")

    for name, amount, purpose in applicants:
        app_id = f"app-{uuid.uuid4().hex[:8]}"
        print(f"--- Processing {app_id} ({name}) ---")
        
        # 1. Submit
        await handle_submit_application(store, app_id, name.lower().replace(" ", "-"), amount, purpose, "Web-Portal")
        
        # 2. Agent Session
        agent_id = random.choice(agents)
        session_id = f"s-{uuid.uuid4().hex[:6]}"
        await handle_start_agent_session(store, agent_id, session_id, "Memory-VectorDB", 0, 200, "v2.1")
        
        # 3. Analysis
        confidence = random.uniform(0.65, 0.99)
        risk = "TIER_A" if confidence > 0.8 else "TIER_B" if confidence > 0.7 else "TIER_C"
        await handle_credit_analysis_completed(
            store, app_id, agent_id, session_id, "v2.1", confidence, risk, amount * 1.1, 85, {"fico": random.randint(600, 850)}
        )
        
        # 4. Compliance (Random check)
        passed = random.random() > 0.2
        await handle_record_compliance_check(store, app_id, "REG-Z-01", "v1", passed, {"checked_by": "auto-bot"})
        
        if passed:
            # 5. Decision
            rec = "APPROVE" if amount < 60000 else "REFER"
            await handle_generate_decision(
                store, app_id, "orchestrator", rec, confidence, [f"{agent_id}-{session_id}"], 
                f"Automated review based on {risk} tier.", {"engine": "v1.2"}
            )
            
            if rec == "REFER":
                # 6. Human Review
                await handle_record_human_review(store, app_id, "manager-01", True, "APPROVE", "Client has strong collateral.")
        
        print(f"✅ Lifecycle complete for {app_id}")

    await store.disconnect()
    print("\n✨ Database successfully populated with rich event data.")

if __name__ == "__main__":
    asyncio.run(populate_data())
