import asyncio
import os
import time
from dotenv import load_dotenv
from src.mcp.server import (
    submit_application,
    start_agent_session,
    record_credit_analysis,
    record_compliance_check,
    request_compliance,
    generate_decision,
    get_application_summary
)
from src.event_store import EventStore
from src.upcasting.registry import registry
from rich.console import Console

load_dotenv()

async def run_mcp_lifecycle():
    console = Console()
    console.print("[bold blue]🌟 Starting Mastery MCP Lifecycle Verification[/]")
    
    app_id = f"mcp-app-{int(time.time())}"
    agent_id = "ai-orchestrator"
    session_id = f"session-{int(time.time())}"
    
    # 1. Start Session
    console.print(f"🛠️ [Step 1] Initializing Agent Session: {session_id}")
    res = await start_agent_session(agent_id, session_id, "test-suite", 0, 100, "gpt-4")
    assert res["status"] == "SUCCESS"
    
    # 2. Submit Application
    console.print(f"🛠️ [Step 2] Submitting Application: {app_id}")
    res = await submit_application(app_id, "user-456", 25000, "Expansion", "Mobile")
    assert res["status"] == "SUCCESS"

    # 3. Request Compliance (Mastery: Explicit Intent)
    console.print("🛠️ [Step 3] Requesting Compliance (KYC & AML)...")
    res = await request_compliance(app_id, ["KYC", "AML"], "v1")
    assert res["status"] == "SUCCESS"
    
    # 4. record_credit_analysis
    console.print("🛠️ [Step 4] Recording Credit Analysis...")
    res = await record_credit_analysis(
        app_id, agent_id, session_id, "gpt-4", 0.95, "Low", 30000, 1500, {"income": 100000}
    )
    assert res["status"] == "SUCCESS"
    
    # 5. record_compliance_check (Mandatory for decision: KYC and AML)
    console.print("🛠️ [Step 5] Recording Compliance Results...")
    await record_compliance_check(app_id, "KYC", "v1", True, {"id_verified": True})
    res = await record_compliance_check(app_id, "AML", "v1", True, {"check_date": "2026-01-01"})
    assert res["status"] == "SUCCESS"
    
    # 5. generate_decision (Requires Compliance=PASSED)
    console.print("🛠️ [Step 5] Generating Decision...")
    res = await generate_decision(
        app_id, agent_id, "APPROVE", 0.98, [session_id], "Strong profile", {"gpt-4": "v1"}
    )
    if res["status"] != "SUCCESS":
        console.print(f"[bold red]❌ Decision Failed: {res.get('message')}[/]")
        console.print(f"💡 Suggestion: {res.get('suggested_action')}")
    assert res["status"] == "SUCCESS"
    
    # 6. Verify via Audit Trail (Immediate consistency)
    console.print("🛠️ [Step 6] Verifying final state via Audit Trail...")
    from src.mcp.server import get_audit_trail
    audit_str = await get_audit_trail(f"loan-{app_id}")
    console.print(f"📊 Audit Trail contains {audit_str.count('event_type')} events.")
    assert "DecisionGenerated" in audit_str
    assert "APPROVE" in audit_str

    # 7. Test Error Handling (Precondition Violation)
    console.print("🛠️ [Step 7] Testing Structured Error (Double Submit)...")
    res = await submit_application(app_id, "user-456", 25000, "Expansion")
    console.print(f"❌ Received Expected Error: {res.get('message')}")
    console.print(f"💡 Suggested Action: {res.get('suggested_action')}")
    assert res["status"] == "ERROR"
    assert "suggested_action" in res

    console.print("[bold green]✨ END-TO-END MASTERY VERIFIED: All business rules, Gas Town patterns, and MCP tools are operational.[/]")

if __name__ == "__main__":
    asyncio.run(run_mcp_lifecycle())
