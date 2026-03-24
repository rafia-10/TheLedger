import os
import asyncio
import json
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()
from datetime import datetime
from typing import List, Optional, Dict, Any
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from src.event_store import EventStore
from src.kafka_publisher import OutboxPublisher
from src.integrity.audit_chain import AuditChain
from src.what_if import WhatIfSimulator

# Database configuration — Cloud only (Supabase Transaction Pooler)
CLOUD_DSN = "postgresql://postgres.dxscaeckamkplshxqkae:supabase1224@aws-0-eu-central-1.pooler.supabase.com:6543/postgres?sslmode=require"
DSN = os.getenv("DATABASE_URL", CLOUD_DSN)
store = EventStore(DSN)

app = FastAPI(title="The Ledger: Dashboard")

# Ensure static directory exists
os.makedirs("src/api/static", exist_ok=True)

# ── Pydantic Models ──────────────────────────────────────────────────────────

class ApplicationSummary(BaseModel):
    application_id: str
    state: str
    requested_amount_usd: float
    applicant_id: str
    compliance_status: str
    decision: Optional[str] = None
    last_event_at: datetime

class AgentPerformance(BaseModel):
    agent_id: str
    model_version: str
    analyses_completed: int
    decisions_generated: int
    avg_confidence_score: float

# ── Outbox Publisher & WebSocket Manager ─────────────────────────────────────

_publisher: Optional[OutboxPublisher] = None
_ws_connections: List[WebSocket] = []


@app.on_event("startup")
async def startup():
    global _publisher
    await store.connect()

    # Start outbox publisher (in-memory bus, no Kafka required)
    _publisher = OutboxPublisher(store)
    asyncio.create_task(_publisher.start())

    # Subscribe to in-memory bus for WebSocket broadcasting
    _publisher.event_bus.subscribe(_broadcast_to_ws)


@app.on_event("shutdown")
async def shutdown():
    if _publisher:
        await _publisher.shutdown()
    await store.disconnect()


async def _broadcast_to_ws(event: Dict[str, Any]):
    """Broadcast events to all connected WebSocket clients."""
    message = json.dumps(event, default=str)
    disconnected = []
    for ws in _ws_connections:
        try:
            await ws.send_text(message)
        except Exception:
            disconnected.append(ws)
    for ws in disconnected:
        _ws_connections.remove(ws)


# ── REST API ─────────────────────────────────────────────────────────────────

@app.get("/api/summary", response_model=List[ApplicationSummary])
async def get_summary():
    async with store.transaction() as conn:
        rows = await conn.fetch("SELECT * FROM application_summary ORDER BY last_event_at DESC")
        return [dict(r) for r in rows]

@app.get("/api/agents", response_model=List[AgentPerformance])
async def get_agents():
    async with store.transaction() as conn:
        rows = await conn.fetch("SELECT * FROM agent_performance ORDER BY avg_confidence_score DESC")
        return [dict(r) for r in rows]

@app.get("/api/audit/{app_id}")
async def get_audit(app_id: str):
    async with store.transaction() as conn:
        rows = await conn.fetch("SELECT * FROM compliance_audit WHERE application_id = $1 ORDER BY global_position ASC", app_id)
        return [dict(r) for r in rows]

@app.get("/api/events/{stream_id}")
async def get_events(stream_id: str):
    events = await store.load_stream(stream_id)
    return [e.model_dump(mode="json") for e in events]

@app.get("/api/integrity/{stream_id}")
async def check_integrity(stream_id: str):
    return await AuditChain.run_integrity_check(store, stream_id)

@app.get("/api/lag")
async def get_lag():
    """Return projection lag (events behind global position)."""
    from src.projections.daemon import ProjectionDaemon
    from src.projections.application_summary import ApplicationSummaryProjection
    daemon = ProjectionDaemon(store, [ApplicationSummaryProjection()])
    lag = await daemon.get_lag()
    return {"lag_events": lag}

# ── WebSocket (Live Event Stream) ────────────────────────────────────────────

@app.websocket("/ws/events")
async def websocket_events(websocket: WebSocket):
    await websocket.accept()
    _ws_connections.append(websocket)
    try:
        while True:
            # Keep connection alive, wait for client messages (ping/pong)
            await websocket.receive_text()
    except WebSocketDisconnect:
        _ws_connections.remove(websocket)

# ── UI ───────────────────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def dashboard_ui():
    with open("src/api/static/index.html", "r") as f:
        return f.read()

# Mount static files for CSS/JS
app.mount("/static", StaticFiles(directory="src/api/static"), name="static")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
