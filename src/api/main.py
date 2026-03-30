import os
import asyncio
import json
from contextlib import asynccontextmanager
from dotenv import load_dotenv

load_dotenv()

from datetime import datetime
from typing import List, Optional, Dict, Any
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from src.event_store import EventStore
from src.kafka_publisher import OutboxPublisher
from src.integrity.audit_chain import AuditChain
from src.projections.daemon import ProjectionDaemon
from src.projections.application_summary import ApplicationSummaryProjection
from src.projections.agent_performance import AgentPerformanceProjection
from src.projections.compliance_audit import ComplianceAuditProjection

# ── Config ────────────────────────────────────────────────────────────────────

DSN = os.getenv(
    "DATABASE_URL",
    "postgresql://postgres.dxscaeckamkplshxqkae:supabase1224"
    "@aws-0-eu-central-1.pooler.supabase.com:6543/postgres?sslmode=require",
)
store = EventStore(DSN)
os.makedirs("src/api/static", exist_ok=True)

# ── Shared state ──────────────────────────────────────────────────────────────

_publisher: Optional[OutboxPublisher] = None
_ws_connections: List[WebSocket] = []


async def _broadcast_to_ws(event: Dict[str, Any]):
    message = json.dumps(event, default=str)
    dead = []
    for ws in _ws_connections:
        try:
            await ws.send_text(message)
        except Exception:
            dead.append(ws)
    for ws in dead:
        _ws_connections.remove(ws)


# ── Lifespan ──────────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    global _publisher
    await store.connect()

    # Outbox publisher → Kafka (or in-memory bus fallback)
    kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
    _publisher = OutboxPublisher(store, kafka_bootstrap=kafka_bootstrap)
    asyncio.create_task(_publisher.start())
    _publisher.event_bus.subscribe(_broadcast_to_ws)

    # Projection daemon — keeps dashboard read models current
    daemon = ProjectionDaemon(store, [
        ApplicationSummaryProjection(),
        AgentPerformanceProjection(),
        ComplianceAuditProjection(),
    ])
    asyncio.create_task(daemon.run_forever(poll_interval_ms=300))

    yield  # ← server runs here

    if _publisher:
        await _publisher.shutdown()
    await store.disconnect()


app = FastAPI(title="The Ledger: Dashboard", lifespan=lifespan)

# ── Pydantic response models ──────────────────────────────────────────────────

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

# ── REST API ──────────────────────────────────────────────────────────────────

@app.get("/api/summary", response_model=List[ApplicationSummary])
async def get_summary():
    async with store.transaction() as conn:
        rows = await conn.fetch(
            "SELECT * FROM application_summary ORDER BY last_event_at DESC"
        )
        return [dict(r) for r in rows]

@app.get("/api/agents", response_model=List[AgentPerformance])
async def get_agents():
    async with store.transaction() as conn:
        rows = await conn.fetch(
            "SELECT * FROM agent_performance ORDER BY avg_confidence_score DESC"
        )
        return [dict(r) for r in rows]

@app.get("/api/audit/{app_id}")
async def get_audit(app_id: str):
    async with store.transaction() as conn:
        rows = await conn.fetch(
            "SELECT * FROM compliance_audit WHERE application_id = $1 "
            "ORDER BY global_position ASC",
            app_id,
        )
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
    async with store.transaction() as conn:
        max_pos = await conn.fetchval(
            "SELECT COALESCE(MAX(global_position), 0) FROM events"
        )
        rows = await conn.fetch(
            "SELECT projection_name, last_position FROM projection_checkpoints"
        )
        checkpoints = {r["projection_name"]: r["last_position"] for r in rows}
        lag = max(0, max_pos - min(checkpoints.values(), default=max_pos))
        return {"lag_events": lag}

# ── WebSocket ─────────────────────────────────────────────────────────────────

@app.websocket("/ws/events")
async def websocket_events(websocket: WebSocket):
    await websocket.accept()
    _ws_connections.append(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        if websocket in _ws_connections:
            _ws_connections.remove(websocket)

# ── UI ────────────────────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def dashboard_ui():
    with open("src/api/static/index.html") as f:
        return f.read()

app.mount("/static", StaticFiles(directory="src/api/static"), name="static")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", 8000)))
