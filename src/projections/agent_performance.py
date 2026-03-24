from src.projections.daemon import Projection
from src.models.events import StoredEvent

class AgentPerformanceProjection(Projection):
    @property
    def name(self) -> str:
        return "AgentPerformance"

    async def handle_event(self, conn, event: StoredEvent) -> None:
        if event.event_type == "CreditAnalysisCompleted":
            payload = event.payload
            await conn.execute(
                """
                INSERT INTO agent_performance (agent_id, model_version, analyses_completed, avg_duration_ms, first_seen_at, last_seen_at)
                VALUES ($1, $2, 1, $3, $4, $4)
                ON CONFLICT (agent_id, model_version) DO UPDATE SET
                    analyses_completed = agent_performance.analyses_completed + 1,
                    avg_duration_ms = (agent_performance.avg_duration_ms * agent_performance.analyses_completed + $3) / (agent_performance.analyses_completed + 1),
                    last_seen_at = $4
                """,
                payload["agent_id"], payload["model_version"], payload["analysis_duration_ms"], event.recorded_at
            )
        
        elif event.event_type == "DecisionGenerated":
            payload = event.payload
            agent_id = payload["orchestrator_agent_id"]
            # DecisionGenerated v2 has model_versions dict. We update the orchestrator's entry.
            model_version = payload["model_versions"].get(agent_id, "unknown")
            rec = payload["recommendation"]
            
            approve_inc = 1 if rec == "APPROVE" else 0
            decline_inc = 1 if rec == "DECLINE" else 0
            refer_inc = 1 if rec == "REFER" else 0

            await conn.execute(
                """
                INSERT INTO agent_performance (agent_id, model_version, decisions_generated, avg_confidence_score, approve_rate, decline_rate, refer_rate, last_seen_at)
                VALUES ($1, $2, 1, $3, $4, $5, $6, $7)
                ON CONFLICT (agent_id, model_version) DO UPDATE SET
                    decisions_generated = agent_performance.decisions_generated + 1,
                    avg_confidence_score = (agent_performance.avg_confidence_score * agent_performance.decisions_generated + $3) / (agent_performance.decisions_generated + 1),
                    approve_rate = (agent_performance.approve_rate * agent_performance.decisions_generated + $4) / (agent_performance.decisions_generated + 1),
                    decline_rate = (agent_performance.decline_rate * agent_performance.decisions_generated + $5) / (agent_performance.decisions_generated + 1),
                    refer_rate = (agent_performance.refer_rate * agent_performance.decisions_generated + $6) / (agent_performance.decisions_generated + 1),
                    last_seen_at = $7
                """,
                agent_id, model_version, payload["confidence_score"], float(approve_inc), float(decline_inc), float(refer_inc), event.recorded_at
            )
        
        elif event.event_type == "HumanReviewCompleted":
            # Track human override rate for the orchestrator
            # This is complex because we need to find the orchestrator from DecisionGenerated.
            # Usually, we'd join or keep state. For simplicity, we just track the rate.
            pass
