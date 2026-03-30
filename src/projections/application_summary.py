from src.projections.daemon import Projection
from src.models.events import StoredEvent


class ApplicationSummaryProjection(Projection):
    @property
    def name(self) -> str:
        return "ApplicationSummary"

    async def handle_event(self, conn, event: StoredEvent) -> None:

        if event.event_type == "ApplicationSubmitted":
            await conn.execute(
                """
                INSERT INTO application_summary
                  (application_id, state, applicant_id, requested_amount_usd,
                   compliance_status, last_event_type, last_event_at)
                VALUES ($1, 'SUBMITTED', $2, $3, 'PENDING', $4, $5)
                ON CONFLICT (application_id) DO NOTHING
                """,
                event.payload["application_id"],
                event.payload["applicant_id"],
                event.payload["requested_amount_usd"],
                event.event_type,
                event.recorded_at,
            )

        elif event.event_type == "CreditAnalysisRequested":
            await conn.execute(
                """
                UPDATE application_summary
                   SET state = 'AWAITING_ANALYSIS',
                       last_event_type = $1, last_event_at = $2
                 WHERE application_id = $3
                """,
                event.event_type, event.recorded_at,
                event.payload["application_id"],
            )

        elif event.event_type == "CreditAnalysisCompleted":
            session_ref = (
                f"agent-{event.payload['agent_id']}"
                f"-{event.payload.get('session_id', 'unknown')}"
            )
            await conn.execute(
                """
                UPDATE application_summary
                   SET state = 'ANALYSIS_COMPLETE',
                       risk_tier = $1,
                       agent_sessions_completed =
                           array_append(agent_sessions_completed, $2),
                       last_event_type = $3, last_event_at = $4
                 WHERE application_id = $5
                """,
                event.payload["risk_tier"],
                session_ref,
                event.event_type,
                event.recorded_at,
                event.payload["application_id"],
            )

        elif event.event_type == "FraudScreeningCompleted":
            session_ref = (
                f"agent-{event.payload['agent_id']}"
                f"-{event.payload.get('session_id', 'fraud-screen')}"
            )
            await conn.execute(
                """
                UPDATE application_summary
                   SET fraud_score = $1,
                       agent_sessions_completed =
                           array_append(agent_sessions_completed, $2),
                       last_event_type = $3, last_event_at = $4
                 WHERE application_id = $5
                """,
                event.payload["fraud_score"],
                session_ref,
                event.event_type,
                event.recorded_at,
                event.payload["application_id"],
            )

        elif event.event_type == "ComplianceRulePassed":
            await conn.execute(
                """
                UPDATE application_summary
                   SET compliance_status = 'PASSED',
                       last_event_type = $1, last_event_at = $2
                 WHERE application_id = $3
                """,
                event.event_type, event.recorded_at,
                event.payload["application_id"],
            )

        elif event.event_type == "ComplianceRuleFailed":
            await conn.execute(
                """
                UPDATE application_summary
                   SET compliance_status = 'FAILED',
                       last_event_type = $1, last_event_at = $2
                 WHERE application_id = $3
                """,
                event.event_type, event.recorded_at,
                event.payload["application_id"],
            )

        elif event.event_type == "DecisionGenerated":
            await conn.execute(
                """
                UPDATE application_summary
                   SET state = 'PENDING_DECISION', decision = $1,
                       last_event_type = $2, last_event_at = $3
                 WHERE application_id = $4
                """,
                event.payload["recommendation"],
                event.event_type,
                event.recorded_at,
                event.payload["application_id"],
            )

        elif event.event_type == "HumanReviewCompleted":
            await conn.execute(
                """
                UPDATE application_summary
                   SET human_reviewer_id = $1,
                       last_event_type = $2, last_event_at = $3,
                       final_decision_at = $3
                 WHERE application_id = $4
                """,
                event.payload["reviewer_id"],
                event.event_type,
                event.recorded_at,
                event.payload["application_id"],
            )

        elif event.event_type == "ApplicationApproved":
            await conn.execute(
                """
                UPDATE application_summary
                   SET state = 'FINAL_APPROVED',
                       approved_amount_usd = $1,
                       last_event_type = $2, last_event_at = $3
                 WHERE application_id = $4
                """,
                event.payload["approved_amount_usd"],
                event.event_type,
                event.recorded_at,
                event.payload["application_id"],
            )

        elif event.event_type == "ApplicationDeclined":
            await conn.execute(
                """
                UPDATE application_summary
                   SET state = 'FINAL_DECLINED',
                       last_event_type = $1, last_event_at = $2
                 WHERE application_id = $3
                """,
                event.event_type,
                event.recorded_at,
                event.payload["application_id"],
            )
