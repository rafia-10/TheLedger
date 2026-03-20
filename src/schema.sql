-- src/schema.sql
-- The Ledger: Event Store Schema

-- Enable UUID extension
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- Individual events within streams
CREATE TABLE IF NOT EXISTS events (
  event_id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  stream_id        TEXT NOT NULL,
  stream_position  BIGINT NOT NULL, -- Position within the specific stream (starts at 1)
  global_position  BIGINT GENERATED ALWAYS AS IDENTITY, -- Monotonic global sequence for projections
  event_type       TEXT NOT NULL,
  event_version    SMALLINT NOT NULL DEFAULT 1,
  payload          JSONB NOT NULL,
  metadata         JSONB NOT NULL DEFAULT '{}'::jsonb, -- correlation_id, causation_id, actor_id, etc.
  recorded_at      TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  -- Ensure only one event can exist at a specific position in a specific stream
  -- This is the foundation of Optimistic Concurrency Control
  CONSTRAINT uq_stream_position UNIQUE (stream_id, stream_position)
);

-- Optimize stream loads (ordered)
CREATE INDEX IF NOT EXISTS idx_events_stream_id ON events (stream_id, stream_position);
-- Optimize projection replays (global order)
CREATE INDEX IF NOT EXISTS idx_events_global_pos ON events (global_position);
CREATE INDEX IF NOT EXISTS idx_events_type ON events (event_type);
CREATE INDEX IF NOT EXISTS idx_events_recorded ON events (recorded_at);

-- Stream metadata and current version tracking
CREATE TABLE IF NOT EXISTS event_streams (
  stream_id        TEXT PRIMARY KEY,
  aggregate_type   TEXT NOT NULL,
  current_version  BIGINT NOT NULL DEFAULT 0, -- Simplifies OCC checks and dashboard queries
  created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  archived_at      TIMESTAMPTZ, -- For "soft-delete" or cold storage scenarios
  metadata         JSONB NOT NULL DEFAULT '{}'::jsonb
);

-- Checkpoint tracking for projections (CQRS Read Side)
CREATE TABLE IF NOT EXISTS projection_checkpoints (
  projection_name  TEXT PRIMARY KEY,
  last_position    BIGINT NOT NULL DEFAULT 0, -- References global_position in events table
  updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Reliable messaging outbox (Transactional safe publishing)
CREATE TABLE IF NOT EXISTS outbox (
  id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  event_id         UUID REFERENCES events(event_id),
  event_type       TEXT NOT NULL,
  destination      TEXT NOT NULL DEFAULT 'DEFAULT',
  payload          JSONB NOT NULL,
  metadata         JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  published_at     TIMESTAMPTZ,
  attempts         SMALLINT NOT NULL DEFAULT 0
);

-- Projection 1: ApplicationSummary
CREATE TABLE IF NOT EXISTS application_summary (
  application_id        TEXT PRIMARY KEY,
  state                 TEXT NOT NULL,
  applicant_id          TEXT NOT NULL,
  requested_amount_usd  NUMERIC(15,2) NOT NULL,
  approved_amount_usd   NUMERIC(15,2) DEFAULT 0,
  risk_tier             TEXT,
  fraud_score           NUMERIC(3,2),
  compliance_status     TEXT DEFAULT 'PENDING',
  decision              TEXT,
  agent_sessions_completed TEXT[] DEFAULT '{}',
  last_event_type       TEXT,
  last_event_at         TIMESTAMPTZ,
  human_reviewer_id     TEXT,
  final_decision_at     TIMESTAMPTZ
);

-- Projection 2: AgentPerformanceLedger
CREATE TABLE IF NOT EXISTS agent_performance (
  agent_id              TEXT,
  model_version         TEXT,
  analyses_completed    INT DEFAULT 0,
  decisions_generated   INT DEFAULT 0,
  avg_confidence_score  NUMERIC(3,2) DEFAULT 0,
  avg_duration_ms       NUMERIC(15,2) DEFAULT 0,
  approve_rate          NUMERIC(3,2) DEFAULT 0,
  decline_rate          NUMERIC(3,2) DEFAULT 0,
  refer_rate            NUMERIC(3,2) DEFAULT 0,
  human_override_rate   NUMERIC(3,2) DEFAULT 0,
  first_seen_at         TIMESTAMPTZ DEFAULT NOW(),
  last_seen_at          TIMESTAMPTZ DEFAULT NOW(),
  PRIMARY KEY (agent_id, model_version)
);

-- Projection 3: ComplianceAuditView
CREATE TABLE IF NOT EXISTS compliance_audit (
  application_id        TEXT,
  rule_id               TEXT,
  rule_version          TEXT,
  status                TEXT,
  failure_reason        TEXT,
  evaluation_at         TIMESTAMPTZ,
  evidence_hash         TEXT,
  regulation_version    TEXT,
  global_position       BIGINT NOT NULL, -- For temporal queries
  PRIMARY KEY (application_id, rule_id, global_position)
);

CREATE INDEX IF NOT EXISTS idx_compliance_audit_app_pos ON compliance_audit (application_id, global_position);
CREATE INDEX IF NOT EXISTS idx_compliance_audit_at ON compliance_audit (evaluation_at);
