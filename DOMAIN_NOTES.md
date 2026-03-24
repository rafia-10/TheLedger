# Domain Reconnaissance: The Ledger

## EDA vs. ES Distinction
**Question**: A component uses callbacks (like LangChain traces) to capture event-like data. Is this Event-Driven Architecture (EDA) or Event Sourcing (ES)? If you redesigned it using The Ledger, what exactly would change in the architecture and what would you gain?

**Answer**: This is **Event-Driven Architecture (EDA)**. In this model, events are side effects or "telemetry" emitted by a system whose primary state is likely stored in a CRUD database (or not stored at all, in the case of volatile AI traces). The callbacks are "fire-and-forget" notifications.
If redesigned using **The Ledger (Event Sourcing)**, the events would become the **primary source of truth**. Instead of the agent updating a "CurrentStatus" column and firing a callback, it would append a `CreditAnalysisCompleted` event to its stream. The system state is then derived by replaying these events.
**Gains**:
1. **Perfect Auditability**: Every intermediate thought and decision is preserved, not just the final result.
2. **Temporal Querying**: We can reconstruct exactly what the agent "knew" at T=10s, even if it changed its mind at T=15s.
3. **Reliability**: No divergence between "what happened" and "what was logged."

## The Aggregate Question
**Question**: Identify one alternative boundary you considered and rejected. What coupling problem does your chosen boundary prevent?

**Answer**: I considered merging `LoanApplication` and `ComplianceRecord` into a single `LoanProcess` aggregate.
**Rejected because**: In the Apex scenario, specialized agents (Credit, Fraud, Compliance) work in parallel. If they all target the same `LoanProcess` stream, they would constantly collide on `expected_version`, leading to high `OptimisticConcurrencyError` rates and unnecessary retries.
**Chosen Boundary (`LoanApplication` vs `ComplianceRecord`)**: Prevents **lock contention coupling**. The Compliance agent can record its 15 different rule evaluations in the `ComplianceRecord` stream without blocking the Fraud agent from updating the `LoanApplication` risk tier. They only sync at the "Decision" gate.

## Concurrency in Practice
**Question**: Two AI agents simultaneously process the same loan application and both call `append_events` with `expected_version=3`. Trace the exact sequence of operations. What does the losing agent receive, and what must it do next?

**Answer**:
1. **Agent A** and **Agent B** both read the stream and see 3 events (Version 3).
2. **Agent A** sends `INSERT INTO events ...` with a constraint check that the current version is 3.
3. **Postgres** acquires a row-level lock on the `event_streams` record for `loan-123`.
4. **Agent A**'s transaction succeeds, version becomes 4.
5. **Agent B**'s transaction attempts to commit. The unique constraint `uq_stream_position (stream_id, stream_position)` (where position would be 4) or the version check fails.
6. **Losing Agent (B)** receives an `OptimisticConcurrencyError`.
7. **Action**: Agent B must **reload** the stream, **re-apply** its logic to the new state (Version 4), and **retry** the append with `expected_version=4`.

## Projection Lag and its Consequences
**Question**: Your `LoanApplication` projection has a 200ms lag. A loan officer queries "available credit limit" immediately after an agent commits a disbursement. They see the old limit. What does your system do?

**Answer**:
Our system implements **Read-Your-Writes** consistency (where possible) and **Version Tracking**.
1. **UI/API Layer**: When the agent commits, it receives the new version (e.g., `v5`).
2. **The Query**: The UI includes `min_version=5` in its query to the `ApplicationSummary` resource.
3. **Response**: If the projection hasn't reached `v5`, the API returns a `202 Accepted` with the current lag or blocks briefly (long-polling) until the projection catches up.
4. **Communication**: The UI displays a "Syncing..." spinner or a "Data as of [Timestamp]" label, ensuring the user understands they are looking at a slightly stale view if they don't wait.

## The Upcasting Scenario
**Question**: `CreditDecisionMade` v1 {id, decision, reason} -> v2 {id, decision, reason, model_version, confidence_score, regulatory_basis}. Write the upcaster. What is your inference strategy?

**Answer**:
```python
@registry.register("CreditDecisionMade", from_version=1)
def upcast_decision_v1_to_v2(payload: dict, metadata: dict) -> dict:
    recorded_at = metadata["recorded_at"]
    return {
        **payload,
        "model_version": infer_model_from_date(recorded_at),
        "confidence_score": None, # Cannot fabricate risk data
        "regulatory_basis": infer_reg_from_date(recorded_at)
    }
```
**Inference Strategy**: We map `recorded_at` timestamps to the production deployment calendar. Events before 2026-01-01 are tagged with `model-v1-legacy`. `confidence_score` is left `null` because fabricating a "high confidence" for a legacy decision would be a compliance violation.

## The Marten Async Daemon Parallel
**Question**: How would you achieve distributed projection execution in Python? What coordination primitive? What failure mode?

**Answer**:
I would use **PostgreSQL Advisory Locks** (`pg_advisory_lock`) as the coordination primitive. Each projection "shard" or group is assigned a unique integer ID.
1. **Coordination**: Each daemon instance attempts to acquire a session-level advisory lock for a specific set of projections. Only one node can hold the lock for "ProjectionGroup-A".
2. **Pattern**: I would implement a "Leader Election per Shard" pattern.
3. **Failure Mode**: Guards against **Double-Processing**. If two nodes simultaneously processed the same event for the same read-model, it could lead to "Double-Counting" in metrics or primary key violations in summaries. If a node crashes, Postgres releases the advisory lock, and another node automatically picks up the shard.
