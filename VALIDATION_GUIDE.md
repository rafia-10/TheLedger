# EventLedger Validation Guide

This guide describes how to execute the 6 validation steps to demonstrate the core capabilities of the EventLedger system.

## Prerequisites
Ensure your environment is configured with the necessary database connection in `.env`.

## Prerequisites
1. Ensure **PostgreSQL** is installed and running on your computer.
2. Create a database named `eventledger`:
   ```bash
   createdb eventledger
   ```
3. Ensure your `.env` has the correct credentials. Default is `postgres:postgres@localhost:5432`.

## Execution Commands

### Step 1: The Week Standard (Audit Trail)
```bash
./.venv/bin/python3 validation_suite.py --step 1
```

### Step 2: Concurrency (Optimistic Locking)
```bash
./.venv/bin/python3 validation_suite.py --step 2
```

### Step 3: Temporal Query (Time-Travel)
```bash
./.venv/bin/python3 validation_suite.py --step 3
```

### Step 4: Upcasting (Schema Evolution)
```bash
./.venv/bin/python3 validation_suite.py --step 4
```

### Step 5: Gas Town Recovery (Resilience)
```bash
./.venv/bin/python3 validation_suite.py --step 5
```

### Step 6: What-If Counterfactual (Simulation)
```bash
./.venv/bin/python3 validation_suite.py --step 6
```

---

### Run All Steps at Once
```bash
./.venv/bin/python3 validation_suite.py --all
```

## What each step demonstrates:

1.  **The Week Standard**: Shows the full, cryptographically-chained event stream for a loan application. It verifies that every action is recorded and the integrity is intact.
2.  **Concurrency Under Pressure**: Simulates two agents attempting to modify the same stream simultaneously. One succeeds, the other receives an `OptimisticConcurrencyError` and then successfully retries.
3.  **Temporal Compliance Query**: Demonstrates the ability to query the compliance state of an application at any point in the past, distinct from its current state.
4.  **Upcasting & Immutability**: Proves that the system can evolve its event schema (v1 to v2) at read-time while keeping the original event data in the database untouched.
5.  **Gas Town Recovery**: Shows how an agent can reconstruct its entire memory/context from the event ledger after a process crash.
6.  **What-If Counterfactual**: Replays a sequence of events with a hypothetical change (e.g., changing risk tier from MEDIUM to HIGH) to see how the final decision would have changed.
