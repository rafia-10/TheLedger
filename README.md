# The Ledger: Agentic Event Store & Enterprise Audit Infrastructure

Building the immutable memory and governance backbone for multi-agent AI systems at production scale.

## Overview
The Ledger is a production-quality event sourcing infrastructure built on PostgreSQL. It provides an append-only, ACID-compliant ledger that makes agent decisions auditable, reproducible, and compliant.

## Phase 1 & 2: Core Event Store & Domain Logic
- **Optimistic Concurrency Control**: Prevents split-brain state in multi-agent systems via `expected_version` checks.
- **Gas Town Pattern**: Enforces persistent memory by requiring a context load before any agent action.
- **CQRS Foundation**: Separates command handlers (Aggregates) from query handlers (Projections).
- **Audit Ledger**: Immutable record of every decision with causal link tracking.

## Getting Started

### Prerequisites
- Python 3.12+
- PostgreSQL 16+ (Local installation)
- [uv](https://github.com/astral-sh/uv) (for dependency management)

### Installation
```bash
uv sync
```

### Local Database Setup
1. Ensure your local PostgreSQL is running.
2. Create the database:
   ```bash
   createdb eventledger
   ```
3. Initialize the schema:
   ```bash
   psql -d eventledger -f src/schema.sql
   ```

## Validation Suite (The Big 6)
A comprehensive validation suite is provided to demonstrate the core architectural capabilities of **The Ledger**.

### Run All Steps
```bash
./.venv/bin/python3 validation_suite.py --all
```

### Run Individual Steps
- **Step 1 (Audit Trail):** `./.venv/bin/python3 validation_suite.py --step 1`
- **Step 2 (Concurrency):** `./.venv/bin/python3 validation_suite.py --step 2`
- **Step 3 (Temporal Query):** `./.venv/bin/python3 validation_suite.py --step 3`
- **Step 4 (Upcasting):** `./.venv/bin/python3 validation_suite.py --step 4`
- **Step 5 (Resilience):** `./.venv/bin/python3 validation_suite.py --step 5`
- **Step 6 (Simulation):** `./.venv/bin/python3 validation_suite.py --step 6`

## Documentation
- **[VALIDATION_GUIDE.md](VALIDATION_GUIDE.md)**: Detailed commands and expected outputs for each validation step.
- **[PRESENTATION_GUIDE.md](PRESENTATION_GUIDE.md)**: Talking points and "wow factors" for client demonstrations.
- **[DOMAIN_NOTES.md](DOMAIN_NOTES.md)**: Technical architecture and design rationale.

## Project Structure
- `src/schema.sql`: PostgreSQL schema and projection tables.
- `src/event_store.py`: Core `EventStore` with cryptographic chaining.
- `src/mcp/server.py`: FastMCP server for tool and resource exposure.
- `validation_suite.py`: The unified 6-step validation demonstration script.
