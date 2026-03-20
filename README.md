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
- PostgreSQL 16+
- [uv](https://github.com/astral-sh/uv) (for dependency management)

### Installation
```bash
uv sync
```

### Database Setup
If you have a local postgres instance:
```bash
psql -h localhost -p 5432 -d postgres -f src/schema.sql
```

### Running Tests
The test suite utilizes a local PostgreSQL instance for validation.

```bash
export PYTHONPATH=$PYTHONPATH:.
uv run pytest tests/test_concurrency.py
```

## Project Structure
- `src/schema.sql`: PostgreSQL schema definitions.
- `src/event_store.py`: Core `EventStore` class for append/load operations.
- `src/models/events.py`: Pydantic models for all event types.
- `src/aggregates/`: Domain logic and business rule enforcement.
- `src/commands/`: Handlers for external actions.
- `tests/`: Verification suite.
