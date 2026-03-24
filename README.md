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
PGPASSWORD=supabase1224 psql -h db.dxscaeckamkplshxqkae.supabase.co -p 5432 -U postgres -d postgres -f src/schema.sql
```

### Running the Showroom Demo (The Big 5)
A comprehensive demonstration script is provided to showcase the 5 core features: Live Workflow, Event Log, Dashboard Projections, Concurrency, and Replay & Upcasting.

```bash
export PYTHONPATH=$PYTHONPATH:.
uv run python demo_full_lifecycle.py
```

## Running Tests
The test suite validates concurrency, integrity, and projections.

```bash
uv run pytest tests/test_concurrency.py tests/test_integrity.py tests/test_projections.py
```

## Project Structure
- `src/schema.sql`: PostgreSQL schema and projection tables.
- `src/event_store.py`: Core `EventStore` with cryptographic chaining.
- `src/mcp/server.py`: FastMCP server for tool and resource exposure.
- `demo_full_lifecycle.py`: High-fidelity showroom demonstration.
- `DOMAIN_NOTES.md`: Technical architecture and design rationale.
