# The Ledger: Showroom Presentation Guide

This guide provides a structured script to demonstrate the full capabilities of your event-sourced infrastructure.

## 🧰 Preparation
Before starting the demo, ensure your environment is ready:

1.  **Switch to Supabase**: The project is now configured to use the Supabase cloud instance.
2.  **Start the Dashboard API**:
    ```bash
    export PYTHONPATH=$PYTHONPATH:.
    uv run python src/api/main.py
    ```
2.  **Open the Dashboard**: [http://localhost:8000](http://localhost:8000)

---

## 🎭 The 5-Act Demonstration

### Act 1: The Live Workflow (Happy Path)
**Goal**: Show a loan application moving from submission to final decision.

1.  **Action**: Run the full lifecycle demo script.
    ```bash
    export PYTHONPATH=$PYTHONPATH:.
    uv run python demo_full_lifecycle.py
    ```
2.  **Talking Points**: 
    - "We are simulating a multi-agent AI system where specialized agents (Credit, Fraud, Compliance) collaborate on a single loan application."
    - "Every action is an immutable event appended to a PostgreSQL-backed event store."
    - "Notice the 'Gas Town' pattern: agents cannot act until they prove they've loaded the current context."

### Act 2: Real-Time Dashboard (Projections)
**Goal**: Show how events are projected into read-optimized models.

1.  **Action**: Switch to the browser ([http://localhost:8000](http://localhost:8000)).
2.  **Talking Points**:
    - "The UI you see is populated in real-time by asynchronous projection daemons."
    - "Point to the **AI Agent Performance** section: 'We track exactly which model version made which decision, providing full accountability for our AI workforce.'"

### Act 3: Regulatory Audit (Temporal Proofs)
**Goal**: Show how to verify decisions for compliance auditors.

1.  **Action**: Click the **VIEW TRAIL** button on `demo-app-888`.
2.  **Talking Points**:
    - "If a regulator asks why a loan was approved, we don't just show the current state; we show the **Proof of Compliance**."
    - "Every check has a unique SHA-256 hash. Because of our cryptographic chaining, if even one bit changed in the history, the audit would fail."

### Act 4: Edge Cases (Concurrency & Rules)
**Goal**: Demonstrate system robustness under stress.

1.  **Action**: Reference the terminal output from Phase 5 of the demo script.
2.  **Talking Points**:
    - "We simulated two agents trying to make a decision at the exact same time."
    - "The Ledger used **Optimistic Concurrency Control** (OCC) to ensure only the first agent succeeded, preventing 'split-brain' decisions or state corruption."

### Act 5: Replay & Schema Evolution (Future-Proofing)
**Goal**: Show how to upgrade the system without losing history.

1.  **Action**: Reference Phase 6 of the demo script (Upcasting).
2.  **Talking Points**:
    - "What happens if we change our data schema next year? We don't run expensive migrations."
    - "We use **Upcasters** to transform historical events into the new format on-the-fly as they are read from the stream."

---

## 🏆 Summary Checklist
- [ ] **Immutable Record**: Every "click" and "thought" is recorded.
- [ ] **Cryptographic Trust**: Tamper-proof event chains.
- [ ] **Real-Time Visibility**: High-fidelity dashboard for operations.
- [ ] **Regulatory Ready**: Instant time-travel and compliance proofs.
