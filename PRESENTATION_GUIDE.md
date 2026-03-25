## Before You Start
1. Ensure your local PostgreSQL is running.
2. Create the database: `createdb eventledger`

---

## Step 1: The Week Standard (Full Auditability)
**Command:** `./.venv/bin/python3 validation_suite.py --step 1`

**The "Wow" Factor:**
- Highlight the **Cryptographic Integrity Check**. Mention that this prevents any "back-dating" or manual tampering with financial records.
- Show the **Event Stream**. Explain that we don't just store the "current state," but the entire journey of the application.

---

## Step 2: Concurrency Under Pressure
**Command:** `./.venv/bin/python3 validation_suite.py --step 2`

**The "Wow" Factor:**
- Explain how the system handles **Race Conditions**. This is critical for multi-agent systems where two AI agents might try to decide on the same loan.
- Mention **Optimistic Concurrency Control (OCC)**. It ensures that the "latest" version wins, and the other must retry with updated data.

---

## Step 3: Temporal Compliance Query
**Command:** `./.venv/bin/python3 validation_suite.py --step 3`

**The "Wow" Factor:**
- This is **Regulatory Time-Travel**. 
- Demonstrate that you can recreate the exact compliance state of an application from months ago, even if the rules or the applicant's status have changed since then.

---

## Step 4: Upcasting & Immutability
**Command:** `./.venv/bin/python3 validation_suite.py --step 4`

**The "Wow" Factor:**
- Show that the database **never changes (Immutable)**. 
- Explain **Upcasting**: We can evolve our code (v1 -> v2) without ever needing complex data migrations. The system "upgrades" the data on-the-fly as it reads it.

---

## Step 5: Gas Town Recovery
**Command:** `./.venv/bin/python3 validation_suite.py --step 5`

**The "Wow" Factor:**
- This is **Resilience**. 
- Simulate a crash, and show that the AI agent can "reconstruct its memory" instantly by replaying the ledger. No work is ever lost.

---

## Step 6: What-If Counterfactual
**Command:** `./.venv/bin/python3 validation_suite.py --step 6`

**The "Wow" Factor:**
- **Simulation and Risk Analysis**. 
- Show how easy it is to test "What if we changed this one risk factor?" across the entire decision history. This is invaluable for refining business rules.
