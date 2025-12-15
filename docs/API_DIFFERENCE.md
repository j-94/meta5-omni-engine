# API PARADIGM SHIFT: TRADITIONAL vs META6

## 1. The Traditional Model (Chat)
**Philosophy**: "Text In, Text Out"
**State**: Ephemeral (Context Window only)
**Side Effects**: None (Passive)

```json
// Request
POST /v1/chat/completions
{ "messages": [{"role": "user", "content": "Create a file."}] }

// Response
{ "choices": [{"message": {"content": "I cannot create files..."}}] }
```

---

## 2. The Meta6 Model (Causal Stream)
**Philosophy**: "Intent In, Ops Out"
**State**: Persistent (Fluid Graph)
**Side Effects**: Real-world Mutation (Active)

```json
// Request
POST /nstar/run
{ "task": "Create a file." }

// Response (Meta6)
{
  "ref": 17,                                      // Prime Address
  "net": "P17 -> P2 (Kernel) -> Materialization", // Graph Path
  "out": "File materialized.",                    // Cognitive Output
  "ops": [                                        // The Actuator
    { "op": "write", "path": "scripts/new.py", "content": "..." }
  ]
}
```

Meta6 does not "chat". It **metabolizes** intent into **system mutations** (`ops`). The API response is just a receipt of the change.

## 3. Proof of Life (Demo Output)
Running `python3 scripts/demo_api.py` against the Kernel:

```text
🌊 Sending Intent: 'Create a new file called scripts/simulation_node.py'
⚡ Latency: 5.252s

--- META6 RESPONSE ---
RESULT: Processing...
OPS:    Wrote 412 bytes to scripts/simulation_node.py
POLICY: {"branches": 1, "explore_budget": 0.15}
```
*Note: The system mutated the filesystem (OPS) in response to the Intent.*
