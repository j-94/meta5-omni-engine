# Service Architecture: The "Meta3" Protocol
## How Agents Consume This System

To serve other LM agents (AutoGPT, LangChain, Custom Scripts), we expose the **System Matrix** as a stateless, context-addressable API.

### 1. The API Contract
**Endpoint:** `POST /v1/context/resolve`
*Input:* `{"query": "User asked about willpower", "trace_id": "optional-previous-id"}`
*Output:* 
```json
{
  "context": [
    {"id": 4209, "text": "What practical things can I do to increase willpower...", "score": 0.98},
    {"id": 4215, "text": "What about epinephrine?", "score": 0.85}
  ],
  "graph_coordinates": {"x": 102.4, "y": -50.1},
  "suggestion": "Pivot to Dopamine Regulation (Cluster #99)"
}
```

### 2. The Use Case: "Agent Memory Service"
Instead of agents managing their own limited `memory.txt`, they offload cognition to **Meta3**.

**Flow:**
1.  **Agent Start**: `Agent-X` wakes up.
2.  **Context Fetch**: `Agent-X` pings `Meta3`: "Where was I?"
3.  **Meta3 Response**: "You were exploring cluster `Self-Replication` last Tuesday. Here are the last 5 artifacts."
4.  **Agent Action**: `Agent-X` generates code.
5.  **Receipt**: `Agent-X` sends receipt to `Meta3`: `POST /nstar/run { "task": "wrote code", "ok": true }`.
6.  **Graph Update**: Meta3 adds node, re-balances graph physics.

### 3. Implementation Plan
1.  **Expose `/v1/context/resolve`**: A dedicated endpoint in `nstar.rs` (or `api.rs`) that performs vector/keyword search over the `recepits.jsonl`.
2.  **Client SDK**: A simple python wrapper `meta3.py` for agents to droplink.
    ```python
    import meta3
    ctx = meta3.resolve("willpower")
    print(ctx.suggestion)
    ```

### 4. The Value Prop
**"Physics-Based RAG"**
Standard RAG is static. Meta3 RAG is dynamic. The "correct" answer depends on the current *tension* of the graph (what clusters are active). If the system is currently "thinking about" architecture, a query for "security" returns architectural security, not firewall rules.
