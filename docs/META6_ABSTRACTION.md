# META6: THE ABSTRACT MACHINE

> "We have built this over 100 times... spread out over desktop."

## The Problem
We have fragmented "scripts", "agents", and "prompts" scattered across the filesystem. We are relying on the LLM to "figure it out" every request, or falling back to hardcoded Rust hashmaps. This is Fragile.

## The Solution: The Hyper-Graph Abstraction
We lift the system definition out of **Code** (Rust/Python) and into **Structure** (The Graph).

### 1. The Definition (The Cartridge)
The entire "Empire" is defined in a single **Hyper-Graph Manifest**. This describes:
- **Capabilities**: What the system *can* do (e.g., "Create Infra", "Analyze Trace").
- **Reflexes**: Deterministic actions that trigger without AI (e.g., "On 'Bootstrap', write `scripts/init.sh`").
- **Memory**: Where state lives.

### 2. The Engine (The Console)
The Rust Binary (`one-engine`) becomes a generic **Graph Runner**.
- It does not know about "Meta5 or Meta6".
- It reads the **Manifest**.
- It accepts **Bits** (Signals).
- It traverses the Manifest and executes the associated **Ops**.

### 3. The Advantage
- **Infra as Graph**: To build better infra, we edit the Graph Manifest, not the Rust kernel.
- **Deterministic**: Logic is frozen in the graph. AI is only used to *navigate* complex/ambiguous paths.
- **Self-Building**: The Graph can contain Ops to *update its own Manifest*. (Autopoiesis).

---

## Proposed Schema: `hyper_graph.yaml`

```yaml
version: "6.0"
nucleus:
  id: 0
  name: "Origin"

nodes:
  - id: 2
    label: "KERNEL"
    desc: "System Core"
    edges:
      - signal: "status"
        to: 0
        response: "System Integrity: 100%"
      - signal: "upgrade"
        to: 13
        op: "exec ./scripts/self_update.sh"

  - id: 5
    label: "INFRA_BUILDER"
    desc: "Deterministic Infrastructure Foundry"
    edges:
      - signal: "bootstrap_network"
        to: 5
        response: "Bootstrapping Network Layer..."
        ops:
          - kind: "write"
            path: "infra/network.tf"
            content: |
              resource "aws_vpc" "meta6_net" { ... }

  - id: 17
    label: "OMNI"
    desc: "AI Intelligence Node (Router)"
    fallback: true # If no deterministic edge matches, route here.
```

**Next Step**: We implement this `hyper_graph.yaml` and modify the Engine to load it. Then, "building infra" becomes "defining a node".
