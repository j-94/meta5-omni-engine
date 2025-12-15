import yaml
import json
import os
import glob

# GRAPH INGESTOR
# "Loading the State back into the Graph"
# Scans memory (trace) and body (scripts) to populate the Manifest.

MANIFEST = "hyper_graph.yaml"
TRACE_FILE = "trace/receipts.jsonl"

def load_manifest():
    if os.path.exists(MANIFEST):
        with open(MANIFEST, "r") as f:
            return yaml.safe_load(f)
    return {"nodes": []}

def save_manifest(data):
    with open(MANIFEST, "w") as f:
        yaml.dump(data, f, sort_keys=False)
    print(f"💾 Graph Persisted to {MANIFEST}")

def ingest_trace(graph):
    # Turn History into Knowledge Nodes
    if not os.path.exists(TRACE_FILE):
        return
        
    print("🔍 Scanning Trace History...")
    new_nodes = 0
    with open(TRACE_FILE, "r") as f:
        for line in f:
            try:
                rec = json.loads(line)
                run_id = rec.get("run_id", "0")
                task = rec.get("task", "unknown")
                reply = rec.get("best", "")
                
                # Check if node exists (by label/ID)
                # Simplified: Hash run_id to ID
                nid = int(hash(run_id)) % 1000 + 2000 # Offset to avoid conflict
                
                # Dedupe
                if any(n['id'] == nid for n in graph['nodes']):
                    continue

                # Create Memory Node
                node = {
                    "id": nid,
                    "label": f"MEM_{run_id[:6]}",
                    "behavior": f"Recall: {task}",
                    "edges": [
                        {
                            "signal": f"recall {run_id[:6]}",
                            "response": f"I remember doing '{task}'. Result was: {reply[:50]}...",
                            "ops": []
                        }
                    ]
                }
                graph['nodes'].append(node)
                new_nodes += 1
            except:
                pass
    print(f"✅ Ingested {new_nodes} Memory Nodes.")

def ingest_capabilities(graph):
    # Turn Scripts into Ability Nodes
    print("🔍 Scanning Capabilities (Scripts)...")
    scripts = glob.glob("scripts/*.py")
    new_nodes = 0
    
    for script in scripts:
        name = os.path.basename(script)
        nid = int(hash(name)) % 1000 + 3000
        
        if any(n['id'] == nid for n in graph['nodes']):
            continue
            
        node = {
            "id": nid,
            "label": f"TOOL_{name.upper().replace('.PY','')}",
            "behavior": f"Executes {name}",
            "edges": [
                {
                    "signal": f"run {name}",
                    "response": f"Executing capability {name}...",
                    "ops": [
                        {
                            "op": "exec",
                            "cmd": "python3",
                            "args": [script]
                        }
                    ]
                }
            ]
        }
        graph['nodes'].append(node)
        new_nodes += 1
        
    print(f"✅ Ingested {new_nodes} Capability Nodes.")

if __name__ == "__main__":
    graph = load_manifest()
    
    # 1. Load History
    ingest_trace(graph)
    
    # 2. Load Abilities
    ingest_capabilities(graph)
    
    save_manifest(graph)
    print("\n✨ State Loaded. usage: python3 scripts/cli.py 'run demo_api.py'")
