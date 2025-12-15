import sys
import yaml
import json
import os
import subprocess

# META6 PYTHON HYPER-KERNEL (CLI)
# Implements the Abstract Machine defined in hyper_graph.yaml
# Bypasses Rust Compilation issues to demonstrate the Logic Layer.

MANIFEST_FILE = "hyper_graph.yaml"

class HyperKernel:
    def __init__(self):
        self.load_manifest()

    def load_manifest(self):
        try:
            with open(MANIFEST_FILE, "r") as f:
                self.manifest = yaml.safe_load(f)
                print(f"🔮 Hyper-Graph Loaded: {len(self.manifest.get('nodes', []))} Nodes")
        except Exception as e:
            print(f"❌ Failed to load manifest: {e}")
            self.manifest = {"nodes": []}

    def traverse(self, signal):
        signal = signal.lower()
        nodes = self.manifest.get("nodes", [])
        
        # 1. Direct Edge Match
        for node in nodes:
            edges = node.get("edges", [])
            for edge in edges:
                if edge["signal"].lower() in signal:
                    return self.execute_edge(node, edge, signal)
        
        # 2. Fallback (Router)
        return self.router_fallback(signal)

    def execute_edge(self, node, edge, signal):
        print(f"\n[P{node['id']}] {node['label']} -> {edge['response']}")
        
        ops = edge.get("ops", [])
        if ops:
            print("\n⚡ EXEC OPS:")
            for op in ops:
               self.run_op(op)
        else:
            print("(No Ops)")
            
        return True

    def run_op(self, op):
        kind = op.get("op")
        if kind == "write":
            path = op.get("path")
            content = op.get("content")
            print(f"  📝 Writing {len(content)} bytes to {path}...")
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, "w") as f:
                f.write(content)
        elif kind == "exec":
            cmd = op.get("cmd")
            args = op.get("args", [])
            full_cmd = f"{cmd} {' '.join(args)}"
            print(f"  ⚙️ Executing: {full_cmd}")
            subprocess.run([cmd] + args)

    def router_fallback(self, signal):
        print(f"\n[P17] AI_ROUTER -> Routing '{signal}' to OMNI...")
        # In a real system, this calls LLM. 
        # Here we just acknowledge the routing.
        print("  (Signal buffered for Intelligence Layer)")

if __name__ == "__main__":
    kernel = HyperKernel()
    
    if len(sys.argv) > 1:
        task = " ".join(sys.argv[1:])
        kernel.traverse(task)
    else:
        print("\nUsage: python3 scripts/cli.py <intent>")
        print("Try: 'build infra' or 'status check'")
