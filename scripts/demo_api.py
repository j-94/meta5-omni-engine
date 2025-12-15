import requests
import json
import time

# Meta6 API Demo
# Showcasing the "Intent In, Ops Out" Protocol

URL = "http://127.0.0.1:8080/nstar/run"

def send_intent(task):
    print(f"\n🌊 Sending Intent: '{task}'")
    ts_start = time.time()
    
    try:
        response = requests.post(URL, json={"task": task}, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        latency = time.time() - ts_start
        print(f"⚡ Latency: {latency:.3f}s")
        
        # Display the Meta6 Protocol Structure
        print("\n--- META6 RESPONSE ---")
        
        # The Result (Cognition)
        print(f"RESULT: {data.get('result')}")
        
        # The Adapt (Ops/Mutation)
        adapt = data.get('adapt', {})
        ops_summary = adapt.get('ops_summary', 'No ops')
        print(f"OPS:    \033[93m{ops_summary}\033[0m") # Yellow for Ops
        
        # Policy
        print(f"POLICY: {json.dumps(data.get('policy'), indent=None)}")
        
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    print("🔮 Connecting to Meta6 Hyper-Kernel...")
    
    # 1. Pure Query (Logic Traversal)
    send_intent("Status Check")
    
    # 2. Mutation Intent (Graph Actuation)
    send_intent("Create a new file called scripts/simulation_node.py")
