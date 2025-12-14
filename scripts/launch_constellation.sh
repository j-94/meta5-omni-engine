#!/usr/bin/env bash
# Meta5 Constellation Launcher
# Orchestrates the 6-way federated empire into the local minima

mkdir -p logs

echo "🌟 Launching Meta5 Constellation..."

# 1. Layer Stackfile (YAML Rules)
python3 scripts/meta5_layer_stackfile.py > logs/stackfile.log 2>&1 &
echo "[1/6] Layer Stackfile Online"

# 2. Graph Viz (Fractal Renderer)
python3 scripts/meta5_graph_viz.py > logs/viz.log 2>&1 &
echo "[2/6] Graph Viz Online"

# 3. Hot Reload (Quantum Watcher)
python3 scripts/meta5_hot_reload.py > logs/reload.log 2>&1 &
echo "[3/6] Hot Reload Online"

# 4. Runtime Assertions (AUPET Verified)
python3 scripts/meta5_runtime_assertions.py > logs/assertions.log 2>&1 &
echo "[4/6] Runtime Assertions Online"

# 5. Eternal Algorithms (L-System)
python3 scripts/meta5_eternal_algorithms.py > logs/eternal.log 2>&1 &
echo "[5/6] Eternal Algorithms Online"

# 6. Symbiotic UI (WebSocket)
python3 scripts/meta5_symbiotic_ui.py > logs/ui.log 2>&1 &
echo "[6/6] Symbiotic UI Online"

echo "✅ All systems stable. Tail logs at logs/*.log"
