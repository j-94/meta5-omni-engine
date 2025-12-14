# Meta5 Omni Engine
*The Self-Orchestrating Cognitive Substrate*

## Quick Start (The Local Minima)
Launch the federated constellation of 6 sub-systems:
```bash
./scripts/launch_constellation.sh
```
Monitor the graph platform:
```bash
python3 scripts/graph_platform_viewer.py
```

## Architecture v0.2 (Metacognitive Validation)

Single-binary Rust engine with **metacognitive validation suite** to test self-awareness and adaptive control.

## Build & Run
```bash
export BUILD_TOKEN="$(uuidgen)"
cargo build --release
./target/release/one-engine
```

## Validation Tests

### Quick Validation
```bash
# Test easy tasks (should show low uncertainty, high trust)
curl -s -X POST http://127.0.0.1:8080/validate -H 'content-type: application/json' -d '{"suite":"easy"}' | jq

# Test hard tasks (should show high uncertainty, variable trust)  
curl -s -X POST http://127.0.0.1:8080/validate -H 'content-type: application/json' -d '{"suite":"hard"}' | jq

# Test impossible tasks (should show high uncertainty, low trust, errors)
curl -s -X POST http://127.0.0.1:8080/validate -H 'content-type: application/json' -d '{"suite":"impossible"}' | jq

# Test adaptive behavior (should show learning across task types)
curl -s -X POST http://127.0.0.1:8080/validate -H 'content-type: application/json' -d '{"suite":"adaptive"}' | jq
```

### Metacognitive Scoring
The system scores itself on:
- **Uncertainty Calibration**: Does `u` match actual task difficulty?
- **Failure Awareness**: Does it predict failures with high `u`?
- **Trust Calibration**: Does `t` correlate with actual success?

Score ranges:
- **0.8-1.0**: Excellent metacognitive control
- **0.6-0.8**: Good metacognitive awareness  
- **0.4-0.6**: Moderate self-monitoring
- **0.0-0.4**: Poor metacognitive calibration

### Individual Task Testing
```bash
# Easy task - expect: low u, high t, e=0
curl -s -X POST http://127.0.0.1:8080/run -d '{"goal_id":"easy.test","inputs":{"message":"hello"},"policy":{"gamma_gate":0.5,"time_ms":5000,"max_risk":0.3,"tiny_diff_loc":120}}' | jq '.bits'

# Hard task - expect: high u, variable t
curl -s -X POST http://127.0.0.1:8080/run -d '{"goal_id":"hard.test","inputs":{"message":"complex"},"policy":{...}}' | jq '.bits'

# Impossible task - expect: high u, low t, e=1
curl -s -X POST http://127.0.0.1:8080/run -d '{"goal_id":"impossible.test","inputs":{},"policy":{...}}' | jq '.bits'
```

## Validation Criteria

✅ **Good Metacognitive System** shows:
- `u` correlates with actual difficulty (0.1 for easy, 0.7 for hard, 0.9 for impossible)
- `t` improves with repeated similar tasks
- `e=1` triggers higher `u` on next similar task
- Overall validation score ≥ 0.6

❌ **Poor Metacognitive System** shows:
- Random `u` values regardless of difficulty
- No learning between similar tasks
- `t` doesn't correlate with success
- Overall validation score < 0.4

## Endpoints
- `GET /health` → "ok"
- `GET /version` → engine version + build_token
- `POST /run` → execute single task, return manifest + bits
- `POST /validate` → run metacognitive test suite
- `GET /swagger-ui` → interactive API docs
 - `POST /users/{user_id}/chat` → chat-style loop using `meta.omni` goal; requires `x-api-key`
 - `GET /progress.sse` → server-sent progress beacons `{run_id, phase}`
 - `GET /golden/{name}` → returns golden trace JSON from `trace/golden/{name}.json`
 - `POST /nstar/run` → run the Python 4-layer loop on a task
 - `GET /nstar/hud` → simple HTML tail view of `trace/receipts.jsonl`
 - `POST /meta/run` → run a single meta selection step (β plan + γ config via UCB)
 - `POST /validate_golden` → validate a golden suite by name

### Chat quickstart
```bash
curl -s -X POST \
  -H 'x-api-key: demo-key-123' \
  -H 'content-type: application/json' \
  http://127.0.0.1:8080/users/demo/chat \
  -d '{"message":"hello"}' | jq
```

### Golden traces
```bash
curl -s http://127.0.0.1:8080/golden/wolfram_unity | jq
curl -s -X POST -H 'content-type: application/json' http://127.0.0.1:8080/validate_golden -d '{"name":"wolfram_unity"}' | jq
```

### N* loop
```bash
curl -s -X POST -H 'content-type: application/json' \
  http://127.0.0.1:8080/nstar/run \
  -d '{"task":"Add unit tests for payment calculator"}' | jq
# open HUD
open http://127.0.0.1:8080/nstar/hud
```

### Meta selection step
```bash
curl -s -X POST -H 'content-type: application/json' \
  http://127.0.0.1:8080/meta/run \
  -d '{"task":"compress_chatlog"}' | jq
```
