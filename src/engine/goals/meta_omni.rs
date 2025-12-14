use anyhow::Result;
use serde_json::{json, Value};

use crate::engine::router;

pub async fn handle(inputs: &Value) -> Result<Value> {
    let user_msg = inputs.get("message").and_then(|v| v.as_str()).unwrap_or("");
    let loop_mode = inputs
        .get("loop_mode")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);

    // 1. High-Priority CodeAct Intercept (The "Kernel Override")
    let msg_lower = user_msg.to_lowercase();
    let (profile_name, seed, rules): (&str, &str, Vec<(&str, &str)>) = if msg_lower.contains("real") || msg_lower.contains("system") || msg_lower.contains("trace") {
         ("System Matrix (Real Trace)", "", vec![])
    } else if msg_lower.contains("mvs") || msg_lower.contains("skeleton") || msg_lower.contains("viewport") {
        ("Mutating Viewport Skeleton (MVS)", "P", vec![("P", "PL"), ("L", "P")])
    } else if msg_lower.contains("grow") || msg_lower.contains("bio") {
        ("Biological Growth", "A", vec![("A", "AB"), ("B", "A")])
    } else if msg_lower.contains("decay") || msg_lower.contains("simple") {
        ("Digital Decay", "10101", vec![("10", "0"), ("01", "1")])
    } else if msg_lower.contains("cycle") || msg_lower.contains("loop") {
        ("Cyclic Stagnation", "A", vec![("A", "B"), ("B", "C"), ("C", "A")])
    } else if msg_lower.contains("divine") || msg_lower.contains("chaos") || msg_lower.contains("matrix") {
         ("Chaotic Expansion (Divine)", "A", vec![("A", "BC"), ("B", "CA"), ("C", "AB")])
    } else {
         ("", "", vec![])
    };

    if !profile_name.is_empty() {
        let mut impact_url = None;
        let url_res = if profile_name.contains("Real") {
            crate::nstar::execute_system_matrix().await
        } else {
            let rules_vec: Vec<(String, String)> = rules.iter().map(|(p, r)| (p.to_string(), r.to_string())).collect();
            crate::nstar::execute_divine_ruliad(&seed.to_string(), rules_vec, 8).await
        };

        let url_msg = match url_res {
            Ok(u) => {
                impact_url = Some(u.clone());
                format!("\n\n🔮 World Generated: {}", u)
            },
            Err(_) => "\n\n(World generation failed)".to_string()
        };

        let reply = format!("CodeAct: Detected intent '{}'.\nAction: Visualizing Causal Graph.\nObservation: {}", profile_name, url_msg);
        let run_payload = json!({
            "goal_id": "ruliad.kernel",
            "inputs": {
                "seed": seed,
                "rules": rules,
                "depth": 8,
                "mode": if profile_name.contains("Real") { "real" } else { "simulated" }
            }
        });

        let mut resp = json!({
            "intent": {"goal": "meta.divine", "constraints": ["interactive", "intercepted"], "evidence": ["code_act_override"]},
            "intent_profile": profile_name,
            "bits": {"A": 1, "U": 0, "P": 1, "E": 0, "Δ": 0, "I": 1, "R": 1, "T": 1, "M": 0},
            "reply": reply,
            "run_payload": run_payload,
            "patch": Value::Null,
            "explanation": {"assumptions": ["kernel override", "direct execution"], "evidence": []}
        });
        
        if let Some(u) = impact_url {
            resp.as_object_mut().unwrap().insert("impact_url".to_string(), json!(u));
        }
        return Ok(resp);
    }

    // 2. Standard LLM Route
    let persona = std::fs::read_to_string("prompts/META_OMNI.md").unwrap_or_else(|_| {
        "You are One Engine, designed to reason about goals and build autonomy loops.".to_string()
    });

    let mut messages = vec![json!({"role": "system", "content": persona})];
    if loop_mode {
        messages.push(json!({"role":"system","content":"LOOP MODE: Always include a runnable run_payload. If uncertain, default to {\"goal_id\":\"wiki.generate\",\"inputs\":{}}. Keep reply short and include what will run."}));
    }
    if let Some(arr) = inputs.get("history").and_then(|v| v.as_array()) {
        for m in arr {
            let role = m.get("role").and_then(|v| v.as_str()).unwrap_or("");
            let content = m.get("content").and_then(|v| v.as_str()).unwrap_or("");
            if content.trim().is_empty() {
                continue;
            }
            if role == "user" || role == "assistant" {
                messages.push(json!({"role": role, "content": content}));
            } else if role == "system" || role == "tool" {
                // Treat tool outputs / injected context as system messages so the LM can use them.
                messages.push(json!({"role": "system", "content": content}));
            }
        }
    }
    messages.push(json!({"role": "user", "content": user_msg}));

    match router::chat_messages(messages).await {
        Ok(mut response) => {
            if response.get("intent").is_none() {
                response
                    .as_object_mut()
                    .map(|obj| {
                        obj.insert(
                            "intent".to_string(),
                            serde_json::json!({"goal": "chat", "constraints": [], "evidence": [persona.clone()]}),
                        )
                    })
                    .unwrap_or_default();
            }
            if response.get("bits").is_none() {
                response
                    .as_object_mut()
                    .map(|obj| {
                        obj.insert(
                            "bits".to_string(),
                            serde_json::json!({"A": 1, "U": 0, "P": 1, "E": 0, "Δ": 0, "I": 0, "R": 0, "T": 1, "M": 0}),
                        )
                    })
                    .unwrap_or_default();
            }
            if response.get("reply").is_none() {
                // Graceful fallback if the model returned a different JSON shape.
                let reply = response
                    .get("response")
                    .and_then(|v| v.as_str())
                    .unwrap_or("ok")
                    .to_string();
                if let Some(obj) = response.as_object_mut() {
                    obj.insert("reply".to_string(), json!(reply));
                }
            }
            if response.get("run_payload").is_none() {
                if let Some(obj) = response.as_object_mut() {
                    if loop_mode {
                        obj.insert(
                            "run_payload".to_string(),
                            json!({"goal_id":"wiki.generate","inputs":{}}),
                        );
                    } else {
                        obj.insert("run_payload".to_string(), Value::Null);
                    }
                }
            }
            if loop_mode {
                let user_says_build = {
                    let t = user_msg.to_lowercase();
                    t.contains("build") || t.contains("compile") || t.contains("meta3")
                };
                let mut override_to_wiki = false;
                if let Some(rp) = response.get("run_payload") {
                    let goal = rp.get("goal_id").and_then(|v| v.as_str()).unwrap_or("");
                    if goal.is_empty() {
                        override_to_wiki = true;
                    } else if goal == "meta3.build" && !user_says_build {
                        // Keep /loop fast + safe unless the user explicitly asked to build.
                        override_to_wiki = true;
                    }
                } else {
                    override_to_wiki = true;
                }
                if override_to_wiki {
                    if let Some(obj) = response.as_object_mut() {
                        obj.insert(
                            "run_payload".to_string(),
                            json!({"goal_id":"wiki.generate","inputs":{}}),
                        );
                    }
                }
            }
            Ok(response)
        }
        Err(err) => {
            eprintln!("router error: {}, falling back to simulation", err);
            
            let (reply, run_payload) = (
                "Chat router unavailable. Using fallback simulation.\n\nTry interactive commands like:\n- 'generate divine chaos'\n- 'show me biological growth'\n- 'simulate decay'\n- 'create a cycle'".to_string(),
                if loop_mode {
                    json!({"goal_id":"wiki.generate","inputs":{}})
                } else {
                    Value::Null
                }
            );

            let intent = json!({"goal": "chat", "constraints": [], "evidence": ["fallback"]});
            
            let resp = json!({
                "intent": intent,
                "bits": {"A": 1, "U": 0, "P": 1, "E": 0, "Δ": 0, "I": 1, "R": 1, "T": 1, "M": 0},
                "reply": reply,
                "run_payload": run_payload,
                "patch": Value::Null,
                "explanation": {"assumptions": ["router unavailable"], "evidence": [err.to_string()]}
            });
            
            Ok(resp)
        }
    }
}
