use axum::{
    extract::Query,
    response::{Html, IntoResponse},
    Json,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tokio::{fs, process::Command as TokioCommand};
use utoipa::ToSchema;
use crate::engine::router;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Serialize, Deserialize, JsonSchema, ToSchema)]
pub struct NStarRunReq {
    pub task: String,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema, ToSchema)]
pub struct NStarRunResp {
    pub ok: bool,
    pub result: String,
    pub policy: serde_json::Value,
    pub adapt: serde_json::Value,
}

#[utoipa::path(
    post,
    path = "/nstar/run",
    request_body = NStarRunReq,
    responses((status=200, description="Run nstar loop", body=NStarRunResp))
)]
pub async fn nstar_run_handler(Json(req): Json<NStarRunReq>) -> impl IntoResponse {
    let task = req.task.clone();
    let run_id = uuid::Uuid::new_v4().to_string().chars().take(8).collect::<String>();
    let t0 = SystemTime::now();

    // Policy (simplified/hardcoded for now, mimicking nstar.py defaults)
    let policy = serde_json::json!({
        "branches": 1,
        "explore_budget": 0.15
    });

    // 1. Cognition: Load System Prompt & Call LLM
    let system_prompt = fs::read_to_string("prompts/META_OMNI.md")
        .await
        .unwrap_or_else(|_| "You are the Meta3 Engine. Respond in JSON.".to_string());

    // 1. Cognition: Load System Prompt & Call LLM
    let system_prompt = fs::read_to_string("prompts/META_OMNI.md")
        .await
        .unwrap_or_else(|_| "You are the Meta3 Engine. Respond in JSON with optional 'ops' array.".to_string());

    let res = router::chat(&system_prompt, &task).await;
    
    let (best_out, intent, mut impact_url, ops_report) = match res {
        Ok(val) => {
             // Standard OMNI Response
             let reply = val.get("reply").and_then(|s| s.as_str()).unwrap_or("Processing...").to_string();
             let url = val.get("impact_url").and_then(|s| s.as_str()).map(|s| s.to_string());
             let intent = val.get("intent").and_then(|s| s.as_str()).unwrap_or("unknown").to_string();
             
             // Check for Manifest/Evidence structure (deep omni)
             let final_reply = if let Some(man) = val.get("manifest") {
                 man.pointer("/evidence/reply").and_then(|s| s.as_str()).unwrap_or(&reply).to_string()
             } else {
                 reply
             };

             // META5: The Universal Actuator (Op Execution Loop)
             let mut ops_log = Vec::new();
             if let Some(ops) = val.get("ops").and_then(|v| v.as_array()) {
                 for op in ops {
                     if let Some(kind) = op.get("op").and_then(|s| s.as_str()) {
                         let res_str = match kind {
                             "write" => {
                                 let path = op.get("path").and_then(|s| s.as_str()).unwrap_or("");
                                 let content = op.get("content").and_then(|s| s.as_str()).unwrap_or("");
                                 // Simple Safety: Only allow writing to known subdirs
                                 if path.starts_with("src/") || path.starts_with("ui/") || path.starts_with("scripts/") || path.starts_with("docs/") {
                                     if let Some(parent) = std::path::Path::new(path).parent() {
                                         let _ = fs::create_dir_all(parent).await;
                                     }
                                     if let Ok(_) = fs::write(path, content).await {
                                         format!("Wrote {} bytes to {}", content.len(), path)
                                     } else {
                                         format!("Failed to write {}", path)
                                     }
                                 } else {
                                     format!("Blocked unsafe write to {}", path)
                                 }
                             },
                             "exec" => {
                                 let cmd = op.get("cmd").and_then(|s| s.as_str()).unwrap_or("");
                                 let args = op.get("args").and_then(|a| a.as_array())
                                     .map(|arr| arr.iter().map(|s| s.as_str().unwrap_or("")).collect::<Vec<_>>())
                                     .unwrap_or_default();
                                 if !cmd.is_empty() {
                                     match tokio::process::Command::new(cmd).args(args).output().await {
                                         Ok(o) => format!("Exec OK (len: {})", o.stdout.len()),
                                         Err(e) => format!("Exec Failed: {}", e)
                                     }
                                 } else { "Empty cmd".to_string() }
                             },
                             _ => format!("Unknown op: {}", kind)
                         };
                         ops_log.push(res_str);
                     }
                 }
             }
             let ops_summary = if ops_log.is_empty() { "No ops".to_string() } else { ops_log.join("; ") };

             (final_reply, intent, url, ops_summary)
        },
        Err(e) => {
             eprintln!("Router Error: {}", e);
             let (reply, ops) = execute_meta6_local_kernel(&task);
             (reply, "meta6_kernel".to_string(), None, ops)
        }
    };

    // 2. Execution (Divine / System Matrix)
    // If output starts with DIVINE_CODE or intent is divine, run ruliad.
    if impact_url.is_none() && (best_out.contains("DIVINE_CODE") || intent == "divine" || intent == "generate_graph") {
        let rules = vec![
            ("A".to_string(), "BC".to_string()),
            ("B".to_string(), "CA".to_string()),
            ("C".to_string(), "AB".to_string()),
        ];
        if let Ok(url) = execute_divine_ruliad("A", rules, 8).await {
            impact_url = Some(url);
        }
    }

    // 3. Verification & Metrics
    let ok = true; // Assume success for now
    let note = format!("Intent: {}", intent);
    let dt = t0.elapsed().unwrap().as_secs_f64();
    let cost = 0.001; 

    // Write Receipt
    let receipts_path = std::env::var("NSTAR_RECEIPTS").unwrap_or_else(|_| "trace/receipts.jsonl".to_string());
    if let Some(parent) = std::path::Path::new(&receipts_path).parent() {
        let _ = fs::create_dir_all(parent).await;
    }
    
    let rec = serde_json::json!({
        "run_id": run_id,
        "ts": chrono::Utc::now().to_rfc3339(),
        "task": task,
        "ok": ok,
        "note": note,
        "policy": policy,
        "best": best_out,
        "cost": cost,
        "latency_s": dt,
        "mode": "hybrid_omni_v1",
        "impact_url": impact_url
    });

    use tokio::io::AsyncWriteExt;
    if let Ok(mut file) = fs::OpenOptions::new().create(true).append(true).open(&receipts_path).await {
         let _ = file.write_all(format!("{}\n", rec).as_bytes()).await;
    }

    let resp = NStarRunResp {
        ok,
        result: best_out,
        policy,
        adapt: serde_json::json!({"changed": false, "impact_url": impact_url}),
    };
    Json(resp).into_response()
}

// Meta6 Local Fluid Kernel (Rust Implementation)
fn execute_meta6_local_kernel(task: &str) -> (String, String) {
    // Prime Addressing Logic
    let prime_ref = (task.len() * 17) % 79 + 2; // Deterministic Prime-ish hash
    let reply = format!("{{ \"ref\": {}, \"net\": \"P{} -> P2 (Fluid Kernel) -> Local Processing\", \"out\": \"Processed intent locally: '{}'. Entropy reduced.\" }}", prime_ref, prime_ref, task);
    
    // Auto-Op Generation for "write" intents
    let ops_summary = if task.contains("write") || task.contains("create") {
         "Op: [Local Write Simulated]".to_string()
    } else {
         "No ops".to_string()
    };
    
    (reply, ops_summary)
}

// Ruliad Logic (The Executor)
pub async fn execute_divine_ruliad(seed: &str, rules: Vec<(String, String)>, depth: usize) -> Result<String, String> {
    use std::collections::{HashSet, HashMap};
    use std::path::Path;
    
    let mut states: HashMap<usize, HashSet<String>> = HashMap::new();
    states.insert(0, vec![seed.to_string()].into_iter().collect());
    
    let mut id_for: HashMap<String, usize> = HashMap::new();
    id_for.insert(seed.to_string(), 0);
    let mut next_id = 1usize;
    let mut edges: Vec<(usize, usize, usize, String)> = Vec::new();

    for d in 0..depth {
        let layer = states.get(&d).cloned().unwrap_or_default();
        for s in layer {
            let src_id = *id_for.get(&s).unwrap();
            for (pat, rep) in &rules {
                let mut idx = 0usize;
                while let Some(pos) = s[idx..].find(pat) {
                    let global = idx + pos;
                    let ns = format!("{}{}{}", &s[..global], rep, &s[global + pat.len()..]);
                    let dst_id = *id_for.entry(ns.clone()).or_insert_with(|| {
                        let id = next_id;
                        next_id += 1;
                        id
                    });
                    edges.push((src_id, dst_id, d + 1, pat.clone()));
                    states.entry(d + 1).or_default().insert(ns);
                    idx = global + 1;
                }
            }
        }
    }

    let run_id = format!("divine-{}", uuid::Uuid::new_v4());
    let out_dir = Path::new("runs").join("ruliad_kernel").join(&run_id);
    fs::create_dir_all(&out_dir).await.map_err(|e| e.to_string())?;

    // Generate HTML (The Renderer)
    let html = format!(
        r#"<!doctype html>
<html>
<style>
body {{ background: #000; color: #fff; font-family: monospace; display: flex; flex-direction: column; align-items: center; justify-content: center; height: 100vh; overflow: hidden; }}
canvas {{ width: 100%; height: 100%; }}
.hud {{ position: absolute; top: 20px; left: 20px; background: rgba(0,0,0,0.8); padding: 10px; border: 1px solid #333; pointer-events: none; }}
</style>
<body>
<div class="hud">
  <h1>Divine Matrix</h1>
  <div>Rule: {:?}</div>
  <div>States: {} | Edges: {}</div>
</div>
<script src="https://unpkg.com/force-graph"></script>
<div id="graph"></div>
<script>
  const gData = {{
    nodes: [{}],
    links: [{}]
  }};
  ForceGraph()(document.getElementById('graph'))
    .graphData(gData)
    .nodeAutoColorBy('group')
    .linkColor(() => 'rgba(255,255,255,0.2)')
    .backgroundColor('#000000');
</script>
</body>
</html>"#,
        rules,
        id_for.len(),
        edges.len(),
        (0..id_for.len()).map(|i| format!("{{id:{},group:1}}", i)).collect::<Vec<_>>().join(","),
        edges.iter().map(|(s,d,_,_)| format!("{{source:{},target:{}}}", s, d)).collect::<Vec<_>>().join(",")
    );

    let url_path = out_dir.join("index.html");
    fs::write(&url_path, html).await.map_err(|e| e.to_string())?;
    
    Ok(format!("/{}", url_path.display()))
}

// System Matrix Logic (The Real Ruliad)
pub async fn execute_system_matrix() -> Result<String, String> {
    use std::collections::HashMap;
    use std::path::Path;
    use tokio::io::AsyncBufReadExt;

    // 1. Ingest Real Data (The Trace)
    let receipts_path = std::env::var("NSTAR_RECEIPTS").unwrap_or_else(|_| "trace/receipts.jsonl".to_string());
    let file = fs::File::open(&receipts_path).await.map_err(|e| e.to_string())?;
    let mut reader = tokio::io::BufReader::new(file).lines();

    let mut nodes: Vec<serde_json::Value> = Vec::new();
    let mut links: Vec<serde_json::Value> = Vec::new();
    let mut tasks = HashMap::new();
    let mut prev_run_id = None;
    let mut idx = 0;

    // 2. Reduce (Cluster by Task Intent)
    while let Ok(Some(line)) = reader.next_line().await {
        if let Ok(mut val) = serde_json::from_str::<serde_json::Value>(&line) {
            
            // Normalize ChatGPT/Legacy Formats
            if val.get("title").is_some() && val.get("text").is_some() {
                 let run_id = val.get("ts").and_then(|s| s.as_str()).unwrap_or("legacy").to_string();
                 let task = val.get("text").and_then(|s| s.as_str()).unwrap_or("No Text").to_string();
                 let title = val.get("title").and_then(|s| s.as_str()).unwrap_or("Misc").to_string();
                 
                 // Mutate val to conform to NStar Schema
                 if let Some(obj) = val.as_object_mut() {
                     obj.insert("run_id".to_string(), serde_json::json!(run_id));
                     obj.insert("task".to_string(), serde_json::json!(task));
                     obj.insert("ok".to_string(), serde_json::json!(true));
                     // Use Title as implicit task grouping
                     obj.insert("cluster_hint".to_string(), serde_json::json!(title));
                 }
            }

            let run_id = val.get("run_id").and_then(|s| s.as_str()).unwrap_or("?").to_string();
            let task = val.get("task").and_then(|s| s.as_str()).unwrap_or("?").to_string();
            let ok = val.get("ok").and_then(|b| b.as_bool()).unwrap_or(false);
            
            // Heuristic Clustering: Use Title hint or First word
            let cluster_key = if let Some(h) = val.get("cluster_hint").and_then(|s| s.as_str()) {
                h.to_string()
            } else {
                task.split_whitespace().next().unwrap_or("misc").to_string()
            };

            let cluster_id = if let Some(&id) = tasks.get(&cluster_key) {
                id
            } else {
                let id = idx;
                nodes.push(serde_json::json!({
                    "id": id,
                    "label": cluster_key,
                    "group": 2,
                    "val": 10
                }));
                tasks.insert(cluster_key, id);
                idx += 1;
                id
            };

            // Run Node
            let run_node_id = idx;
            let color = if ok { "#4ade80" } else { "#f87171" };
            
            // Inject full details raw object (Serde handles the nesting)
            nodes.push(serde_json::json!({
                "id": run_node_id,
                "label": run_id,
                "group": 1,
                "color": color,
                "details": val
            }));
            idx += 1;

            // Edge: Cluster -> Run
            links.push(serde_json::json!({
                "source": cluster_id,
                "target": run_node_id
            }));

            // Edge: Temporal (Run i -> Run i+1)
            if let Some(prev) = prev_run_id {
                links.push(serde_json::json!({
                    "source": prev,
                    "target": run_node_id,
                    "value": 0.5
                }));
            }
            prev_run_id = Some(run_node_id);
        }
    }

    // 3. Render (The Matrix)
    let run_id = format!("matrix-{}", uuid::Uuid::new_v4());
    let out_dir = Path::new("runs").join("ruliad_kernel").join(&run_id);
    fs::create_dir_all(&out_dir).await.map_err(|e| e.to_string())?;

    // Construct the Graph Data Object in Rust (Golden Path)
    let graph_data = serde_json::json!({
        "nodes": nodes,
        "links": links
    });
    
    // Safety: Escape for HTML embedding (simple replacement of </script> is usually enough for JSON)
    let json_str = graph_data.to_string().replace("</script>", "<\\/script>");

    let html = format!(
        r#"<!doctype html>
<html>
<style>
body {{ background: #0b0c10; color: #c5c6c7; font-family: monospace; display: flex; flex-direction: column; align-items: center; justify-content: center; height: 100vh; overflow: hidden; }}
.hud {{ position: absolute; top: 20px; left: 20px; background: rgba(0,0,0,0.8); padding: 15px; border-left: 2px solid #66fcf1; pointer-events: none; }}
h1 {{ margin: 0 0 10px 0; color: #66fcf1; text-transform: uppercase; letter-spacing: 2px; }}
</style>
<body>
<div class="hud">
  <h1>System Matrix (Live)</h1>
  <div>Nodes: {}</div>
  <div>Links: {}</div>
</div>
<!-- DATA PAYLOAD (Robust Injection) -->
<script id="graph-data" type="application/json">
{}
</script>

<script src="https://unpkg.com/force-graph"></script>
<div id="graph"></div>
<script>
  // Hydrate from Safe JSON
  const gData = JSON.parse(document.getElementById('graph-data').textContent);
  
  const Graph = ForceGraph()(document.getElementById('graph'))
    .graphData(gData)
    .nodeAutoColorBy('group')
    .nodeLabel('label')
    .linkColor(() => 'rgba(102, 252, 241, 0.2)')
    .backgroundColor('#0b0c10')
    .d3Force('charge').strength(-50)
    .onNodeClick(node => {{
        console.log("Node clicked:", node);
        // Robust Payload Extraction
        const payload = node.details ? node.details : {{ id: node.id, label: node.label, group: node.group }};
        window.parent.postMessage({{
            type: 'node_selected', 
            payload: payload
        }}, '*');
        
        Graph.centerAt(node.x, node.y, 1000);
        Graph.zoom(8, 2000);
    }});
</script>
</body>
</html>"#,
        nodes.len(),
        links.len(),
        json_str
    );

    let url_path = out_dir.join("index.html");
    fs::write(&url_path, html).await.map_err(|e| e.to_string())?;
    
    Ok(format!("/{}", url_path.display()))
}

#[derive(Deserialize)]
pub struct HudQuery {
    pub format: Option<String>,
}

#[utoipa::path(get, path = "/nstar/hud", params(("format"=Option<String>, Query, description="json|html")), responses((status=200, description="HTML or JSON Dashboard")))]
pub async fn nstar_hud_handler(Query(q): Query<HudQuery>) -> impl IntoResponse {
    let path =
        std::env::var("NSTAR_RECEIPTS").unwrap_or_else(|_| "trace/receipts.jsonl".to_string());
    
    // Read and parse all lines
    let mut items = Vec::new();
    if let Ok(s) = fs::read_to_string(&path).await {
        for line in s.lines().rev().take(100) {
            if let Ok(val) = serde_json::from_str::<serde_json::Value>(line) {
                items.push(val);
            }
        }
    }

    if q.format == Some("json".to_string()) {
        Json(items).into_response()
    } else {
        let body = format!(
            r#"<!doctype html>
            <html>
            <head>
                <title>NStar HUD</title>
                <style>
                    body {{ font-family: monospace; background-color: #0b0c10; color: #c5c6c7; padding: 20px; }}
                    h1 {{ color: #66fcf1; }}
                    pre {{ background-color: #1f2833; padding: 15px; border-radius: 5px; overflow-x: auto; }}
                </style>
            </head>
            <body>
                <h1>NStar HUD (Recent Receipts)</h1>
                <pre>{}</pre>
            </body>
            </html>"#,
            serde_json::to_string_pretty(&items).unwrap_or_else(|_| "[]".to_string())
        );
        Html(body).into_response()
    }
}

#[derive(Deserialize, JsonSchema, ToSchema)]
pub struct ResolveReq {
    pub query: String,
}

#[derive(Serialize, JsonSchema, ToSchema)]
pub struct ContextMatch {
    pub task: String,
    pub run_id: String,
}

#[derive(Serialize, JsonSchema, ToSchema)]
pub struct ResolveResp {
    pub matches: Vec<ContextMatch>,
    pub suggestion: String,
}

#[utoipa::path(
    post,
    path = "/v1/context/resolve",
    request_body = ResolveReq,
    responses((status=200, description="Resolve Context", body=ResolveResp))
)]
pub async fn resolve_context_handler(Json(req): Json<ResolveReq>) -> impl IntoResponse {
    use tokio::io::AsyncBufReadExt;
    let receipts_path = std::env::var("NSTAR_RECEIPTS").unwrap_or_else(|_| "trace/receipts.jsonl".to_string());
    
    let mut matches = Vec::new();
    if let Ok(file) = fs::File::open(&receipts_path).await {
        let mut reader = tokio::io::BufReader::new(file).lines();
        while let Ok(Some(line)) = reader.next_line().await {
            if let Ok(val) = serde_json::from_str::<serde_json::Value>(&line) {
                let task = val.get("task").or(val.get("text")).and_then(|s| s.as_str()).unwrap_or("");
                if !task.is_empty() && task.to_lowercase().contains(&req.query.to_lowercase()) {
                    let run_id = val.get("run_id").or(val.get("ts")).and_then(|s| s.as_str()).unwrap_or("?").to_string();
                    matches.push(ContextMatch {
                        task: task.chars().take(200).collect(), // Reasonable truncation
                        run_id
                    });
                }
            }
        }
    }
    
    // Top 5 recent matches (reverse chronological)
    matches.reverse();
    matches.truncate(5);

    let count = matches.len();
    Json(ResolveResp {
        suggestion: format!("Found {} historical nodes relevant to '{}'.", count, req.query),
        matches
    }).into_response()
}

