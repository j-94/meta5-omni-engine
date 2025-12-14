pub mod bits;
pub mod executor;
pub mod goals;
pub mod golden;
pub mod kernel;
pub mod meta_prompt;
pub mod policy;
pub mod router;
pub mod types;
pub mod validate;
pub mod verify;
pub mod graphs;
pub mod thread_report;
pub mod wiki;

use std::{fs, path::{Path, PathBuf}, time::UNIX_EPOCH};

use crate::engine::validate::set_align_boost;
use anyhow::Context;
use bits::Bits;
use chrono::{DateTime, Utc};
use kernel::{ExtendedBits, KernelLoop, Meta2Proposal};
use serde::Deserialize;
use serde_json::json;
use sha2::{Digest, Sha256};
use types::{Manifest, Policy};
use uuid::Uuid;

static mut KERNEL: Option<KernelLoop> = None;
static mut KPI_HISTORY: Vec<f32> = Vec::new();
static mut TRACE_HISTORY: Vec<ExtendedBits> = Vec::new();

#[derive(Debug, Deserialize)]
struct PoliciesFile {
    #[serde(default)]
    meta3_build: Option<Meta3BuildPolicy>,
}

#[derive(Debug, Deserialize)]
struct Meta3BuildPolicy {
    #[serde(default)]
    default_cmd: Option<String>,
    #[serde(default)]
    forbid_global_installs: Option<bool>,
}

fn contains_global_install(cmd: &str) -> bool {
    let s = cmd.to_lowercase();
    s.contains("npm install -g")
        || s.contains(" npm -g")
        || s.contains("brew install")
        || s.contains("sudo ")
}

fn load_meta3_build_cmd_from_policies() -> Option<String> {
    let path = std::env::var("ONE_ENGINE_POLICIES_FILE")
        .ok()
        .unwrap_or_else(|| "config/policies.yaml".to_string());
    let raw = fs::read_to_string(&path).ok()?;
    let parsed: PoliciesFile = serde_yaml::from_str(&raw).ok()?;
    let policy = parsed.meta3_build?;
    let cmd = policy.default_cmd?.trim().to_string();
    if cmd.is_empty() {
        return None;
    }
    if policy.forbid_global_installs.unwrap_or(true) && contains_global_install(&cmd) {
        return Some(
            "echo \"[meta3.build] blocked: global installs/sudo in policy\"; exit 2".to_string(),
        );
    }
    Some(cmd)
}

pub async fn run(
    goal_id: &str,
    inputs: serde_json::Value,
    policy: &Policy,
) -> anyhow::Result<(Manifest, ExtendedBits, Option<Meta2Proposal>)> {
    let kernel = unsafe { KERNEL.get_or_insert_with(KernelLoop::new) };
    let mut bits = ExtendedBits::init();
    // Freshness filter: set Δ when any context item is expired
    if let Some(ctx_items) = inputs.get("context").and_then(|v| v.as_array()) {
        for item in ctx_items {
            if let (Some(ts), Some(ttl)) = (
                item.get("ts").and_then(|v| v.as_str()),
                item.get("ttl").and_then(|v| v.as_i64()),
            ) {
                if let Ok(parsed) =
                    chrono::DateTime::parse_from_rfc3339(ts).map(|dt| dt.with_timezone(&Utc))
                {
                    let age = (Utc::now() - parsed).num_seconds();
                    if age > ttl {
                        bits.d = 1.0;
                    }
                }
            }
        }
    }

    // Set uncertainty based on goal difficulty
    bits.u = match goal_id {
        id if id.contains("easy") => 0.1,
        id if id.contains("hard") => 0.7,
        id if id.contains("impossible") => 0.9,
        _ => 0.3,
    };

    // Ask-Act gate (inherent)
    if !kernel.ask_act_gate(&bits) {
        return Err(anyhow::anyhow!(
            "Ask-Act gate failed: A={}, P={}, Δ={}",
            bits.a,
            bits.p,
            bits.d
        ));
    }

    // Evidence gate (inherent)
    let needs_verification = !kernel.evidence_gate(&bits);
    if needs_verification {
        tracing::info!(
            "Evidence gate triggered: U={:.2} >= τ={:.2}",
            bits.u,
            kernel.l2_params.confidence_gate_tau
        );
        // In real system: run dry-run first
    }

    // Handle align.sota: apply alignment boost, echo message
    if goal_id.contains("align.sota") {
        set_align_boost(0.1);
        let message = inputs
            .get("message")
            .and_then(|v| v.as_str())
            .unwrap_or("align.sota");
        let action =
            executor::Action::Cli(format!("echo {}", shell_escape::escape(message.into())));
        let res = executor::execute(action, policy).await?;
        let manifest = Manifest {
            run_id: format!("r-{}", uuid::Uuid::new_v4()),
            goal_id: goal_id.to_string(),
            deliverables: vec![],
            evidence: serde_json::json!({
                "stdout": res.stdout,
                "alignment_boost": 0.1,
                "meta2_triggered": false
            }),
            bits: bits.clone().into(),
        };
        bits.t = (bits.t + 0.1).min(1.5);
        return Ok((manifest, bits, None));
    }

    // Handle research.read: read a file and return snippet + stats
    if goal_id.contains("research.read") {
        let path = inputs
            .get("path")
            .or_else(|| inputs.get("context_path"))
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow::anyhow!("path or context_path is required"))?;

        let content = std::fs::read_to_string(path)
            .map_err(|e| anyhow::anyhow!("read failed for {}: {}", path, e))?;

        let lines = content.lines().count();
        let bytes = content.as_bytes().len();
        let snippet: String = content.chars().take(2000).collect();
        let summary: String = {
            let first_line = content.lines().next().unwrap_or("");
            format!("lines={} bytes={} first_line={}", lines, bytes, first_line)
        };

        let sha = format!("{:x}", Sha256::digest(content.as_bytes()));
        let meta = fs::metadata(path).ok();
        let mtime = meta
            .and_then(|m| m.modified().ok())
            .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);

        let expected = inputs.get("context_manifest");
        let mut stale = false;
        let mut stale_reason = None;
        if let Some(exp) = expected {
            if let Some(exp_sha) = exp.get("sha256").and_then(|v| v.as_str()) {
                if exp_sha != sha {
                    stale = true;
                    stale_reason = Some("sha256_mismatch");
                }
            }
            if let Some(exp_m) = exp.get("mtime").and_then(|v| v.as_i64()) {
                if exp_m != mtime {
                    stale = true;
                    stale_reason = Some("mtime_mismatch");
                }
            }
        }

        bits.u = 0.2;
        bits.e = 0.0;
        bits.t = if stale { 0.4 } else { 0.95 };

        let manifest = Manifest {
            run_id: format!("r-{}", uuid::Uuid::new_v4()),
            goal_id: goal_id.to_string(),
            deliverables: vec![],
            evidence: serde_json::json!({
                "path": path,
                "lines": lines,
                "bytes": bytes,
                "snippet": snippet,
                "summary": summary,
                "sha256": sha,
                "mtime": mtime,
                "stale": stale,
                "stale_reason": stale_reason,
                "actual_success": !stale,
                "expected_success": true,
                "meta2_triggered": false
            }),
            bits: bits.clone().into(),
        };

        return Ok((manifest, bits, None));
    }

    // Handle wiki.generate: generate a local wiki snapshot under META3_ROOT/runs/wiki/<run_id>/
    if goal_id.contains("wiki.generate") {
        let external_run_id = inputs
            .get("__run_id")
            .and_then(|v| v.as_str())
            .unwrap_or_else(|| "wiki-unknown");

        let res = wiki::generate(external_run_id).await?;
        bits.u = 0.2;
        bits.e = 0.0;
        bits.t = 0.95;

        let manifest = Manifest {
            run_id: format!("r-{}", uuid::Uuid::new_v4()),
            goal_id: goal_id.to_string(),
            deliverables: vec![
                res.out_dir.join("index.html").display().to_string(),
                res.out_dir.join("static.html").display().to_string(),
                res.out_dir.join("index.md").display().to_string(),
                res.out_dir.join("files.txt").display().to_string(),
                res.out_dir.join("topfiles.txt").display().to_string(),
                res.out_dir.join("folder_summary.md").display().to_string(),
            ],
            evidence: serde_json::json!({
                "actual_success": true,
                "expected_success": true,
                "wiki_dir": res.out_dir.display().to_string(),
                "index_html_url": format!("/runs/wiki/{}/index.html", external_run_id),
                "static_html_url": format!("/runs/wiki/{}/static.html", external_run_id),
                "files_count": res.files_count,
                "topfiles_count": res.topfiles_count,
                "readme_copied": res.readme_copied,
                "stdout": format!("[wiki.generate] wrote {} ({} files, {} topfiles)", res.out_dir.display(), res.files_count, res.topfiles_count),
                "meta2_triggered": bits.m > 0.0
            }),
            bits: bits.clone().into(),
        };

        return Ok((manifest, bits, None));
    }

    // Handle graphs.thread: render a coherent graph of a chat thread (events -> receipts)
    if goal_id.contains("graphs.thread") || goal_id.contains("graph.thread") {
        let external_run_id = inputs
            .get("__run_id")
            .and_then(|v| v.as_str())
            .unwrap_or("graph-unknown");
        let user_id = inputs
            .get("user_id")
            .and_then(|v| v.as_str())
            .unwrap_or("demo");
        let thread = inputs
            .get("thread")
            .and_then(|v| v.as_str())
            .unwrap_or("t-default");
        let max_events = inputs
            .get("max_events")
            .and_then(|v| v.as_u64())
            .unwrap_or(80) as usize;
        let content_chars = inputs
            .get("content_chars")
            .and_then(|v| v.as_u64())
            .unwrap_or(120) as usize;
        let label_mode = inputs
            .get("label_mode")
            .and_then(|v| v.as_str())
            .unwrap_or("nl+goal")
            .to_string();
        let filter_text = inputs
            .get("filter_text")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let filter_goal = inputs
            .get("filter_goal")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let recursive = inputs
            .get("recursive")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let depth = inputs
            .get("depth")
            .and_then(|v| v.as_u64())
            .unwrap_or(1) as usize;
        let max_nodes = inputs
            .get("max_nodes")
            .and_then(|v| v.as_u64())
            .unwrap_or(200) as usize;
        let include_bits = inputs
            .get("include_bits")
            .and_then(|v| v.as_bool())
            .unwrap_or(true);

        let res = graphs::thread_graph_with_opts(
            external_run_id,
            user_id,
            thread,
            graphs::ThreadGraphOpts {
                max_events,
                content_chars,
                label_mode,
                filter_text,
                filter_goal,
                recursive,
                depth,
                max_nodes,
                include_bits,
            },
        )?;
        bits.u = 0.2;
        bits.e = 0.0;
        bits.t = 0.95;

        let manifest = Manifest {
            run_id: format!("r-{}", uuid::Uuid::new_v4()),
            goal_id: goal_id.to_string(),
            deliverables: vec![
                res.out_dir.join("index.html").display().to_string(),
                res.out_dir.join("graph.dot").display().to_string(),
                res.out_dir.join("events.json").display().to_string(),
            ],
            evidence: serde_json::json!({
                "actual_success": true,
                "expected_success": true,
                "user_id": user_id,
                "thread": res.thread,
                "nodes": res.nodes,
                "edges": res.edges,
                "index_html_url": format!("/runs/graphs/{}/index.html", external_run_id),
                "dot_url": format!("/runs/graphs/{}/graph.dot", external_run_id),
                "events_url": format!("/runs/graphs/{}/events.json", external_run_id),
                "stdout": format!("[graphs.thread] wrote {} ({} nodes)", res.out_dir.display(), res.nodes),
                "meta2_triggered": bits.m > 0.0
            }),
            bits: bits.clone().into(),
        };

        return Ok((manifest, bits, None));
    }

    // Handle graphs.receipts: graph recent receipts into a coherent timeline
    if goal_id.contains("graphs.receipts") || goal_id.contains("graph.receipts") {
        let external_run_id = inputs
            .get("__run_id")
            .and_then(|v| v.as_str())
            .unwrap_or("graph-unknown");
        let limit = inputs
            .get("limit")
            .and_then(|v| v.as_u64())
            .unwrap_or(200) as usize;

        let res = graphs::receipts_graph(external_run_id, limit)?;
        bits.u = 0.2;
        bits.e = 0.0;
        bits.t = 0.95;

        let manifest = Manifest {
            run_id: format!("r-{}", uuid::Uuid::new_v4()),
            goal_id: goal_id.to_string(),
            deliverables: vec![
                res.out_dir.join("index.html").display().to_string(),
                res.out_dir.join("graph.dot").display().to_string(),
                res.out_dir.join("events.json").display().to_string(),
            ],
            evidence: serde_json::json!({
                "actual_success": true,
                "expected_success": true,
                "nodes": res.nodes,
                "edges": res.edges,
                "index_html_url": format!("/runs/graphs/{}/index.html", external_run_id),
                "dot_url": format!("/runs/graphs/{}/graph.dot", external_run_id),
                "events_url": format!("/runs/graphs/{}/events.json", external_run_id),
                "stdout": format!("[graphs.receipts] wrote {} ({} nodes)", res.out_dir.display(), res.nodes),
                "meta2_triggered": bits.m > 0.0
            }),
            bits: bits.clone().into(),
        };

        return Ok((manifest, bits, None));
    }

    // Handle graphs.api: show "API jumping" (endpoint hops + mutations) from runs/api_trace.jsonl
    if goal_id.contains("graphs.api") || goal_id.contains("graph.api") {
        let external_run_id = inputs
            .get("__run_id")
            .and_then(|v| v.as_str())
            .unwrap_or("graph-unknown");
        let limit = inputs
            .get("limit")
            .and_then(|v| v.as_u64())
            .unwrap_or(800) as usize;
        let only_mutations = inputs
            .get("only_mutations")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let collapse = inputs
            .get("collapse")
            .and_then(|v| v.as_bool())
            .unwrap_or(true);
        let run_id = inputs
            .get("run_id")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let thread = inputs
            .get("thread")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let user_id = inputs
            .get("user_id")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());

        let res = graphs::api_graph(
            external_run_id,
            graphs::ApiGraphOpts {
                limit,
                only_mutations,
                run_id,
                thread,
                user_id,
                collapse,
            },
        )?;
        bits.u = 0.2;
        bits.e = 0.0;
        bits.t = 0.95;

        let manifest = Manifest {
            run_id: format!("r-{}", uuid::Uuid::new_v4()),
            goal_id: goal_id.to_string(),
            deliverables: vec![
                res.out_dir.join("index.html").display().to_string(),
                res.out_dir.join("graph.dot").display().to_string(),
                res.out_dir.join("events.json").display().to_string(),
            ],
            evidence: serde_json::json!({
                "actual_success": true,
                "expected_success": true,
                "nodes": res.nodes,
                "edges": res.edges,
                "index_html_url": format!("/runs/graphs/{}/index.html", external_run_id),
                "dot_url": format!("/runs/graphs/{}/graph.dot", external_run_id),
                "events_url": format!("/runs/graphs/{}/events.json", external_run_id),
                "stdout": format!("[graphs.api] wrote {} ({} nodes)", res.out_dir.display(), res.nodes),
                "meta2_triggered": bits.m > 0.0
            }),
            bits: bits.clone().into(),
        };

        return Ok((manifest, bits, None));
    }

    // Handle meta3.build: run real build/lint/tests in META3_PATH (or provided repo_path)
    if goal_id.contains("meta3.build") {
        // Prefer per-run override, then env, then fallback.
        let repo = inputs
            .get("repo_path")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .or_else(|| std::env::var("META3_PATH").ok())
            .unwrap_or_else(|| "meta3-monorepo".to_string());
        let build_cmd = inputs
            .get("build_cmd")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .or_else(|| std::env::var("META3_BUILD_DEFAULT_CMD").ok())
            .or_else(load_meta3_build_cmd_from_policies)
            .unwrap_or_else(|| {
                // Last-resort fallback (prefer config/policies.yaml).
                "echo \"[meta3.build] start\"; npx turbo run build --filter '!@meta3/cli' --filter '!@meta3/kernel' --no-cache; status=$?; echo \"[meta3.build] done\"; exit $status".to_string()
            });

        let run_id = format!("r-{}", Uuid::new_v4());
        let meta_root =
            PathBuf::from(std::env::var("META3_ROOT").unwrap_or_else(|_| ".".to_string()));
        let log_dir = meta_root.join("runs/meta3-build");
        fs::create_dir_all(&log_dir)
            .with_context(|| format!("failed to create log directory {}", log_dir.display()))?;
        let log_path = log_dir.join(format!("{}.log", run_id));

        let cmd = format!(
            "cd {} && {} 2>&1",
            shell_escape::escape(repo.clone().into()),
            build_cmd
        );
        let action = executor::Action::Cli(cmd);
        let res = executor::execute(action, policy).await?;

        let combined = if res.stderr.is_empty() {
            res.stdout.clone()
        } else {
            format!("STDOUT:\\n{}\\nSTDERR:\\n{}", res.stdout, res.stderr)
        };

        fs::write(&log_path, combined.as_bytes())
            .with_context(|| format!("failed to write log {}", log_path.display()))?;

        if res.drift {
            bits.d = 1.0;
        }
        if !res.ok {
            bits.e = 1.0;
            bits.u = (bits.u + 0.2).min(1.0);
        }

        let passed = verify::check_minimal(&res);
        let legacy_bits: types::Bits = bits.clone().into();
        bits.t = policy::trust_from(passed, &legacy_bits);
        if passed != true {
            bits.t *= 0.8;
        }

        unsafe {
            TRACE_HISTORY.push(bits.clone());
            if TRACE_HISTORY.len() > 100 {
                TRACE_HISTORY.remove(0);
            }
        }

        let manifest = Manifest {
            run_id: run_id.clone(),
            goal_id: goal_id.to_string(),
            deliverables: vec![log_path.display().to_string()],
            evidence: serde_json::json!({
                "stdout": res.stdout,
                "repo_path": repo,
                "build_cmd": build_cmd,
                "log_path": log_path.display().to_string(),
                "expected_success": true,
                "actual_success": passed,
                "run_id": run_id,
                "meta2_triggered": bits.m > 0.0
            }),
            bits: bits.clone().into(),
        };

        return Ok((manifest, bits, None));
    }

    // Handle ruliad.kernel: generate a multiway slice + causal graph and artifacts
    if goal_id.contains("ruliad") {
        use std::collections::{HashMap, HashSet};

        let seed = inputs
            .get("seed")
            .and_then(|v| v.as_str())
            .unwrap_or("01")
            .to_string();
        let depth = inputs.get("depth").and_then(|v| v.as_u64()).unwrap_or(8) as usize;

        // Rules: default to [(01 -> 10), (10 -> 011)]
        let rules: Vec<(String, String)> = inputs
            .get("rules")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|pair| {
                        if let (Some(a), Some(b)) = (
                            pair.get(0).and_then(|x| x.as_str()),
                            pair.get(1).and_then(|x| x.as_str()),
                        ) {
                            Some((a.to_string(), b.to_string()))
                        } else {
                            None
                        }
                    })
                    .collect()
            })
            .unwrap_or_else(|| vec![("01".into(), "10".into()), ("10".into(), "011".into())]);

        // BFS over string rewrites to build multiway graph
        let mut states: HashMap<usize, HashSet<String>> = HashMap::new();
        states.insert(0, [seed.clone()].into_iter().collect());
        let mut id_for: HashMap<String, usize> = HashMap::new();
        id_for.insert(seed.clone(), 0);
        let mut next_id = 1usize;
        let mut edges: Vec<(usize, usize, usize, String)> = Vec::new();

        for d in 0..depth {
            let layer = states.get(&d).cloned().unwrap_or_default();
            for s in layer {
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
                        edges.push((*id_for.get(&s).unwrap(), dst_id, d + 1, pat.clone()));
                        states.entry(d + 1).or_default().insert(ns);
                        idx = global + 1;
                    }
                }
            }
        }

        // Prepare output dir under runs/ruliad_kernel/<run_id>
        let run_id = format!("r-{}", Uuid::new_v4());
        let out_dir = Path::new("runs").join("ruliad_kernel").join(&run_id);
        fs::create_dir_all(&out_dir)
            .with_context(|| format!("failed to create {}", out_dir.display()))?;

        // states.jsonl
        let mut states_lines = Vec::new();
        let mut inv: Vec<String> = vec!["".into(); id_for.len()];
        for (k, v) in id_for.iter() {
            inv[*v] = k.clone();
        }
        for (sid, s) in inv.iter().enumerate() {
            let d = states
                .iter()
                .find_map(|(depth, set)| if set.contains(s) { Some(*depth) } else { None })
                .unwrap_or(0);
            states_lines.push(json!({ "id": sid, "string": s, "depth": d }).to_string());
        }
        fs::write(out_dir.join("states.jsonl"), states_lines.join("\n"))?;

        // edges.jsonl
        let mut edge_lines = Vec::new();
        for (src, dst, d, pat) in &edges {
            edge_lines.push(json!({ "src": src, "dst": dst, "depth": d, "rule": pat }).to_string());
        }
        fs::write(out_dir.join("edges.jsonl"), edge_lines.join("\n"))?;

        // multiway DOT
        let mut dot = String::from("digraph multiway {\nrankdir=LR;\n");
        for (sid, s) in inv.iter().enumerate() {
            dot.push_str(&format!("  n{} [label=\"{}\"];\n", sid, s));
        }
        for (src, dst, d, pat) in &edges {
            dot.push_str(&format!(
                "  n{} -> n{} [label=\"{}@{}\"];\n",
                src, dst, pat, d
            ));
        }
        dot.push_str("}\n");
        fs::write(out_dir.join("multiway.dot"), dot)?;

        // causal DOT (approx: same edges without depth labels)
        let mut causal = String::from("digraph causal {\nrankdir=LR;\n");
        for (sid, s) in inv.iter().enumerate() {
            causal.push_str(&format!("  n{} [label=\"{}\"];\n", sid, s));
        }
        for (src, dst, pat) in edges.iter().map(|(s, d, _, p)| (s, d, p)) {
            causal.push_str(&format!("  n{} -> n{} [label=\"{}\"];\n", src, dst, pat));
        }
        causal.push_str("}\n");
        fs::write(out_dir.join("causal.dot"), causal)?;

        // Minimal HTML viewer
        let html = format!(
            "<!doctype html><html><body><h1>Ruliad slice</h1><p>Rule {:?}, depth {}</p><pre id='multiway'></pre><pre id='causal'></pre><script>fetch('multiway.dot').then(r=>r.text()).then(t=>multiway.textContent=t);fetch('causal.dot').then(r=>r.text()).then(t=>causal.textContent=t);</script></body></html>",
            rules, depth
        );
        fs::write(out_dir.join("index.html"), html)?;

        bits.u = 0.1;
        bits.e = 0.0;
        bits.t = 0.95;

        let manifest = Manifest {
            run_id: run_id.clone(),
            goal_id: goal_id.to_string(),
            deliverables: vec![
                out_dir.join("states.jsonl").display().to_string(),
                out_dir.join("edges.jsonl").display().to_string(),
                out_dir.join("multiway.dot").display().to_string(),
                out_dir.join("causal.dot").display().to_string(),
                out_dir.join("index.html").display().to_string(),
            ],
            evidence: serde_json::json!({
                "rule": rules,
                "seed": seed,
                "depth": depth,
                "states": inv.len(),
                "edges": edges.len(),
                "expected_success": true,
                "actual_success": true,
                "meta2_triggered": false
            }),
            bits: bits.clone().into(),
        };

        return Ok((manifest, bits, None));
    }

    // Handle threads.report: render a human-friendly report of a chat thread (events -> receipts)
    if goal_id.contains("threads.report") || goal_id.contains("thread.report") {
        let external_run_id = inputs
            .get("__run_id")
            .and_then(|v| v.as_str())
            .unwrap_or("thread-unknown");
        let user_id = inputs
            .get("user_id")
            .and_then(|v| v.as_str())
            .unwrap_or("demo")
            .to_string();
        let thread = inputs
            .get("thread")
            .and_then(|v| v.as_str())
            .unwrap_or("auto")
            .to_string();
        let max_events = inputs
            .get("max_events")
            .and_then(|v| v.as_u64())
            .unwrap_or(200) as usize;
        let content_chars = inputs
            .get("content_chars")
            .and_then(|v| v.as_u64())
            .unwrap_or(220) as usize;

        let res = thread_report::generate(
            external_run_id,
            thread_report::ThreadReportOpts {
                user_id: user_id.clone(),
                thread: thread.clone(),
                max_events,
                content_chars,
            },
        )?;

        bits.u = 0.2;
        bits.e = 0.0;
        bits.t = 0.95;

        let manifest = Manifest {
            run_id: format!("r-{}", uuid::Uuid::new_v4()),
            goal_id: goal_id.to_string(),
            deliverables: vec![
                res.out_dir.join("index.html").display().to_string(),
                res.out_dir.join("report.json").display().to_string(),
            ],
            evidence: serde_json::json!({
                "expected_success": true,
                "actual_success": true,
                "user_id": user_id,
                "thread": res.thread,
                "nodes": res.nodes,
                "threads_dir": res.out_dir.display().to_string(),
                "index_html_url": format!("/runs/threads/{}/index.html", external_run_id),
                "report_json_url": format!("/runs/threads/{}/report.json", external_run_id),
                "stdout": format!("[threads.report] wrote {} ({} nodes)", res.out_dir.display(), res.nodes),
                "meta2_triggered": bits.m > 0.0
            }),
            bits: bits.clone().into(),
        };

        return Ok((manifest, bits, None));
    }

    // Handle shell.exec: run arbitrary shell command
    if goal_id.contains("shell.exec") {
        let cmd = inputs
            .get("cmd")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow::anyhow!("cmd is required"))?;

        let action = executor::Action::Cli(cmd.to_string());
        let res = executor::execute(action, policy).await?;

        bits.u = 0.2;
        bits.e = if res.ok { 0.0 } else { 1.0 };
        bits.t = if res.ok { 0.95 } else { 0.4 };

        let manifest = Manifest {
            run_id: format!("r-{}", uuid::Uuid::new_v4()),
            goal_id: goal_id.to_string(),
            deliverables: vec![],
            evidence: serde_json::json!({
                "cmd": cmd,
                "stdout": res.stdout,
                "stderr": res.stderr,
                "exit_ok": res.ok,
                "meta2_triggered": bits.m > 0.0
            }),
            bits: bits.clone().into(),
        };
        return Ok((manifest, bits, None));
    }

    // Handle file.write: write content to file
    if goal_id.contains("file.write") {
        let path_str = inputs
            .get("path")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow::anyhow!("path required"))?;
        let content = inputs
            .get("content")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow::anyhow!("content required"))?;

        let path = Path::new(path_str);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("failed to create dir {}", parent.display()))?;
        }

        fs::write(path, content).with_context(|| format!("failed to write {}", path.display()))?;

        bits.u = 0.1;
        bits.e = 0.0;
        bits.t = 1.0;

        let manifest = Manifest {
            run_id: format!("r-{}", uuid::Uuid::new_v4()),
            goal_id: goal_id.to_string(),
            deliverables: vec![path.display().to_string()],
            evidence: serde_json::json!({
                "path": path.display().to_string(),
                "bytes": content.len(),
                "actual_success": true,
                "meta2_triggered": bits.m > 0.0
            }),
            bits: bits.clone().into(),
        };
        return Ok((manifest, bits, None));
    }

    // Handle meta.omni through LM persona
    if goal_id.contains("meta.omni") {
        let lm_result = goals::meta_omni::handle(&inputs).await?;

        // Extract reply from LM response
        let reply = lm_result
            .get("reply")
            .and_then(|v| v.as_str())
            .unwrap_or("⟂ no reply");
        let lm_bits = lm_result
            .get("bits")
            .cloned()
            .unwrap_or_else(|| serde_json::json!({}));

        // Update bits from LM response
        if let Some(a) = lm_bits.get("A").and_then(|v| v.as_f64()) {
            bits.a = a as f32;
        }
        if let Some(u) = lm_bits.get("U").and_then(|v| v.as_f64()) {
            bits.u = u as f32;
        }
        if let Some(p) = lm_bits.get("P").and_then(|v| v.as_f64()) {
            bits.p = p as f32;
        }
        if let Some(e) = lm_bits.get("E").and_then(|v| v.as_f64()) {
            bits.e = e as f32;
        }

        let action = executor::Action::Cli(format!("echo {}", shell_escape::escape(reply.into())));
        let res = executor::execute(action, policy).await?;

        let manifest = Manifest {
            run_id: format!("r-{}", uuid::Uuid::new_v4()),
            goal_id: goal_id.to_string(),
            deliverables: vec![],
            evidence: lm_result
                .get("manifest")
                .and_then(|m| m.get("evidence"))
                .cloned()
                .unwrap_or(lm_result.clone()),
            bits: bits.clone().into(),
        };

        return Ok((manifest, bits, None));
    }

    let message = if goal_id.contains("meta.omni") {
        // This branch won't be reached due to early return above
        "".to_string()
    } else {
        inputs
            .get("message")
            .and_then(|v| v.as_str())
            .unwrap_or("hello from one-engine")
            .to_string()
    };

    // Simulate different outcomes based on goal type
    let (action, expected_success) = match goal_id {
        id if id.contains("impossible") => (executor::Action::Cli("false".to_string()), false),
        id if id.contains("hard") => (
            executor::Action::Cli(format!(
                "sleep 0.1 && echo {}",
                shell_escape::escape(message.clone().into())
            )),
            true,
        ),
        _ => (
            executor::Action::Cli(format!(
                "echo {}",
                shell_escape::escape(message.clone().into())
            )),
            true,
        ),
    };

    let res = executor::execute(action, policy).await?;

    if res.drift {
        bits.d = 1.0;
    }
    if !res.ok {
        bits.e = 1.0;
        // L2 micro-adaptation: increase uncertainty for future similar tasks
        bits.u = (bits.u + 0.2).min(1.0);
    }

    let passed = verify::check_minimal(&res);
    let legacy_bits: types::Bits = bits.clone().into();
    bits.t = policy::trust_from(passed, &legacy_bits);

    // Adjust trust based on expectation vs reality
    if expected_success != passed {
        bits.t *= 0.7; // Lower trust when predictions are wrong
    }

    // L3 meta² check: should we propose policy changes?
    let current_evidence_coverage = bits.t; // Simplified: use trust as proxy
    unsafe {
        KPI_HISTORY.push(current_evidence_coverage);
    }

    let meta2_proposal = if kernel.should_wake_l3(unsafe { &KPI_HISTORY }) {
        bits.m = 1.0; // Meta-change bit set
        kernel.propose_meta2_change("evidence_coverage", current_evidence_coverage)
    } else {
        None
    };

    // STRUCTURAL VALIDATION: Enforce kernel contract
    if let Err(e) = kernel.validate_bits_complete(&bits) {
        return Err(anyhow::anyhow!("Kernel contract violation: {}", e));
    }

    // STRUCTURAL GATE: Ask-Act enforcement
    if goal_id.contains("action") || goal_id.contains("execute") {
        if let Err(e) = kernel.enforce_ask_act_gate(&bits) {
            tracing::warn!("Ask-Act gate blocked action: {}", e);
            // Return clarification request instead of proceeding
            let clarification = format!("Ask-Act gate: {}. Need P=1, A=1, Δ=0", e);
            let blocked_manifest = Manifest {
                run_id: uuid::Uuid::new_v4().to_string(),
                goal_id: goal_id.to_string(),
                deliverables: vec!["clarification_required".to_string()],
                evidence: serde_json::json!({"stdout": clarification, "stderr": "", "files": []}),
                bits: Bits {
                    a: bits.a,
                    u: bits.u,
                    p: bits.p,
                    e: bits.e,
                    d: bits.d,
                    i: bits.i,
                    r: bits.r,
                    t: bits.t,
                    m: bits.m,
                },
            };
            return Ok((blocked_manifest, bits, None));
        }
    }

    // Store trace for self-observation
    unsafe {
        TRACE_HISTORY.push(bits.clone());
        if TRACE_HISTORY.len() > 100 {
            TRACE_HISTORY.remove(0);
        }
    }

    let manifest = Manifest {
        run_id: format!("r-{}", Uuid::new_v4()),
        goal_id: goal_id.to_string(),
        deliverables: vec![],
        evidence: serde_json::json!({
            "stdout": res.stdout,
            "expected_success": expected_success,
            "actual_success": passed,
            "l2_params": kernel.l2_params,
            "meta2_triggered": bits.m > 0.0
        }),
        bits: bits.clone().into(), // Convert to legacy Bits for compatibility
    };

    Ok((manifest, bits, meta2_proposal))
}

// Convert ExtendedBits to legacy Bits for API compatibility
impl From<ExtendedBits> for types::Bits {
    fn from(ext: ExtendedBits) -> Self {
        Self {
            a: ext.a,
            u: ext.u,
            p: ext.p,
            e: ext.e,
            d: ext.d,
            i: ext.i,
            r: ext.r,
            t: ext.t,
            m: ext.m,
        }
    }
}
