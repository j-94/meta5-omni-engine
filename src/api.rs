use crate::engine::{
    self,
    types::{Bits, Manifest, Policy},
    validate,
};
use crate::integrations::{self, AgentGoal, UIState};
use crate::{meta, nstar};
use axum::{
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::{
        Html,
        sse::{Event, Sse},
        IntoResponse,
    },
    Json,
};
use once_cell::sync::Lazy;
use one_engine::research::{self, ResearchArtifact};
use regex::Regex;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};
use std::convert::Infallible;
use std::path::{Path as StdPath, PathBuf};
use std::time::{Duration, Instant, SystemTime};
use tokio::fs;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::sync::{broadcast, Mutex};
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::StreamExt;
use utoipa::{OpenApi, ToSchema};

#[derive(Clone)]
pub struct AppState {
    pub users: HashMap<String, UserContext>,
}

#[derive(Clone, Debug)]
pub struct UserContext {
    pub user_id: String,
    pub api_key: String,
    pub quota_remaining: u32,
    pub policy_overrides: Option<Policy>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct MpayloadCtx {
    kind: String, // run|chat|dsl
    user_id: Option<String>,
    thread: Option<String>,
    run_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Mpayload {
    goal_id: String,
    inputs: Value,
    policy_effective: Policy,
    policy_request: Option<Policy>,
    ctx: MpayloadCtx,
}

fn default_policy_run() -> Policy {
    Policy::default()
}

fn default_policy_chat() -> Policy {
    // Keep chat fast by default; can be overridden per-user/per-request.
    Policy {
        time_ms: 30_000,
        ..Policy::default()
    }
}

fn resolve_policy(kind: &str, user: Option<&UserContext>, req_policy: Option<Policy>) -> Policy {
    if let Some(p) = req_policy {
        return p;
    }
    if let Some(u) = user.and_then(|u| u.policy_overrides.clone()) {
        return u;
    }
    match kind {
        "chat" => default_policy_chat(),
        _ => default_policy_run(),
    }
}

impl Default for AppState {
    fn default() -> Self {
        let mut users = HashMap::new();
        // Demo users
        users.insert(
            "demo".to_string(),
            UserContext {
                user_id: "demo".to_string(),
                api_key: "demo-key-123".to_string(),
                quota_remaining: 1000,
                policy_overrides: None,
            },
        );
        users.insert(
            "premium".to_string(),
            UserContext {
                user_id: "premium".to_string(),
                api_key: "premium-key-456".to_string(),
                quota_remaining: 10000,
                policy_overrides: Some(Policy {
                    gamma_gate: 0.3, // Lower threshold for premium
                    time_ms: 60000,  // Longer timeout
                    max_risk: 0.5,   // Higher risk tolerance
                    tiny_diff_loc: 500,
                }),
            },
        );
        Self { users }
    }
}

// Simple progress bus
static mut PROGRESS_TX: Option<broadcast::Sender<String>> = None;
fn progress_tx() -> broadcast::Sender<String> {
    unsafe {
        if let Some(tx) = &PROGRESS_TX {
            tx.clone()
        } else {
            let (tx, _rx) = broadcast::channel(100);
            PROGRESS_TX = Some(tx.clone());
            tx
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, ToSchema)]
pub struct ActiveRun {
    pub run_id: String,
    pub goal_id: String,
    pub status: String, // queued|running
    pub ts: String,
    pub receipt_url: String,
    pub sse_url: String,
}

static ACTIVE_RUNS: Lazy<Mutex<HashMap<String, ActiveRun>>> = Lazy::new(|| Mutex::new(HashMap::new()));

async fn set_active_run(run_id: &str, goal_id: &str, status: &str) {
    if !is_safe_segment(run_id) {
        return;
    }
    let ts = chrono::Utc::now().to_rfc3339();
    let mut m = ACTIVE_RUNS.lock().await;
    m.insert(
        run_id.to_string(),
        ActiveRun {
            run_id: run_id.to_string(),
            goal_id: goal_id.to_string(),
            status: status.to_string(),
            ts,
            receipt_url: format!("/runs/receipts/{}/RECEIPT.md", run_id),
            sse_url: format!("/progress.sse?run_id={}", run_id),
        },
    );
}

async fn clear_active_run(run_id: &str) {
    let mut m = ACTIVE_RUNS.lock().await;
    m.remove(run_id);
}

fn emit_progress(run_id: &str, goal_id: &str, phase: &str, extra: serde_json::Value) {
    let payload = json!({
        "run_id": run_id,
        "goal_id": goal_id,
        "phase": phase,
        "ts": chrono::Utc::now().to_rfc3339(),
        "extra": extra
    });
    let _ = progress_tx().send(payload.to_string());
}

fn extract_api_key(headers: &HeaderMap) -> Option<String> {
    headers
        .get("x-api-key")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string())
}

fn authenticate_user(state: &AppState, api_key: &str) -> Option<UserContext> {
    state
        .users
        .values()
        .find(|user| user.api_key == api_key)
        .cloned()
}

#[derive(Debug, Serialize, Deserialize, JsonSchema, ToSchema)]
#[schema(example = json!({
    "goal_id": "demo.ping",
    "inputs": {"message": "hello"},
    "policy": {"gamma_gate": 0.5, "time_ms": 8000, "max_risk": 0.3, "tiny_diff_loc": 120}
}))]
pub struct UserRunReq {
    pub goal_id: String,
    #[serde(default)]
    pub inputs: serde_json::Value,
    pub policy: Option<Policy>, // User can override default policy
}

#[derive(Debug, Serialize, Deserialize, JsonSchema, ToSchema)]
pub struct UserRunResp {
    pub user_id: String,
    pub quota_remaining: u32,
    pub manifest: Manifest,
    pub bits: Bits,
    pub pr_created: Option<String>,
    pub meta2_proposal: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema, ToSchema)]
#[schema(example = json!({
    "message": "hello",
    "thread": null,
    "policy": null,
    "run_id": "r-demo-chat-123"
}))]
pub struct ChatReq {
    pub message: String,
    #[serde(default)]
    pub thread: Option<String>,
    #[serde(default)]
    pub loop_mode: Option<bool>,
    #[serde(default)]
    pub policy: Option<Policy>,
    #[serde(default)]
    pub run_id: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema, ToSchema)]
pub struct ChatResp {
    pub run_id: String,
    pub user_id: String,
    #[serde(default)]
    pub thread: Option<String>,
    pub reply: String,
    #[serde(default)]
    pub run_payload: Option<serde_json::Value>,
    pub manifest: Manifest,
    pub bits: Bits,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema, ToSchema)]
#[schema(example = json!({"run_id":"r-demo-123","note":"attach last run output"}))]
pub struct AttachRunReq {
    pub run_id: String,
    #[serde(default)]
    pub note: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema, ToSchema)]
pub struct AttachRunResp {
    pub ok: bool,
    pub user_id: String,
    pub thread: String,
    pub run_id: String,
    pub goal_id: Option<String>,
    pub summary: String,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema, ToSchema)]
pub struct ThreadSummaryResp {
    pub user_id: String,
    pub thread: String,
    pub messages_total: usize,
    pub messages_user: usize,
    pub messages_assistant: usize,
    pub messages_system: usize,
    pub messages_tool: usize,
    pub bytes_total: u64,
    pub approx_tokens: u64,
    pub last_run_ids: Vec<String>,
    pub last_updated: Option<String>,
}

#[utoipa::path(
    post,
    path = "/users/{user_id}/run",
    request_body = UserRunReq,
    responses(
        (status = 200, description = "Run completed", body = UserRunResp),
        (status = 401, description = "Unauthorized"),
        (status = 429, description = "Quota exceeded")
    )
)]
pub async fn user_run_handler(
    State(mut state): State<AppState>,
    Path(user_id): Path<String>,
    headers: HeaderMap,
    Json(req): Json<UserRunReq>,
) -> impl IntoResponse {
    // Authenticate
    let api_key = match extract_api_key(&headers) {
        Some(key) => key,
        None => {
            return (
                axum::http::StatusCode::UNAUTHORIZED,
                "Missing x-api-key header".to_string(),
            )
                .into_response()
        }
    };

    let mut user = match authenticate_user(&state, &api_key) {
        Some(user) if user.user_id == user_id => user,
        _ => {
            return (
                axum::http::StatusCode::UNAUTHORIZED,
                "Invalid API key or user ID".to_string(),
            )
                .into_response()
        }
    };

    // Check quota
    if user.quota_remaining == 0 {
        return (
            axum::http::StatusCode::TOO_MANY_REQUESTS,
            "Quota exceeded".to_string(),
        )
            .into_response();
    }

    let policy = resolve_policy("run", Some(&user), req.policy.clone());

    // Namespace goal with user ID to prevent conflicts
    let namespaced_goal = format!("user:{}.{}", user_id, req.goal_id);
    let run_id = format!("r-{}", uuid::Uuid::new_v4());

    match run_with_integrations(&namespaced_goal, req.inputs, &policy, &run_id).await {
        Ok((mut manifest, bits, pr_id, meta2_proposal)) => {
            manifest.run_id = run_id;
            // Decrement quota
            user.quota_remaining -= 1;
            state.users.insert(user_id.clone(), user.clone());

            Json(UserRunResp {
                user_id: user.user_id,
                quota_remaining: user.quota_remaining,
                manifest,
                bits,
                pr_created: pr_id,
                meta2_proposal,
            })
            .into_response()
        }
        Err(e) => (axum::http::StatusCode::BAD_REQUEST, e.to_string()).into_response(),
    }
}

#[utoipa::path(
    get,
    path = "/users/{user_id}/status",
    responses(
        (status = 200, description = "User status", body = UserStatus)
    )
)]
pub async fn user_status_handler(
    State(state): State<AppState>,
    Path(user_id): Path<String>,
    headers: HeaderMap,
) -> impl IntoResponse {
    let api_key = match extract_api_key(&headers) {
        Some(key) => key,
        None => {
            return (
                axum::http::StatusCode::UNAUTHORIZED,
                "Missing x-api-key header".to_string(),
            )
                .into_response()
        }
    };

    let user = match authenticate_user(&state, &api_key) {
        Some(user) if user.user_id == user_id => user,
        _ => {
            return (
                axum::http::StatusCode::UNAUTHORIZED,
                "Invalid API key or user ID".to_string(),
            )
                .into_response()
        }
    };

    Json(UserStatus {
        user_id: user.user_id,
        quota_remaining: user.quota_remaining,
        has_premium_policy: user.policy_overrides.is_some(),
    })
    .into_response()
}

#[derive(Debug, Serialize, Deserialize, JsonSchema, ToSchema)]
pub struct UserStatus {
    pub user_id: String,
    pub quota_remaining: u32,
    pub has_premium_policy: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema, ToSchema)]
#[schema(example = json!({
    "goal_id": "wiki.generate",
    "inputs": {},
    "policy": {"gamma_gate": 0.5, "time_ms": 300000, "max_risk": 0.2, "tiny_diff_loc": 120},
    "run_id": "wiki-example"
}))]
pub struct RunReq {
    pub goal_id: String,
    #[serde(default)]
    pub inputs: serde_json::Value,
    #[serde(default)]
    pub policy: Option<Policy>,
    #[serde(default)]
    pub run_id: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema, ToSchema)]
pub struct RunResp {
    pub manifest: Manifest,
    pub bits: Bits,
    pub pr_created: Option<String>,
    pub meta2_proposal: Option<String>, // JSON serialized Meta2Proposal
}

#[derive(Debug, Serialize, Deserialize, JsonSchema, ToSchema)]
pub struct RunAsyncResp {
    pub run_id: String,
    pub goal_id: String,
    pub status: String, // queued|running|done|error
    pub receipt_url: String,
    pub sse_url: String,
}

// --- DSL compatibility structs (tau / execute / seed/config) ---
#[derive(Debug, Serialize, Deserialize, JsonSchema, ToSchema)]
pub struct TauReq {
    #[serde(default)]
    pub input: Option<String>,
    #[serde(default)]
    pub preset: Option<String>,
    #[serde(default)]
    pub context: Option<serde_json::Value>,
    #[serde(default)]
    pub options: Option<serde_json::Value>,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema, ToSchema)]
pub struct ExecuteReq {
    pub goal: String,
    #[serde(default)]
    pub task_type: Option<String>,
    #[serde(default)]
    pub parameters: serde_json::Value,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema, ToSchema, Default)]
pub struct MineReq {
    #[serde(default)]
    pub sources: Vec<String>,
    #[serde(default)]
    pub patterns: Vec<String>,
    #[serde(default)]
    pub output_format: Option<String>,
}

static START_TS: Lazy<SystemTime> = Lazy::new(SystemTime::now);

#[derive(Debug, Serialize, Deserialize, JsonSchema, ToSchema)]
#[schema(example = json!({"suite": "easy"}))]
pub struct ValidateReq {
    pub suite: String,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema, ToSchema)]
pub struct ValidateResp {
    pub metacognitive_score: f32,
    pub results: Vec<ValidationResult>,
    pub summary: String,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema, ToSchema)]
pub struct GoldenReq {
    pub name: String,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema, ToSchema)]
pub struct GoldenResp {
    pub name: String,
    pub total: usize,
    pub passed: usize,
    pub failed: usize,
    pub details: Vec<engine::golden::GoldenCase>,
    pub bits: Bits,
}

// -------- Ruliad kernel artifact serving --------

fn is_safe_segment(seg: &str) -> bool {
    !seg.is_empty()
        && !seg.contains('/')
        && !seg.contains('\\')
        && !seg.contains("..")
        && seg
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.')
}

// -------- Codex history serving (gated) --------

fn codex_history_enabled() -> bool {
    match std::env::var("ONE_ENGINE_ENABLE_CODEX_HISTORY") {
        Ok(v) => {
            let v = v.to_ascii_lowercase();
            v == "1" || v == "true" || v == "yes" || v == "y"
        }
        Err(_) => false,
    }
}

fn meta3_root() -> PathBuf {
    std::env::var("META3_ROOT")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("."))
}

fn thread_path(user_id: &str, thread: &str) -> Option<PathBuf> {
    if !is_safe_segment(user_id) || !is_safe_segment(thread) {
        return None;
    }
    Some(
        meta3_root()
            .join("users")
            .join(user_id)
            .join("threads")
            .join(format!("{thread}.jsonl")),
    )
}

#[derive(Debug, Serialize, Deserialize)]
struct ThreadEvent {
    ts: String,
    role: String,
    content: String,
    run_id: String,
}

async fn append_thread_event(path: &PathBuf, role: &str, content: &str, run_id: &str) {
    let ts = chrono::Utc::now().to_rfc3339();
    let ev = ThreadEvent {
        ts,
        role: role.to_string(),
        content: redact(content),
        run_id: run_id.to_string(),
    };
    let line = serde_json::to_string(&ev).unwrap_or_else(|_| {
        "{\"role\":\"error\",\"content\":\"serialize failed\",\"run_id\":\"\"}".to_string()
    });

    if let Some(parent) = path.parent() {
        let _ = fs::create_dir_all(parent).await;
    }

    let mut f = match fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .await
    {
        Ok(f) => f,
        Err(_) => return,
    };
    let _ = f.write_all(line.as_bytes()).await;
    let _ = f.write_all(b"\n").await;
}

async fn load_thread_history(path: &PathBuf, max_messages: usize) -> Vec<Value> {
    let mut out = Vec::new();
    let std_path = StdPath::new(path);
    let lines = tail_lines(std_path, max_messages, 200_000).await.unwrap_or_default();
    for line in lines {
        if let Ok(v) = serde_json::from_str::<Value>(&line) {
            let role = v.get("role").and_then(|x| x.as_str()).unwrap_or("");
            let content = v.get("content").and_then(|x| x.as_str()).unwrap_or("");
            if content.trim().is_empty() {
                continue;
            }
            if role == "user" || role == "assistant" {
                out.push(json!({"role": role, "content": content}));
            } else if role == "tool" || role == "system" {
                // Keep tool/system events in the chat context as system messages.
                out.push(json!({"role": "system", "content": content}));
            }
        }
    }
    out
}

async fn thread_summary(path: &PathBuf, user_id: &str, thread: &str) -> ThreadSummaryResp {
    let std_path = StdPath::new(path);
    let meta = tokio::fs::metadata(std_path).await.ok();
    let bytes_total = meta.as_ref().map(|m| m.len()).unwrap_or(0);
    let last_updated = meta.as_ref().and_then(fmt_mtime);

    let lines = tail_lines(std_path, 200, 500_000).await.unwrap_or_default();
    let mut messages_total = 0usize;
    let mut messages_user = 0usize;
    let mut messages_assistant = 0usize;
    let mut messages_system = 0usize;
    let mut messages_tool = 0usize;

    for line in &lines {
        if let Ok(v) = serde_json::from_str::<Value>(&line) {
            let role = v.get("role").and_then(|x| x.as_str()).unwrap_or("");
            if !role.is_empty() {
                messages_total += 1;
                match role {
                    "user" => messages_user += 1,
                    "assistant" => messages_assistant += 1,
                    "system" => messages_system += 1,
                    "tool" => messages_tool += 1,
                    _ => {}
                }
            }
        }
    }

    // Most-recent-first run ids, de-duped.
    let mut last_run_ids: Vec<String> = Vec::new();
    for line in lines.iter().rev() {
        if let Ok(v) = serde_json::from_str::<Value>(line) {
            if let Some(r) = v.get("run_id").and_then(|x| x.as_str()) {
                if is_safe_segment(r) && !last_run_ids.contains(&r.to_string()) {
                    last_run_ids.push(r.to_string());
                }
            }
        }
        if last_run_ids.len() >= 8 {
            break;
        }
    }
    if last_run_ids.len() > 8 {
        last_run_ids.truncate(8);
    }

    // Rough heuristic (not “token accurate”): 4 chars ≈ 1 token.
    let approx_tokens = (bytes_total / 4).max(1);

    ThreadSummaryResp {
        user_id: user_id.to_string(),
        thread: thread.to_string(),
        messages_total,
        messages_user,
        messages_assistant,
        messages_system,
        messages_tool,
        bytes_total,
        approx_tokens,
        last_run_ids,
        last_updated,
    }
}

async fn read_receipt_response_json(run_id: &str) -> Result<Value, String> {
    if !is_safe_segment(run_id) {
        return Err("Invalid run_id".to_string());
    }
    let root = meta3_root();
    let p = root
        .join("runs")
        .join("receipts")
        .join(run_id)
        .join("response.json");
    let txt = fs::read_to_string(&p)
        .await
        .map_err(|_| "Missing receipt response.json".to_string())?;
    serde_json::from_str::<Value>(&txt).map_err(|e| format!("Bad receipt JSON: {e}"))
}

fn summarize_receipt_for_context(run_id: &str, resp: &Value, note: Option<&str>) -> (String, Option<String>) {
    let mut lines: Vec<String> = Vec::new();
    lines.push(format!("Tool: attached run output `{}`.", run_id));

    let manifest = resp.get("manifest");
    let mut goal_id: Option<String> = None;
    if let Some(m) = manifest.and_then(|v| v.as_object()) {
        goal_id = m.get("goal_id").and_then(|v| v.as_str()).map(|s| s.to_string());
        if let Some(g) = goal_id.as_deref() {
            lines.push(format!("- goal_id: `{}`", g));
        }
        if let Some(ev) = m.get("evidence") {
            let actual = ev
                .get("actual_success")
                .and_then(|v| v.as_bool())
                .map(|b| b.to_string());
            if let Some(a) = actual.as_deref() {
                lines.push(format!("- success: `{}`", a));
            }
            let view = ev
                .get("static_html_url")
                .and_then(|v| v.as_str())
                .or_else(|| ev.get("index_html_url").and_then(|v| v.as_str()));
            if let Some(u) = view {
                lines.push(format!("- view: `{}`", u));
            }

            let mut strs: Vec<String> = Vec::new();
            let mut budget = 8usize;
            extract_strings_limited(ev, &mut strs, 4, &mut budget);
            strs.retain(|s| s.len() <= 240);
            if !strs.is_empty() {
                lines.push("- evidence_snippets:".to_string());
                for s in strs.into_iter().take(6) {
                    lines.push(format!("  - {}", s));
                }
            }
        }
    } else {
        // Likely a queued stub or an unexpected response type.
        let status = resp.get("status").and_then(|v| v.as_str()).unwrap_or("");
        if !status.is_empty() {
            lines.push(format!("- status: `{}`", status));
        }
    }

    lines.push(format!("- receipt: `/runs/receipts/{}/RECEIPT.md`", run_id));
    if let Some(n) = note {
        if !n.trim().is_empty() {
            lines.push(format!("- note: {}", redact(n)));
        }
    }

    (lines.join("\n"), goal_id)
}

async fn write_receipt_bundle<Req: Serialize, Resp: Serialize>(
    run_id: &str,
    goal_id: &str,
    bits: &Bits,
    deliverables: &[String],
    evidence: &Value,
    default_success: bool,
    request: &Req,
    response: &Resp,
) {
    if !is_safe_segment(run_id) {
        return;
    }

    let root = meta3_root();
    let receipt_dir = root.join("runs/receipts").join(run_id);
    let _ = fs::create_dir_all(&receipt_dir).await;

    let _ = fs::write(
        receipt_dir.join("request.json"),
        serde_json::to_string_pretty(request).unwrap_or_default(),
    )
    .await;
    let _ = fs::write(
        receipt_dir.join("response.json"),
        serde_json::to_string_pretty(response).unwrap_or_default(),
    )
    .await;

    let wrote_stdout = if let Some(s) = evidence.get("stdout").and_then(|v| v.as_str()) {
        let _ = fs::write(receipt_dir.join("stdout.txt"), s).await;
        true
    } else {
        false
    };

    let wrote_reply = if let Some(s) = evidence.get("reply").and_then(|v| v.as_str()) {
        if !s.trim().is_empty() {
            let _ = fs::write(receipt_dir.join("reply.txt"), s).await;
            true
        } else {
            false
        }
    } else {
        false
    };

    let actual_success = evidence
        .get("actual_success")
        .and_then(|v| v.as_bool())
        .unwrap_or(default_success);

    let view = evidence
        .get("static_html_url")
        .and_then(|v| v.as_str())
        .or_else(|| evidence.get("index_html_url").and_then(|v| v.as_str()));

    let mut md = String::new();
    md.push_str("# RECEIPT\n\n");
    md.push_str(&format!("- run_id: `{}`\n", run_id));
    md.push_str(&format!("- goal_id: `{}`\n", goal_id));
    md.push_str(&format!("- success: `{}`\n", actual_success));
    md.push_str(&format!(
        "- bits: a={} u={} p={} e={} d={} t={}\n",
        bits.a, bits.u, bits.p, bits.e, bits.d, bits.t
    ));
    if let Some(u) = view {
        md.push_str(&format!("- view: `{}`\n", u));
    }

    md.push_str("\n## Files\n");
    md.push_str(&format!("- request: `/runs/receipts/{}/request.json`\n", run_id));
    md.push_str(&format!("- response: `/runs/receipts/{}/response.json`\n", run_id));
    md.push_str(&format!("- receipt: `/runs/receipts/{}/RECEIPT.md`\n", run_id));
    if wrote_stdout {
        md.push_str(&format!("- stdout: `/runs/receipts/{}/stdout.txt`\n", run_id));
    }
    if wrote_reply {
        md.push_str(&format!("- reply: `/runs/receipts/{}/reply.txt`\n", run_id));
    }

    if !deliverables.is_empty() {
        md.push_str("\n## Deliverables\n");
        for d in deliverables {
            md.push_str("- ");
            md.push_str(d);
            md.push('\n');
        }
    }

    let _ = fs::write(receipt_dir.join("RECEIPT.md"), md).await;
}

static RE_URL: Lazy<Regex> = Lazy::new(|| Regex::new(r#"https?://[^\s"'<>]+"#).unwrap());
static RE_PATH_HINT: Lazy<Regex> = Lazy::new(|| {
    Regex::new(
        r#"(?P<path>/(?:v\d+|api|users|meta|nstar|progress\.sse|swagger-ui|api-docs)[^\s"']*)"#,
    )
    .unwrap()
});
static RE_X_API_KEY: Lazy<Regex> =
    Lazy::new(|| Regex::new(r#"(?i)(x-api-key\s*[:=]\s*)([^\s"'\\]+)"#).unwrap());
static RE_AUTH_BEARER: Lazy<Regex> =
    Lazy::new(|| Regex::new(r#"(?i)(authorization\s*:\s*bearer\s+)([^\s"'\\]+)"#).unwrap());
static RE_SK: Lazy<Regex> = Lazy::new(|| Regex::new(r"sk-[A-Za-z0-9_-]{10,}").unwrap());

fn fmt_mtime(meta: &std::fs::Metadata) -> Option<String> {
    meta.modified()
        .ok()
        .map(|t| chrono::DateTime::<chrono::Utc>::from(t).to_rfc3339())
}

fn redact(s: &str) -> String {
    let mut out = s.to_string();
    out = RE_X_API_KEY.replace_all(&out, "${1}[REDACTED]").to_string();
    out = RE_AUTH_BEARER
        .replace_all(&out, "${1}[REDACTED]")
        .to_string();
    out = RE_SK.replace_all(&out, "sk-[REDACTED]").to_string();
    out
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ApiTraceEvent {
    ts: String,
    method: String,
    path: String,
    query: Option<String>,
    status: u16,
    ms: u64,
    mutation: bool,
    run_id: Option<String>,
    user_id: Option<String>,
    thread: Option<String>,
}

fn parse_run_id_from_query(q: &str) -> Option<String> {
    for part in q.split('&') {
        let mut it = part.splitn(2, '=');
        let k = it.next().unwrap_or("");
        let v = it.next().unwrap_or("");
        if k == "run_id" && is_safe_segment(v) {
            return Some(v.to_string());
        }
    }
    None
}

fn parse_run_id_from_path(path: &str) -> Option<String> {
    // /runs/receipts/<run_id>/...
    let parts: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();
    if parts.len() >= 3 && parts[0] == "runs" && parts[1] == "receipts" && is_safe_segment(parts[2]) {
        return Some(parts[2].to_string());
    }
    if parts.len() >= 3
        && parts[0] == "runs"
        && (parts[1] == "wiki" || parts[1] == "graphs")
        && is_safe_segment(parts[2])
    {
        return Some(parts[2].to_string());
    }
    None
}

fn parse_user_id_from_path(path: &str) -> Option<String> {
    // /users/<user_id>/...
    let parts: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();
    if parts.len() >= 2 && parts[0] == "users" && is_safe_segment(parts[1]) {
        return Some(parts[1].to_string());
    }
    None
}

async fn append_api_trace(ev: ApiTraceEvent) {
    let root = meta3_root();
    let p = root.join("runs").join("api_trace.jsonl");
    let _ = fs::create_dir_all(root.join("runs")).await;
    let line = serde_json::to_string(&ev).unwrap_or_default();
    let mut f = match fs::OpenOptions::new().create(true).append(true).open(&p).await {
        Ok(f) => f,
        Err(_) => return,
    };
    let _ = f.write_all(line.as_bytes()).await;
    let _ = f.write_all(b"\n").await;
}

pub async fn api_trace_middleware(
    req: axum::http::Request<axum::body::Body>,
    next: axum::middleware::Next,
) -> axum::response::Response {
    let start = Instant::now();

    let method = req.method().to_string();
    let uri = req.uri().clone();
    let path = uri.path().to_string();
    let query = uri.query().map(redact);
    let mutation = matches!(req.method().as_str(), "POST" | "PUT" | "PATCH" | "DELETE");

    let headers = req.headers().clone();
    let run_id = headers
        .get("x-run-id")
        .and_then(|v| v.to_str().ok())
        .filter(|s| is_safe_segment(s))
        .map(|s| s.to_string())
        .or_else(|| uri.query().and_then(parse_run_id_from_query))
        .or_else(|| parse_run_id_from_path(&path));

    let thread = headers
        .get("x-thread")
        .and_then(|v| v.to_str().ok())
        .filter(|s| is_safe_segment(s))
        .map(|s| s.to_string());

    let user_id = headers
        .get("x-user-id")
        .and_then(|v| v.to_str().ok())
        .filter(|s| is_safe_segment(s))
        .map(|s| s.to_string())
        .or_else(|| parse_user_id_from_path(&path));

    let resp = next.run(req).await;
    let status = resp.status().as_u16();
    let ms = start.elapsed().as_millis() as u64;

    let ev = ApiTraceEvent {
        ts: chrono::Utc::now().to_rfc3339(),
        method,
        path: redact(&path),
        query,
        status,
        ms,
        mutation,
        run_id,
        user_id,
        thread,
    };
    tokio::spawn(async move { append_api_trace(ev).await });

    resp
}

fn split_url(url: &str) -> Option<(String, String)> {
    let rest = url
        .strip_prefix("http://")
        .or_else(|| url.strip_prefix("https://"))?;
    let host_end = rest
        .find('/')
        .unwrap_or_else(|| rest.find('?').unwrap_or(rest.len()));
    let host = rest[..host_end]
        .trim_end_matches('.')
        .trim_end_matches(',')
        .to_string();
    let after_host = &rest[host_end..];
    let path_end = after_host
        .find('?')
        .unwrap_or_else(|| after_host.find('#').unwrap_or(after_host.len()));
    let mut path = after_host[..path_end].to_string();
    if path.is_empty() {
        path = "/".to_string();
    }
    Some((host, path))
}

fn extract_strings_limited(v: &Value, out: &mut Vec<String>, depth: usize, budget: &mut usize) {
    if *budget == 0 || depth == 0 {
        return;
    }
    match v {
        Value::String(s) => {
            if *budget == 0 {
                return;
            }
            let s = redact(s);
            if !s.trim().is_empty() {
                out.push(s);
                *budget -= 1;
            }
        }
        Value::Array(arr) => {
            for item in arr {
                if *budget == 0 {
                    break;
                }
                extract_strings_limited(item, out, depth - 1, budget);
            }
        }
        Value::Object(map) => {
            for k in [
                "text", "message", "content", "cmd", "command", "url", "path",
            ] {
                if *budget == 0 {
                    break;
                }
                if let Some(v) = map.get(k) {
                    extract_strings_limited(v, out, depth - 1, budget);
                }
            }
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort();
            for k in keys {
                if *budget == 0 {
                    break;
                }
                if matches!(
                    k.as_str(),
                    "text" | "message" | "content" | "cmd" | "command" | "url" | "path"
                ) {
                    continue;
                }
                if let Some(v) = map.get(k) {
                    extract_strings_limited(v, out, depth - 1, budget);
                }
            }
        }
        _ => {}
    }
}

async fn tail_lines(path: &StdPath, limit: usize, max_bytes: u64) -> Result<Vec<String>, String> {
    let meta = tokio::fs::metadata(path)
        .await
        .map_err(|e| format!("metadata: {e}"))?;
    let len = meta.len();
    let start = if len > max_bytes { len - max_bytes } else { 0 };

    let mut f = tokio::fs::File::open(path)
        .await
        .map_err(|e| format!("open: {e}"))?;
    if start > 0 {
        f.seek(std::io::SeekFrom::Start(start))
            .await
            .map_err(|e| format!("seek: {e}"))?;
    }

    let mut buf = Vec::new();
    f.read_to_end(&mut buf)
        .await
        .map_err(|e| format!("read: {e}"))?;

    let mut s = String::from_utf8(buf).map_err(|e| format!("utf8: {e}"))?;
    if start > 0 {
        if let Some(idx) = s.find('\n') {
            s = s[idx + 1..].to_string();
        }
    }

    let mut lines: Vec<&str> = s.lines().filter(|l| !l.trim().is_empty()).collect();
    if lines.len() > limit {
        lines = lines[lines.len() - limit..].to_vec();
    }
    Ok(lines.into_iter().map(|l| l.to_string()).collect())
}

#[derive(Debug, Serialize, Deserialize, JsonSchema, ToSchema)]
pub struct CodexSourceInfo {
    pub id: String,
    pub kind: String,
    pub description: String,
    pub available: bool,
    pub size_bytes: Option<u64>,
    pub file_count: Option<u64>,
    pub mtime: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema, ToSchema)]
pub struct CodexSourcesResp {
    pub sources: Vec<CodexSourceInfo>,
}

#[derive(Debug, Deserialize)]
pub struct CodexTailQuery {
    pub limit: Option<usize>,
}

#[derive(Debug, Deserialize)]
pub struct CodexListQuery {
    pub limit: Option<usize>,
}

#[derive(Debug, Deserialize)]
pub struct CodexCapabilitiesQuery {
    pub limit_files: Option<usize>,
    pub limit_lines: Option<usize>,
    pub max_bytes: Option<u64>,
    pub include_archive: Option<bool>,
}

#[derive(Debug, Deserialize)]
pub struct CodexSearchQuery {
    pub q: String,
    pub limit: Option<usize>,
    pub limit_files: Option<usize>,
    pub limit_lines: Option<usize>,
    pub max_bytes: Option<u64>,
    /// Comma-separated: archive,rollouts,utir (default: all)
    pub sources: Option<String>,
    pub case_sensitive: Option<bool>,
    pub regex: Option<bool>,
}

fn clamp_limit(v: Option<usize>, default: usize, max: usize) -> usize {
    v.unwrap_or(default).min(max).max(1)
}

fn clamp_u64(v: Option<u64>, default: u64, max: u64) -> u64 {
    v.unwrap_or(default).min(max).max(1024)
}

fn unauthorized(msg: &str) -> axum::response::Response {
    (axum::http::StatusCode::UNAUTHORIZED, msg.to_string()).into_response()
}

fn disabled() -> axum::response::Response {
    // Hide the surface unless explicitly enabled.
    (axum::http::StatusCode::NOT_FOUND, "not found".to_string()).into_response()
}

#[utoipa::path(
    get,
    path = "/codex/sources",
    responses(
        (status = 200, description = "Available Codex history sources", body = CodexSourcesResp),
        (status = 401, description = "Unauthorized"),
        (status = 404, description = "Not found (disabled)")
    )
)]
pub async fn codex_sources_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> impl IntoResponse {
    if !codex_history_enabled() {
        return disabled();
    }

    let api_key = match extract_api_key(&headers) {
        Some(k) => k,
        None => return unauthorized("Missing x-api-key"),
    };
    if authenticate_user(&state, &api_key).is_none() {
        return unauthorized("Invalid x-api-key");
    }

    let root = meta3_root();

    let archive_dir = root
        .join("agents")
        .join("NIX.codecli")
        .join("orchestrator")
        .join("runs")
        .join("archives");
    let rollouts_dir = root
        .join("agents")
        .join("NIX.codecli")
        .join("meta3")
        .join("logs");
    let utir_codex = root
        .join("runs")
        .join("utir")
        .join("normalized_codex.jsonl");
    let utir_history = root
        .join("runs")
        .join("utir")
        .join("normalized_history.jsonl");

    let mut sources = Vec::new();

    // Archive
    let mut archive_available = false;
    let mut archive_mtime = None;
    let mut archive_size = None;
    if let Ok(mut rd) = tokio::fs::read_dir(&archive_dir).await {
        while let Ok(Some(entry)) = rd.next_entry().await {
            let name = entry.file_name().to_string_lossy().to_string();
            if name.starts_with("codex_history_") && name.ends_with(".jsonl") {
                if let Ok(m) = entry.metadata().await {
                    archive_available = true;
                    archive_size = Some(archive_size.unwrap_or(0) + m.len());
                    if let Some(ts) = fmt_mtime(&m) {
                        if archive_mtime.as_deref() < Some(ts.as_str()) {
                            archive_mtime = Some(ts);
                        }
                    }
                }
            }
        }
    }
    sources.push(CodexSourceInfo {
        id: "orchestrator_archives".to_string(),
        kind: "dir".to_string(),
        description: "Codex Time Machine era archives (codex_history_*.jsonl)".to_string(),
        available: archive_available,
        size_bytes: archive_size,
        file_count: None,
        mtime: archive_mtime,
    });

    // Rollouts
    let mut rollout_count: u64 = 0;
    let mut rollouts_size: u64 = 0;
    let mut rollouts_mtime = None;
    if let Ok(mut rd) = tokio::fs::read_dir(&rollouts_dir).await {
        while let Ok(Some(entry)) = rd.next_entry().await {
            let name = entry.file_name().to_string_lossy().to_string();
            if name.ends_with(".jsonl") {
                if let Ok(m) = entry.metadata().await {
                    if m.is_file() {
                        rollout_count += 1;
                        rollouts_size += m.len();
                        if let Some(ts) = fmt_mtime(&m) {
                            if rollouts_mtime.as_deref() < Some(ts.as_str()) {
                                rollouts_mtime = Some(ts);
                            }
                        }
                    }
                }
            }
        }
    }
    sources.push(CodexSourceInfo {
        id: "meta3_rollouts".to_string(),
        kind: "dir".to_string(),
        description: "Meta3 rollout logs (*.jsonl)".to_string(),
        available: rollout_count > 0,
        size_bytes: Some(rollouts_size),
        file_count: Some(rollout_count),
        mtime: rollouts_mtime,
    });

    // UTIR normalized files
    for (id, desc, path) in [
        (
            "utir_normalized_codex",
            "UTIR normalized Codex JSONL",
            utir_codex,
        ),
        (
            "utir_normalized_history",
            "UTIR normalized history/event ledger JSONL",
            utir_history,
        ),
    ] {
        match tokio::fs::metadata(&path).await {
            Ok(m) => sources.push(CodexSourceInfo {
                id: id.to_string(),
                kind: "file".to_string(),
                description: desc.to_string(),
                available: m.is_file(),
                size_bytes: Some(m.len()),
                file_count: None,
                mtime: fmt_mtime(&m),
            }),
            Err(_) => sources.push(CodexSourceInfo {
                id: id.to_string(),
                kind: "file".to_string(),
                description: desc.to_string(),
                available: false,
                size_bytes: None,
                file_count: None,
                mtime: None,
            }),
        }
    }

    Json(CodexSourcesResp { sources }).into_response()
}

#[utoipa::path(
    get,
    path = "/codex/archive",
    params(
        ("limit" = Option<usize>, Query, description = "Number of events to return from the end (max 2000)")
    ),
    responses(
        (status = 200, description = "Tail of latest codex_history_*.jsonl as parsed JSON", body = Value),
        (status = 401, description = "Unauthorized"),
        (status = 404, description = "Not found (disabled/missing)")
    )
)]
pub async fn codex_archive_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(q): Query<CodexTailQuery>,
) -> impl IntoResponse {
    if !codex_history_enabled() {
        return disabled();
    }

    let api_key = match extract_api_key(&headers) {
        Some(k) => k,
        None => return unauthorized("Missing x-api-key"),
    };
    if authenticate_user(&state, &api_key).is_none() {
        return unauthorized("Invalid x-api-key");
    }

    let limit = clamp_limit(q.limit, 200, 2000);
    let root = meta3_root();
    let archive_dir = root
        .join("agents")
        .join("NIX.codecli")
        .join("orchestrator")
        .join("runs")
        .join("archives");

    let mut best: Option<(String, PathBuf)> = None;
    if let Ok(mut rd) = tokio::fs::read_dir(&archive_dir).await {
        while let Ok(Some(entry)) = rd.next_entry().await {
            let name = entry.file_name().to_string_lossy().to_string();
            if name.starts_with("codex_history_") && name.ends_with(".jsonl") {
                let path = entry.path();
                match best.as_ref() {
                    Some((best_name, _)) if best_name >= &name => {}
                    _ => best = Some((name, path)),
                }
            }
        }
    }

    let Some((file, path)) = best else {
        return (
            axum::http::StatusCode::NOT_FOUND,
            "no codex_history_*.jsonl found".to_string(),
        )
            .into_response();
    };

    let lines = match tail_lines(&path, limit, 10 * 1024 * 1024).await {
        Ok(v) => v,
        Err(e) => return (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e).into_response(),
    };
    let mut events: Vec<Value> = Vec::new();
    for line in lines {
        match serde_json::from_str::<Value>(&line) {
            Ok(v) => events.push(v),
            Err(_) => events.push(json!({"raw": line})),
        }
    }

    Json(json!({
        "source": "orchestrator_archives",
        "file": file,
        "events": events
    }))
    .into_response()
}

#[utoipa::path(
    get,
    path = "/codex/rollouts",
    params(
        ("limit" = Option<usize>, Query, description = "Max files to return (max 1000)")
    ),
    responses(
        (status = 200, description = "List available rollout logs (*.jsonl)", body = Value),
        (status = 401, description = "Unauthorized"),
        (status = 404, description = "Not found (disabled/missing)")
    )
)]
pub async fn codex_rollouts_list_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(q): Query<CodexListQuery>,
) -> impl IntoResponse {
    if !codex_history_enabled() {
        return disabled();
    }

    let api_key = match extract_api_key(&headers) {
        Some(k) => k,
        None => return unauthorized("Missing x-api-key"),
    };
    if authenticate_user(&state, &api_key).is_none() {
        return unauthorized("Invalid x-api-key");
    }

    let limit = clamp_limit(q.limit, 200, 1000);
    let root = meta3_root();
    let rollouts_dir = root
        .join("agents")
        .join("NIX.codecli")
        .join("meta3")
        .join("logs");
    if !rollouts_dir.exists() {
        return (
            axum::http::StatusCode::NOT_FOUND,
            "rollouts dir missing".to_string(),
        )
            .into_response();
    }

    let mut items = Vec::new();
    match tokio::fs::read_dir(&rollouts_dir).await {
        Ok(mut rd) => {
            while let Ok(Some(entry)) = rd.next_entry().await {
                let name = entry.file_name().to_string_lossy().to_string();
                if !name.ends_with(".jsonl") {
                    continue;
                }
                if !is_safe_segment(&name) {
                    continue;
                }
                if let Ok(m) = entry.metadata().await {
                    if !m.is_file() {
                        continue;
                    }
                    items.push(json!({
                        "name": name,
                        "size_bytes": m.len(),
                        "mtime": fmt_mtime(&m),
                    }));
                }
            }
        }
        Err(e) => {
            return (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
        }
    }

    // Sort by name descending (newest-ish if names are timestamped)
    items.sort_by(|a, b| {
        let an = a.get("name").and_then(|v| v.as_str());
        let bn = b.get("name").and_then(|v| v.as_str());
        bn.cmp(&an)
    });
    if items.len() > limit {
        items.truncate(limit);
    }

    Json(json!({
        "source": "meta3_rollouts",
        "count": items.len(),
        "files": items
    }))
    .into_response()
}

#[utoipa::path(
    get,
    path = "/codex/rollouts/{file}",
    params(
        ("file" = String, Path, description = "Rollout log filename (must be safe)"),
        ("limit" = Option<usize>, Query, description = "Number of events to return from the end (max 2000)")
    ),
    responses(
        (status = 200, description = "Tail of rollout JSONL as parsed JSON", body = Value),
        (status = 400, description = "Invalid file"),
        (status = 401, description = "Unauthorized"),
        (status = 404, description = "Not found (disabled/missing)")
    )
)]
pub async fn codex_rollout_file_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(file): Path<String>,
    Query(q): Query<CodexTailQuery>,
) -> impl IntoResponse {
    if !codex_history_enabled() {
        return disabled();
    }
    if !is_safe_segment(&file) || !file.ends_with(".jsonl") {
        return (
            axum::http::StatusCode::BAD_REQUEST,
            "invalid file".to_string(),
        )
            .into_response();
    }

    let api_key = match extract_api_key(&headers) {
        Some(k) => k,
        None => return unauthorized("Missing x-api-key"),
    };
    if authenticate_user(&state, &api_key).is_none() {
        return unauthorized("Invalid x-api-key");
    }

    let limit = clamp_limit(q.limit, 200, 2000);
    let root = meta3_root();
    let rollouts_dir = root
        .join("agents")
        .join("NIX.codecli")
        .join("meta3")
        .join("logs");
    let path = rollouts_dir.join(&file);
    if !path.starts_with(&rollouts_dir) || !path.exists() {
        return (
            axum::http::StatusCode::NOT_FOUND,
            "file not found".to_string(),
        )
            .into_response();
    }

    let lines = match tail_lines(&path, limit, 10 * 1024 * 1024).await {
        Ok(v) => v,
        Err(e) => return (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e).into_response(),
    };
    let mut events: Vec<Value> = Vec::new();
    for line in lines {
        match serde_json::from_str::<Value>(&line) {
            Ok(v) => events.push(v),
            Err(_) => events.push(json!({"raw": line})),
        }
    }

    Json(json!({
        "source": "meta3_rollouts",
        "file": file,
        "events": events
    }))
    .into_response()
}

#[derive(Debug, Serialize, Deserialize, JsonSchema, ToSchema)]
pub struct CountedItem {
    pub key: String,
    pub count: u64,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema, ToSchema)]
pub struct CapScanFile {
    pub source: String,
    pub file: String,
    pub size_bytes: u64,
    pub tailed_lines: usize,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema, ToSchema)]
pub struct CodexCapabilitiesResp {
    pub generated_at: String,
    pub files_scanned: Vec<CapScanFile>,
    pub events_parsed: u64,
    pub strings_extracted: u64,
    pub top_hosts: Vec<CountedItem>,
    pub top_paths: Vec<CountedItem>,
    pub tool_signals: Vec<CountedItem>,
    pub samples: Vec<String>,
    pub next_actions: Vec<String>,
}

fn top_n(map: HashMap<String, u64>, n: usize) -> Vec<CountedItem> {
    let mut v: Vec<(String, u64)> = map.into_iter().collect();
    v.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    v.into_iter()
        .take(n)
        .map(|(key, count)| CountedItem { key, count })
        .collect()
}

struct FileScanAccum {
    events_parsed: u64,
    strings_extracted: u64,
    host_counts: HashMap<String, u64>,
    path_counts: HashMap<String, u64>,
    tool_counts: HashMap<String, u64>,
    samples: Vec<String>,
    tailed_lines: usize,
    size_bytes: u64,
}

fn merge_counts(dst: &mut HashMap<String, u64>, src: HashMap<String, u64>) {
    for (k, v) in src {
        *dst.entry(k).or_insert(0) += v;
    }
}

#[derive(Debug, Serialize, Deserialize, JsonSchema, ToSchema)]
pub struct CodexSearchResult {
    pub source: String,
    pub file: String,
    pub line: u64,
    pub ts: Option<String>,
    pub kind: Option<String>,
    pub snippet: String,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema, ToSchema)]
pub struct CodexSearchResp {
    pub generated_at: String,
    pub query: String,
    pub scanned_files: u64,
    pub results: Vec<CodexSearchResult>,
    pub truncated: bool,
}

fn parse_sources(s: Option<&str>) -> HashSet<String> {
    let mut out: HashSet<String> = HashSet::new();
    if let Some(s) = s {
        for part in s.split(',') {
            let p = part.trim().to_ascii_lowercase();
            if !p.is_empty() {
                out.insert(p);
            }
        }
    }
    if out.is_empty() {
        out.insert("archive".to_string());
        out.insert("rollouts".to_string());
        out.insert("utir".to_string());
    }
    out
}

fn line_matches(line: &str, q: &str, case_sensitive: bool, re: Option<&Regex>) -> bool {
    if let Some(re) = re {
        return re.is_match(line);
    }
    if case_sensitive {
        line.contains(q)
    } else {
        line.to_ascii_lowercase().contains(&q.to_ascii_lowercase())
    }
}

fn try_extract_meta(v: &Value) -> (Option<String>, Option<String>) {
    let ts = v
        .get("ts")
        .and_then(|x| x.as_str())
        .or_else(|| v.get("timestamp").and_then(|x| x.as_str()))
        .map(|s| s.to_string());
    let kind = v
        .get("type")
        .and_then(|x| x.as_str())
        .or_else(|| v.get("event").and_then(|x| x.as_str()))
        .or_else(|| v.get("kind").and_then(|x| x.as_str()))
        .map(|s| s.to_string());
    (ts, kind)
}

async fn search_jsonl_file_tail(
    source: &str,
    file_label: &str,
    path: &StdPath,
    q: &str,
    case_sensitive: bool,
    re: Option<&Regex>,
    limit_lines: usize,
    max_bytes: u64,
    max_results: usize,
    results: &mut Vec<CodexSearchResult>,
) -> Result<(), String> {
    if results.len() >= max_results {
        return Ok(());
    }

    let lines = tail_lines(path, limit_lines, max_bytes).await?;
    for (idx, raw) in lines.into_iter().enumerate() {
        if results.len() >= max_results {
            break;
        }
        let red = redact(&raw);
        if !line_matches(&red, q, case_sensitive, re) {
            continue;
        }

        let match_range = if let Some(re) = re {
            re.find(&red).map(|m| (m.start(), m.end()))
        } else if case_sensitive {
            red.find(q).map(|i| (i, i + q.len()))
        } else {
            let hay = red.to_ascii_lowercase();
            let needle = q.to_ascii_lowercase();
            hay.find(&needle).map(|i| (i, i + needle.len()))
        };

        fn excerpt_around(s: &str, range: Option<(usize, usize)>) -> String {
            let Some((start, end)) = range else {
                return s.chars().take(500).collect();
            };

            // Window around match (bytes), then adjust to char boundaries.
            let mut a = start.saturating_sub(220);
            let mut b = (end + 220).min(s.len());
            while a > 0 && !s.is_char_boundary(a) {
                a -= 1;
            }
            while b < s.len() && !s.is_char_boundary(b) {
                b += 1;
            }
            let mut out = s[a..b].to_string();
            if a > 0 {
                out = format!("…{}", out);
            }
            if b < s.len() {
                out.push('…');
            }
            if out.chars().count() > 500 {
                out = out.chars().take(500).collect();
                out.push('…');
            }
            out
        }

        let mut ts = None;
        let mut kind = None;
        if let Ok(v) = serde_json::from_str::<Value>(&raw) {
            let (t, k) = try_extract_meta(&v);
            ts = t;
            kind = k;
        }

        results.push(CodexSearchResult {
            source: source.to_string(),
            file: file_label.to_string(),
            line: (idx as u64) + 1,
            ts,
            kind,
            snippet: excerpt_around(&red, match_range),
        });
    }

    Ok(())
}

async fn scan_jsonl_file(
    path: &StdPath,
    limit_lines: usize,
    max_bytes: u64,
    tools: &[&str],
) -> Result<FileScanAccum, String> {
    let meta = tokio::fs::metadata(path)
        .await
        .map_err(|e| format!("metadata: {e}"))?;
    let size_bytes = meta.len();

    let lines = tail_lines(path, limit_lines, max_bytes).await?;

    let mut acc = FileScanAccum {
        events_parsed: 0,
        strings_extracted: 0,
        host_counts: HashMap::new(),
        path_counts: HashMap::new(),
        tool_counts: HashMap::new(),
        samples: Vec::new(),
        tailed_lines: lines.len(),
        size_bytes,
    };

    for line in lines {
        let v = match serde_json::from_str::<Value>(&line) {
            Ok(v) => {
                acc.events_parsed += 1;
                v
            }
            Err(_) => {
                let s = redact(&line);
                if !s.trim().is_empty() {
                    acc.strings_extracted += 1;
                    for m in RE_URL.find_iter(&s) {
                        if let Some((host, path)) = split_url(m.as_str()) {
                            *acc.host_counts.entry(host).or_insert(0) += 1;
                            *acc.path_counts.entry(path).or_insert(0) += 1;
                        }
                    }
                    for cap in RE_PATH_HINT.captures_iter(&s) {
                        if let Some(p) = cap.name("path") {
                            *acc.path_counts.entry(p.as_str().to_string()).or_insert(0) += 1;
                        }
                    }
                    let low = s.to_ascii_lowercase();
                    for t in tools {
                        if low.contains(t) {
                            *acc.tool_counts.entry(t.to_string()).or_insert(0) += 1;
                        }
                    }
                    if acc.samples.len() < 30
                        && (s.contains("http://")
                            || s.contains("https://")
                            || low.contains("curl ")
                            || low.contains("cargo ")
                            || low.contains("git ")
                            || low.contains("ast-grep"))
                    {
                        acc.samples.push(s.chars().take(400).collect());
                    }
                }
                continue;
            }
        };

        let mut strings: Vec<String> = Vec::new();
        let mut budget = 200usize;
        extract_strings_limited(&v, &mut strings, 6, &mut budget);

        for s in strings {
            if s.trim().is_empty() {
                continue;
            }
            acc.strings_extracted += 1;

            for m in RE_URL.find_iter(&s) {
                if let Some((host, path)) = split_url(m.as_str()) {
                    *acc.host_counts.entry(host).or_insert(0) += 1;
                    *acc.path_counts.entry(path).or_insert(0) += 1;
                }
            }
            for cap in RE_PATH_HINT.captures_iter(&s) {
                if let Some(p) = cap.name("path") {
                    *acc.path_counts.entry(p.as_str().to_string()).or_insert(0) += 1;
                }
            }

            let low = s.to_ascii_lowercase();
            for t in tools {
                if low.contains(t) {
                    *acc.tool_counts.entry(t.to_string()).or_insert(0) += 1;
                }
            }

            if acc.samples.len() < 30
                && (s.contains("http://")
                    || s.contains("https://")
                    || low.contains("curl ")
                    || low.contains("cargo ")
                    || low.contains("git ")
                    || low.contains("ast-grep"))
            {
                acc.samples.push(s.chars().take(400).collect());
            }
        }
    }

    Ok(acc)
}

#[utoipa::path(
    get,
    path = "/codex/capabilities",
    params(
        ("limit_files" = Option<usize>, Query, description = "Max rollout files to scan (max 200)"),
        ("limit_lines" = Option<usize>, Query, description = "Max tailed lines per file (max 10000)"),
        ("max_bytes" = Option<u64>, Query, description = "Max tailed bytes per file (max 20MB)"),
        ("include_archive" = Option<bool>, Query, description = "Include codex_history_*.jsonl archive tail")
    ),
    responses(
        (status = 200, description = "Auto capabilities report over Codex history", body = CodexCapabilitiesResp),
        (status = 401, description = "Unauthorized"),
        (status = 404, description = "Not found (disabled)")
    )
)]
pub async fn codex_capabilities_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(q): Query<CodexCapabilitiesQuery>,
) -> impl IntoResponse {
    if !codex_history_enabled() {
        return disabled();
    }

    let api_key = match extract_api_key(&headers) {
        Some(k) => k,
        None => return unauthorized("Missing x-api-key"),
    };
    if authenticate_user(&state, &api_key).is_none() {
        return unauthorized("Invalid x-api-key");
    }

    let include_archive = q.include_archive.unwrap_or(true);
    let limit_files = clamp_limit(q.limit_files, 25, 200);
    let limit_lines = clamp_limit(q.limit_lines, 2000, 10_000);
    let max_bytes = clamp_u64(q.max_bytes, 5 * 1024 * 1024, 20 * 1024 * 1024);

    let root = meta3_root();
    let archive_dir = root
        .join("agents")
        .join("NIX.codecli")
        .join("orchestrator")
        .join("runs")
        .join("archives");
    let rollouts_dir = root
        .join("agents")
        .join("NIX.codecli")
        .join("meta3")
        .join("logs");

    let mut files_scanned: Vec<CapScanFile> = Vec::new();
    let mut events_parsed: u64 = 0;
    let mut strings_extracted: u64 = 0;

    let mut host_counts: HashMap<String, u64> = HashMap::new();
    let mut path_counts: HashMap<String, u64> = HashMap::new();
    let mut tool_counts: HashMap<String, u64> = HashMap::new();
    let mut samples: Vec<String> = Vec::new();

    let tools = [
        "curl", "cargo", "git", "npm", "node", "python", "jq", "yq", "fd", "rg", "ast-grep",
        "docker", "gcloud",
    ];

    // 1) Latest archive file (by lexicographic name)
    if include_archive {
        let mut best: Option<(String, PathBuf)> = None;
        if let Ok(mut rd) = tokio::fs::read_dir(&archive_dir).await {
            while let Ok(Some(entry)) = rd.next_entry().await {
                let name = entry.file_name().to_string_lossy().to_string();
                if name.starts_with("codex_history_") && name.ends_with(".jsonl") {
                    let path = entry.path();
                    match best.as_ref() {
                        Some((best_name, _)) if best_name >= &name => {}
                        _ => best = Some((name, path)),
                    }
                }
            }
        }
        if let Some((name, path)) = best {
            match scan_jsonl_file(&path, limit_lines, max_bytes, &tools).await {
                Ok(acc) => {
                    events_parsed += acc.events_parsed;
                    strings_extracted += acc.strings_extracted;
                    merge_counts(&mut host_counts, acc.host_counts);
                    merge_counts(&mut path_counts, acc.path_counts);
                    merge_counts(&mut tool_counts, acc.tool_counts);
                    for s in acc.samples {
                        if samples.len() >= 30 {
                            break;
                        }
                        samples.push(s);
                    }
                    files_scanned.push(CapScanFile {
                        source: "orchestrator_archives".to_string(),
                        file: name,
                        size_bytes: acc.size_bytes,
                        tailed_lines: acc.tailed_lines,
                    });
                }
                Err(e) => {
                    return (
                        axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                        format!("archive scan failed: {e}"),
                    )
                        .into_response();
                }
            }
        }
    }

    // 2) Recent rollout files (by mtime desc)
    let mut rollout_files: Vec<(String, PathBuf, std::time::SystemTime)> = Vec::new();
    if let Ok(mut rd) = tokio::fs::read_dir(&rollouts_dir).await {
        while let Ok(Some(entry)) = rd.next_entry().await {
            let name = entry.file_name().to_string_lossy().to_string();
            if !name.ends_with(".jsonl") || !is_safe_segment(&name) {
                continue;
            }
            if let Ok(m) = entry.metadata().await {
                if !m.is_file() {
                    continue;
                }
                let mt = m.modified().unwrap_or(std::time::UNIX_EPOCH);
                rollout_files.push((name, entry.path(), mt));
            }
        }
    }
    rollout_files.sort_by(|a, b| b.2.cmp(&a.2).then_with(|| b.0.cmp(&a.0)));
    rollout_files.truncate(limit_files);

    for (name, path, _mt) in rollout_files {
        match scan_jsonl_file(&path, limit_lines, max_bytes, &tools).await {
            Ok(acc) => {
                events_parsed += acc.events_parsed;
                strings_extracted += acc.strings_extracted;
                merge_counts(&mut host_counts, acc.host_counts);
                merge_counts(&mut path_counts, acc.path_counts);
                merge_counts(&mut tool_counts, acc.tool_counts);
                for s in acc.samples {
                    if samples.len() >= 30 {
                        break;
                    }
                    samples.push(s);
                }
                files_scanned.push(CapScanFile {
                    source: "meta3_rollouts".to_string(),
                    file: name,
                    size_bytes: acc.size_bytes,
                    tailed_lines: acc.tailed_lines,
                });
            }
            Err(e) => {
                return (
                    axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                    format!("rollout scan failed: {e}"),
                )
                    .into_response();
            }
        }
    }

    let top_hosts = top_n(host_counts, 50);
    let top_paths = top_n(path_counts, 50);
    let tool_signals = top_n(tool_counts, 30);

    let next_actions = vec![
        "Promote top_hosts/top_paths into a stable API surface doc.".to_string(),
        "Add /codex/search to find exact prior commands/snippets.".to_string(),
        "Add /codex/capabilities/export.md to publish a readable report.".to_string(),
    ];

    Json(CodexCapabilitiesResp {
        generated_at: chrono::Utc::now().to_rfc3339(),
        files_scanned,
        events_parsed,
        strings_extracted,
        top_hosts,
        top_paths,
        tool_signals,
        samples,
        next_actions,
    })
    .into_response()
}

#[utoipa::path(
    get,
    path = "/codex/search",
    params(
        ("q" = String, Query, description = "Query string (substring by default)"),
        ("limit" = Option<usize>, Query, description = "Max results to return (max 500)"),
        ("limit_files" = Option<usize>, Query, description = "Max rollout files to scan (max 500)"),
        ("limit_lines" = Option<usize>, Query, description = "Max tailed lines per file (max 20000)"),
        ("max_bytes" = Option<u64>, Query, description = "Max tailed bytes per file (max 50MB)"),
        ("sources" = Option<String>, Query, description = "Comma-separated: archive,rollouts,utir (default all)"),
        ("case_sensitive" = Option<bool>, Query, description = "Case-sensitive substring match (default false)"),
        ("regex" = Option<bool>, Query, description = "Interpret q as regex (default false)")
    ),
    responses(
        (status = 200, description = "Search Codex history sources (tailed)", body = CodexSearchResp),
        (status = 400, description = "Invalid query"),
        (status = 401, description = "Unauthorized"),
        (status = 404, description = "Not found (disabled)")
    )
)]
pub async fn codex_search_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(q): Query<CodexSearchQuery>,
) -> impl IntoResponse {
    if !codex_history_enabled() {
        return disabled();
    }

    let api_key = match extract_api_key(&headers) {
        Some(k) => k,
        None => return unauthorized("Missing x-api-key"),
    };
    if authenticate_user(&state, &api_key).is_none() {
        return unauthorized("Invalid x-api-key");
    }

    let query = q.q.trim().to_string();
    if query.is_empty() || query.len() > 4000 {
        return (
            axum::http::StatusCode::BAD_REQUEST,
            "q must be non-empty and <= 4000 chars".to_string(),
        )
            .into_response();
    }

    let sources = parse_sources(q.sources.as_deref());
    let case_sensitive = q.case_sensitive.unwrap_or(false);
    let use_regex = q.regex.unwrap_or(false);

    let limit = clamp_limit(q.limit, 50, 500);
    let limit_files = clamp_limit(q.limit_files, 50, 500);
    let limit_lines = clamp_limit(q.limit_lines, 5000, 20_000);
    let max_bytes = clamp_u64(q.max_bytes, 10 * 1024 * 1024, 50 * 1024 * 1024);

    let compiled = if use_regex {
        match Regex::new(&query) {
            Ok(r) => Some(r),
            Err(e) => {
                return (
                    axum::http::StatusCode::BAD_REQUEST,
                    format!("invalid regex: {e}"),
                )
                    .into_response()
            }
        }
    } else {
        None
    };

    let root = meta3_root();
    let archive_dir = root
        .join("agents")
        .join("NIX.codecli")
        .join("orchestrator")
        .join("runs")
        .join("archives");
    let rollouts_dir = root
        .join("agents")
        .join("NIX.codecli")
        .join("meta3")
        .join("logs");
    let utir_history = root
        .join("runs")
        .join("utir")
        .join("normalized_history.jsonl");
    let utir_codex = root
        .join("runs")
        .join("utir")
        .join("normalized_codex.jsonl");

    let mut results: Vec<CodexSearchResult> = Vec::new();
    let mut scanned_files: u64 = 0;

    // Archive (single best file)
    if sources.contains("archive") {
        let mut best: Option<(String, PathBuf)> = None;
        if let Ok(mut rd) = tokio::fs::read_dir(&archive_dir).await {
            while let Ok(Some(entry)) = rd.next_entry().await {
                let name = entry.file_name().to_string_lossy().to_string();
                if name.starts_with("codex_history_") && name.ends_with(".jsonl") {
                    let path = entry.path();
                    match best.as_ref() {
                        Some((best_name, _)) if best_name >= &name => {}
                        _ => best = Some((name, path)),
                    }
                }
            }
        }
        if let Some((name, path)) = best {
            scanned_files += 1;
            if let Err(e) = search_jsonl_file_tail(
                "orchestrator_archives",
                &name,
                &path,
                &query,
                case_sensitive,
                compiled.as_ref(),
                limit_lines,
                max_bytes,
                limit,
                &mut results,
            )
            .await
            {
                return (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e).into_response();
            }
        }
    }

    // Rollouts (most recent N by mtime)
    if sources.contains("rollouts") && results.len() < limit {
        let mut rollout_files: Vec<(String, PathBuf, std::time::SystemTime)> = Vec::new();
        if let Ok(mut rd) = tokio::fs::read_dir(&rollouts_dir).await {
            while let Ok(Some(entry)) = rd.next_entry().await {
                let name = entry.file_name().to_string_lossy().to_string();
                if !name.ends_with(".jsonl") || !is_safe_segment(&name) {
                    continue;
                }
                if let Ok(m) = entry.metadata().await {
                    if !m.is_file() {
                        continue;
                    }
                    let mt = m.modified().unwrap_or(std::time::UNIX_EPOCH);
                    rollout_files.push((name, entry.path(), mt));
                }
            }
        }
        rollout_files.sort_by(|a, b| b.2.cmp(&a.2).then_with(|| b.0.cmp(&a.0)));
        rollout_files.truncate(limit_files);

        for (name, path, _mt) in rollout_files {
            if results.len() >= limit {
                break;
            }
            scanned_files += 1;
            if let Err(e) = search_jsonl_file_tail(
                "meta3_rollouts",
                &name,
                &path,
                &query,
                case_sensitive,
                compiled.as_ref(),
                limit_lines,
                max_bytes,
                limit,
                &mut results,
            )
            .await
            {
                return (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e).into_response();
            }
        }
    }

    // UTIR normalized (option b)
    if sources.contains("utir") && results.len() < limit {
        for (label, path) in [
            ("normalized_history.jsonl", utir_history),
            ("normalized_codex.jsonl", utir_codex),
        ] {
            if results.len() >= limit {
                break;
            }
            if let Ok(m) = tokio::fs::metadata(&path).await {
                if m.is_file() && m.len() > 0 {
                    scanned_files += 1;
                    if let Err(e) = search_jsonl_file_tail(
                        "utir",
                        label,
                        &path,
                        &query,
                        case_sensitive,
                        compiled.as_ref(),
                        limit_lines,
                        max_bytes,
                        limit,
                        &mut results,
                    )
                    .await
                    {
                        return (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e).into_response();
                    }
                }
            }
        }
    }

    let truncated = results.len() >= limit;
    Json(CodexSearchResp {
        generated_at: chrono::Utc::now().to_rfc3339(),
        query,
        scanned_files,
        results,
        truncated,
    })
    .into_response()
}

#[utoipa::path(
    get,
    path = "/ruliad/{run_id}",
    responses((status = 200, description = "List ruliad.kernel artifacts", body = Value))
)]
pub async fn ruliad_list_handler(Path(run_id): Path<String>) -> impl IntoResponse {
    if !is_safe_segment(&run_id) {
        return (
            axum::http::StatusCode::BAD_REQUEST,
            "invalid run_id".to_string(),
        )
            .into_response();
    }
    let base = std::path::Path::new("runs")
        .join("ruliad_kernel")
        .join(&run_id);
    if !base.exists() {
        return (
            axum::http::StatusCode::NOT_FOUND,
            format!("run {} not found", run_id),
        )
            .into_response();
    }

    let mut files = Vec::new();
    match tokio::fs::read_dir(&base).await {
        Ok(mut rd) => {
            while let Ok(Some(entry)) = rd.next_entry().await {
                if let Ok(meta) = entry.metadata().await {
                    if meta.is_file() {
                        let name = entry.file_name().to_string_lossy().to_string();
                        files.push(serde_json::json!({
                            "name": name,
                            "len": meta.len(),
                        }));
                    }
                }
            }
        }
        Err(e) => {
            return (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response();
        }
    }

    Json(serde_json::json!({
        "run_id": run_id,
        "dir": base.to_string_lossy(),
        "files": files
    }))
    .into_response()
}

#[utoipa::path(
    get,
    path = "/ruliad/{run_id}/{file}",
    responses((status = 200, description = "Fetch ruliad.kernel artifact"))
)]
pub async fn ruliad_file_handler(
    Path((run_id, file)): Path<(String, String)>,
) -> impl IntoResponse {
    if !is_safe_segment(&run_id) || !is_safe_segment(&file) {
        return (
            axum::http::StatusCode::BAD_REQUEST,
            "invalid path".to_string(),
        )
            .into_response();
    }
    let base = std::path::Path::new("runs")
        .join("ruliad_kernel")
        .join(&run_id);
    let path = base.join(&file);
    if !path.starts_with(&base) || !path.exists() {
        return (
            axum::http::StatusCode::NOT_FOUND,
            "file not found".to_string(),
        )
            .into_response();
    }
    let body = match tokio::fs::read(&path).await {
        Ok(b) => b,
        Err(e) => {
            return (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response();
        }
    };

    let ctype = if file.ends_with(".dot") {
        "text/vnd.graphviz; charset=utf-8"
    } else if file.ends_with(".json") || file.ends_with(".jsonl") {
        "application/json"
    } else if file.ends_with(".html") {
        "text/html; charset=utf-8"
    } else {
        "application/octet-stream"
    };

    (
        axum::http::StatusCode::OK,
        [(axum::http::header::CONTENT_TYPE, ctype)],
        body,
    )
        .into_response()
}

#[derive(Debug, Serialize, Deserialize, JsonSchema, ToSchema)]
pub struct ValidationResult {
    pub task: String,
    pub expected_difficulty: f32,
    pub actual_bits: Bits,
    pub score: f32,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema, ToSchema)]
pub struct VersionInfo {
    pub engine: &'static str,
    pub build_token: Option<&'static str>,
    pub git_ref: Option<&'static str>,
    pub ts: String,
}

impl VersionInfo {
    pub fn current() -> Self {
        let ts = chrono::Utc::now().to_rfc3339();
        Self {
            engine: env!("CARGO_PKG_VERSION"),
            build_token: option_env!("BUILD_TOKEN"),
            git_ref: option_env!("GIT_REF"),
            ts,
        }
    }
}

#[utoipa::path(
    get,
    path = "/version",
    responses(
        (status = 200, description = "Engine version", body = VersionInfo)
    )
)]
pub async fn version_handler() -> impl IntoResponse {
    Json(VersionInfo::current())
}

#[utoipa::path(
    post,
    path = "/run",
    request_body = RunReq,
    responses(
        (status = 200, description = "Run completed", body = RunResp)
    )
)]
pub async fn run_handler(
    State(_state): State<AppState>,
    Json(req): Json<RunReq>,
) -> impl IntoResponse {
    let mpayload = Mpayload {
        goal_id: req.goal_id.clone(),
        inputs: req.inputs.clone(),
        policy_effective: resolve_policy("run", None, req.policy.clone()),
        policy_request: req.policy.clone(),
        ctx: MpayloadCtx {
            kind: "run".to_string(),
            user_id: None,
            thread: None,
            run_id: req
                .run_id
                .clone()
                .unwrap_or_else(|| "auto".to_string()),
        },
    };
    let policy = mpayload.policy_effective.clone();
    let requested = req.run_id.clone();
    let run_id = requested
        .as_deref()
        .filter(|s| is_safe_segment(s))
        .map(|s| s.to_string())
        .unwrap_or_else(|| format!("r-{}", uuid::Uuid::new_v4()));
    emit_progress(&run_id, &req.goal_id, "init", json!({}));
    match run_with_integrations(&req.goal_id, req.inputs, &policy, &run_id).await {
        Ok((mut manifest, bits, pr_id, meta2_proposal)) => {
            manifest.run_id = run_id;
            emit_progress(
                &manifest.run_id,
                &manifest.goal_id,
                "done",
                json!({
                    "pr": pr_id,
                    "bits": bits,
                    "deliverables": manifest.deliverables,
                    "meta2_proposal": meta2_proposal
                }),
            );

            let resp = RunResp {
                manifest: manifest.clone(),
                bits: bits.clone(),
                pr_created: pr_id.clone(),
                meta2_proposal: meta2_proposal.clone(),
            };

            // Persist a receipt so the UI can show “good data” with stable links.
            // Log the effective mpayload for reproducibility.
            write_receipt_bundle(
                &resp.manifest.run_id,
                &resp.manifest.goal_id,
                &resp.bits,
                &resp.manifest.deliverables,
                &resp.manifest.evidence,
                false,
                &mpayload,
                &resp,
            )
            .await;

            Json(resp).into_response()
        }
        Err(e) => (axum::http::StatusCode::BAD_REQUEST, e.to_string()).into_response(),
    }
}

// DSL-compatible tau endpoint -> maps to run_with_integrations
pub async fn tau_handler(
    State(_state): State<AppState>,
    Json(req): Json<TauReq>,
) -> impl IntoResponse {
    let goal_id = req
        .preset
        .clone()
        .unwrap_or_else(|| "tau.default".to_string());
    let inputs = json!({
        "input": req.input,
        "context": req.context,
        "options": req.options
    });
    let policy = resolve_policy("dsl", None, None);
    let run_id = format!("r-{}", uuid::Uuid::new_v4());
    match run_with_integrations(&goal_id, inputs, &policy, &run_id).await {
        Ok((mut manifest, bits, pr_id, meta2_proposal)) => {
            manifest.run_id = run_id;
            Json(RunResp {
                manifest,
                bits,
                pr_created: pr_id,
                meta2_proposal,
            })
            .into_response()
        }
        Err(e) => (
            axum::http::StatusCode::BAD_REQUEST,
            format!("tau error: {}", e),
        )
            .into_response(),
    }
}

// DSL-compatible execute endpoint -> maps to run_with_integrations using goal
pub async fn execute_handler(
    State(_state): State<AppState>,
    Json(req): Json<ExecuteReq>,
) -> impl IntoResponse {
    let inputs = json!({
        "task_type": req.task_type,
        "parameters": req.parameters
    });
    let policy = resolve_policy("dsl", None, None);
    let run_id = format!("r-{}", uuid::Uuid::new_v4());
    match run_with_integrations(&req.goal, inputs, &policy, &run_id).await {
        Ok((mut manifest, bits, pr_id, meta2_proposal)) => {
            manifest.run_id = run_id;
            Json(RunResp {
                manifest,
                bits,
                pr_created: pr_id,
                meta2_proposal,
            })
            .into_response()
        }
        Err(e) => (
            axum::http::StatusCode::BAD_REQUEST,
            format!("execute error: {}", e),
        )
            .into_response(),
    }
}

// Health alias for DSL
pub async fn healthz_handler() -> impl IntoResponse {
    "ok"
}

// Minimal metrics stub to avoid 404s
pub async fn metrics_handler() -> impl IntoResponse {
    let uptime_s = START_TS
        .elapsed()
        .unwrap_or(Duration::from_secs(0))
        .as_secs();
    Json(json!({
        "status": "ok",
        "uptime_s": uptime_s,
        "note": "metrics stub (DSL compatibility)",
        "build": VersionInfo::current(),
    }))
}

// Seed/config helpers: surface current kernel and DSL file contents
pub async fn seed_handler() -> impl IntoResponse {
    let kernel = tokio::fs::read_to_string(".oneengine/kernel.json").await;
    match kernel {
        Ok(s) => Json(json!({"kernel": serde_json::from_str::<Value>(&s).unwrap_or(json!({}))})),
        Err(_) => Json(json!({"kernel": "not_found"})),
    }
}

pub async fn config_handler() -> impl IntoResponse {
    let dsl = tokio::fs::read_to_string(".oneengine/engine.dsl").await;
    match dsl {
        Ok(s) => Json(json!({"engine_dsl": s})),
        Err(_) => Json(json!({"engine_dsl": "not_found"})),
    }
}

// Pattern endpoints (stubbed)
pub async fn patterns_handler() -> impl IntoResponse {
    Json(json!({"patterns": []}))
}

pub async fn pattern_detail_handler(Path(_id): Path<String>) -> impl IntoResponse {
    (StatusCode::NOT_FOUND, "pattern not found")
}

// Mining stub
pub async fn mine_handler(payload: Option<Json<MineReq>>) -> impl IntoResponse {
    let req = payload.map(|j| j.0).unwrap_or_default();
    Json(json!({
        "status": "not_implemented",
        "sources": req.sources,
        "patterns": req.patterns,
        "output_format": req.output_format,
        "hint": "mining pipeline not wired; stub provided for DSL compatibility"
    }))
}

#[utoipa::path(
    post,
    path = "/validate",
    request_body = ValidateReq,
    responses(
        (status = 200, description = "Validation completed", body = ValidateResp)
    )
)]
pub async fn validate_handler(
    State(_state): State<AppState>,
    Json(req): Json<ValidateReq>,
) -> impl IntoResponse {
    match validate::run_suite(&req.suite).await {
        Ok(resp) => Json(resp).into_response(),
        Err(e) => (axum::http::StatusCode::BAD_REQUEST, e.to_string()).into_response(),
    }
}

#[utoipa::path(
    post,
    path = "/validate_golden",
    request_body = GoldenReq,
    responses((status = 200, description = "Golden validation", body = GoldenResp))
)]
pub async fn validate_golden_handler(Json(req): Json<GoldenReq>) -> impl IntoResponse {
    match engine::golden::validate_golden(&req.name).await {
        Ok(sum) => {
            let bits: Bits = sum.bits.into();
            Json(GoldenResp {
                name: sum.name,
                total: sum.total,
                passed: sum.passed,
                failed: sum.failed,
                details: sum.details,
                bits,
            })
            .into_response()
        }
        Err(e) => (axum::http::StatusCode::BAD_REQUEST, e.to_string()).into_response(),
    }
}

#[utoipa::path(
    get,
    path = "/dashboard",
    responses(
        (status = 200, description = "Unified dashboard state", body = UIState)
    )
)]
pub async fn dashboard_handler() -> impl IntoResponse {
    match integrations::ui::render_unified_state().await {
        Ok(state) => Json(state).into_response(),
        Err(e) => (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

#[utoipa::path(
    get,
    path = "/planning",
    responses(
        (status = 200, description = "Weekly planning goals", body = Vec<AgentGoal>)
    )
)]
pub async fn planning_handler() -> impl IntoResponse {
    match integrations::kpi::weekly_planning().await {
        Ok(goals) => Json(goals).into_response(),
        Err(e) => (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

#[utoipa::path(
    post,
    path = "/users/{user_id}/chat",
    request_body = ChatReq,
    responses((status = 200, description = "Chat reply", body = ChatResp))
)]
pub async fn user_chat_handler(
    State(state): State<AppState>,
    Path(user_id): Path<String>,
    headers: HeaderMap,
    Json(req): Json<ChatReq>,
) -> impl IntoResponse {
    // Auth
    let api_key = match extract_api_key(&headers) {
        Some(k) => k,
        None => return (axum::http::StatusCode::UNAUTHORIZED, "Missing x-api-key").into_response(),
    };
    let user = match authenticate_user(&state, &api_key) {
        Some(u) if u.user_id == user_id => u,
        _ => return (axum::http::StatusCode::UNAUTHORIZED, "Invalid user").into_response(),
    };
    let policy = resolve_policy("chat", Some(&user), req.policy.clone());

    let requested = req.run_id.clone();
    let run_id = requested
        .as_deref()
        .filter(|s| is_safe_segment(s))
        .map(|s| s.to_string())
        .unwrap_or_else(|| format!("r-{}", uuid::Uuid::new_v4()));
    let tx = progress_tx();
    let _ = tx.send(format!("{{\"run_id\":\"{}\",\"phase\":\"start\"}}", run_id));

    let thread = req
        .thread
        .as_deref()
        .unwrap_or("t-default")
        .to_string();
    if !is_safe_segment(&thread) {
        return (
            axum::http::StatusCode::BAD_REQUEST,
            "Invalid thread id".to_string(),
        )
            .into_response();
    }
    let thread_file = match thread_path(&user.user_id, &thread) {
        Some(p) => p,
        None => {
            return (
                axum::http::StatusCode::BAD_REQUEST,
                "Invalid thread path".to_string(),
            )
                .into_response()
        }
    };
    let history = load_thread_history(&thread_file, 24).await;
    append_thread_event(&thread_file, "user", &req.message, &run_id).await;

    // Use goal meta.omni
    let thread_id_for_resp = thread.clone();
    let loop_mode = req.loop_mode.unwrap_or(false);
    let inputs =
        serde_json::json!({"message": req.message, "thread": thread, "history": history, "loop_mode": loop_mode});
    let mpayload = Mpayload {
        goal_id: "meta.omni".to_string(),
        inputs: inputs.clone(),
        policy_effective: policy.clone(),
        policy_request: req.policy.clone(),
        ctx: MpayloadCtx {
            kind: "chat".to_string(),
            user_id: Some(user.user_id.clone()),
            thread: Some(thread.clone()),
            run_id: run_id.clone(),
        },
    };
    match run_with_integrations("meta.omni", inputs, &policy, &run_id).await {
        Ok((mut manifest, bits, _pr, _m2)) => {
            // Align manifest.run_id with the externally-visible run_id (for receipts + UI).
            manifest.run_id = run_id.clone();
            let reply = manifest
                .evidence
                .get("reply")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let _ = tx.send(format!("{{\"run_id\":\"{}\",\"phase\":\"done\"}}", run_id));

            append_thread_event(&thread_file, "assistant", &reply, &run_id).await;

            let run_payload = manifest.evidence.get("run_payload").cloned();
            let resp = ChatResp {
                run_id: run_id.clone(),
                user_id: user.user_id,
                thread: Some(thread_id_for_resp),
                reply,
                run_payload,
                manifest,
                bits,
            };

            // Persist a receipt so the terminal UI can link to a stable artifact.
            write_receipt_bundle(
                &resp.run_id,
                &resp.manifest.goal_id,
                &resp.bits,
                &resp.manifest.deliverables,
                &resp.manifest.evidence,
                true,
                &mpayload,
                &resp,
            )
            .await;

            Json(resp)
            .into_response()
        }
        Err(e) => (axum::http::StatusCode::BAD_REQUEST, e.to_string()).into_response(),
    }
}

#[utoipa::path(
    post,
    path = "/users/{user_id}/threads/{thread}/attach_run",
    request_body = AttachRunReq,
    responses(
        (status = 200, description = "Attached run output to thread context", body = AttachRunResp),
        (status = 401, description = "Unauthorized"),
        (status = 404, description = "Receipt not found"),
        (status = 409, description = "Run not ready")
    )
)]
pub async fn user_thread_attach_run_handler(
    State(state): State<AppState>,
    Path((user_id, thread)): Path<(String, String)>,
    headers: HeaderMap,
    Json(req): Json<AttachRunReq>,
) -> impl IntoResponse {
    // Auth
    let api_key = match extract_api_key(&headers) {
        Some(k) => k,
        None => return unauthorized("Missing x-api-key"),
    };
    let user = match authenticate_user(&state, &api_key) {
        Some(u) if u.user_id == user_id => u,
        _ => return unauthorized("Invalid user"),
    };

    if !is_safe_segment(&thread) {
        return (axum::http::StatusCode::BAD_REQUEST, "Invalid thread id".to_string())
            .into_response();
    }
    let thread_file = match thread_path(&user.user_id, &thread) {
        Some(p) => p,
        None => {
            return (axum::http::StatusCode::BAD_REQUEST, "Invalid thread path".to_string())
                .into_response()
        }
    };

    let run_id = req.run_id.trim().to_string();
    if !is_safe_segment(&run_id) {
        return (axum::http::StatusCode::BAD_REQUEST, "Invalid run_id".to_string()).into_response();
    }

    let resp = match read_receipt_response_json(&run_id).await {
        Ok(v) => v,
        Err(_) => {
            return (axum::http::StatusCode::NOT_FOUND, "Receipt not found".to_string())
                .into_response()
        }
    };

    // Only attach completed outputs (RunResp has manifest).
    if resp.get("manifest").is_none() {
        return (
            axum::http::StatusCode::CONFLICT,
            "Run not ready (no manifest yet)".to_string(),
        )
            .into_response();
    }

    let (summary, goal_id) = summarize_receipt_for_context(&run_id, &resp, req.note.as_deref());
    append_thread_event(&thread_file, "tool", &summary, &run_id).await;

    Json(AttachRunResp {
        ok: true,
        user_id: user.user_id,
        thread,
        run_id,
        goal_id,
        summary,
    })
    .into_response()
}

#[utoipa::path(
    get,
    path = "/users/{user_id}/threads/{thread}/summary",
    responses(
        (status = 200, description = "Thread summary (context growth)", body = ThreadSummaryResp),
        (status = 401, description = "Unauthorized"),
        (status = 404, description = "Thread not found")
    )
)]
pub async fn user_thread_summary_handler(
    State(state): State<AppState>,
    Path((user_id, thread)): Path<(String, String)>,
    headers: HeaderMap,
) -> impl IntoResponse {
    // Auth
    let api_key = match extract_api_key(&headers) {
        Some(k) => k,
        None => return unauthorized("Missing x-api-key"),
    };
    let user = match authenticate_user(&state, &api_key) {
        Some(u) if u.user_id == user_id => u,
        _ => return unauthorized("Invalid user"),
    };

    if !is_safe_segment(&thread) {
        return (axum::http::StatusCode::BAD_REQUEST, "Invalid thread id".to_string())
            .into_response();
    }
    let thread_file = match thread_path(&user.user_id, &thread) {
        Some(p) => p,
        None => {
            return (axum::http::StatusCode::BAD_REQUEST, "Invalid thread path".to_string())
                .into_response()
        }
    };

    if tokio::fs::metadata(&thread_file).await.is_err() {
        return (axum::http::StatusCode::NOT_FOUND, "Thread not found".to_string()).into_response();
    }

    Json(thread_summary(&thread_file, &user.user_id, &thread).await).into_response()
}

#[utoipa::path(
    post,
    path = "/run.async",
    request_body = RunReq,
    responses(
        (status = 202, description = "Run queued", body = RunAsyncResp)
    )
)]
pub async fn run_async_handler(
    State(_state): State<AppState>,
    Json(req): Json<RunReq>,
) -> impl IntoResponse {
    let requested = req.run_id.clone();
    let run_id = requested
        .as_deref()
        .filter(|s| is_safe_segment(s))
        .map(|s| s.to_string())
        .unwrap_or_else(|| format!("r-{}", uuid::Uuid::new_v4()));

    let goal_id = req.goal_id.clone();
    let mpayload = Mpayload {
        goal_id: req.goal_id.clone(),
        inputs: req.inputs.clone(),
        policy_effective: resolve_policy("run", None, req.policy.clone()),
        policy_request: req.policy.clone(),
        ctx: MpayloadCtx {
            kind: "run".to_string(),
            user_id: None,
            thread: None,
            run_id: run_id.clone(),
        },
    };
    let policy = mpayload.policy_effective.clone();
    let inputs = req.inputs.clone();

    emit_progress(&run_id, &goal_id, "queued", json!({}));
    set_active_run(&run_id, &goal_id, "queued").await;

    // Write an immediate placeholder receipt so links don't 404.
    let mut stub_bits = Bits::init();
    stub_bits.u = 0.2;
    let stub_evidence = json!({
        "expected_success": true,
        "actual_success": false,
        "status": "queued",
        "run_id": run_id,
        "goal_id": goal_id
    });
    let stub_resp = RunAsyncResp {
        run_id: run_id.clone(),
        goal_id: goal_id.clone(),
        status: "queued".to_string(),
        receipt_url: format!("/runs/receipts/{}/RECEIPT.md", run_id),
        sse_url: format!("/progress.sse?run_id={}", run_id),
    };
    write_receipt_bundle(
        &run_id,
        &goal_id,
        &stub_bits,
        &[],
        &stub_evidence,
        false,
        &mpayload,
        &stub_resp,
    )
    .await;

    // Run in background.
    let run_id_bg = run_id.clone();
    let goal_id_bg = goal_id.clone();
    tokio::spawn(async move {
        set_active_run(&run_id_bg, &goal_id_bg, "running").await;
        emit_progress(&run_id_bg, &goal_id_bg, "start", json!({}));
        match run_with_integrations(&goal_id_bg, inputs, &policy, &run_id_bg).await {
            Ok((mut manifest, bits, pr_id, meta2_proposal)) => {
                manifest.run_id = run_id_bg.clone();
                emit_progress(
                    &manifest.run_id,
                    &manifest.goal_id,
                    "done",
                    json!({
                        "pr": pr_id,
                        "bits": bits,
                        "deliverables": manifest.deliverables,
                        "meta2_proposal": meta2_proposal
                    }),
                );

                let resp = RunResp {
                    manifest: manifest.clone(),
                    bits: bits.clone(),
                    pr_created: pr_id.clone(),
                    meta2_proposal: meta2_proposal.clone(),
                };

                write_receipt_bundle(
                    &resp.manifest.run_id,
                    &resp.manifest.goal_id,
                    &resp.bits,
                    &resp.manifest.deliverables,
                    &resp.manifest.evidence,
                    false,
                    &mpayload,
                    &resp,
                )
                .await;
                clear_active_run(&run_id_bg).await;
            }
            Err(e) => {
                let mut bits = Bits::init();
                bits.e = 1.0;
                bits.u = 1.0;
                bits.t = 0.0;
                let manifest = Manifest {
                    run_id: run_id_bg.clone(),
                    goal_id: goal_id_bg.clone(),
                    deliverables: vec![],
                    evidence: json!({
                        "expected_success": true,
                        "actual_success": false,
                        "error": e.to_string()
                    }),
                    bits: bits.clone(),
                };
                let resp = RunResp {
                    manifest: manifest.clone(),
                    bits: bits.clone(),
                    pr_created: None,
                    meta2_proposal: None,
                };
                write_receipt_bundle(
                    &manifest.run_id,
                    &manifest.goal_id,
                    &bits,
                    &[],
                    &manifest.evidence,
                    false,
                    &mpayload,
                    &resp,
                )
                .await;
                emit_progress(&run_id_bg, &goal_id_bg, "error", json!({ "error": e.to_string() }));
                clear_active_run(&run_id_bg).await;
            }
        }
    });

    (StatusCode::ACCEPTED, Json(stub_resp)).into_response()
}

#[utoipa::path(
    get,
    path = "/runs.active.json",
    responses((status = 200, description = "Active queued/running runs", body = [ActiveRun]))
)]
pub async fn runs_active_json_handler() -> impl IntoResponse {
    let m = ACTIVE_RUNS.lock().await;
    let mut v: Vec<ActiveRun> = m.values().cloned().collect();
    v.sort_by(|a, b| b.ts.cmp(&a.ts).then_with(|| b.run_id.cmp(&a.run_id)));
    Json(v)
}

#[derive(Debug, Deserialize)]
pub struct ProgressQuery {
    pub run_id: Option<String>,
}

#[utoipa::path(
    get,
    path = "/progress.sse",
    responses((status = 200, description = "SSE progress stream"))
)]
pub async fn progress_sse_handler(
    Query(q): Query<ProgressQuery>,
) -> Sse<impl futures_core::Stream<Item = Result<Event, Infallible>>> {
    let target = q.run_id.clone();
    let rx = progress_tx().subscribe();
    let stream = BroadcastStream::new(rx)
        .filter_map(move |evt| match evt {
            Ok(s) => {
                if let Some(ref rid) = target {
                    if let Ok(v) = serde_json::from_str::<Value>(&s) {
                        if v.get("run_id").and_then(|r| r.as_str()) == Some(rid.as_str()) {
                            return Some(s);
                        } else {
                            return None;
                        }
                    }
                }
                Some(s)
            }
            Err(_) => None,
        })
        .map(|s| Ok(Event::default().data(s)));

    // Send real events (not just ":" comments) so reverse proxies (e.g. Cloudflare) keep the
    // connection alive and flush bytes regularly.
    // Cloudflare/HTTP2 can buffer small SSE payloads; include padding so clients see bytes.
    let pad = "x".repeat(1200);
    let keepalive = tokio_stream::wrappers::IntervalStream::new(tokio::time::interval(
        Duration::from_secs(15),
    ))
    .map(move |_| {
        Ok(Event::default()
            .event("keepalive")
            .data(format!("{{\"keepalive\":true,\"pad\":\"{}\"}}", pad)))
    });

    Sse::new(stream.merge(keepalive))
}

#[utoipa::path(get, path = "/browse", responses((status = 200, description = "Simple HTML browse page")))]
pub async fn browse_handler() -> impl IntoResponse {
    let root = PathBuf::from(std::env::var("META3_ROOT").unwrap_or_else(|_| ".".to_string()));

    async fn list_dirs(base: &PathBuf, rel: &str, limit: usize) -> Vec<String> {
        let mut out: Vec<(u64, String)> = Vec::new();
        let dir = base.join(rel);
        if let Ok(mut rd) = fs::read_dir(dir).await {
            while let Ok(Some(ent)) = rd.next_entry().await {
                if let Ok(ft) = ent.file_type().await {
                    if ft.is_dir() {
                        let name = ent.file_name().to_string_lossy().to_string();
                        let mtime = ent
                            .metadata()
                            .await
                            .ok()
                            .and_then(|m| m.modified().ok())
                            .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                            .map(|d| d.as_secs())
                            .unwrap_or(0);
                        out.push((mtime, name));
                    }
                }
            }
        }
        out.sort_by(|a, b| b.0.cmp(&a.0).then_with(|| b.1.cmp(&a.1)));
        out.truncate(limit);
        out.into_iter().map(|(_, n)| n).collect()
    }

    async fn list_files(base: &PathBuf, rel: &str, limit: usize) -> Vec<String> {
        let mut out: Vec<(u64, String)> = Vec::new();
        let dir = base.join(rel);
        if let Ok(mut rd) = fs::read_dir(dir).await {
            while let Ok(Some(ent)) = rd.next_entry().await {
                if let Ok(ft) = ent.file_type().await {
                    if ft.is_file() {
                        let name = ent.file_name().to_string_lossy().to_string();
                        let mtime = ent
                            .metadata()
                            .await
                            .ok()
                            .and_then(|m| m.modified().ok())
                            .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                            .map(|d| d.as_secs())
                            .unwrap_or(0);
                        out.push((mtime, name));
                    }
                }
            }
        }
        out.sort_by(|a, b| b.0.cmp(&a.0).then_with(|| b.1.cmp(&a.1)));
        out.truncate(limit);
        out.into_iter().map(|(_, n)| n).collect()
    }

    let receipts = list_dirs(&root, "runs/receipts", 30).await;
    let meta3_logs = list_files(&root, "runs/meta3-build", 50).await;

    let mut html = String::new();
    html.push_str("<!doctype html><html><head><meta charset=\"utf-8\"><title>One Engine Browse</title>");
    html.push_str("<meta name=\"viewport\" content=\"width=device-width,initial-scale=1\">");
    html.push_str("<style>body{font-family:system-ui,-apple-system,Segoe UI,Roboto,Arial;margin:24px} a{color:#1f6feb;text-decoration:none} a:hover{text-decoration:underline} code{background:#f6f8fa;padding:2px 6px;border-radius:6px} .muted{color:#57606a}</style>");
    html.push_str("</head><body>");
    html.push_str("<h1>One Engine</h1>");
    html.push_str("<p class=\"muted\">Quick links: ");
    html.push_str("<a href=\"/terminal\">Terminal</a> · <a href=\"/docs/openapi_request_examples.md\">API examples</a>");
    html.push_str("</p>");

    html.push_str("<h2>Latest receipts</h2><ul>");
    for r in receipts {
        let href = format!("/runs/receipts/{}/RECEIPT.md", r);
        html.push_str(&format!("<li><a href=\"{}\">{}</a></li>", href, r));
    }
    html.push_str("</ul>");

    html.push_str("<h2>meta3.build logs</h2><ul>");
    for f in meta3_logs {
        let href = format!("/runs/meta3-build/{}", f);
        html.push_str(&format!("<li><a href=\"{}\">{}</a></li>", href, f));
    }
    html.push_str("</ul>");

    html.push_str("<p class=\"muted\">Note: directory listing is intentionally disabled; this page links to concrete artifacts.</p>");
    html.push_str("</body></html>");

    Html(html)
}

#[utoipa::path(get, path = "/browse.json", responses((status = 200, description = "Browse index JSON")))]
pub async fn browse_json_handler() -> impl IntoResponse {
    let root = PathBuf::from(std::env::var("META3_ROOT").unwrap_or_else(|_| ".".to_string()));

    async fn list_dirs(base: &PathBuf, rel: &str, limit: usize) -> Vec<String> {
        let mut out: Vec<(u64, String)> = Vec::new();
        let dir = base.join(rel);
        if let Ok(mut rd) = fs::read_dir(dir).await {
            while let Ok(Some(ent)) = rd.next_entry().await {
                if let Ok(ft) = ent.file_type().await {
                    if ft.is_dir() {
                        let name = ent.file_name().to_string_lossy().to_string();
                        let mtime = ent
                            .metadata()
                            .await
                            .ok()
                            .and_then(|m| m.modified().ok())
                            .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                            .map(|d| d.as_secs())
                            .unwrap_or(0);
                        out.push((mtime, name));
                    }
                }
            }
        }
        out.sort_by(|a, b| b.0.cmp(&a.0).then_with(|| b.1.cmp(&a.1)));
        out.truncate(limit);
        out.into_iter().map(|(_, n)| n).collect()
    }

    async fn list_files(base: &PathBuf, rel: &str, limit: usize) -> Vec<String> {
        let mut out: Vec<(u64, String)> = Vec::new();
        let dir = base.join(rel);
        if let Ok(mut rd) = fs::read_dir(dir).await {
            while let Ok(Some(ent)) = rd.next_entry().await {
                if let Ok(ft) = ent.file_type().await {
                    if ft.is_file() {
                        let name = ent.file_name().to_string_lossy().to_string();
                        let mtime = ent
                            .metadata()
                            .await
                            .ok()
                            .and_then(|m| m.modified().ok())
                            .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                            .map(|d| d.as_secs())
                            .unwrap_or(0);
                        out.push((mtime, name));
                    }
                }
            }
        }
        out.sort_by(|a, b| b.0.cmp(&a.0).then_with(|| b.1.cmp(&a.1)));
        out.truncate(limit);
        out.into_iter().map(|(_, n)| n).collect()
    }

    let receipts = list_dirs(&root, "runs/receipts", 50).await;
    let meta3_logs = list_files(&root, "runs/meta3-build", 100).await;

    Json(json!({
        "meta3_root": root.display().to_string(),
        "receipts": receipts,
        "meta3_build_logs": meta3_logs,
        "links": {
            "ui": "/ui/",
            "browse": "/browse",
            "swagger": "/swagger-ui",
            "staleness": "/docs/staleness_matrix.md",
            "impact": "/docs/financial_impact.md"
        }
    }))
    .into_response()
}

#[derive(Debug, Serialize, Deserialize)]
struct StalenessEntry {
    #[serde(default)]
    feature: String,
    #[serde(default)]
    status: String,
    #[serde(default)]
    ts: String,
    #[serde(default)]
    detail: String,
}

#[derive(Debug, Serialize)]
struct Nudge {
    id: String,
    title: String,
    severity: String, // info | warn | error
    action: String,
    link: Option<String>,
    command: Option<String>,
    run_payload: Option<serde_json::Value>,
}

fn escape_html(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#39;")
}

fn nudge_for_feature(entry: &StalenessEntry) -> Option<Nudge> {
    let feature = entry.feature.trim();
    let status = entry.status.trim().to_ascii_lowercase();
    if status == "pass" {
        return None;
    }

    let (title, action, link, command, run_payload) = match feature {
        "health" => (
            "Health check failing".to_string(),
            "Restart the engine and re-check /health".to_string(),
            Some("/health".to_string()),
            Some("curl -s http://127.0.0.1:8080/health".to_string()),
            None,
        ),
        "version" => (
            "Version endpoint failing".to_string(),
            "Check /version output and logs".to_string(),
            Some("/version".to_string()),
            Some("curl -s http://127.0.0.1:8080/version | jq".to_string()),
            None,
        ),
        f if f.starts_with("run.demo.ping") => (
            "demo.ping failing".to_string(),
            "Run demo.ping and confirm a receipt is written".to_string(),
            Some("/browse".to_string()),
            None,
            Some(json!({
                "goal_id": "demo.ping",
                "inputs": {"message": "staleness nudge ping"},
                "policy": {"gamma_gate": 0.5, "time_ms": 8000, "max_risk": 0.3, "tiny_diff_loc": 120}
            })),
        ),
        f if f.contains("research.read") => (
            "research.read failing".to_string(),
            "Re-run the read and verify stale=false".to_string(),
            Some("/browse".to_string()),
            None,
            Some(json!({
                "goal_id": "research.read",
                "inputs": {"path": "/Users/jobs/Desktop/tmp-meta3-engine-test/research/sources/history_miner_folder/memory/policy_ucb.json"},
                "policy": {"gamma_gate": 0.5, "time_ms": 12000, "max_risk": 0.3, "tiny_diff_loc": 120}
            })),
        ),
        f if f.contains("meta3.build") => (
            "meta3.build failing".to_string(),
            "Re-run meta3.build (policy-driven default_cmd) and inspect log".to_string(),
            Some("/browse".to_string()),
            None,
            Some(json!({
                "goal_id": "meta3.build",
                "inputs": {"repo_path": "/Users/jobs/Desktop/meta3-monorepo"},
                "policy": {"gamma_gate": 0.5, "time_ms": 300000, "max_risk": 0.3, "tiny_diff_loc": 120}
            })),
        ),
        "progress.sse" => (
            "progress.sse missing/unknown".to_string(),
            "Connect SSE and verify events are emitted".to_string(),
            Some("/progress.sse".to_string()),
            Some("curl -N 'http://127.0.0.1:8080/progress.sse?run_id=r-sseprobe' | head -n 30".to_string()),
            None,
        ),
        _ => (
            format!("{} is {}", feature, entry.status),
            "Inspect staleness detail and logs".to_string(),
            Some("/docs/staleness_matrix.json".to_string()),
            None,
            None,
        ),
    };

    let severity = match status.as_str() {
        "fail" => "error",
        "unknown" => "warn",
        _ => "warn",
    }
    .to_string();

    Some(Nudge {
        id: format!("staleness:{}", feature),
        title,
        severity,
        action,
        link,
        command,
        run_payload,
    })
}

fn evergreen_nudges() -> Vec<Nudge> {
    let root = meta3_root();
    vec![
        Nudge {
            id: "evergreen:wiki_local".to_string(),
            title: "Generate a local DeepWiki snapshot".to_string(),
            severity: "info".to_string(),
            action: "Generate wiki under /runs/wiki/<run_id>/index.html".to_string(),
            link: Some("/browse".to_string()),
            command: None,
            run_payload: Some(json!({
                "goal_id": "wiki.generate",
                "inputs": {},
                "policy": {"gamma_gate": 0.5, "time_ms": 300000, "max_risk": 0.2, "tiny_diff_loc": 120}
            })),
        },
        Nudge {
            id: "evergreen:green_build".to_string(),
            title: "Produce a fresh green receipt (fast)".to_string(),
            severity: "info".to_string(),
            action: "Run a real build of the engine repo and write a receipt".to_string(),
            link: Some("/browse".to_string()),
            command: None,
            run_payload: Some(json!({
                "goal_id": "meta3.build",
                "inputs": {"repo_path": root.display().to_string(), "build_cmd": "cargo build --profile release-fast --bin one-engine"},
                "policy": {"gamma_gate": 0.5, "time_ms": 300000, "max_risk": 0.3, "tiny_diff_loc": 120}
            })),
        },
        Nudge {
            id: "evergreen:threads_report".to_string(),
            title: "Summarize this chat thread (auto)".to_string(),
            severity: "info".to_string(),
            action: "Generate an HTML report from recent chat turns + receipts".to_string(),
            link: Some("/terminal".to_string()),
            command: None,
            run_payload: Some(json!({
                "goal_id": "threads.report",
                "inputs": {"user_id": "demo", "thread": "auto", "max_events": 600, "content_chars": 240},
                "policy": {"gamma_gate": 0.5, "time_ms": 120000, "max_risk": 0.2, "tiny_diff_loc": 120}
            })),
        },
        Nudge {
            id: "evergreen:graphs_thread".to_string(),
            title: "Generate a thread graph (auto)".to_string(),
            severity: "info".to_string(),
            action: "Generate a recursive, bits-native graph from recent chat turns".to_string(),
            link: Some("/terminal".to_string()),
            command: None,
            run_payload: Some(json!({
                "goal_id": "graphs.thread",
                "inputs": {"user_id": "demo", "thread": "auto", "recursive": true, "depth": 2, "max_nodes": 400, "include_bits": true},
                "policy": {"gamma_gate": 0.5, "time_ms": 120000, "max_risk": 0.2, "tiny_diff_loc": 120}
            })),
        },
    ]
}

async fn compute_nudges(root: &PathBuf) -> (usize, Vec<Nudge>) {
    let staleness_path = root.join("docs/staleness_matrix.json");
    let mut nudges: Vec<Nudge> = Vec::new();
    let mut staleness: Vec<StalenessEntry> = Vec::new();

    if let Ok(raw) = fs::read_to_string(&staleness_path).await {
        if let Ok(parsed) = serde_json::from_str::<Vec<StalenessEntry>>(&raw) {
            staleness = parsed;
            for e in &staleness {
                if let Some(n) = nudge_for_feature(e) {
                    nudges.push(n);
                }
            }
        } else {
            nudges.push(Nudge {
                id: "staleness:parse_error".to_string(),
                title: "Staleness report parse error".to_string(),
                severity: "warn".to_string(),
                action: "Fix docs/staleness_matrix.json or regenerate it".to_string(),
                link: Some("/docs/staleness_matrix.json".to_string()),
                command: Some("cd /Users/jobs/Desktop && ./scripts/workflows/run_all.sh".to_string()),
                run_payload: None,
            });
        }
    } else {
        nudges.push(Nudge {
            id: "staleness:missing".to_string(),
            title: "Staleness report missing".to_string(),
            severity: "warn".to_string(),
            action: "Generate docs/staleness_matrix.json to populate nudges".to_string(),
            link: Some("/docs/".to_string()),
            command: Some("cd /Users/jobs/Desktop && ./scripts/workflows/run_all.sh".to_string()),
            run_payload: None,
        });
    }

    // If no wiki snapshots exist, surface a higher-priority nudge (even if staleness has other failures).
    let wiki_dir = root.join("runs/wiki");
    let mut has_any_wiki = false;
    if let Ok(mut rd) = fs::read_dir(&wiki_dir).await {
        while let Ok(Some(ent)) = rd.next_entry().await {
            if ent.file_type().await.ok().map(|ft| ft.is_dir()).unwrap_or(false) {
                has_any_wiki = true;
                break;
            }
        }
    }
    if !has_any_wiki {
        nudges.push(Nudge {
            id: "wiki:missing".to_string(),
            title: "No wiki snapshots yet".to_string(),
            severity: "warn".to_string(),
            action: "Run wiki.generate to create the first /runs/wiki/<run_id>/index.html".to_string(),
            link: Some("/browse".to_string()),
            command: None,
            run_payload: Some(json!({
                "goal_id": "wiki.generate",
                "inputs": {},
                "policy": {"gamma_gate": 0.5, "time_ms": 300000, "max_risk": 0.2, "tiny_diff_loc": 120}
            })),
        });
    }

    // Always append evergreen nudges (dedup by id) so the UI always has “Run this” actions.
    let mut seen: HashSet<String> = nudges.iter().map(|n| n.id.clone()).collect();
    for n in evergreen_nudges() {
        if seen.insert(n.id.clone()) {
            nudges.push(n);
        }
    }

    (staleness.len(), nudges)
}

#[utoipa::path(get, path = "/nudges.json", responses((status = 200, description = "Actionable next steps")))]
pub async fn nudges_json_handler() -> impl IntoResponse {
    let root = PathBuf::from(std::env::var("META3_ROOT").unwrap_or_else(|_| ".".to_string()));
    let (staleness_entries, nudges) = compute_nudges(&root).await;

    Json(json!({
        "meta3_root": root.display().to_string(),
        "staleness_entries": staleness_entries,
        "nudges": nudges
    }))
    .into_response()
}

#[utoipa::path(get, path = "/nudges", responses((status = 200, description = "Simple HTML nudges page")))]
pub async fn nudges_handler() -> impl IntoResponse {
    let root = PathBuf::from(std::env::var("META3_ROOT").unwrap_or_else(|_| ".".to_string()));
    let (_staleness_entries, nudges) = compute_nudges(&root).await;

    let mut html = String::new();
    html.push_str("<!doctype html><html><head><meta charset=\"utf-8\"><title>One Engine Nudges</title>");
    html.push_str("<meta name=\"viewport\" content=\"width=device-width,initial-scale=1\">");
    html.push_str("<style>body{font-family:system-ui,-apple-system,Segoe UI,Roboto,Arial;margin:24px} a{color:#1f6feb;text-decoration:none} a:hover{text-decoration:underline} code{background:#f6f8fa;padding:2px 6px;border-radius:6px} .muted{color:#57606a} .pill{display:inline-block;padding:2px 8px;border-radius:999px;font-size:12px;background:#eef2ff;margin-right:8px} .pill.warn{background:#fff7ed} .pill.error{background:#fee2e2}</style>");
    html.push_str("</head><body>");
    html.push_str("<h1>Nudges</h1>");
    html.push_str("<p class=\"muted\">Next steps computed from <a href=\"/docs/staleness_matrix.json\">staleness_matrix.json</a>. ");
    html.push_str("Quick links: <a href=\"/ui/\">UI</a> · <a href=\"/browse\">Browse</a> · <a href=\"/swagger-ui\">Swagger</a></p>");
    html.push_str("<ul>");
    for n in nudges {
        let title = n.title;
        let action = n.action;
        let severity = n.severity;
        let link = n.link;
        let command = n.command;
        html.push_str("<li style=\"margin:12px 0\">");
        html.push_str(&format!(
            "<span class=\"pill {}\">{}</span><strong>{}</strong><div class=\"muted\">{}</div>",
            severity,
            severity,
            escape_html(&title),
            escape_html(&action),
        ));
        if let Some(h) = link.as_deref() {
            html.push_str(&format!(
                "<div><a href=\"{}\" target=\"_blank\" rel=\"noreferrer\">{}</a></div>",
                h,
                escape_html(h)
            ));
        }
        if let Some(c) = command.as_deref() {
            html.push_str(&format!(
                "<div><code>{}</code></div>",
                escape_html(c)
            ));
        }
        html.push_str("</li>");
    }
    html.push_str("</ul>");
    html.push_str("</body></html>");

    Html(html)
}

#[utoipa::path(
    get,
    path = "/golden/{name}",
    responses((status = 200, description = "Golden trace JSON"))
)]
pub async fn golden_handler(Path(name): Path<String>) -> impl IntoResponse {
    // basic sanitization: allow [a-zA-Z0-9_\-]
    if !name
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
    {
        return (axum::http::StatusCode::BAD_REQUEST, "invalid name").into_response();
    }
    let path = format!("trace/golden/{}.json", name);
    match fs::read_to_string(&path).await {
        Ok(s) => match serde_json::from_str::<serde_json::Value>(&s) {
            Ok(v) => Json(v).into_response(),
            Err(e) => (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("invalid JSON: {}", e),
            )
                .into_response(),
        },
        Err(e) => (
            axum::http::StatusCode::NOT_FOUND,
            format!("not found: {}", e),
        )
            .into_response(),
    }
}

async fn run_with_integrations(
    goal_id: &str,
    inputs: serde_json::Value,
    policy: &Policy,
    run_id: &str,
) -> anyhow::Result<(Manifest, Bits, Option<String>, Option<String>)> {
    let tx = progress_tx();
    let _ = tx.send(json!({"run_id": run_id, "goal_id": goal_id, "phase": "plan"}).to_string());

    // Demo long-running goal with incremental progress updates.
    if goal_id == "demo.wait" {
        let seconds = inputs
            .get("seconds")
            .and_then(|v| v.as_u64())
            .unwrap_or(30)
            .clamp(1, 3600);
        let tick_ms = inputs
            .get("tick_ms")
            .and_then(|v| v.as_u64())
            .unwrap_or(1000)
            .clamp(100, 5000);
        let label = inputs
            .get("label")
            .and_then(|v| v.as_str())
            .unwrap_or("demo.wait");

        let total_ms = seconds.saturating_mul(1000);
        let total_ticks = ((total_ms + tick_ms - 1) / tick_ms).max(1);

        let _ = tx.send(json!({"run_id": run_id, "goal_id": goal_id, "phase": "act"}).to_string());
        for i in 0..=total_ticks {
            let pct = ((i as f64) / (total_ticks as f64)).min(1.0);
            let eta_s = ((total_ticks.saturating_sub(i)) * tick_ms + 999) / 1000;
            let _ = tx.send(
                json!({
                    "run_id": run_id,
                    "goal_id": goal_id,
                    "phase": "tick",
                    "extra": {
                        "label": label,
                        "i": i,
                        "total": total_ticks,
                        "pct": pct,
                        "eta_s": eta_s,
                        "tick_ms": tick_ms
                    }
                })
                .to_string(),
            );
            if i < total_ticks {
                tokio::time::sleep(Duration::from_millis(tick_ms)).await;
            }
        }

        let _ = tx.send(json!({"run_id": run_id, "goal_id": goal_id, "phase": "verify"}).to_string());

        let mut bits = Bits::init();
        bits.u = 0.2;
        bits.t = 0.95;

        let manifest = Manifest {
            run_id: run_id.to_string(),
            goal_id: goal_id.to_string(),
            deliverables: vec![],
            evidence: json!({
                "expected_success": true,
                "actual_success": true,
                "seconds": seconds,
                "tick_ms": tick_ms,
                "label": label
            }),
            bits: bits.clone(),
        };

        let _ = tx.send(json!({"run_id": run_id, "goal_id": goal_id, "phase": "done"}).to_string());
        return Ok((manifest, bits, None, None));
    }

    // 1. Search flywheel for context
    let _context = integrations::flywheel::search(goal_id).await?;

    let _ = tx.send(json!({"run_id": run_id, "goal_id": goal_id, "phase": "act"}).to_string());

    // 2. Run engine with meta² layer
    // Inject the external run_id so goals can name artifacts deterministically.
    let inputs = match inputs {
        Value::Object(mut map) => {
            map.insert("__run_id".to_string(), json!(run_id));
            Value::Object(map)
        }
        other => other,
    };
    let (manifest, ext_bits, meta2_proposal) = engine::run(goal_id, inputs, policy).await?;
    let bits: Bits = ext_bits.into(); // Convert to legacy format

    let _ = tx.send(json!({"run_id": run_id, "goal_id": goal_id, "phase": "verify"}).to_string());

    // 3. Update flywheel metadata
    integrations::flywheel::update_metadata(goal_id, &manifest, bits.t).await?;

    // 4. Create PR if confident
    let pr = integrations::monorepo::create_pr_if_confident(&manifest, &bits).await?;
    let pr_id = pr.map(|p| p.id);

    // 5. Serialize meta² proposal if present
    let meta2_json = meta2_proposal.map(|p| serde_json::to_string(&p).unwrap_or_default());

    let _ = tx.send(json!({"run_id": run_id, "goal_id": goal_id, "phase": "done"}).to_string());

    Ok((manifest, bits, pr_id, meta2_json))
}

#[derive(OpenApi)]
#[openapi(
    info(
        title = "one-engine",
        description = "What this engine does:\n- /run and /users/{id}/run execute goals and emit SSE beacons per run_id (plan, act, verify, done) via /progress.sse (filterable by run_id).\n- /validate supports suites: easy, hard, adaptive, impossible; returns metacognitive_score and per-task bits/score.\n- /research/index lists artifacts from research/index.jsonl.\n- /dashboard shows recent runs/evals; /planning returns weekly goals.\n- /meta/* and /nstar/* provide meta selection and N* loop hooks.\n- Auth: /users/* endpoints expect x-api-key; /run is open in this build.\n",
    ),
    paths(
        version_handler,
        run_handler,
        run_async_handler,
        runs_active_json_handler,
        validate_handler,
        validate_golden_handler,
        dashboard_handler,
        planning_handler,
        user_run_handler,
        user_status_handler,
        user_chat_handler,
        user_thread_attach_run_handler,
        user_thread_summary_handler,
        progress_sse_handler,
        golden_handler,
        research_index_handler,
        codex_sources_handler,
        codex_archive_handler,
        codex_rollouts_list_handler,
        codex_rollout_file_handler,
        codex_capabilities_handler,
        codex_search_handler,
        ruliad_list_handler,
        ruliad_file_handler,
        meta::meta_run_handler,
        meta::meta_state_handler,
        meta::meta_reset_handler,
        nstar::nstar_run_handler,
        nstar::nstar_hud_handler
    ),
    components(schemas(Bits, Policy, Manifest, RunReq, RunResp, RunAsyncResp, ActiveRun, VersionInfo, ValidateReq, ValidateResp, GoldenReq, GoldenResp, ValidationResult, UIState, AgentGoal, UserRunReq, UserRunResp, UserStatus, ChatReq, ChatResp, AttachRunReq, AttachRunResp, ThreadSummaryResp, CodexSourceInfo, CodexSourcesResp, CountedItem, CapScanFile, CodexCapabilitiesResp, CodexSearchResult, CodexSearchResp, nstar::NStarRunReq, nstar::NStarRunResp, meta::MetaRunReq, meta::MetaRunResp, meta::MetaState)),
    tags((name="one-engine", description="Multi-tenant metacognitive system"))
)]
pub struct ApiDoc;

#[utoipa::path(
    get,
    path = "/research/index",
    responses((status = 200, description = "Research artifact index", body = [ResearchArtifact]))
)]
pub async fn research_index_handler() -> impl IntoResponse {
    // Prefer on-disk index if present; else build from current workspace.
    let disk = tokio::fs::read_to_string("research/index.jsonl").await;
    let mut items: Vec<ResearchArtifact> = Vec::new();
    if let Ok(s) = disk {
        for line in s.lines() {
            if line.trim().is_empty() {
                continue;
            }
            if let Ok(a) = serde_json::from_str::<ResearchArtifact>(line) {
                items.push(a);
            }
        }
    } else {
        // Fallback: build ephemeral index from '.' (no network)
        if let Ok(v) = research::build_index(std::path::Path::new(".")) {
            items = v;
        }
    }
    Json(items)
}
