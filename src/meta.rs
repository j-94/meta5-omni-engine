use axum::{response::IntoResponse, Json};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use tokio::fs;
use tokio::process::Command as TokioCommand;
use utoipa::ToSchema;

#[derive(Debug, Serialize, Deserialize, JsonSchema, ToSchema)]
pub struct MetaRunReq {
    pub task: String,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema, ToSchema)]
pub struct MetaRunResp {
    pub run_id: String,
    pub task: String,
    pub plan: String,
    pub config: serde_json::Value,
    pub artifact: String,
    pub telemetry: serde_json::Value,
    pub score: f32,
    pub latency_s: f32,
}

#[utoipa::path(
    post,
    path = "/meta/run",
    request_body = MetaRunReq,
    responses((status=200, description="Run one meta selection step", body=MetaRunResp))
)]
pub async fn meta_run_handler(Json(req): Json<MetaRunReq>) -> impl IntoResponse {
    let script =
        std::env::var("META_SCRIPT").unwrap_or_else(|_| "scripts/meta_loop.py".to_string());
    let out = TokioCommand::new("python3")
        .arg(&script)
        .arg(&req.task)
        .output()
        .await;
    match out {
        Ok(o) if o.status.success() => {
            let text = String::from_utf8_lossy(&o.stdout);
            match serde_json::from_str::<serde_json::Value>(&text) {
                Ok(v) => {
                    let resp = MetaRunResp {
                        run_id: v
                            .get("run_id")
                            .and_then(|x| x.as_str())
                            .unwrap_or("")
                            .to_string(),
                        task: v
                            .get("task")
                            .and_then(|x| x.as_str())
                            .unwrap_or("")
                            .to_string(),
                        plan: v
                            .get("plan")
                            .and_then(|x| x.as_str())
                            .unwrap_or("")
                            .to_string(),
                        config: v.get("config").cloned().unwrap_or(serde_json::json!({})),
                        artifact: v
                            .get("artifact")
                            .and_then(|x| x.as_str())
                            .unwrap_or("")
                            .to_string(),
                        telemetry: v.get("telemetry").cloned().unwrap_or(serde_json::json!({})),
                        score: v.get("score").and_then(|x| x.as_f64()).unwrap_or(0.0) as f32,
                        latency_s: v.get("latency_s").and_then(|x| x.as_f64()).unwrap_or(0.0)
                            as f32,
                    };
                    Json(resp).into_response()
                }
                Err(e) => (
                    axum::http::StatusCode::BAD_REQUEST,
                    format!("meta invalid json: {}", e),
                )
                    .into_response(),
            }
        }
        Ok(o) => {
            let stderr = String::from_utf8_lossy(&o.stderr);
            (
                axum::http::StatusCode::BAD_REQUEST,
                format!("meta failed: {}", stderr),
            )
                .into_response()
        }
        Err(e) => (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            format!("spawn error: {}", e),
        )
            .into_response(),
    }
}

#[derive(Debug, Serialize, Deserialize, JsonSchema, ToSchema)]
pub struct MetaState {
    pub beta: serde_json::Value,
    pub gamma: serde_json::Value,
    pub beta_ids: Vec<String>,
    pub gamma_ids: Vec<String>,
    pub rubric: Option<String>,
    pub ts: Option<String>,
}

#[utoipa::path(
    get,
    path = "/meta/state",
    responses((status=200, description="Current meta UCB state", body=MetaState))
)]
pub async fn meta_state_handler() -> impl IntoResponse {
    let path =
        std::env::var("META_STATE").unwrap_or_else(|_| "trace/meta_ucb_state.json".to_string());
    match fs::read_to_string(&path).await {
        Ok(s) => match serde_json::from_str::<MetaState>(&s) {
            Ok(v) => Json(v).into_response(),
            Err(e) => (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("invalid meta state: {}", e),
            )
                .into_response(),
        },
        Err(_) => (
            axum::http::StatusCode::NOT_FOUND,
            "no meta state".to_string(),
        )
            .into_response(),
    }
}
#[utoipa::path(
    post,
    path = "/meta/reset",
    responses((status=200, description="Reset meta state"))
)]
pub async fn meta_reset_handler() -> impl IntoResponse {
    let path =
        std::env::var("META_STATE").unwrap_or_else(|_| "trace/meta_ucb_state.json".to_string());
    let _ = fs::remove_file(&path).await;
    (axum::http::StatusCode::OK, "reset").into_response()
}
