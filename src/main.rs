mod api;
mod engine;
mod integrations;
mod meta;
mod nstar;

use axum::http::StatusCode;
use axum::{
    middleware,
    response::Redirect,
    routing::{get, get_service, post},
    Router,
};
use std::path::PathBuf;
use tokio::net::TcpListener;
use tower_http::services::ServeDir;
use tracing_subscriber::{fmt, EnvFilter};
use utoipa::OpenApi;
use utoipa_swagger_ui::SwaggerUi;

fn load_dotenv_if_present() {
    // Minimal .env loader (no deps): does not override existing env vars.
    // Supports lines like KEY=VALUE, KEY="VALUE", KEY='VALUE', ignores comments/blank lines.
    let path = std::path::Path::new(".env");
    let Ok(raw) = std::fs::read_to_string(path) else {
        return;
    };
    for line in raw.lines() {
        let s = line.trim();
        if s.is_empty() || s.starts_with('#') {
            continue;
        }
        let Some((k, v)) = s.split_once('=') else {
            continue;
        };
        let key = k.trim();
        if key.is_empty() {
            continue;
        }
        if std::env::var_os(key).is_some() {
            continue;
        }
        let mut val = v.trim().to_string();
        if (val.starts_with('"') && val.ends_with('"') && val.len() >= 2)
            || (val.starts_with('\'') && val.ends_with('\'') && val.len() >= 2)
        {
            val = val[1..val.len() - 1].to_string();
        }
        if !val.is_empty() {
            std::env::set_var(key, val);
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    load_dotenv_if_present();

    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    fmt().with_env_filter(env_filter).init();

    let state = api::AppState::default();
    let openapi = api::ApiDoc::openapi();
    let enable_swagger = std::env::var("ENABLE_SWAGGER").ok().as_deref() == Some("1");

    let meta_root = PathBuf::from(std::env::var("META3_ROOT").unwrap_or_else(|_| ".".to_string()));
    let docs_root = meta_root.join("docs");
    let runs_root = meta_root.join("runs");

    let docs_service = get_service(ServeDir::new(docs_root))
        .handle_error(|_| async move { (StatusCode::INTERNAL_SERVER_ERROR, "static file error") });

    let runs_service = get_service(ServeDir::new(runs_root))
        .handle_error(|_| async move { (StatusCode::INTERNAL_SERVER_ERROR, "static file error") });

    let ui_service = get_service(ServeDir::new("ui").append_index_html_on_directories(true))
        .handle_error(|_| async move { (StatusCode::INTERNAL_SERVER_ERROR, "static file error") });

    let mut app = Router::new()

        .route("/", get(|| async { Redirect::temporary("/ui/") }))
        .route(
            "/terminal",
            get(|| async { Redirect::temporary("/ui/") }),
        )
        .route("/health", get(|| async { "ok" }))
        .route("/healthz", get(api::healthz_handler))
        .route("/version", get(api::version_handler))
        .route("/metrics", get(api::metrics_handler))
        .route("/tau", post(api::tau_handler))
        .route("/execute", post(api::execute_handler))
        .route("/execute/:task_id", get(api::execute_handler))
        .route("/mine", post(api::mine_handler))
        .route("/patterns", get(api::patterns_handler))
        .route("/patterns/:pattern_id", get(api::pattern_detail_handler))
        .route("/seed", get(api::seed_handler))
        .route("/config", get(api::config_handler))
        .route("/run", post(api::run_handler))
        .route("/run.async", post(api::run_async_handler))
        .route("/runs.active.json", get(api::runs_active_json_handler))
        .route("/ruliad/:run_id", get(api::ruliad_list_handler))
        .route("/ruliad/:run_id/:file", get(api::ruliad_file_handler))
        .route("/validate", post(api::validate_handler))
        .route("/validate_golden", post(api::validate_golden_handler))
        .route("/golden/:name", get(api::golden_handler))
        .route("/dashboard", get(api::dashboard_handler))
        .route("/planning", get(api::planning_handler))
        .route("/research/index", get(api::research_index_handler))
        .route("/codex/sources", get(api::codex_sources_handler))
        .route("/codex/archive", get(api::codex_archive_handler))
        .route("/codex/rollouts", get(api::codex_rollouts_list_handler))
        .route(
            "/codex/rollouts/:file",
            get(api::codex_rollout_file_handler),
        )
        .route("/codex/capabilities", get(api::codex_capabilities_handler))
        .route("/codex/search", get(api::codex_search_handler))
        .route("/browse", get(api::browse_handler))
        .route("/browse.json", get(api::browse_json_handler))
        .route("/nudges", get(api::nudges_handler))
        .route("/nudges.json", get(api::nudges_json_handler))
        .nest_service("/ui", ui_service)
        .nest_service("/docs", docs_service)
        .nest_service("/runs", runs_service)
        // Multi-tenant user endpoints
        .route("/users/:user_id/run", post(api::user_run_handler))
        .route("/users/:user_id/chat", post(api::user_chat_handler))
        .route(
            "/users/:user_id/threads/:thread/attach_run",
            post(api::user_thread_attach_run_handler),
        )
        .route(
            "/users/:user_id/threads/:thread/summary",
            get(api::user_thread_summary_handler),
        )
        .route("/progress.sse", get(api::progress_sse_handler))
        .route("/users/:user_id/status", get(api::user_status_handler))
        .route("/nstar/run", post(nstar::nstar_run_handler))
        .route("/nstar/hud", get(nstar::nstar_hud_handler))
        .route("/meta/run", post(meta::meta_run_handler))
        .route("/meta/state", get(meta::meta_state_handler))
        .route("/meta/reset", post(meta::meta_reset_handler))
        .route("/v1/context/resolve", post(nstar::resolve_context_handler))
        .layer(middleware::from_fn(api::api_trace_middleware))
        .with_state(state);

    if enable_swagger {
        app = app.merge(SwaggerUi::new("/swagger-ui").url("/api-docs/openapi.json", openapi));
    }

    let addr = std::net::SocketAddr::from(([127, 0, 0, 1], 8080));
    tracing::info!("🚀 Integrated One Engine listening on http://{addr}");
    tracing::info!("📊 Dashboard: http://{addr}/dashboard");
    tracing::info!("📋 Planning: http://{addr}/planning");
    if enable_swagger {
        tracing::info!("📖 Docs: http://{addr}/swagger-ui");
    }

    let listener = TcpListener::bind(&addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}
