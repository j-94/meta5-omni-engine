use super::{CostSummary, EvalResult, KPIDashboard, SearchResult, UIState};
use crate::engine::types::Manifest;
use chrono::Utc;

pub async fn render_unified_state() -> anyhow::Result<UIState> {
    let state = UIState {
        search_hits: get_recent_searches().await,
        agent_runs: get_recent_runs().await,
        eval_scores: get_recent_evals().await,
        cost_tracking: get_cost_summary().await,
        kpi_dashboard: super::kpi::current_scores().await,
    };

    Ok(state)
}

async fn get_recent_searches() -> Vec<SearchResult> {
    // Simulate recent flywheel searches
    vec![SearchResult {
        id: "search-1".to_string(),
        content: "Recent search result".to_string(),
        relevance: 0.92,
        metadata: serde_json::json!({"component": "flywheel"}),
    }]
}

async fn get_recent_runs() -> Vec<Manifest> {
    // Simulate recent agent runs
    vec![]
}

async fn get_recent_evals() -> Vec<EvalResult> {
    // Simulate recent evaluations
    vec![EvalResult {
        eval_id: "eval-1".to_string(),
        score: 0.85,
        component: "agent".to_string(),
        timestamp: Utc::now().to_rfc3339(),
    }]
}

async fn get_cost_summary() -> CostSummary {
    CostSummary {
        total_tokens: 125000,
        total_cost: 2.50,
        cost_per_success: 0.15,
    }
}
