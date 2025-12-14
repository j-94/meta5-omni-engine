pub mod flywheel;
pub mod kpi;
pub mod monorepo;
pub mod telemetry;
pub mod ui;

use crate::engine::types::{Bits, Manifest};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Debug, Serialize, Deserialize, JsonSchema, ToSchema)]
pub struct TelemetryEvent {
    pub ts: String, // ISO 8601 timestamp
    pub component: String,
    pub event_type: String,
    pub run_id: Option<String>,
    pub bits: Option<Bits>,
    pub cost: Option<f32>,
    pub kpi_impact: Option<f32>,
    pub metadata: serde_json::Value,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema, ToSchema)]
pub struct AgentGoal {
    pub id: String,
    pub kpi_target: String,
    pub priority: f32,
    pub estimated_impact: f32,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema, ToSchema)]
pub struct UIState {
    pub search_hits: Vec<SearchResult>,
    pub agent_runs: Vec<Manifest>,
    pub eval_scores: Vec<EvalResult>,
    pub cost_tracking: CostSummary,
    pub kpi_dashboard: KPIDashboard,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema, ToSchema)]
pub struct SearchResult {
    pub id: String,
    pub content: String,
    pub relevance: f32,
    pub metadata: serde_json::Value,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema, ToSchema)]
pub struct EvalResult {
    pub eval_id: String,
    pub score: f32,
    pub component: String,
    pub timestamp: String, // ISO 8601 timestamp
}

#[derive(Debug, Serialize, Deserialize, JsonSchema, ToSchema)]
pub struct CostSummary {
    pub total_tokens: u64,
    pub total_cost: f32,
    pub cost_per_success: f32,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema, ToSchema)]
pub struct KPIDashboard {
    pub signal_density: f32,
    pub flow_minutes: f32,
    pub knowledge_yield: f32,
    pub noise_ratio: f32,
    pub weekly_trend: Vec<f32>,
}
