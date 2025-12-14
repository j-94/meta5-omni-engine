use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

pub use super::bits::Bits;

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, ToSchema)]
pub struct Policy {
    pub gamma_gate: f32,
    pub time_ms: u64,
    pub max_risk: f32,
    pub tiny_diff_loc: u32,
}

impl Default for Policy {
    fn default() -> Self {
        Self {
            gamma_gate: 0.5,
            time_ms: 300_000,
            max_risk: 0.2,
            tiny_diff_loc: 120,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, ToSchema)]
pub struct Manifest {
    pub run_id: String,
    pub goal_id: String,
    pub deliverables: Vec<String>,
    pub evidence: serde_json::Value,
    pub bits: Bits,
}
