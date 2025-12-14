use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, ToSchema)]
pub struct ExtendedBits {
    pub a: f32,
    pub u: f32,
    pub p: f32,
    pub e: f32,
    #[serde(rename = "d")]
    pub d: f32, // drift
    pub i: f32,
    pub r: f32,
    pub t: f32,
    pub m: f32, // meta-change bit: 1=policy update proposed
}

impl ExtendedBits {
    pub fn init() -> Self {
        Self {
            a: 1.0,
            p: 1.0,
            t: 0.5,
            m: 0.0,
            u: 0.0,
            e: 0.0,
            d: 0.0,
            i: 0.0,
            r: 0.0,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct KernelLoop {
    pub l2_params: L2Params,
    pub l3_rules: L3Rules,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct L2Params {
    pub ask_act_threshold: f32,
    pub confidence_gate_tau: f32,
    pub backoff_k: u32,
    pub retry_strategies: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct L3Rules {
    pub evidence_coverage_min: f32,
    pub rollback_rate_max: f32,
    pub weekly_param_delta_max: f32,
    pub shadow_rollout_pct: f32,
}

impl KernelLoop {
    pub fn new() -> Self {
        Self {
            l2_params: L2Params {
                ask_act_threshold: 0.8,
                confidence_gate_tau: 0.7,
                backoff_k: 3,
                retry_strategies: vec!["cache".to_string(), "alt_api".to_string()],
            },
            l3_rules: L3Rules {
                evidence_coverage_min: 0.9,
                rollback_rate_max: 0.08,
                weekly_param_delta_max: 0.15,
                shadow_rollout_pct: 0.2,
            },
        }
    }

    pub fn enforce_ask_act_gate(&self, bits: &ExtendedBits) -> Result<(), String> {
        // STRUCTURAL INVARIANT: A>=1 && P>=1 && Δ==0
        if !(bits.a >= 1.0 && bits.p >= 1.0 && bits.d == 0.0) {
            return Err(format!(
                "Ask-Act gate failed: A={:.1}, P={:.1}, Δ={:.1}",
                bits.a, bits.p, bits.d
            ));
        }
        Ok(())
    }

    pub fn validate_bits_complete(&self, bits: &ExtendedBits) -> Result<(), String> {
        // STRUCTURAL INVARIANT: All 9 bits must be present
        let bit_names = ["A", "U", "P", "E", "Δ", "I", "R", "T", "M"];
        let bit_values = [
            bits.a, bits.u, bits.p, bits.e, bits.d, bits.i, bits.r, bits.t, bits.m,
        ];

        for (name, value) in bit_names.iter().zip(bit_values.iter()) {
            if value.is_nan() || *value < 0.0 || *value > 1.0 {
                return Err(format!("Invalid bit {}: {:.3}", name, value));
            }
        }
        Ok(())
    }

    pub fn ask_act_gate(&self, bits: &ExtendedBits) -> bool {
        bits.a >= 1.0 && bits.p >= 1.0 && bits.d == 0.0
    }

    pub fn evidence_gate(&self, bits: &ExtendedBits) -> bool {
        if bits.u >= self.l2_params.confidence_gate_tau {
            // Require verification mode first
            false
        } else {
            true
        }
    }

    pub fn should_wake_l3(&self, kpi_history: &[f32]) -> bool {
        // Degrade-twice rule: need at least three points to compare trend safely
        let len = kpi_history.len();
        if len < 3 {
            return false;
        }

        // Extra guard against underflow if the buffer is mutated concurrently
        let a = match kpi_history.get(len.saturating_sub(3)) {
            Some(v) => *v,
            None => return false,
        };
        let b = match kpi_history.get(len.saturating_sub(2)) {
            Some(v) => *v,
            None => return false,
        };
        let c = match kpi_history.get(len.saturating_sub(1)) {
            Some(v) => *v,
            None => return false,
        };

        b > c && c > a
    }

    pub fn get_self_snapshot(&self, trace_history: &[ExtendedBits]) -> String {
        let recent: Vec<_> = trace_history.iter().rev().take(5).collect();
        let effectiveness = if recent.is_empty() {
            0.5
        } else {
            recent.iter().filter(|b| b.a >= 1.0 && b.p >= 1.0).count() as f32 / recent.len() as f32
        };
        let trust_trend = if recent.len() >= 3 {
            recent[0].t - recent[2].t
        } else {
            0.0
        };

        format!(
            "effectiveness={:.2}, trust_trend={:.2}, recent_drift={}",
            effectiveness,
            trust_trend,
            recent.iter().any(|b| b.d > 0.0)
        )
    }

    pub fn propose_meta2_change(
        &mut self,
        kpi_name: &str,
        current_value: f32,
    ) -> Option<Meta2Proposal> {
        if current_value < self.l3_rules.evidence_coverage_min {
            Some(Meta2Proposal {
                symptom: format!("{} fell to {:.3}", kpi_name, current_value),
                hypothesis: "confidence gate too restrictive".to_string(),
                change: Meta2Change::ConfidenceGate {
                    old_tau: self.l2_params.confidence_gate_tau,
                    new_tau: (self.l2_params.confidence_gate_tau - 0.05).max(0.5),
                },
                shadow_pct: self.l3_rules.shadow_rollout_pct,
                rollback_condition: "evidence_coverage < 0.85 for 3d".to_string(),
            })
        } else {
            None
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Meta2Proposal {
    pub symptom: String,
    pub hypothesis: String,
    pub change: Meta2Change,
    pub shadow_pct: f32,
    pub rollback_condition: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Meta2Change {
    ConfidenceGate {
        old_tau: f32,
        new_tau: f32,
    },
    BackoffStrategy {
        old_k: u32,
        new_k: u32,
    },
    AskActThreshold {
        old_threshold: f32,
        new_threshold: f32,
    },
}
