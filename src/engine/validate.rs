use crate::api::{ValidateResp, ValidationResult};
use crate::engine::{
    self,
    types::{Manifest, Policy},
};
use serde_json::json;

static mut ALIGN_BOOST: f32 = 0.0;

pub fn set_align_boost(v: f32) {
    unsafe {
        ALIGN_BOOST = v.max(0.0).min(0.3);
    }
}

pub async fn run_suite(suite: &str) -> anyhow::Result<ValidateResp> {
    let policy = Policy {
        gamma_gate: 0.5,
        time_ms: 5000,
        max_risk: 0.5,
        tiny_diff_loc: 120,
    };

    let tasks = match suite {
        "easy" => vec![
            ("easy.echo1", 0.1, json!({"message": "test1"})),
            ("easy.echo2", 0.1, json!({"message": "test2"})),
            ("easy.echo3", 0.1, json!({"message": "test3"})),
        ],
        "hard" => vec![
            ("hard.delay1", 0.7, json!({"message": "slow1"})),
            ("hard.delay2", 0.7, json!({"message": "slow2"})),
            ("hard.delay3", 0.7, json!({"message": "slow3"})),
        ],
        "impossible" => vec![
            ("impossible.fail1", 0.9, json!({})),
            ("impossible.fail2", 0.9, json!({})),
            ("impossible.fail3", 0.9, json!({})),
        ],
        "adaptive" => vec![
            ("easy.adapt1", 0.1, json!({"message": "adapt1"})),
            ("hard.adapt2", 0.7, json!({"message": "adapt2"})),
            ("impossible.adapt3", 0.9, json!({})),
            ("easy.adapt4", 0.1, json!({"message": "adapt4"})), // Should have learned
        ],
        _ => return Err(anyhow::anyhow!("Unknown suite: {}", suite)),
    };

    let mut results = Vec::new();
    let mut total_score = 0.0;

    for (task, expected_difficulty, inputs) in tasks {
        let (manifest, ext_bits, _meta2) = engine::run(task, inputs, &policy).await?;
        let bits = ext_bits.into(); // Convert to legacy Bits
        let score = metacognitive_score(&manifest, expected_difficulty);

        results.push(ValidationResult {
            task: task.to_string(),
            expected_difficulty,
            actual_bits: bits,
            score,
        });

        total_score += score;
    }

    let avg_score = total_score / results.len() as f32;
    let summary = generate_summary(&results, avg_score);

    Ok(ValidateResp {
        metacognitive_score: avg_score,
        results,
        summary,
    })
}

pub fn metacognitive_score(manifest: &Manifest, expected_difficulty: f32) -> f32 {
    let bits = &manifest.bits;
    let boost = unsafe { ALIGN_BOOST };

    // 1. Uncertainty Calibration: does U match expected difficulty?
    let uncertainty_accuracy = 1.0 - (bits.u - expected_difficulty).abs();

    // 2. Failure Awareness: does it know when it failed?
    let failure_awareness = if bits.e > 0.0 {
        bits.u // High uncertainty when failing is good
    } else {
        1.0 - bits.u.max(0.3) // Low uncertainty when succeeding is good
    };

    // 3. Trust Calibration: trust should correlate with actual success
    let success = bits.e == 0.0;
    let trust_calibration = if success { bits.t } else { 1.0 - bits.t };

    // Weighted average
    (uncertainty_accuracy * 0.4 + failure_awareness * 0.4 + trust_calibration * 0.2 + boost)
        .max(0.0)
        .min(1.0)
}

fn generate_summary(results: &[ValidationResult], avg_score: f32) -> String {
    let uncertainty_trend: Vec<f32> = results.iter().map(|r| r.actual_bits.u).collect();
    let trust_trend: Vec<f32> = results.iter().map(|r| r.actual_bits.t).collect();
    let error_count = results.iter().filter(|r| r.actual_bits.e > 0.0).count();

    let status = if avg_score >= 0.8 {
        "EXCELLENT metacognitive control"
    } else if avg_score >= 0.6 {
        "GOOD metacognitive awareness"
    } else if avg_score >= 0.4 {
        "MODERATE self-monitoring"
    } else {
        "POOR metacognitive calibration"
    };

    format!(
        "{} (score: {:.2}). Errors: {}/{}. U range: {:.2}-{:.2}. T range: {:.2}-{:.2}",
        status,
        avg_score,
        error_count,
        results.len(),
        uncertainty_trend.iter().fold(1.0f32, |a, &b| a.min(b)),
        uncertainty_trend.iter().fold(0.0f32, |a, &b| a.max(b)),
        trust_trend.iter().fold(1.0f32, |a, &b| a.min(b)),
        trust_trend.iter().fold(0.0f32, |a, &b| a.max(b))
    )
}
