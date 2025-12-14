use super::TelemetryEvent;
use crate::engine::types::{Bits, Manifest};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use serde_json::json;

#[derive(Debug, Serialize, Deserialize)]
pub struct PullRequest {
    pub id: String,
    pub title: String,
    pub branch: String,
    pub files_changed: Vec<String>,
    pub run_id: String,
    pub confidence: f32,
}

pub async fn create_pr_if_confident(
    manifest: &Manifest,
    bits: &Bits,
) -> anyhow::Result<Option<PullRequest>> {
    // Gate: Only create PR if confidence is high
    if bits.t < 0.8 || bits.e > 0.0 {
        emit_telemetry(
            "monorepo",
            "pr_rejected",
            Some(manifest.run_id.clone()),
            Some(bits.clone()),
            json!({
                "reason": "low_confidence",
                "trust": bits.t,
                "errors": bits.e
            }),
        )
        .await;
        return Ok(None);
    }

    let pr = PullRequest {
        id: format!("pr-{}", uuid::Uuid::new_v4()),
        title: format!("Agent: {}", manifest.goal_id),
        branch: format!("agent/{}", manifest.run_id),
        files_changed: manifest.deliverables.clone(),
        run_id: manifest.run_id.clone(),
        confidence: bits.t,
    };

    emit_telemetry(
        "monorepo",
        "pr_created",
        Some(manifest.run_id.clone()),
        Some(bits.clone()),
        json!({
            "pr_id": pr.id,
            "files_changed": pr.files_changed.len(),
            "confidence": pr.confidence
        }),
    )
    .await;

    tracing::info!("Created PR {} with confidence {:.2}", pr.id, pr.confidence);
    Ok(Some(pr))
}

pub async fn ci_gate_check(pr: &PullRequest) -> anyhow::Result<bool> {
    // Simulate CI checks
    let passed = pr.confidence >= 0.8;

    emit_telemetry(
        "monorepo",
        "ci_check",
        Some(pr.run_id.clone()),
        None,
        json!({
            "pr_id": pr.id,
            "passed": passed,
            "confidence": pr.confidence
        }),
    )
    .await;

    Ok(passed)
}

async fn emit_telemetry(
    component: &str,
    event_type: &str,
    run_id: Option<String>,
    bits: Option<Bits>,
    metadata: serde_json::Value,
) {
    let event = TelemetryEvent {
        ts: Utc::now().to_rfc3339(),
        component: component.to_string(),
        event_type: event_type.to_string(),
        run_id,
        bits,
        cost: None,
        kpi_impact: None,
        metadata,
    };

    tracing::debug!("Telemetry: {:?}", event);
}
