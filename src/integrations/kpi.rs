use super::{AgentGoal, KPIDashboard, TelemetryEvent};
use chrono::Utc;
use serde_json::json;

pub async fn current_scores() -> KPIDashboard {
    // Simulate current KPI state
    KPIDashboard {
        signal_density: 0.75,
        flow_minutes: 0.60, // Lowest - needs focus
        knowledge_yield: 0.85,
        noise_ratio: 0.70,
        weekly_trend: vec![0.65, 0.68, 0.72, 0.75, 0.73],
    }
}

pub async fn weekly_planning() -> anyhow::Result<Vec<AgentGoal>> {
    let kpis = current_scores().await;
    let mut goals = Vec::new();

    // Focus on lowest performing KPIs
    if kpis.flow_minutes < 0.7 {
        goals.push(AgentGoal {
            id: "improve-flow-minutes".to_string(),
            kpi_target: "flow_minutes".to_string(),
            priority: 1.0 - kpis.flow_minutes, // Higher priority for lower scores
            estimated_impact: 0.15,
        });
    }

    if kpis.noise_ratio < 0.8 {
        goals.push(AgentGoal {
            id: "reduce-noise-ratio".to_string(),
            kpi_target: "noise_ratio".to_string(),
            priority: 1.0 - kpis.noise_ratio,
            estimated_impact: 0.10,
        });
    }

    emit_telemetry(
        "kpi",
        "weekly_planning",
        None,
        json!({
            "goals_generated": goals.len(),
            "focus_areas": goals.iter().map(|g| &g.kpi_target).collect::<Vec<_>>()
        }),
    )
    .await;

    Ok(goals)
}

pub async fn track_kpi_impact(goal: &AgentGoal, actual_impact: f32) -> anyhow::Result<()> {
    let accuracy = 1.0 - (goal.estimated_impact - actual_impact).abs();

    emit_telemetry(
        "kpi",
        "impact_tracking",
        None,
        json!({
            "goal_id": goal.id,
            "kpi_target": goal.kpi_target,
            "estimated_impact": goal.estimated_impact,
            "actual_impact": actual_impact,
            "prediction_accuracy": accuracy
        }),
    )
    .await;

    tracing::info!(
        "KPI impact for {}: estimated {:.2}, actual {:.2}",
        goal.id,
        goal.estimated_impact,
        actual_impact
    );
    Ok(())
}

async fn emit_telemetry(
    component: &str,
    event_type: &str,
    run_id: Option<String>,
    metadata: serde_json::Value,
) {
    let event = TelemetryEvent {
        ts: Utc::now().to_rfc3339(),
        component: component.to_string(),
        event_type: event_type.to_string(),
        run_id,
        bits: None,
        cost: None,
        kpi_impact: None,
        metadata,
    };

    tracing::debug!("Telemetry: {:?}", event);
}
