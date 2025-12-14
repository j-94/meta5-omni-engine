use super::TelemetryEvent;
use chrono::{DateTime, Utc};
use std::collections::HashMap;

pub struct TelemetryStore {
    events: Vec<TelemetryEvent>,
}

impl TelemetryStore {
    pub fn new() -> Self {
        Self { events: Vec::new() }
    }

    pub async fn append(&mut self, event: TelemetryEvent) {
        self.events.push(event);

        // In production: write to persistent store (JSONL, DB, etc.)
        tracing::debug!("Telemetry event stored");
    }

    pub async fn nightly_scorecard(&self) -> HashMap<String, f32> {
        let mut scores = HashMap::new();

        // Calculate component health scores
        let components = ["agent", "flywheel", "monorepo", "kpi"];
        for component in components {
            let component_events: Vec<_> = self
                .events
                .iter()
                .filter(|e| e.component == component)
                .collect();

            let success_rate = if !component_events.is_empty() {
                let successes = component_events
                    .iter()
                    .filter(|e| e.bits.as_ref().map_or(true, |b| b.e == 0.0))
                    .count();
                successes as f32 / component_events.len() as f32
            } else {
                1.0
            };

            scores.insert(component.to_string(), success_rate);
        }

        scores
    }

    pub async fn prune_or_invest_decisions(&self) -> Vec<String> {
        let scorecard = self.nightly_scorecard().await;
        let mut decisions = Vec::new();

        for (component, score) in scorecard {
            if score < 0.5 {
                decisions.push(format!("PRUNE: {} (score: {:.2})", component, score));
            } else if score > 0.9 {
                decisions.push(format!("INVEST: {} (score: {:.2})", component, score));
            }
        }

        decisions
    }
}
