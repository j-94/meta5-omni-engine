use anyhow::Result;
use serde_json::{json, Value};
use std::fs;

use crate::engine::router;

pub async fn handle(message: &str, bits: &crate::engine::bits::Bits) -> Result<Value> {
    let persona = fs::read_to_string("prompts/meta_omni.md").unwrap_or_else(|_| {
        "You are One Engine v0.2, a metacognitive AI system. Act with clarity.".to_string()
    });

    match router::chat(&persona, message).await {
        Ok(mut response) => {
            response
                .as_object_mut()
                .map(|obj| obj.insert("bits".to_string(), serde_json::json!(bits)))
                .unwrap_or_default();
            Ok(response)
        }
        Err(err) => {
            eprintln!("router error: {}", err);
            Ok(json!({
                "intent": {
                    "goal": "meta.prompt",
                    "constraints": ["Stay within self-model"],
                    "evidence": [persona]
                },
                "bits": bits,
                "explanation": {"reason": "router unavailable, fallback only"},
                "manifest": {"evidence": {"reply": "router unavailable"}}
            }))
        }
    }
}

// Legacy function for compatibility
pub fn process_meta_prompt(
    _system: &str,
    message: &str,
    _history: &[Value],
    _self_obs: Option<&str>,
) -> String {
    // Simple fallback response
    match message.to_lowercase().as_str() {
        msg if msg.contains("who am i") => "I am One Engine v0.2, a metacognitive AI system with self-awareness capabilities.".to_string(),
        msg if msg.contains("hello") => "Hello! I'm One Engine, ready to assist with metacognitive validation and adaptive control.".to_string(),
        msg if msg.contains("help") => "I can process tasks with uncertainty tracking, trust calibration, and failure awareness. Try asking about my capabilities!".to_string(),
        _ => "I'm processing your request with metacognitive awareness. How can I help you today?".to_string()
    }
}
