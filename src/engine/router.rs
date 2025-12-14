use anyhow::{anyhow, Result};
use reqwest::{Client, StatusCode};
use serde_json::{json, Value};
use std::time::Duration;

// Defaults are set for OpenRouter; override via ROUTER_URL / OPENROUTER_URL and ROUTER_MODEL / OPENROUTER_MODEL.
const DEFAULT_URL: &str = "https://openrouter.ai/api/v1/chat/completions";
const DEFAULT_MODEL: &str = "moonshotai/kimi-k2";
const ALTERNATE_PREFIX: &str = "OPENROUTER";

fn first_env(keys: &[&str]) -> Option<String> {
    for key in keys {
        if let Ok(val) = std::env::var(key) {
            if !val.trim().is_empty() {
                return Some(val);
            }
        }
    }
    None
}

fn api_url() -> String {
    first_env(&["ROUTER_URL", "OPENROUTER_URL"]).unwrap_or_else(|| DEFAULT_URL.to_string())
}

fn model_name() -> String {
    first_env(&["ROUTER_MODEL", "OPENROUTER_MODEL"]).unwrap_or_else(|| DEFAULT_MODEL.to_string())
}

fn api_key() -> Result<String> {
    first_env(&["ROUTER_API_KEY", "OPENROUTER_API_KEY"])
        .ok_or_else(|| anyhow!("router API key not set (ROUTER_API_KEY or OPENROUTER_API_KEY)"))
}

fn timeout_secs() -> u64 {
    first_env(&["ROUTER_TIMEOUT_SECS", "OPENROUTER_TIMEOUT_SECS"])
        .and_then(|s| s.parse::<u64>().ok())
        .filter(|&n| n >= 5 && n <= 600)
        .unwrap_or(60)
}

pub async fn chat(system: &str, user: &str) -> Result<Value> {
    let url = api_url();
    let model = model_name();
    let key = api_key()?;
    let client = Client::builder()
        .timeout(Duration::from_secs(timeout_secs()))
        .build()?;

    let payload = json!({
      "model": model,
      "messages": [
        {"role": "system", "content": system},
        {"role": "user", "content": user}
      ],
      "response_format": {"type": "json_object"}
    });
    let resp = client
        .post(&url)
        .bearer_auth(key)
        .json(&payload)
        .send()
        .await?;
    let status = resp.status();
    let body = resp.json::<Value>().await?;
    if status != StatusCode::OK {
        return Err(anyhow!("router error {}: {}", status, body));
    }
    let content = body
        .pointer("/choices/0/message/content")
        .and_then(|v| v.as_str())
        .unwrap_or("{}");
    let parsed =
        serde_json::from_str::<Value>(content).unwrap_or_else(|_| json!({"reply": content}));
    Ok(parsed)
}

pub async fn chat_messages(mut messages: Vec<Value>) -> Result<Value> {
    let url = api_url();
    let model = model_name();
    let key = api_key()?;

    if messages.is_empty() {
        messages.push(json!({"role": "user", "content": ""}));
    }

    let client = Client::builder()
        .timeout(Duration::from_secs(timeout_secs()))
        .build()?;

    let payload = json!({
        "model": model,
        "messages": messages,
        "response_format": {"type": "json_object"}
    });

    let resp = client
        .post(&url)
        .bearer_auth(key)
        .json(&payload)
        .send()
        .await?;

    let status = resp.status();
    let body = resp.json::<Value>().await?;
    if status != StatusCode::OK {
        return Err(anyhow!("router error {}: {}", status, body));
    }

    let content = body
        .pointer("/choices/0/message/content")
        .and_then(|v| v.as_str())
        .unwrap_or("{}");
    let parsed =
        serde_json::from_str::<Value>(content).unwrap_or_else(|_| json!({"reply": content}));
    Ok(parsed)
}
