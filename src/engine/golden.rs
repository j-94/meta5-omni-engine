use crate::engine::bits::Bits as RuntimeBits;
use anyhow::Result;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use utoipa::ToSchema;

#[derive(Debug, Deserialize, Serialize, Clone, JsonSchema, ToSchema)]
pub struct GoldenCaseRaw {
    pub test: String,
    pub assertion: Value, // allow string or object
    pub result: Value,
    pub bits: Value,
}

#[derive(Debug, Serialize, Deserialize, Clone, JsonSchema, ToSchema)]
pub struct GoldenCase {
    pub test: String,
    pub ok: bool,
    pub reason: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone, JsonSchema, ToSchema)]
pub struct GoldenSummary {
    pub name: String,
    pub total: usize,
    pub passed: usize,
    pub failed: usize,
    pub details: Vec<GoldenCase>,
    pub bits: RuntimeBits,
}

fn parse_bits(v: &Value) -> Option<RuntimeBits> {
    // Accept either Δ or d
    let a = v
        .get("A")?
        .as_f64()
        .or_else(|| v.get("A")?.as_i64().map(|x| x as f64))? as f32;
    let u = v
        .get("U")?
        .as_f64()
        .or_else(|| v.get("U")?.as_i64().map(|x| x as f64))? as f32;
    let p = v
        .get("P")?
        .as_f64()
        .or_else(|| v.get("P")?.as_i64().map(|x| x as f64))? as f32;
    let e = v
        .get("E")?
        .as_f64()
        .or_else(|| v.get("E")?.as_i64().map(|x| x as f64))? as f32;
    let d = v
        .get("Δ")
        .and_then(|vv| vv.as_f64())
        .or_else(|| v.get("d").and_then(|vv| vv.as_f64()))
        .or_else(|| v.get("Δ").and_then(|vv| vv.as_i64().map(|x| x as f64)))
        .or_else(|| v.get("d").and_then(|vv| vv.as_i64().map(|x| x as f64)))? as f32;
    let i = v
        .get("I")?
        .as_f64()
        .or_else(|| v.get("I")?.as_i64().map(|x| x as f64))? as f32;
    let r = v
        .get("R")?
        .as_f64()
        .or_else(|| v.get("R")?.as_i64().map(|x| x as f64))? as f32;
    let t = v
        .get("T")?
        .as_f64()
        .or_else(|| v.get("T")?.as_i64().map(|x| x as f64))? as f32;
    // M is optional for legacy traces; default 0
    let m = v.get("M").and_then(|vv| vv.as_f64()).unwrap_or(0.0) as f32;
    Some(RuntimeBits {
        a,
        u,
        p,
        e,
        d,
        i,
        r,
        t,
        m,
    })
}

fn bits_valid(b: &RuntimeBits) -> bool {
    let vals = [b.a, b.u, b.p, b.e, b.d, b.i, b.r, b.t, b.m];
    vals.iter().all(|v| *v >= 0.0 && *v <= 1.0 && !v.is_nan())
}

pub async fn validate_golden(name: &str) -> Result<GoldenSummary> {
    let path = format!("trace/golden/{}.json", name);
    let s = tokio::fs::read_to_string(&path).await?;
    let raw: Vec<GoldenCaseRaw> = serde_json::from_str(&s)?;

    let mut details = Vec::new();
    let mut passed = 0usize;
    for case in raw.into_iter() {
        let ok_bits = match parse_bits(&case.bits) {
            Some(b) => bits_valid(&b),
            None => false,
        };
        let (ok, reason) = if ok_bits {
            (true, None)
        } else {
            (false, Some("invalid or out-of-range bits".to_string()))
        };
        if ok {
            passed += 1;
        }
        details.push(GoldenCase {
            test: case.test,
            ok,
            reason,
        });
    }
    let total = details.len();
    let failed = total - passed;
    let bits = if failed == 0 {
        RuntimeBits {
            a: 1.0,
            u: 0.0,
            p: 1.0,
            e: 0.0,
            d: 0.0,
            i: 0.0,
            r: 0.0,
            t: 1.0,
            m: 0.0,
        }
    } else {
        RuntimeBits {
            a: 0.0,
            u: 1.0,
            p: 1.0,
            e: 1.0,
            d: 0.0,
            i: 0.0,
            r: 0.0,
            t: 0.3,
            m: 0.0,
        }
    };
    Ok(GoldenSummary {
        name: name.to_string(),
        total,
        passed,
        failed,
        details,
        bits,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn golden_wolfram_unity_structure_is_valid() {
        let summary = validate_golden("wolfram_unity").await.expect("load golden");
        assert_eq!(summary.failed, 0, "golden cases should have valid bits");
        assert!(summary.total >= 1);
    }
}
