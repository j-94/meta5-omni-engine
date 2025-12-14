use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, ToSchema)]
pub struct Bits {
    pub a: f32,
    pub u: f32,
    pub p: f32,
    pub e: f32,
    #[serde(rename = "d")]
    pub d: f32,
    pub i: f32,
    pub r: f32,
    pub t: f32,
    pub m: f32,
}

impl Bits {
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
