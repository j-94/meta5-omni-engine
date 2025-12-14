use super::bits::Bits;

pub fn trust_from(passed: bool, b: &Bits) -> f32 {
    if passed && b.e == 0.0 {
        0.9
    } else if passed {
        0.6
    } else {
        0.3
    }
}
