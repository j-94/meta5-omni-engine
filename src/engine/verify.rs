use super::executor::ExecResult;

pub fn check_minimal(res: &ExecResult) -> bool {
    res.ok && !res.stdout.trim().is_empty()
}
