use super::types::Policy;
use anyhow::{anyhow, Context};
use std::process::Stdio;
use tokio::io::AsyncReadExt;
use tokio::process::Command;
use tokio::time::{timeout, Duration};

#[derive(Debug)]
pub enum Action {
    Cli(String),
}

pub struct ExecResult {
    pub ok: bool,
    pub drift: bool,
    pub stdout: String,
    pub stderr: String,
}

pub async fn execute(action: Action, policy: &Policy) -> anyhow::Result<ExecResult> {
    match action {
        Action::Cli(cmd) => {
            // Capability gate (simple heuristic). If STRICT_CAPS=1, block risky ops.
            if let Some(cap) = detect_capability(&cmd) {
                if std::env::var("STRICT_CAPS").ok().as_deref() == Some("1") {
                    return Err(anyhow!("capability gate blocked: {}", cap));
                }
            }
            let mut child = Command::new("bash")
                .arg("-lc")
                .arg(&cmd)
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
                .with_context(|| format!("failed to spawn: {}", cmd))?;

            let time_limit = Duration::from_millis(policy.time_ms as u64);

            let mut stdout_pipe = child
                .stdout
                .take()
                .ok_or_else(|| anyhow!("missing stdout pipe"))?;
            let mut stderr_pipe = child
                .stderr
                .take()
                .ok_or_else(|| anyhow!("missing stderr pipe"))?;

            let stdout_task = tokio::spawn(async move {
                let mut buf = Vec::new();
                let _ = stdout_pipe.read_to_end(&mut buf).await;
                buf
            });
            let stderr_task = tokio::spawn(async move {
                let mut buf = Vec::new();
                let _ = stderr_pipe.read_to_end(&mut buf).await;
                buf
            });

            let mut timed_out = false;
            let status_success = match timeout(time_limit, child.wait()).await {
                Ok(res) => res
                    .with_context(|| format!("failed to wait: {}", cmd))?
                    .success(),
                Err(_) => {
                    timed_out = true;
                    let _ = child.kill().await;
                    let _ = child.wait().await;
                    false
                }
            };

            let stdout_bytes = stdout_task.await.unwrap_or_default();
            let stderr_bytes = stderr_task.await.unwrap_or_default();
            let stdout = String::from_utf8_lossy(&stdout_bytes).to_string();
            let mut stderr = String::from_utf8_lossy(&stderr_bytes).to_string();
            if timed_out {
                if !stderr.is_empty() {
                    stderr.push('\n');
                }
                stderr.push_str(&format!("timeout after {}ms", policy.time_ms));
            }
            Ok(ExecResult {
                ok: status_success && !timed_out,
                drift: false,
                stdout,
                stderr,
            })
        }
    }
}

fn detect_capability(cmd: &str) -> Option<&'static str> {
    let s = cmd.to_lowercase();
    if s.contains("curl ") || s.contains("wget ") {
        return Some("network");
    }
    if s.contains(" rm ") || s.contains("rm -rf") || s.contains(" mv ") {
        return Some("file_write");
    }
    if s.contains("git push") || s.contains("gh release") {
        return Some("identity");
    }
    None
}
