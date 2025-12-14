use serde::{Deserialize, Serialize};
use std::{fs, io::Read, path::Path, time::SystemTime};
use walkdir::WalkDir;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ResearchArtifact {
    pub id: String,
    pub kind: String,
    pub path: String,
    pub ts: String,
    pub ttl: u64,
    pub tags: Vec<String>,
    pub checksum: String,
    pub git_commit: Option<String>,
    pub git_branch: Option<String>,
}

fn kind_for(path: &Path) -> String {
    let p = path.to_string_lossy().to_lowercase();
    if p.contains("/prompts/") {
        return "prompt".into();
    }
    if p.contains("/policies/") {
        return "policy".into();
    }
    if p.contains("/schemas/") {
        return "schema".into();
    }
    if p.contains("/trace/golden/") {
        return "trace".into();
    }
    if p.contains("/docs/") || p.ends_with("readme.md") {
        return "doc".into();
    }
    if p.ends_with(".json") || p.ends_with(".yaml") || p.ends_with(".yml") {
        return "dataset".into();
    }
    "other".into()
}

fn adler32(bytes: &[u8]) -> u32 {
    let mut a: u32 = 1;
    let mut b: u32 = 0;
    const MOD: u32 = 65521;
    for chunk in bytes.chunks(5552) {
        for &x in chunk {
            a = (a + x as u32) % MOD;
            b = (b + a) % MOD;
        }
    }
    (b << 16) | a
}

fn ts_from(path: &Path) -> String {
    match fs::metadata(path).and_then(|m| m.modified()) {
        Ok(st) => match st.duration_since(SystemTime::UNIX_EPOCH) {
            Ok(d) => chrono::DateTime::<chrono::Utc>::from(std::time::UNIX_EPOCH + d).to_rfc3339(),
            Err(_) => chrono::Utc::now().to_rfc3339(),
        },
        Err(_) => chrono::Utc::now().to_rfc3339(),
    }
}

pub fn build_index(root: &Path) -> anyhow::Result<Vec<ResearchArtifact>> {
    let mut out = Vec::new();
    let branch = git_branch().ok();
    for entry in WalkDir::new(root).into_iter().filter_map(|e| e.ok()) {
        if !entry.file_type().is_file() {
            continue;
        }
        let path = entry.path();
        let ext = path.extension().and_then(|s| s.to_str()).unwrap_or("");
        if !matches!(ext, "md" | "json" | "jsonl" | "yaml" | "yml") {
            continue;
        }
        // read file
        let mut f = fs::File::open(path)?;
        let mut buf = Vec::new();
        f.read_to_end(&mut buf)?;
        let checksum = format!("{:08x}", adler32(&buf));
        let ts = ts_from(path);
        let ttl = if path.to_string_lossy().contains("trace/golden/") {
            0
        } else {
            14 * 24 * 3600
        };
        let rel = path
            .strip_prefix(root)
            .unwrap_or(path)
            .to_string_lossy()
            .to_string();
        let kind = kind_for(path);
        // tags from simple front-matter if present
        let mut tags = front_matter_tags(&buf);
        if tags.is_empty() && kind == "policy" {
            tags.push("policy".into());
        }
        let id = format!("{}#{}", rel, checksum);
        let git_commit = git_last_commit(path).ok();
        out.push(ResearchArtifact {
            id,
            kind,
            path: rel,
            ts,
            ttl,
            tags,
            checksum,
            git_commit,
            git_branch: branch.clone(),
        });
    }
    Ok(out)
}

pub fn build_index_multi(roots: &[std::path::PathBuf]) -> anyhow::Result<Vec<ResearchArtifact>> {
    use std::collections::HashSet;
    let mut all = Vec::new();
    let mut seen: HashSet<String> = HashSet::new(); // dedup by checksum
    for r in roots {
        let items = build_index(r)?;
        for a in items.into_iter() {
            if seen.insert(a.checksum.clone()) {
                all.push(a);
            }
        }
    }
    Ok(all)
}

fn front_matter_tags(buf: &[u8]) -> Vec<String> {
    // Minimal YAML front-matter parser: --- ... --- at top
    let s = String::from_utf8_lossy(buf);
    let mut lines = s.lines();
    if !matches!(lines.next(), Some(l) if l.trim()=="---") {
        return Vec::new();
    }
    let mut tags: Vec<String> = Vec::new();
    let mut in_tags = false;
    for line in lines {
        let t = line.trim();
        if t == "---" {
            break;
        }
        if t.starts_with("tags:") {
            in_tags = true;
            continue;
        }
        if in_tags {
            if t.starts_with('-') {
                let val = t.trim_start_matches('-').trim();
                if !val.is_empty() {
                    tags.push(val.to_string());
                }
            } else if t.is_empty() {
                break;
            } else {
                in_tags = false;
            }
        }
    }
    tags
}

fn git_branch() -> anyhow::Result<String> {
    let out = std::process::Command::new("git")
        .arg("rev-parse")
        .arg("--abbrev-ref")
        .arg("HEAD")
        .output()?;
    if out.status.success() {
        Ok(String::from_utf8_lossy(&out.stdout).trim().to_string())
    } else {
        anyhow::bail!("git branch failed")
    }
}

fn git_last_commit(path: &Path) -> anyhow::Result<String> {
    let out = std::process::Command::new("git")
        .args([
            "log",
            "-n",
            "1",
            "--pretty=%h",
            "--",
            path.to_string_lossy().as_ref(),
        ])
        .output()?;
    if out.status.success() {
        Ok(String::from_utf8_lossy(&out.stdout).trim().to_string())
    } else {
        anyhow::bail!("git log failed")
    }
}
