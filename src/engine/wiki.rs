use anyhow::{anyhow, Context, Result};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::Command;
use walkdir::WalkDir;

fn should_skip_component(name: &str) -> bool {
    matches!(
        name,
        ".git"
            | "node_modules"
            | "target"
            | "dist"
            | "build"
            | ".next"
            | ".turbo"
            | ".cache"
            | "runs"
            | "archive"
            | "venv"
            | ".venv"
    )
}

fn rel_display(base: &Path, p: &Path) -> String {
    match p.strip_prefix(base) {
        Ok(rel) => {
            let s = rel.to_string_lossy().to_string();
            if s.is_empty() {
                ".".to_string()
            } else {
                format!("./{}", s)
            }
        }
        Err(_) => p.to_string_lossy().to_string(),
    }
}

fn inventory_files(base: &Path, max_depth: usize) -> Result<Vec<String>> {
    let mut out: Vec<String> = Vec::new();
    for e in WalkDir::new(base)
        .follow_links(false)
        .max_depth(max_depth)
        .into_iter()
        .filter_entry(|e| {
            if e.depth() == 0 {
                return true;
            }
            if e.file_type().is_dir() {
                if let Some(name) = e.file_name().to_str() {
                    return !should_skip_component(name);
                }
            }
            true
        })
    {
        let e = match e {
            Ok(v) => v,
            Err(_) => continue,
        };
        if !e.file_type().is_file() {
            continue;
        }
        out.push(rel_display(base, e.path()));
    }
    out.sort();
    Ok(out)
}

fn topfiles_rg(base: &Path, limit: usize) -> Result<Vec<String>> {
    let out = Command::new("rg")
        .arg("--files")
        .current_dir(base)
        .output();
    let out = match out {
        Ok(v) => v,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return Err(anyhow!("rg not found"));
        }
        Err(e) => return Err(e.into()),
    };
    if !out.status.success() {
        return Err(anyhow!("rg --files failed"));
    }
    let s = String::from_utf8_lossy(&out.stdout);
    Ok(s.lines().take(limit).map(|l| l.to_string()).collect())
}

fn folder_summary(base: &Path, max_files: usize) -> Result<String> {
    let mut counts: HashMap<String, u64> = HashMap::new();
    let mut seen = 0usize;

    for e in WalkDir::new(base)
        .follow_links(false)
        .into_iter()
        .filter_entry(|e| {
            if e.depth() == 0 {
                return true;
            }
            if e.file_type().is_dir() {
                if let Some(name) = e.file_name().to_str() {
                    return !should_skip_component(name);
                }
            }
            true
        })
    {
        let e = match e {
            Ok(v) => v,
            Err(_) => continue,
        };
        let p = e.path();
        if !e.file_type().is_file() {
            continue;
        }

        if let Ok(rel) = p.strip_prefix(base) {
            if let Some(top) = rel.components().next() {
                let top = top.as_os_str().to_string_lossy().to_string();
                *counts.entry(top).or_insert(0) += 1;
            } else {
                *counts.entry(".".to_string()).or_insert(0) += 1;
            }
        }

        seen += 1;
        if seen >= max_files {
            break;
        }
    }

    let mut items: Vec<(String, u64)> = counts.into_iter().collect();
    items.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));

    let mut md = String::new();
    md.push_str("# Folder Summary\n\n");
    for (k, v) in items {
        md.push_str(&format!("- {}: {} files\n", k, v));
    }
    Ok(md)
}

fn index_md(run_id: &str, generated: &str) -> String {
    format!(
        "# Local Wiki Snapshot\n\nRun ID: {run_id}  \nGenerated: {generated}\n\nArtifacts:\n- [files.txt](files.txt) — full inventory (depth ≤4)\n- [topfiles.txt](topfiles.txt) — top 200 files (rg --files or fallback)\n- [README.md](README.md) — workspace README (if present)\n- [folder_summary.md](folder_summary.md) — file counts by top folder\n\nHosting:\n- Open via engine: `http://127.0.0.1:8080/runs/wiki/{run_id}/index.html`\n- Or serve directly: `python3 -m http.server 9000 --directory runs/wiki/{run_id}`\n"
    )
}

fn index_html() -> String {
    r#"<!doctype html>
<html>
<head><meta charset="utf-8"><title>Local Wiki Snapshot</title></head>
<body>
<h1>Local Wiki Snapshot</h1>
<ul>
  <li><a href="files.txt">files.txt</a></li>
  <li><a href="topfiles.txt">topfiles.txt</a></li>
  <li><a href="README.md">README.md</a></li>
  <li><a href="folder_summary.md">folder_summary.md</a></li>
</ul>
<p>Open via engine: <code>/runs/wiki/&lt;run_id&gt;/index.html</code></p>
</body>
</html>
"#
    .to_string()
}

fn static_html(run_id: &str, generated: &str, folder_summary_md: &str, topfiles: &[String]) -> String {
    let topfiles_html = topfiles
        .iter()
        .take(200)
        .map(|l| format!("<li><code>{}</code></li>", html_escape(l)))
        .collect::<Vec<_>>()
        .join("\n");
    let folder_summary_html = html_escape(folder_summary_md);
    format!(
        r#"<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>Wiki Snapshot {run_id}</title>
  <style>
    body{{font-family:system-ui,-apple-system,Segoe UI,Roboto,Arial;margin:24px;max-width:1100px}}
    h1{{margin:0 0 6px 0}}
    .muted{{color:#57606a}}
    code, pre{{background:#f6f8fa;border:1px solid #d0d7de;border-radius:8px}}
    code{{padding:2px 6px}}
    pre{{padding:12px;overflow:auto}}
    .grid{{display:grid;grid-template-columns:1fr 1fr;gap:14px}}
    @media (max-width: 860px){{.grid{{grid-template-columns:1fr}}}}
    a{{color:#1f6feb;text-decoration:none}} a:hover{{text-decoration:underline}}
    input{{width:100%;padding:10px 12px;border:1px solid #d0d7de;border-radius:10px}}
    ul{{margin:8px 0 0 0}}
  </style>
</head>
<body>
  <h1>Wiki Snapshot</h1>
  <div class="muted">run_id: <code>{run_id}</code> · generated: <code>{generated}</code></div>
  <p>
    Links:
    <a href="index.html">index.html</a> ·
    <a href="index.md">index.md</a> ·
    <a href="files.txt">files.txt</a> ·
    <a href="topfiles.txt">topfiles.txt</a> ·
    <a href="folder_summary.md">folder_summary.md</a>
  </p>

  <div class="grid">
    <section>
      <h2>Folder Summary (embedded)</h2>
      <pre>{folder_summary_html}</pre>
    </section>
    <section>
      <h2>Top Files (embedded)</h2>
      <div class="muted">First 200 paths</div>
      <input id="q" placeholder="filter (client-side)..." />
      <ul id="list">{topfiles_html}</ul>
    </section>
  </div>

  <script>
    const q = document.getElementById('q');
    const list = document.getElementById('list');
    const items = Array.from(list.querySelectorAll('li'));
    q.addEventListener('input', () => {{
      const needle = q.value.toLowerCase();
      for (const li of items) {{
        const t = li.textContent.toLowerCase();
        li.style.display = (!needle || t.includes(needle)) ? '' : 'none';
      }}
    }});
  </script>
</body>
</html>
"#
    )
}

fn html_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#39;")
}

pub struct WikiResult {
    pub out_dir: PathBuf,
    pub files_count: usize,
    pub topfiles_count: usize,
    pub readme_copied: bool,
}

pub async fn generate(run_id: &str) -> Result<WikiResult> {
    let meta_root = PathBuf::from(std::env::var("META3_ROOT").unwrap_or_else(|_| ".".to_string()));
    let base = meta_root.clone();
    let out_dir = meta_root.join("runs/wiki").join(run_id);

    tokio::fs::create_dir_all(&out_dir)
        .await
        .with_context(|| format!("create out_dir {}", out_dir.display()))?;

    let generated = chrono::Utc::now().to_rfc3339();

    // Inventory and folder summary are blocking; keep them off the async runtime.
    let base_clone = base.clone();
    let files = tokio::task::spawn_blocking(move || inventory_files(&base_clone, 4))
        .await
        .context("join inventory task")??;

    tokio::fs::write(out_dir.join("files.txt"), files.join("\n") + "\n")
        .await
        .context("write files.txt")?;

    let topfiles: Vec<String> = match tokio::task::spawn_blocking({
        let base = base.clone();
        move || topfiles_rg(&base, 200)
    })
    .await
    .context("join topfiles task")?
    {
        Ok(v) => v,
        Err(_) => files.iter().take(200).cloned().collect(),
    };

    tokio::fs::write(out_dir.join("topfiles.txt"), topfiles.join("\n") + "\n")
        .await
        .context("write topfiles.txt")?;

    let mut readme_copied = false;
    let readme_src = base.join("README.md");
    if tokio::fs::metadata(&readme_src).await.is_ok() {
        let _ = tokio::fs::copy(&readme_src, out_dir.join("README.md")).await;
        readme_copied = true;
    }

    let summary_md = tokio::task::spawn_blocking({
        let base = base.clone();
        move || folder_summary(&base, 250_000)
    })
    .await
    .context("join folder_summary task")??;

    tokio::fs::write(out_dir.join("folder_summary.md"), summary_md)
        .await
        .context("write folder_summary.md")?;

    tokio::fs::write(out_dir.join("index.md"), index_md(run_id, &generated))
        .await
        .context("write index.md")?;
    tokio::fs::write(out_dir.join("index.html"), index_html())
        .await
        .context("write index.html")?;

    // Single-file “show it now” page (embeds summaries; still links to artifacts).
    let summary_embed = tokio::fs::read_to_string(out_dir.join("folder_summary.md"))
        .await
        .unwrap_or_default();
    tokio::fs::write(
        out_dir.join("static.html"),
        static_html(run_id, &generated, &summary_embed, &topfiles),
    )
    .await
    .context("write static.html")?;

    Ok(WikiResult {
        out_dir,
        files_count: files.len(),
        topfiles_count: topfiles.len(),
        readme_copied,
    })
}
