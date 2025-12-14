use anyhow::{anyhow, Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

#[derive(Debug, Clone)]
pub struct ThreadReportResult {
    pub out_dir: PathBuf,
    pub nodes: usize,
    pub thread: String,
}

#[derive(Debug, Clone)]
pub struct ThreadReportOpts {
    pub user_id: String,
    pub thread: String, // explicit thread id, or "auto"
    pub max_events: usize,
    pub content_chars: usize,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct BitsLite {
    pub a: Option<f32>,
    pub u: Option<f32>,
    pub p: Option<f32>,
    pub e: Option<f32>,
    pub d: Option<f32>,
    pub i: Option<f32>,
    pub r: Option<f32>,
    pub t: Option<f32>,
    pub m: Option<f32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ThreadEvent {
    ts: String,
    role: String,
    run_id: String,
    content: String,
}

fn is_safe_segment(seg: &str) -> bool {
    !seg.is_empty()
        && !seg.contains('/')
        && !seg.contains('\\')
        && !seg.contains("..")
        && seg
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.')
}

fn meta3_root() -> PathBuf {
    std::env::var("META3_ROOT")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("."))
}

fn html_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#39;")
}

fn one_line(s: &str) -> String {
    s.replace('\r', " ")
        .replace('\n', " ")
        .replace('\t', " ")
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

fn truncate_chars(s: &str, max_chars: usize) -> String {
    if max_chars == 0 {
        return "".to_string();
    }
    let mut out = String::new();
    for (i, ch) in s.chars().enumerate() {
        if i >= max_chars {
            out.push_str("…");
            break;
        }
        out.push(ch);
    }
    out
}

fn tail_lines(path: &Path, limit: usize, max_bytes: u64) -> Result<Vec<String>> {
    let meta = fs::metadata(path).with_context(|| format!("metadata {}", path.display()))?;
    let len = meta.len();
    let start = if len > max_bytes { len - max_bytes } else { 0 };

    let mut f = fs::File::open(path).with_context(|| format!("open {}", path.display()))?;
    if start > 0 {
        f.seek(SeekFrom::Start(start))
            .with_context(|| format!("seek {}", path.display()))?;
    }
    let mut buf = Vec::new();
    f.read_to_end(&mut buf)
        .with_context(|| format!("read {}", path.display()))?;

    let mut s = String::from_utf8_lossy(&buf).to_string();
    if start > 0 {
        if let Some(idx) = s.find('\n') {
            s = s[idx + 1..].to_string();
        }
    }

    let mut lines: Vec<&str> = s.lines().filter(|l| !l.trim().is_empty()).collect();
    if lines.len() > limit {
        lines = lines[lines.len() - limit..].to_vec();
    }
    Ok(lines.into_iter().map(|l| l.to_string()).collect())
}

fn receipt_response_json(run_id: &str) -> Option<Value> {
    if !is_safe_segment(run_id) {
        return None;
    }
    let root = meta3_root();
    let p = root
        .join("runs")
        .join("receipts")
        .join(run_id)
        .join("response.json");
    let txt = fs::read_to_string(p).ok()?;
    serde_json::from_str::<Value>(&txt).ok()
}

fn get_goal_id(resp: &Value) -> Option<String> {
    resp.get("manifest")
        .and_then(|m| m.get("goal_id"))
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
}

fn get_view_url(resp: &Value) -> Option<String> {
    let ev = resp.get("manifest")?.get("evidence")?;
    ev.get("static_html_url")
        .and_then(|v| v.as_str())
        .or_else(|| ev.get("index_html_url").and_then(|v| v.as_str()))
        .map(|s| s.to_string())
}

fn get_actual_success(resp: &Value) -> Option<bool> {
    resp.get("manifest")?
        .get("evidence")?
        .get("actual_success")
        .and_then(|v| v.as_bool())
}

fn get_bits(resp: &Value) -> BitsLite {
    let bits_v = resp.get("bits").or_else(|| resp.get("manifest").and_then(|m| m.get("bits")));
    let map = match bits_v.and_then(|v| v.as_object()) {
        Some(m) => m,
        None => return BitsLite::default(),
    };
    let get = |k: &str| map.get(k).and_then(|v| v.as_f64()).map(|x| x as f32);
    BitsLite {
        a: get("a").or_else(|| get("A")),
        u: get("u").or_else(|| get("U")),
        p: get("p").or_else(|| get("P")),
        e: get("e").or_else(|| get("E")),
        d: get("d").or_else(|| get("D")),
        i: get("i").or_else(|| get("I")),
        r: get("r").or_else(|| get("R")),
        t: get("t").or_else(|| get("T")),
        m: get("m").or_else(|| get("M")),
    }
}

fn select_thread(threads_dir: &Path, thread: &str) -> Result<String> {
    match thread.trim() {
        "" | "auto" => {
            let mut best: Option<(u64, String)> = None;
            let rd = fs::read_dir(threads_dir)
                .with_context(|| format!("read_dir {}", threads_dir.display()))?;
            for entry in rd.flatten() {
                let p = entry.path();
                if p.extension().and_then(|x| x.to_str()) != Some("jsonl") {
                    continue;
                }
                let name = p
                    .file_stem()
                    .and_then(|x| x.to_str())
                    .unwrap_or("")
                    .to_string();
                if !is_safe_segment(&name) {
                    continue;
                }
                let sz = entry.metadata().map(|m| m.len()).unwrap_or(0);
                match best {
                    None => best = Some((sz, name)),
                    Some((bsz, _)) if sz > bsz => best = Some((sz, name)),
                    _ => {}
                }
            }
            best.map(|(_, t)| t)
                .ok_or_else(|| anyhow!("no threads found in {}", threads_dir.display()))
        }
        t => {
            if !is_safe_segment(t) {
                return Err(anyhow!("invalid thread"));
            }
            Ok(t.to_string())
        }
    }
}

fn stopwords() -> &'static [&'static str] {
    &[
        "the", "a", "an", "and", "or", "but", "to", "of", "in", "on", "for", "with", "as", "is",
        "are", "was", "were", "be", "been", "it", "that", "this", "we", "you", "i", "our", "your",
        "me", "my", "they", "them", "their", "at", "by", "from", "not", "do", "does", "did",
        "can", "could", "should", "would", "will", "just", "now", "so", "if", "then",
    ]
}

fn keywords(user_texts: &[String]) -> Vec<(String, u32)> {
    let sw: std::collections::HashSet<&'static str> = stopwords().iter().copied().collect();
    let mut counts: HashMap<String, u32> = HashMap::new();
    for t in user_texts {
        for w in t
            .to_lowercase()
            .split(|c: char| !(c.is_ascii_alphanumeric() || c == '-' || c == '_'))
        {
            if w.len() < 4 {
                continue;
            }
            if sw.contains(w) {
                continue;
            }
            *counts.entry(w.to_string()).or_insert(0) += 1;
        }
    }
    let mut v: Vec<(String, u32)> = counts.into_iter().collect();
    v.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    v.truncate(24);
    v
}

pub fn generate(external_run_id: &str, mut opts: ThreadReportOpts) -> Result<ThreadReportResult> {
    if !is_safe_segment(external_run_id) {
        return Err(anyhow!("invalid __run_id"));
    }
    if !is_safe_segment(&opts.user_id) {
        return Err(anyhow!("invalid user_id"));
    }

    opts.max_events = opts.max_events.max(20).min(2000);
    opts.content_chars = opts.content_chars.max(60).min(600);

    let root = meta3_root();
    let threads_dir = root.join("users").join(&opts.user_id).join("threads");
    let thread = select_thread(&threads_dir, &opts.thread)?;
    let thread_path = threads_dir.join(format!("{thread}.jsonl"));
    if !thread_path.exists() {
        return Err(anyhow!("thread not found: {}", thread_path.display()));
    }

    let lines = tail_lines(&thread_path, opts.max_events, 4_000_000)?;
    let mut events: Vec<ThreadEvent> = Vec::new();
    let mut counts_by_role: BTreeMap<String, u64> = BTreeMap::new();
    for line in lines {
        let v: Value = match serde_json::from_str(&line) {
            Ok(v) => v,
            Err(_) => continue,
        };
        let ts = v.get("ts").and_then(|x| x.as_str()).unwrap_or("").to_string();
        let role = v.get("role").and_then(|x| x.as_str()).unwrap_or("").to_string();
        let run_id = v.get("run_id").and_then(|x| x.as_str()).unwrap_or("").to_string();
        let content = one_line(v.get("content").and_then(|x| x.as_str()).unwrap_or(""));
        if role.is_empty() || run_id.is_empty() || !is_safe_segment(&run_id) {
            continue;
        }
        *counts_by_role.entry(role.clone()).or_insert(0) += 1;
        events.push(ThreadEvent {
            ts,
            role,
            run_id,
            content: truncate_chars(&content, 1200),
        });
    }
    if events.is_empty() {
        return Err(anyhow!("thread has no parseable events"));
    }

    // Build run index info from receipts.
    #[derive(Debug, Clone, Serialize)]
    struct RunInfo {
        i: usize,
        ts: String,
        role: String,
        run_id: String,
        goal_id: Option<String>,
        actual_success: Option<bool>,
        bits: BitsLite,
        view_url: Option<String>,
        text: String,
        receipt_url: String,
    }
    let mut run_index: Vec<RunInfo> = Vec::new();
    for (i, ev) in events.iter().enumerate() {
        let resp = receipt_response_json(&ev.run_id);
        let goal_id = resp.as_ref().and_then(get_goal_id);
        let view_url = resp.as_ref().and_then(get_view_url);
        let actual_success = resp.as_ref().and_then(get_actual_success);
        let bits = resp.as_ref().map(get_bits).unwrap_or_default();
        run_index.push(RunInfo {
            i: i + 1,
            ts: ev.ts.clone(),
            role: ev.role.clone(),
            run_id: ev.run_id.clone(),
            goal_id,
            actual_success,
            bits,
            view_url,
            text: truncate_chars(&ev.content, opts.content_chars),
            receipt_url: format!("/runs/receipts/{}/RECEIPT.md", ev.run_id),
        });
    }

    let user_msgs: Vec<String> = run_index
        .iter()
        .filter(|r| r.role == "user")
        .map(|r| r.text.clone())
        .collect();
    let topk = keywords(&user_msgs);

    let out_dir = root.join("runs").join("threads").join(external_run_id);
    fs::create_dir_all(&out_dir).with_context(|| format!("mkdir {}", out_dir.display()))?;

    let report_json = serde_json::json!({
        "user_id": opts.user_id,
        "thread": thread,
        "nodes": run_index.len(),
        "counts_by_role": counts_by_role,
        "top_keywords": topk,
        "runs": run_index,
    });
    fs::write(
        out_dir.join("report.json"),
        serde_json::to_string_pretty(&report_json).unwrap_or_default(),
    )
    .with_context(|| "write report.json".to_string())?;

    // HTML
    let mut rows_html = String::new();
    for r in report_json
        .get("runs")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default()
    {
        let role = r.get("role").and_then(|v| v.as_str()).unwrap_or("");
        let rid = r.get("run_id").and_then(|v| v.as_str()).unwrap_or("");
        let goal = r.get("goal_id").and_then(|v| v.as_str()).unwrap_or("");
        let txt = r.get("text").and_then(|v| v.as_str()).unwrap_or("");
        let receipt = r.get("receipt_url").and_then(|v| v.as_str()).unwrap_or("");
        let view = r.get("view_url").and_then(|v| v.as_str()).unwrap_or("");
        let ok = r.get("actual_success").and_then(|v| v.as_bool());
        let bits = r.get("bits").cloned().unwrap_or(Value::Null);
        let t = bits.get("t").and_then(|v| v.as_f64()).unwrap_or(0.0);
        let u = bits.get("u").and_then(|v| v.as_f64()).unwrap_or(0.0);
        let e = bits.get("e").and_then(|v| v.as_f64()).unwrap_or(0.0);

        let data_t = format!("{} {} {} {}", role, goal, rid, txt);
        rows_html.push_str(&format!(
            "<tr data-t=\"{}\">\
              <td>{}</td>\
              <td><code>{}</code></td>\
              <td><code>{}</code></td>\
              <td>{}</td>\
              <td><span class=\"pill\">T {:.2}</span><span class=\"pill\">U {:.2}</span><span class=\"pill\">E {:.2}</span></td>\
              <td>{}</td>\
              <td><a href=\"{}\" target=\"_blank\" rel=\"noreferrer\">receipt</a>{}</td>\
            </tr>\n",
            html_escape(&data_t),
            html_escape(role),
            html_escape(goal),
            html_escape(rid),
            match ok {
                Some(true) => "<span class=\"pill ok\">ok</span>",
                Some(false) => "<span class=\"pill fail\">fail</span>",
                None => "<span class=\"pill\">?</span>",
            },
            t,
            u,
            e,
            html_escape(txt),
            html_escape(receipt),
            if view.is_empty() {
                "".to_string()
            } else {
                format!(" · <a href=\"{}\" target=\"_blank\" rel=\"noreferrer\">view</a>", html_escape(view))
            }
        ));
    }

    let mut kw_html = String::new();
    for (w, c) in topk {
        kw_html.push_str(&format!(
            "<span class=\"pill\">{} ({})</span> ",
            html_escape(&w),
            c
        ));
    }

    let html = format!(
        r#"<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>Thread Report {rid}</title>
  <style>
    body{{font-family:system-ui,-apple-system,Segoe UI,Roboto,Arial;margin:24px;max-width:1200px}}
    .muted{{color:#57606a}}
    code{{background:#f6f8fa;border:1px solid #d0d7de;border-radius:10px;padding:2px 6px}}
    a{{color:#1f6feb;text-decoration:none}} a:hover{{text-decoration:underline}}
    input{{width:100%;padding:10px 12px;border:1px solid #d0d7de;border-radius:10px;margin:12px 0}}
    table{{border-collapse:collapse;width:100%}}
    th,td{{border-bottom:1px solid #f1f3f5;padding:8px 6px;text-align:left;vertical-align:top}}
    th{{font-size:12px;color:#57606a;position:sticky;top:0;background:#fff}}
    .pill{{display:inline-block;padding:1px 8px;border-radius:999px;border:1px solid #d0d7de;background:#f8f9fa;font-size:12px;color:#495057;margin-right:6px}}
    .ok{{background:#ebfbee}} .fail{{background:#fff5f5}}
    .box{{border:1px solid #d0d7de;border-radius:12px;overflow:auto;max-height:75vh}}
  </style>
</head>
<body>
  <h1>Thread Report</h1>
  <div class="muted">user: <code>{user}</code> · thread: <code>{thread}</code> · nodes: <code>{nodes}</code></div>
  <div class="muted" style="margin-top:8px">Links: <a href="report.json">report.json</a></div>

  <h2 style="margin-top:18px">Keywords</h2>
  <div class="muted">Extracted from user messages (heuristic).</div>
  <div style="margin-top:8px">{kw}</div>

  <h2 style="margin-top:18px">Timeline</h2>
  <input id="q" placeholder="filter (role/goal/run_id/text)..." />
  <div class="box">
    <table>
      <thead><tr><th>role</th><th>goal</th><th>run_id</th><th>ok</th><th>bits</th><th>text</th><th>links</th></tr></thead>
      <tbody id="list">{rows}</tbody>
    </table>
  </div>

  <script>
    const q = document.getElementById('q');
    const list = document.getElementById('list');
    q.addEventListener('input', () => {{
      const term = (q.value || '').toLowerCase().trim();
      for (const tr of list.querySelectorAll('tr')) {{
        const t = (tr.getAttribute('data-t') || '').toLowerCase();
        tr.style.display = !term || t.includes(term) ? '' : 'none';
      }}
    }});
  </script>
</body>
</html>
"#,
        rid = html_escape(external_run_id),
        user = html_escape(&opts.user_id),
        thread = html_escape(&thread),
        nodes = events.len(),
        rows = rows_html,
        kw = kw_html
    );
    fs::write(out_dir.join("index.html"), html.as_bytes())
        .with_context(|| "write index.html".to_string())?;

    Ok(ThreadReportResult {
        out_dir,
        nodes: events.len(),
        thread,
    })
}

