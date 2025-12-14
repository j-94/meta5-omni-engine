use anyhow::{anyhow, Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fs;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

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

#[derive(Debug, Clone)]
pub struct ThreadGraphResult {
    pub out_dir: PathBuf,
    pub nodes: usize,
    pub edges: usize,
    pub thread: String,
}

#[derive(Debug, Clone)]
pub struct ThreadGraphOpts {
    pub max_events: usize,
    pub content_chars: usize,
    pub label_mode: String, // "nl" | "goal" | "nl+goal"
    pub filter_text: Option<String>,
    pub filter_goal: Option<String>,
    pub recursive: bool,
    pub depth: usize,
    pub max_nodes: usize,
    pub include_bits: bool,
}

#[derive(Debug, Clone)]
pub struct ReceiptsGraphResult {
    pub out_dir: PathBuf,
    pub nodes: usize,
    pub edges: usize,
}

#[derive(Debug, Clone)]
pub struct ApiGraphResult {
    pub out_dir: PathBuf,
    pub nodes: usize,
    pub edges: usize,
}

#[derive(Debug, Clone)]
pub struct ApiGraphOpts {
    pub limit: usize,
    pub only_mutations: bool,
    pub run_id: Option<String>,
    pub thread: Option<String>,
    pub user_id: Option<String>,
    pub collapse: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ApiTraceEvent {
    ts: String,
    method: String,
    path: String,
    query: Option<String>,
    status: u16,
    ms: u64,
    mutation: bool,
    run_id: Option<String>,
    user_id: Option<String>,
    thread: Option<String>,
}

#[derive(Debug, Clone)]
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
    // Prefer top-level bits (RunResp/ChatResp), fall back to manifest.bits.
    let bits_v = resp.get("bits").or_else(|| resp.get("manifest").and_then(|m| m.get("bits")));
    let mut out = BitsLite::default();
    let map = match bits_v.and_then(|v| v.as_object()) {
        Some(m) => m,
        None => return out,
    };
    let mut get = |k: &str| map.get(k).and_then(|v| v.as_f64()).map(|x| x as f32);
    out.a = get("a").or_else(|| get("A"));
    out.u = get("u").or_else(|| get("U"));
    out.p = get("p").or_else(|| get("P"));
    out.e = get("e").or_else(|| get("E"));
    out.d = get("d").or_else(|| get("D"));
    out.i = get("i").or_else(|| get("I"));
    out.r = get("r").or_else(|| get("R"));
    out.t = get("t").or_else(|| get("T"));
    out.m = get("m").or_else(|| get("M"));
    out
}

fn role_style(role: &str) -> (&'static str, &'static str) {
    match role {
        "user" => ("#e7f5ff", "#1c7ed6"),
        "assistant" => ("#f3f0ff", "#7048e8"),
        "tool" => ("#ebfbee", "#2b8a3e"),
        "system" => ("#fff4e6", "#e8590c"),
        "ref" => ("#f8f9fa", "#495057"),
        _ => ("#f1f3f5", "#495057"),
    }
}

fn build_dot(
    events: &[ThreadEvent],
    goal_ids: &[Option<String>],
    bits: &[BitsLite],
    ok: &[Option<bool>],
    edges: &[(usize, usize, &'static str)],
    opts: &ThreadGraphOpts,
) -> String {
    let mut dot = String::from("digraph thread {\nrankdir=TB;\nnode [shape=box, style=\"rounded,filled\", fontname=\"Helvetica\"];\n");
    for (i, ev) in events.iter().enumerate() {
        let (mut fill, stroke) = role_style(&ev.role);
        if opts.include_bits {
            let e = bits.get(i).and_then(|b| b.e).unwrap_or(0.0);
            let t = bits.get(i).and_then(|b| b.t).unwrap_or(0.0);
            let passed = ok.get(i).and_then(|v| *v).unwrap_or(true);
            if !passed || e >= 0.5 {
                fill = "#fff5f5";
            } else if t >= 0.9 {
                fill = "#ebfbee";
            } else if t >= 0.6 {
                fill = "#fff9db";
            }
        }
        let mut label = format!("{}: {}", i + 1, ev.role);
        let goal = goal_ids.get(i).and_then(|x| x.as_deref()).unwrap_or("");
        let show_goal = opts.label_mode.contains("goal");
        let show_nl = opts.label_mode.contains("nl");
        if show_goal && !goal.is_empty() {
            label.push_str(&format!("\\n{}", goal));
        }
        if show_nl && !ev.content.trim().is_empty() {
            let c = truncate_chars(&ev.content, opts.content_chars).replace('"', "\\\"");
            label.push_str(&format!("\\n{}", c));
        } else {
            label.push_str(&format!("\\n{}", ev.run_id));
        }
        if opts.include_bits {
            let t = bits.get(i).and_then(|b| b.t);
            let u = bits.get(i).and_then(|b| b.u);
            let e = bits.get(i).and_then(|b| b.e);
            if t.is_some() || u.is_some() || e.is_some() {
                label.push_str(&format!(
                    "\\nT={} U={} E={}",
                    t.map(|v| format!("{:.2}", v)).unwrap_or_else(|| "-".to_string()),
                    u.map(|v| format!("{:.2}", v)).unwrap_or_else(|| "-".to_string()),
                    e.map(|v| format!("{:.2}", v)).unwrap_or_else(|| "-".to_string()),
                ));
            }
        }
        dot.push_str(&format!(
            "  n{} [label=\"{}\", fillcolor=\"{}\", color=\"{}\"];\n",
            i,
            label.replace('"', "\\\""),
            fill,
            stroke
        ));
    }
    for (src, dst, kind) in edges {
        let lbl = match *kind {
            "seq" => "",
            "ref" => " [label=\"ref\"]",
            _ => "",
        };
        dot.push_str(&format!("  n{} -> n{}{};\n", src, dst, lbl));
    }
    dot.push_str("}\n");
    dot
}

fn build_svg(
    events: &[ThreadEvent],
    goal_ids: &[Option<String>],
    view_urls: &[Option<String>],
    bits: &[BitsLite],
    ok: &[Option<bool>],
    opts: &ThreadGraphOpts,
) -> String {
    let w = 980;
    let x = 40;
    let node_w = 900;
    let node_h = 56;
    let gap = 18;
    let top = 24;
    let h = top + (node_h + gap) * (events.len().max(1)) + 24;

    let mut s = String::new();
    s.push_str(&format!(
        "<svg width=\"{}\" height=\"{}\" viewBox=\"0 0 {} {}\" xmlns=\"http://www.w3.org/2000/svg\">",
        w, h, w, h
    ));
    s.push_str(
        "<style>text{font-family:system-ui,-apple-system,Segoe UI,Roboto,Arial;font-size:14px;fill:#111}</style>",
    );

    for i in 0..events.len() {
        let y = top + i * (node_h + gap);
        if i + 1 < events.len() {
            let y2 = top + (i + 1) * (node_h + gap);
            s.push_str(&format!(
                "<line x1=\"{}\" y1=\"{}\" x2=\"{}\" y2=\"{}\" stroke=\"#adb5bd\" stroke-width=\"2\"/>",
                x + 18,
                y + node_h,
                x + 18,
                y2
            ));
            s.push_str(&format!(
                "<polygon points=\"{},{} {},{} {},{}\" fill=\"#adb5bd\"/>",
                x + 14,
                y2 - 6,
                x + 22,
                y2 - 6,
                x + 18,
                y2 + 2
            ));
        }
    }

    for (i, ev) in events.iter().enumerate() {
        let y = top + i * (node_h + gap);
        let (mut fill, stroke) = role_style(&ev.role);
        if opts.include_bits {
            let e = bits.get(i).and_then(|b| b.e).unwrap_or(0.0);
            let t = bits.get(i).and_then(|b| b.t).unwrap_or(0.0);
            let passed = ok.get(i).and_then(|v| *v).unwrap_or(true);
            if !passed || e >= 0.5 {
                fill = "#fff5f5";
            } else if t >= 0.9 {
                fill = "#ebfbee";
            } else if t >= 0.6 {
                fill = "#fff9db";
            }
        }
        let goal = goal_ids.get(i).and_then(|x| x.as_deref()).unwrap_or("");
        let view = view_urls.get(i).and_then(|x| x.as_deref()).unwrap_or("");

        let title = if !goal.is_empty() {
            format!("{} · {}", ev.role, goal)
        } else {
            ev.role.clone()
        };
        let show_nl = opts.label_mode.contains("nl");
        let nl = if show_nl { truncate_chars(&ev.content, opts.content_chars) } else { "".to_string() };
        let line2 = if !nl.trim().is_empty() {
            nl
        } else if !view.is_empty() {
            format!("view: {}", view)
        } else {
            format!("run_id: {}", ev.run_id)
        };

        let href = format!("/runs/receipts/{}/RECEIPT.md", ev.run_id);
        s.push_str(&format!("<a href=\"{}\" target=\"_blank\" rel=\"noreferrer\">", html_escape(&href)));
        s.push_str(&format!(
            "<rect x=\"{}\" y=\"{}\" rx=\"10\" ry=\"10\" width=\"{}\" height=\"{}\" fill=\"{}\" stroke=\"{}\" stroke-width=\"2\"/>",
            x,
            y,
            node_w,
            node_h,
            fill,
            stroke
        ));
        s.push_str(&format!(
            "<text x=\"{}\" y=\"{}\">{}</text>",
            x + 16,
            y + 22,
            html_escape(&format!("{}: {}", i + 1, title))
        ));
        s.push_str(&format!(
            "<text x=\"{}\" y=\"{}\" fill=\"#495057\">{}</text>",
            x + 16,
            y + 42,
            html_escape(&line2)
        ));
        if opts.include_bits {
            let t = bits.get(i).and_then(|b| b.t);
            let u = bits.get(i).and_then(|b| b.u);
            let e = bits.get(i).and_then(|b| b.e);
            let pill = format!(
                "T={} U={} E={}",
                t.map(|v| format!("{:.2}", v)).unwrap_or_else(|| "-".to_string()),
                u.map(|v| format!("{:.2}", v)).unwrap_or_else(|| "-".to_string()),
                e.map(|v| format!("{:.2}", v)).unwrap_or_else(|| "-".to_string()),
            );
            s.push_str(&format!(
                "<text x=\"{}\" y=\"{}\" fill=\"#868e96\" font-size=\"12\">{}</text>",
                x + node_w - 10,
                y + 42,
                html_escape(&pill)
            ));
        }
        s.push_str("</a>");

        if !ev.ts.is_empty() {
            s.push_str(&format!(
                "<text x=\"{}\" y=\"{}\" fill=\"#868e96\" font-size=\"12\">{}</text>",
                x + node_w - 10,
                y + 22,
                html_escape(&ev.ts)
            ));
        }
    }

    s.push_str("</svg>");
    s
}

fn index_html(
    run_id: &str,
    user_id: &str,
    thread: &str,
    svg: &str,
    nodes: usize,
    edges: usize,
    table_html: &str,
) -> String {
    format!(
        r#"<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>Thread Graph {run_id}</title>
  <style>
    body{{font-family:system-ui,-apple-system,Segoe UI,Roboto,Arial;margin:24px;max-width:1100px}}
    .muted{{color:#57606a}}
    code, pre{{background:#f6f8fa;border:1px solid #d0d7de;border-radius:10px}}
    code{{padding:2px 6px}}
    pre{{padding:12px;overflow:auto}}
    a{{color:#1f6feb;text-decoration:none}} a:hover{{text-decoration:underline}}
    .row{{display:flex;gap:10px;flex-wrap:wrap;align-items:center}}
    table{{border-collapse:collapse;width:100%;margin-top:14px}}
    th,td{{border-bottom:1px solid #f1f3f5;padding:8px 6px;text-align:left;vertical-align:top}}
    th{{font-size:12px;color:#57606a}}
    .pill{{display:inline-block;padding:1px 8px;border-radius:999px;border:1px solid #d0d7de;background:#f8f9fa;font-size:12px;color:#495057;margin-right:6px}}
  </style>
</head>
<body>
  <h1>Thread Graph</h1>
  <div class="muted">
    run_id: <code>{run_id}</code> · user: <code>{user_id}</code> · thread: <code>{thread}</code> · nodes: <code>{nodes}</code> · edges: <code>{edges}</code>
  </div>
  <div class="row" style="margin-top:10px">
    <a href="graph.dot">graph.dot</a>
    <a href="events.json">events.json</a>
  </div>
  <p class="muted">Click a node to open its receipt.</p>
  <div style="margin-top:12px">{svg}</div>
  <h2 style="margin-top:18px">Nodes</h2>
  <div class="muted">This table is the “user relevant” view: natural language + bits + links.</div>
  <table>
    <thead>
      <tr>
        <th>#</th>
        <th>Role</th>
        <th>Goal</th>
        <th>Bits</th>
        <th>Text</th>
        <th>Links</th>
      </tr>
    </thead>
    <tbody>{table_html}</tbody>
  </table>
</body>
</html>
"#,
        run_id = html_escape(run_id),
        user_id = html_escape(user_id),
        thread = html_escape(thread),
        nodes = nodes,
        edges = edges,
        svg = svg,
        table_html = table_html
    )
}

fn extract_run_ids_limited(v: &Value, out: &mut Vec<String>, depth: usize, budget: &mut usize) {
    if *budget == 0 || depth == 0 {
        return;
    }
    match v {
        Value::String(s) => {
            if *budget == 0 {
                return;
            }
            // Heuristic: strings that are exactly run_ids or contain them.
            // We only extract safe segments starting with r-.
            for token in s
                .split(|c: char| !(c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.'))
            {
                if *budget == 0 {
                    break;
                }
                if token.starts_with("r-") && is_safe_segment(token) && token.len() >= 10 {
                    if !out.contains(&token.to_string()) {
                        out.push(token.to_string());
                        *budget -= 1;
                    }
                }
            }
        }
        Value::Array(arr) => {
            for item in arr {
                if *budget == 0 {
                    break;
                }
                extract_run_ids_limited(item, out, depth - 1, budget);
            }
        }
        Value::Object(map) => {
            for v in map.values() {
                if *budget == 0 {
                    break;
                }
                extract_run_ids_limited(v, out, depth - 1, budget);
            }
        }
        _ => {}
    }
}

pub fn thread_graph(external_run_id: &str, user_id: &str, thread: &str, max_events: usize) -> Result<ThreadGraphResult> {
    thread_graph_with_opts(
        external_run_id,
        user_id,
        thread,
        ThreadGraphOpts {
            max_events,
            content_chars: 120,
            label_mode: "nl+goal".to_string(),
            filter_text: None,
            filter_goal: None,
            recursive: false,
            depth: 1,
            max_nodes: 200,
            include_bits: true,
        },
    )
}

pub fn thread_graph_with_opts(
    external_run_id: &str,
    user_id: &str,
    thread: &str,
    mut opts: ThreadGraphOpts,
) -> Result<ThreadGraphResult> {
    if !is_safe_segment(external_run_id) {
        return Err(anyhow!("invalid __run_id"));
    }
    if !is_safe_segment(user_id) {
        return Err(anyhow!("invalid user_id"));
    }
    let root = meta3_root();
    let threads_dir = root.join("users").join(user_id).join("threads");

    let thread = match thread.trim() {
        "" | "auto" => {
            let mut best: Option<(u64, String)> = None;
            let rd = fs::read_dir(&threads_dir)
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
                .ok_or_else(|| anyhow!("no threads found in {}", threads_dir.display()))?
        }
        t => {
            if !is_safe_segment(t) {
                return Err(anyhow!("invalid thread"));
            }
            t.to_string()
        }
    };

    let thread_path = root
        .join("users")
        .join(user_id)
        .join("threads")
        .join(format!("{thread}.jsonl"));

    if !thread_path.exists() {
        return Err(anyhow!("thread not found: {}", thread_path.display()));
    }

    // Clamp opts for safety.
    opts.max_events = opts.max_events.max(1).min(800);
    opts.content_chars = opts.content_chars.max(20).min(220);
    opts.depth = opts.depth.max(1).min(3);
    opts.max_nodes = opts.max_nodes.max(20).min(1200);
    if opts.label_mode.trim().is_empty() {
        opts.label_mode = "nl+goal".to_string();
    }

    let lines = tail_lines(&thread_path, opts.max_events, 1_200_000)?;
    let mut events: Vec<ThreadEvent> = Vec::new();
    for line in lines {
        let v: Value = match serde_json::from_str(&line) {
            Ok(v) => v,
            Err(_) => continue,
        };
        let ts = v.get("ts").and_then(|x| x.as_str()).unwrap_or("").to_string();
        let role = v
            .get("role")
            .and_then(|x| x.as_str())
            .unwrap_or("")
            .to_string();
        let run_id = v
            .get("run_id")
            .and_then(|x| x.as_str())
            .unwrap_or("")
            .to_string();
        let content_raw = v.get("content").and_then(|x| x.as_str()).unwrap_or("");
        let content = truncate_chars(&one_line(content_raw), 400);
        if role.is_empty() || run_id.is_empty() || !is_safe_segment(&run_id) {
            continue;
        }
        if let Some(ft) = opts.filter_text.as_deref() {
            let ft = ft.to_lowercase();
            if !content.to_lowercase().contains(&ft) {
                continue;
            }
        }
        events.push(ThreadEvent {
            ts,
            role,
            run_id,
            content,
        });
    }

    if events.is_empty() {
        return Err(anyhow!("thread has no parseable events"));
    }

    // Base edges are the chronological sequence.
    let mut edges: Vec<(usize, usize, &'static str)> = Vec::new();
    for i in 0..events.len().saturating_sub(1) {
        edges.push((i, i + 1, "seq"));
    }

    let mut goal_ids: Vec<Option<String>> = Vec::with_capacity(events.len());
    let mut view_urls: Vec<Option<String>> = Vec::with_capacity(events.len());
    let mut oks: Vec<Option<bool>> = Vec::with_capacity(events.len());
    let mut bits: Vec<BitsLite> = Vec::with_capacity(events.len());
    for ev in &events {
        let resp = receipt_response_json(&ev.run_id);
        let goal = resp.as_ref().and_then(get_goal_id);
        let view = resp.as_ref().and_then(get_view_url);
        let ok = resp.as_ref().and_then(get_actual_success);
        let b = resp.as_ref().map(get_bits).unwrap_or_default();
        goal_ids.push(goal);
        view_urls.push(view);
        oks.push(ok);
        bits.push(b);
    }

    let out_dir = root.join("runs").join("graphs").join(external_run_id);
    fs::create_dir_all(&out_dir).with_context(|| format!("mkdir {}", out_dir.display()))?;

    // Optional recursion: discover referenced run_ids from receipts and add them as "ref" nodes.
    if opts.recursive {
        // Map run_id -> node index
        let mut idx_for: std::collections::HashMap<String, usize> = std::collections::HashMap::new();
        for (i, ev) in events.iter().enumerate() {
            idx_for.insert(ev.run_id.clone(), i);
        }

        let mut frontier: Vec<(usize, usize)> = events
            .iter()
            .enumerate()
            .map(|(i, _)| (i, 0usize))
            .collect();
        while let Some((src_idx, d)) = frontier.pop() {
            if d >= opts.depth {
                continue;
            }
            let run_id = events[src_idx].run_id.clone();
            let resp = match receipt_response_json(&run_id) {
                Some(v) => v,
                None => continue,
            };
            let mut refs: Vec<String> = Vec::new();
            let mut budget = 24usize;
            extract_run_ids_limited(&resp, &mut refs, 5, &mut budget);
            for r in refs {
                if r == run_id {
                    continue;
                }
                let dst_idx = if let Some(i) = idx_for.get(&r).copied() {
                    i
                } else {
                    if events.len() >= opts.max_nodes {
                        continue;
                    }
                    // Add new node.
                    let resp2 = receipt_response_json(&r);
                    let g = resp2.as_ref().and_then(get_goal_id);
                    let v = resp2.as_ref().and_then(get_view_url);
                    let ok = resp2.as_ref().and_then(get_actual_success);
                    let b = resp2.as_ref().map(get_bits).unwrap_or_default();

                    let snippet = resp2
                        .as_ref()
                        .and_then(|x| x.get("manifest"))
                        .and_then(|m| m.get("evidence"))
                        .and_then(|e| e.get("reply").or_else(|| e.get("stdout")).or_else(|| e.get("error")))
                        .and_then(|v| v.as_str())
                        .unwrap_or("");

                    let idx = events.len();
                    events.push(ThreadEvent {
                        ts: "".to_string(),
                        role: "ref".to_string(),
                        run_id: r.clone(),
                        content: truncate_chars(&one_line(snippet), 260),
                    });
                    goal_ids.push(g);
                    view_urls.push(v);
                    oks.push(ok);
                    bits.push(b);
                    idx_for.insert(r.clone(), idx);
                    frontier.push((idx, d + 1));
                    idx
                };
                if src_idx != dst_idx {
                    edges.push((src_idx, dst_idx, "ref"));
                }
            }
        }
    }

    if let Some(fg) = opts.filter_goal.as_deref() {
        let fg = fg.to_lowercase();
        let mut filtered = Vec::new();
        let mut filtered_goal_ids = Vec::new();
        let mut filtered_view_urls = Vec::new();
        let mut filtered_ok = Vec::new();
        let mut filtered_bits = Vec::new();
        for (i, ev) in events.iter().enumerate() {
            let g = goal_ids.get(i).and_then(|x| x.as_deref()).unwrap_or("").to_lowercase();
            if g.contains(&fg) {
                filtered.push(ev.clone());
                filtered_goal_ids.push(goal_ids[i].clone());
                filtered_view_urls.push(view_urls[i].clone());
                filtered_ok.push(oks[i]);
                filtered_bits.push(bits[i].clone());
            }
        }
        if !filtered.is_empty() {
            let mut filtered_edges: Vec<(usize, usize, &'static str)> = Vec::new();
            for i in 0..filtered.len().saturating_sub(1) {
                filtered_edges.push((i, i + 1, "seq"));
            }
            let dot = build_dot(
                &filtered,
                &filtered_goal_ids,
                &filtered_bits,
                &filtered_ok,
                &filtered_edges,
                &opts,
            );
            fs::write(out_dir.join("graph.dot"), dot.as_bytes())
                .with_context(|| "write graph.dot".to_string())?;
            let events_json = serde_json::json!({
                "user_id": user_id,
                "thread": thread,
                "filter_goal": fg,
                "events": filtered.iter().enumerate().map(|(i, e)| {
                    serde_json::json!({
                        "i": i + 1,
                        "ts": e.ts,
                        "role": e.role,
                        "run_id": e.run_id,
                        "content": e.content,
                        "goal_id": filtered_goal_ids.get(i).and_then(|x| x.clone()),
                        "view_url": filtered_view_urls.get(i).and_then(|x| x.clone()),
                        "actual_success": filtered_ok.get(i).and_then(|x| *x),
                        "bits": filtered_bits.get(i).cloned().unwrap_or_default(),
                        "receipt_url": format!("/runs/receipts/{}/RECEIPT.md", e.run_id),
                    })
                }).collect::<Vec<_>>()
            });
            fs::write(
                out_dir.join("events.json"),
                serde_json::to_string_pretty(&events_json).unwrap_or_default(),
            )
            .with_context(|| "write events.json".to_string())?;

            let svg = build_svg(
                &filtered,
                &filtered_goal_ids,
                &filtered_view_urls,
                &filtered_bits,
                &filtered_ok,
                &opts,
            );
            let table_html = build_table_html(
                &filtered,
                &filtered_goal_ids,
                &filtered_view_urls,
                &filtered_bits,
                &filtered_ok,
                &opts,
            );
            let html = index_html(
                external_run_id,
                user_id,
                &thread,
                &svg,
                filtered.len(),
                filtered.len().saturating_sub(1),
                &table_html,
            );
            fs::write(out_dir.join("index.html"), html.as_bytes())
                .with_context(|| "write index.html".to_string())?;

            return Ok(ThreadGraphResult {
                out_dir,
                nodes: filtered.len(),
                edges: filtered.len().saturating_sub(1),
                thread,
            });
        }
    }

    let dot = build_dot(&events, &goal_ids, &bits, &oks, &edges, &opts);
    fs::write(out_dir.join("graph.dot"), dot.as_bytes())
        .with_context(|| "write graph.dot".to_string())?;

    let events_json = serde_json::json!({
        "user_id": user_id,
        "thread": thread,
        "events": events.iter().enumerate().map(|(i, e)| {
            serde_json::json!({
                "i": i + 1,
                "ts": e.ts,
                "role": e.role,
                "run_id": e.run_id,
                "content": e.content,
                "goal_id": goal_ids.get(i).and_then(|x| x.clone()),
                "view_url": view_urls.get(i).and_then(|x| x.clone()),
                "actual_success": oks.get(i).and_then(|x| *x),
                "bits": bits.get(i).cloned().unwrap_or_default(),
                "receipt_url": format!("/runs/receipts/{}/RECEIPT.md", e.run_id),
            })
        }).collect::<Vec<_>>()
    });
    fs::write(
        out_dir.join("events.json"),
        serde_json::to_string_pretty(&events_json).unwrap_or_default(),
    )
    .with_context(|| "write events.json".to_string())?;

    let svg = build_svg(&events, &goal_ids, &view_urls, &bits, &oks, &opts);
    let table_html = build_table_html(&events, &goal_ids, &view_urls, &bits, &oks, &opts);
    let html = index_html(
        external_run_id,
        user_id,
        &thread,
        &svg,
        events.len(),
        edges.len(),
        &table_html,
    );
    fs::write(out_dir.join("index.html"), html.as_bytes())
        .with_context(|| "write index.html".to_string())?;

    Ok(ThreadGraphResult {
        out_dir,
        nodes: events.len(),
        edges: edges.len(),
        thread,
    })
}

fn build_table_html(
    events: &[ThreadEvent],
    goal_ids: &[Option<String>],
    view_urls: &[Option<String>],
    bits: &[BitsLite],
    ok: &[Option<bool>],
    opts: &ThreadGraphOpts,
) -> String {
    let mut out = String::new();
    for (i, ev) in events.iter().enumerate() {
        let goal = goal_ids.get(i).and_then(|x| x.as_deref()).unwrap_or("");
        let view = view_urls.get(i).and_then(|x| x.as_deref()).unwrap_or("");
        let receipt = format!("/runs/receipts/{}/RECEIPT.md", ev.run_id);
        let passed = ok.get(i).and_then(|x| *x);

        let t = bits.get(i).and_then(|b| b.t);
        let u = bits.get(i).and_then(|b| b.u);
        let e = bits.get(i).and_then(|b| b.e);
        let bits_txt = if opts.include_bits {
            format!(
                "<span class=\"pill\">T {}</span><span class=\"pill\">U {}</span><span class=\"pill\">E {}</span>{}",
                t.map(|v| format!("{:.2}", v)).unwrap_or_else(|| "-".to_string()),
                u.map(|v| format!("{:.2}", v)).unwrap_or_else(|| "-".to_string()),
                e.map(|v| format!("{:.2}", v)).unwrap_or_else(|| "-".to_string()),
                match passed {
                    Some(true) => "<span class=\"pill\">ok</span>",
                    Some(false) => "<span class=\"pill\">fail</span>",
                    None => "",
                }
            )
        } else {
            "".to_string()
        };

        out.push_str(&format!(
            "<tr>\
              <td>{}</td>\
              <td>{}</td>\
              <td>{}</td>\
              <td>{}</td>\
              <td>{}</td>\
              <td><a href=\"{}\" target=\"_blank\" rel=\"noreferrer\">receipt</a>{}</td>\
            </tr>\n",
            i + 1,
            html_escape(&ev.role),
            html_escape(goal),
            bits_txt,
            html_escape(&truncate_chars(&ev.content, opts.content_chars)),
            html_escape(&receipt),
            if view.is_empty() {
                "".to_string()
            } else {
                format!(" · <a href=\"{}\" target=\"_blank\" rel=\"noreferrer\">view</a>", html_escape(view))
            }
        ));
    }
    out
}

fn index_html_receipts(run_id: &str, nodes: usize, edges: usize, items_html: &str) -> String {
    format!(
        r#"<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>Receipts Graph {run_id}</title>
  <style>
    body{{font-family:system-ui,-apple-system,Segoe UI,Roboto,Arial;margin:24px;max-width:1100px}}
    .muted{{color:#57606a}}
    code{{background:#f6f8fa;border:1px solid #d0d7de;border-radius:10px;padding:2px 6px}}
    a{{color:#1f6feb;text-decoration:none}} a:hover{{text-decoration:underline}}
    input{{width:100%;padding:10px 12px;border:1px solid #d0d7de;border-radius:10px;margin:12px 0}}
    .box{{border:1px solid #d0d7de;border-radius:12px;overflow:auto;max-height:75vh}}
    ol{{margin:0;padding:10px 10px 10px 34px}}
    li{{padding:6px 8px;border-bottom:1px solid #f1f3f5}}
    li:last-child{{border-bottom:none}}
    .pill{{display:inline-block;padding:1px 8px;border-radius:999px;border:1px solid #d0d7de;background:#f8f9fa;font-size:12px;color:#495057;margin-left:8px}}
  </style>
</head>
<body>
  <h1>Receipts Graph</h1>
  <div class="muted">
    run_id: <code>{run_id}</code> · nodes: <code>{nodes}</code> · edges: <code>{edges}</code>
  </div>
  <div class="muted" style="margin-top:8px">
    Links: <a href="graph.dot">graph.dot</a> · <a href="events.json">events.json</a>
  </div>

  <input id="q" placeholder="filter by goal_id / run_id..." />
  <div class="box">
    <ol id="list">{items_html}</ol>
  </div>

  <script>
    const q = document.getElementById('q');
    const list = document.getElementById('list');
    q.addEventListener('input', () => {{
      const term = (q.value || '').toLowerCase().trim();
      for (const li of list.querySelectorAll('li')) {{
        const t = (li.getAttribute('data-t') || '').toLowerCase();
        li.style.display = !term || t.includes(term) ? '' : 'none';
      }}
    }});
  </script>
</body>
</html>
"#,
        run_id = html_escape(run_id),
        nodes = nodes,
        edges = edges,
        items_html = items_html
    )
}

fn normalize_path(p: &str) -> String {
    let parts: Vec<&str> = p.split('/').filter(|s| !s.is_empty()).collect();
    if parts.len() >= 3 && parts[0] == "runs" && parts[1] == "receipts" {
        return "/runs/receipts/<run_id>/…".to_string();
    }
    if parts.len() >= 3 && parts[0] == "runs" && parts[1] == "wiki" {
        return "/runs/wiki/<run_id>/…".to_string();
    }
    if parts.len() >= 3 && parts[0] == "runs" && parts[1] == "graphs" {
        return "/runs/graphs/<run_id>/…".to_string();
    }
    if parts.len() >= 2 && parts[0] == "users" {
        return format!("/users/<user>/{}", parts.get(2).copied().unwrap_or(""));
    }
    p.to_string()
}

fn index_html_api(run_id: &str, nodes: usize, edges: usize, items_html: &str) -> String {
    format!(
        r#"<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>API Jump Graph {run_id}</title>
  <style>
    body{{font-family:system-ui,-apple-system,Segoe UI,Roboto,Arial;margin:24px;max-width:1100px}}
    .muted{{color:#57606a}}
    code{{background:#f6f8fa;border:1px solid #d0d7de;border-radius:10px;padding:2px 6px}}
    a{{color:#1f6feb;text-decoration:none}} a:hover{{text-decoration:underline}}
    input{{width:100%;padding:10px 12px;border:1px solid #d0d7de;border-radius:10px;margin:12px 0}}
    .box{{border:1px solid #d0d7de;border-radius:12px;overflow:auto;max-height:75vh}}
    table{{border-collapse:collapse;width:100%}}
    th,td{{border-bottom:1px solid #f1f3f5;padding:8px 6px;text-align:left;vertical-align:top}}
    th{{font-size:12px;color:#57606a;position:sticky;top:0;background:#fff}}
    .pill{{display:inline-block;padding:1px 8px;border-radius:999px;border:1px solid #d0d7de;background:#f8f9fa;font-size:12px;color:#495057;margin-right:6px}}
    .mut{{background:#fff5f5}}
  </style>
</head>
<body>
  <h1>API Jump Graph</h1>
  <div class="muted">
    run_id: <code>{run_id}</code> · nodes: <code>{nodes}</code> · edges: <code>{edges}</code>
  </div>
  <div class="muted" style="margin-top:8px">
    Links: <a href=\"graph.dot\">graph.dot</a> · <a href=\"events.json\">events.json</a>
  </div>
  <input id=\"q\" placeholder=\"filter by path/method/run_id...\" />
  <div class=\"box\">
    <table>
      <thead><tr><th>#</th><th>ts</th><th>method</th><th>path</th><th>status</th><th>ms</th><th>run_id</th><th>thread</th></tr></thead>
      <tbody id=\"list\">{items_html}</tbody>
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
        run_id = html_escape(run_id),
        nodes = nodes,
        edges = edges,
        items_html = items_html
    )
}

pub fn api_graph(external_run_id: &str, mut opts: ApiGraphOpts) -> Result<ApiGraphResult> {
    if !is_safe_segment(external_run_id) {
        return Err(anyhow!("invalid __run_id"));
    }
    opts.limit = opts.limit.clamp(10, 4000);

    let root = meta3_root();
    let trace_path = root.join("runs").join("api_trace.jsonl");
    if !trace_path.exists() {
        return Err(anyhow!(
            "api trace not found: {} (make some requests first)",
            trace_path.display()
        ));
    }

    let lines = tail_lines(&trace_path, opts.limit, 2_000_000)?;
    let mut evs: Vec<ApiTraceEvent> = Vec::new();
    for line in lines {
        let v: ApiTraceEvent = match serde_json::from_str(&line) {
            Ok(v) => v,
            Err(_) => continue,
        };
        if opts.only_mutations && !v.mutation {
            continue;
        }
        if let Some(rid) = opts.run_id.as_deref() {
            if v.run_id.as_deref() != Some(rid) {
                continue;
            }
        }
        if let Some(th) = opts.thread.as_deref() {
            if v.thread.as_deref() != Some(th) {
                continue;
            }
        }
        if let Some(uid) = opts.user_id.as_deref() {
            if v.user_id.as_deref() != Some(uid) {
                continue;
            }
        }
        evs.push(v);
    }
    if evs.is_empty() {
        return Err(anyhow!("no matching api events"));
    }

    // Collapse adjacent identical endpoints if desired.
    #[derive(Clone)]
    struct Row {
        ts: String,
        method: String,
        path: String,
        status: u16,
        ms: u64,
        mutation: bool,
        run_id: String,
        thread: String,
    }
    let mut rows: Vec<Row> = Vec::new();
    for ev in evs {
        let rid = ev.run_id.unwrap_or_default();
        let th = ev.thread.unwrap_or_default();
        let p = normalize_path(&ev.path);
        let row = Row {
            ts: ev.ts,
            method: ev.method,
            path: p,
            status: ev.status,
            ms: ev.ms,
            mutation: ev.mutation,
            run_id: rid,
            thread: th,
        };
        if opts.collapse {
            if let Some(last) = rows.last_mut() {
                if last.method == row.method && last.path == row.path && last.run_id == row.run_id {
                    last.ms = last.ms.saturating_add(row.ms);
                    last.status = row.status;
                    continue;
                }
            }
        }
        rows.push(row);
        if rows.len() >= opts.limit {
            break;
        }
    }

    // Nodes are endpoint keys.
    let mut node_for: std::collections::HashMap<String, usize> = std::collections::HashMap::new();
    let mut nodes: Vec<String> = Vec::new();
    let mut edges: Vec<(usize, usize)> = Vec::new();
    let mut last_node: Option<usize> = None;
    for r in &rows {
        let key = format!("{} {}", r.method, r.path);
        let idx = *node_for.entry(key.clone()).or_insert_with(|| {
            let i = nodes.len();
            nodes.push(key);
            i
        });
        if let Some(prev) = last_node {
            edges.push((prev, idx));
        }
        last_node = Some(idx);
    }

    let out_dir = root.join("runs").join("graphs").join(external_run_id);
    fs::create_dir_all(&out_dir).with_context(|| format!("mkdir {}", out_dir.display()))?;

    // DOT
    let mut dot = String::from("digraph api {\nrankdir=LR;\nnode [shape=box, style=\"rounded,filled\", fontname=\"Helvetica\", fillcolor=\"#f8f9fa\"];\n");
    for (i, label) in nodes.iter().enumerate() {
        dot.push_str(&format!("  n{} [label=\"{}\"];\n", i, label.replace('\"', "\\\"")));
    }
    for (a, b) in &edges {
        dot.push_str(&format!("  n{} -> n{};\n", a, b));
    }
    dot.push_str("}\n");
    fs::write(out_dir.join("graph.dot"), dot.as_bytes())
        .with_context(|| "write graph.dot".to_string())?;

    // events.json
    let events_json = serde_json::json!({
        "kind": "api",
        "opts": {
            "limit": opts.limit,
            "only_mutations": opts.only_mutations,
            "run_id": opts.run_id,
            "thread": opts.thread,
            "user_id": opts.user_id,
            "collapse": opts.collapse,
        },
        "rows": rows.iter().enumerate().map(|(i, r)| {
            serde_json::json!({
                "i": i + 1,
                "ts": r.ts,
                "method": r.method,
                "path": r.path,
                "status": r.status,
                "ms": r.ms,
                "mutation": r.mutation,
                "run_id": r.run_id,
                "thread": r.thread,
            })
        }).collect::<Vec<_>>(),
        "nodes": nodes,
        "edges": edges,
    });
    fs::write(out_dir.join("events.json"), serde_json::to_string_pretty(&events_json).unwrap_or_default())
        .with_context(|| "write events.json".to_string())?;

    // index.html
    let mut items_html = String::new();
    for (i, r) in rows.iter().enumerate() {
        let data_t = format!("{} {} {} {}", r.method, r.path, r.run_id, r.thread);
        items_html.push_str(&format!(
            "<tr class=\"{}\" data-t=\"{}\"><td>{}</td><td>{}</td><td>{}</td><td><code>{}</code></td><td>{}</td><td>{}</td><td><code>{}</code></td><td><code>{}</code></td></tr>\n",
            if r.mutation { "mut" } else { "" },
            html_escape(&data_t),
            i + 1,
            html_escape(&r.ts),
            html_escape(&r.method),
            html_escape(&r.path),
            r.status,
            r.ms,
            html_escape(&r.run_id),
            html_escape(&r.thread),
        ));
    }
    let html = index_html_api(external_run_id, nodes.len(), edges.len(), &items_html);
    fs::write(out_dir.join("index.html"), html.as_bytes())
        .with_context(|| "write index.html".to_string())?;

    Ok(ApiGraphResult {
        out_dir,
        nodes: nodes.len(),
        edges: edges.len(),
    })
}

pub fn receipts_graph(external_run_id: &str, limit: usize) -> Result<ReceiptsGraphResult> {
    if !is_safe_segment(external_run_id) {
        return Err(anyhow!("invalid __run_id"));
    }
    let limit = limit.clamp(1, 2000);

    let root = meta3_root();
    let receipts_dir = root.join("runs").join("receipts");
    let rd = fs::read_dir(&receipts_dir)
        .with_context(|| format!("read_dir {}", receipts_dir.display()))?;

    #[derive(Clone)]
    struct Item {
        run_id: String,
        goal_id: String,
        ok: Option<bool>,
        view: Option<String>,
        mtime: u64,
    }

    let mut items: Vec<Item> = Vec::new();
    for entry in rd.flatten() {
        let p = entry.path();
        if !p.is_dir() {
            continue;
        }
        let run_id = p
            .file_name()
            .and_then(|x| x.to_str())
            .unwrap_or("")
            .to_string();
        if !is_safe_segment(&run_id) {
            continue;
        }
        let resp_path = p.join("response.json");
        let meta = match fs::metadata(&resp_path) {
            Ok(m) => m,
            Err(_) => continue,
        };
        let mtime = meta
            .modified()
            .ok()
            .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
            .map(|d| d.as_secs())
            .unwrap_or(0);

        let resp = match receipt_response_json(&run_id) {
            Some(v) => v,
            None => continue,
        };
        // Skip queued stubs (no manifest).
        if resp.get("manifest").is_none() {
            continue;
        }
        let goal_id = get_goal_id(&resp).unwrap_or_else(|| "unknown".to_string());
        let ok = resp
            .get("manifest")
            .and_then(|m| m.get("evidence"))
            .and_then(|e| e.get("actual_success"))
            .and_then(|v| v.as_bool());
        let view = get_view_url(&resp);
        items.push(Item {
            run_id,
            goal_id,
            ok,
            view,
            mtime,
        });
    }

    // Keep most recent.
    items.sort_by(|a, b| b.mtime.cmp(&a.mtime).then_with(|| b.run_id.cmp(&a.run_id)));
    items.truncate(limit);
    // Chronological order for edges.
    items.sort_by(|a, b| a.mtime.cmp(&b.mtime).then_with(|| a.run_id.cmp(&b.run_id)));

    let out_dir = root.join("runs").join("graphs").join(external_run_id);
    fs::create_dir_all(&out_dir).with_context(|| format!("mkdir {}", out_dir.display()))?;

    // DOT
    let mut dot = String::from(
        "digraph receipts {\nrankdir=TB;\nnode [shape=box, style=\"rounded,filled\", fontname=\"Helvetica\"];\n",
    );
    for (i, it) in items.iter().enumerate() {
        let ok = it.ok.map(|b| if b { "ok" } else { "fail" }).unwrap_or("?");
        let fill = if it.ok == Some(true) {
            "#ebfbee"
        } else if it.ok == Some(false) {
            "#fff5f5"
        } else {
            "#f1f3f5"
        };
        let label = format!("{}\\n{}\\n{}", it.goal_id, it.run_id, ok).replace('"', "\\\"");
        dot.push_str(&format!("  n{} [label=\"{}\", fillcolor=\"{}\"];\n", i, label, fill));
    }
    for i in 0..items.len().saturating_sub(1) {
        dot.push_str(&format!("  n{} -> n{};\n", i, i + 1));
    }
    dot.push_str("}\n");
    fs::write(out_dir.join("graph.dot"), dot.as_bytes())
        .with_context(|| "write graph.dot".to_string())?;

    // events.json
    let events_json = serde_json::json!({
        "kind": "receipts",
        "limit": limit,
        "items": items.iter().map(|it| {
            serde_json::json!({
                "run_id": it.run_id,
                "goal_id": it.goal_id,
                "actual_success": it.ok,
                "view_url": it.view,
                "receipt_url": format!("/runs/receipts/{}/RECEIPT.md", it.run_id),
                "mtime_s": it.mtime,
            })
        }).collect::<Vec<_>>()
    });
    fs::write(
        out_dir.join("events.json"),
        serde_json::to_string_pretty(&events_json).unwrap_or_default(),
    )
    .with_context(|| "write events.json".to_string())?;

    // index.html list
    let mut items_html = String::new();
    for it in &items {
        let ok = it.ok.map(|b| if b { "ok" } else { "fail" }).unwrap_or("?");
        let receipt = format!("/runs/receipts/{}/RECEIPT.md", it.run_id);
        let view = it.view.clone().unwrap_or_default();
        let text = format!("{} · {}", it.goal_id, it.run_id);
        let data_t = format!("{} {}", it.goal_id, it.run_id);
        items_html.push_str(&format!(
            "<li data-t=\"{}\"><a href=\"{}\" target=\"_blank\" rel=\"noreferrer\">{}</a><span class=\"pill\">{}</span>{}</li>\n",
            html_escape(&data_t),
            html_escape(&receipt),
            html_escape(&text),
            html_escape(ok),
            if view.is_empty() {
                "".to_string()
            } else {
                format!(" <a class=\"pill\" href=\"{}\" target=\"_blank\" rel=\"noreferrer\">view</a>", html_escape(&view))
            }
        ));
    }

    let html = index_html_receipts(
        external_run_id,
        items.len(),
        items.len().saturating_sub(1),
        &items_html,
    );
    fs::write(out_dir.join("index.html"), html.as_bytes())
        .with_context(|| "write index.html".to_string())?;

    Ok(ReceiptsGraphResult {
        out_dir,
        nodes: items.len(),
        edges: items.len().saturating_sub(1),
    })
}
