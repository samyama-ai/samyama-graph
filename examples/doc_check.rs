//! CH-DOC-EXEC — the Cypher in our documentation parses, and the README's runs (DX-04).
//!
//! Documentation drifts silently. A query in a README keeps its syntax
//! highlighting long after the grammar has moved, and the first person to
//! notice is a new user copying it. This walks the repo's markdown, pulls out
//! every fenced `cypher` block, and parses each statement.
//!
//! Parsing is the bar for every document. Most documented queries need a
//! specific graph to return anything, and a query that does not parse is
//! unambiguously broken. Parsing alone is not enough, though: a README example
//! can parse and still fail the moment someone runs it (a vector query whose
//! dimension does not match its index's did exactly that).
//!
//! So a block can name the graph it assumes, in its fence:
//!
//!   ```cypher graph=people
//!
//! The graph is `tests/fixtures/doc-graphs/<name>.cypher`, a few statements that
//! build it. A block that names one is **executed**: a fresh graph is built
//! from the fixture, each statement of the block runs against it in order, every
//! one must succeed, and the last must return at least one row, because a
//! documented query that answers nothing on the graph it declares is not
//! showing what it claims to. GitHub colours the block by its first word only,
//! so the declaration is invisible to a reader.
//!
//! In `README.md` (DX-04's H1 scope, "README + quickstart") every block must
//! name a graph, so a new README example cannot quietly be parse-only. Its
//! quickstart `curl … /api/query -d '{"query": …}'` is executed too, against an
//! empty graph as a fresh server has, and its `columns` and `records` must equal
//! the JSON block the README shows right after it.
//!
//! Blocks can opt out with `cypher,ignore` — for deliberately invalid examples
//! showing what *not* to write. That marker is counted and reported, so opting
//! out stays visible rather than becoming a quiet way to pass.
//!
//!   cargo run --release --example doc_check
//!   cargo run --release --example doc_check -- --json out.json --root .

use samyama::graph::GraphStore;
use samyama::query::executor::record::Value;
use samyama::query::parser::parse_query;
use samyama::query::QueryEngine;
use std::fmt::Write as _;
use std::path::{Path, PathBuf};

/// The documents whose every Cypher block must execute (DX-04's H1 scope).
/// Paths are relative to `--root`.
const MUST_EXECUTE: &[&str] = &["README.md"];

/// Where a block's `graph=<name>` is looked up, relative to `--root`.
const DOC_GRAPHS: &str = "tests/fixtures/doc-graphs";

/// A fenced code block of any language.
struct Fence {
    line: usize,
    /// The info string after the opening backticks, as written.
    info: String,
    body: String,
}

struct Block {
    file: String,
    line: usize,
    query: String,
    ignored: bool,
    graph: Option<String>,
}

/// Every fenced block in a markdown text, with the line its fence opens on.
fn fences(text: &str) -> Vec<Fence> {
    let mut out = Vec::new();
    let mut current: Option<Fence> = None;
    for (i, line) in text.lines().enumerate() {
        let trimmed = line.trim_start();
        if let Some(mut f) = current.take() {
            if trimmed.starts_with("```") {
                out.push(f);
            } else {
                f.body.push_str(line);
                f.body.push('\n');
                current = Some(f);
            }
            continue;
        }
        if let Some(rest) = trimmed.strip_prefix("```") {
            current = Some(Fence { line: i + 1, info: rest.trim().to_string(), body: String::new() });
        }
    }
    out
}

/// The first word of an info string, lowercased: the block's language.
fn language(info: &str) -> String {
    info.split(|c: char| c == ',' || c.is_whitespace())
        .next()
        .unwrap_or("")
        .to_ascii_lowercase()
}

/// Fenced blocks tagged `cypher`, with their `ignore` and `graph=` options.
fn blocks_in(path: &Path, text: &str) -> Vec<Block> {
    fences(text)
        .into_iter()
        .filter(|f| language(&f.info) == "cypher")
        .map(|f| {
            let mut ignored = false;
            let mut graph = None;
            for word in f.info.split(|c: char| c == ',' || c.is_whitespace()).skip(1) {
                if word.eq_ignore_ascii_case("ignore") {
                    ignored = true;
                } else if let Some(name) = word.strip_prefix("graph=") {
                    graph = Some(name.to_string());
                }
            }
            Block { file: path.display().to_string(), line: f.line, query: f.body, ignored, graph }
        })
        .collect()
}

fn walk(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else { return };
    for e in entries.flatten() {
        let p = e.path();
        let name = p.file_name().and_then(|n| n.to_str()).unwrap_or("");
        // Build output, dependencies and vendored checkouts are not our docs.
        if p.is_dir() {
            if matches!(name, "target" | ".git" | "node_modules" | ".harness-cache" | "data") {
                continue;
            }
            walk(&p, out);
        } else if name.ends_with(".md") {
            out.push(p);
        }
    }
}

/// Whether a line is a comment. Cypher's own comment marker is `//`, but our
/// documentation also uses `--` and `#` to annotate example blocks. Treating
/// those as queries produced a dozen "failures" that were prose, which is the
/// fastest way to get a check like this ignored.
fn is_comment(line: &str) -> bool {
    let t = line.trim_start();
    t.starts_with("//") || t.starts_with("--") || t.starts_with('#')
}

/// Split a block into statements. A documented block often shows several
/// queries separated by blank lines; each is parsed on its own, because one
/// failure should name one query. Comment lines are stripped rather than
/// skipped-if-all-comments, so an annotated query is still checked.
fn statements(block: &str) -> Vec<String> {
    block
        .split("\n\n")
        .map(|chunk| {
            chunk
                .lines()
                .filter(|l| !is_comment(l))
                .collect::<Vec<_>>()
                .join("\n")
                .trim()
                .trim_end_matches(';')
                .trim()
                .to_string()
        })
        .filter(|s| !s.is_empty())
        .collect()
}

fn first_line(s: &str) -> String {
    s.lines().next().unwrap_or("").chars().take(90).collect()
}

/// Build the named fixture graph in a fresh store.
fn fixture(root: &Path, name: &str, engine: &QueryEngine) -> Result<GraphStore, String> {
    // A name is a file stem, never a path: `graph=../x` must not reach outside.
    if name.is_empty() || !name.chars().all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_') {
        return Err(format!("`graph={name}` is not a fixture name (letters, digits, `-`, `_`)"));
    }
    let path = root.join(DOC_GRAPHS).join(format!("{name}.cypher"));
    let text = std::fs::read_to_string(&path)
        .map_err(|_| format!("declares `graph={name}`, but {DOC_GRAPHS}/{name}.cypher does not exist"))?;
    let mut store = GraphStore::new();
    for stmt in statements(&text) {
        engine
            .execute_mut(&stmt, &mut store, "default")
            .map_err(|e| format!("fixture `{name}` failed at `{}`: {}", first_line(&stmt), first_line(&e.to_string())))?;
    }
    Ok(store)
}

/// Run a block against its graph. `Ok` is the row count of its last statement.
fn execute_block(root: &Path, b: &Block, graph: &str) -> Result<usize, (String, String)> {
    let engine = QueryEngine::new();
    let mut store = fixture(root, graph, &engine).map_err(|e| (String::new(), e))?;
    let mut last = 0;
    for stmt in statements(&b.query) {
        let rows = engine
            .execute_mut(&stmt, &mut store, "default")
            .map_err(|e| (first_line(&stmt), first_line(&e.to_string())))?;
        last = rows.len();
    }
    if last == 0 {
        return Err((
            first_line(statements(&b.query).last().map(String::as_str).unwrap_or("")),
            format!("returns no rows on the graph it declares (`{graph}`)"),
        ));
    }
    Ok(last)
}

/// A cell as the HTTP API writes it. Only what a quickstart shows is needed;
/// anything else is reported rather than guessed at.
fn cell_json(v: &Value) -> Result<serde_json::Value, String> {
    match v {
        Value::Property(p) => Ok(p.to_json()),
        Value::Null => Ok(serde_json::Value::Null),
        other => Err(format!("returns a {other:?} cell, which this check does not compare")),
    }
}

/// The body of a `curl … /api/query … -d '<json>'` command, if the block has one.
fn curl_query(body: &str) -> Option<String> {
    if !body.contains("/api/query") {
        return None;
    }
    let start = body.find("-d '")? + 4;
    let len = body[start..].find('\'')?;
    Some(body[start..start + len].to_string())
}

/// Execute each quickstart request in a document and compare it with the JSON
/// response shown after it. Returns the requests found, how many passed, and
/// the failures.
fn check_quickstart(file: &str, text: &str) -> (usize, usize, Vec<(String, usize, String, String)>) {
    let mut found = 0;
    let mut passed = 0;
    let mut failed = Vec::new();
    let fs = fences(text);
    for (i, f) in fs.iter().enumerate() {
        if !matches!(language(&f.info).as_str(), "bash" | "sh" | "shell" | "console") {
            continue;
        }
        let Some(request) = curl_query(&f.body) else { continue };
        found += 1;
        match run_request(&request, fs.get(i + 1).filter(|n| language(&n.info) == "json")) {
            Ok(()) => passed += 1,
            Err((q, e)) => failed.push((file.to_string(), f.line, first_line(&q), e)),
        }
    }
    (found, passed, failed)
}

/// One quickstart request: run its query on an empty graph, as a fresh server
/// has, and when the README shows the response, require the same columns and
/// records.
fn run_request(request: &str, shown: Option<&Fence>) -> Result<(), (String, String)> {
    let request: serde_json::Value = serde_json::from_str(request)
        .map_err(|e| (request.to_string(), format!("the request body is not JSON: {e}")))?;
    let query = request
        .get("query")
        .and_then(|q| q.as_str())
        .ok_or_else(|| (String::new(), "the request body has no \"query\"".to_string()))?;
    let fail = |e: String| (query.to_string(), e);
    let engine = QueryEngine::new();
    let mut store = GraphStore::new();
    let batch = engine
        .execute_mut(query, &mut store, "default")
        .map_err(|e| fail(first_line(&e.to_string())))?;
    let Some(shown) = shown else { return Ok(()) };
    let shown: serde_json::Value =
        serde_json::from_str(&shown.body).map_err(|e| fail(format!("the response shown is not JSON: {e}")))?;
    let mut records = Vec::new();
    for r in &batch.records {
        let row = batch
            .columns
            .iter()
            .map(|c| r.get(c).map(cell_json).unwrap_or(Ok(serde_json::Value::Null)))
            .collect::<Result<Vec<_>, _>>()
            .map_err(fail)?;
        records.push(serde_json::Value::Array(row));
    }
    let got_columns = serde_json::json!(batch.columns);
    let got_records = serde_json::Value::Array(records);
    if shown.get("columns") != Some(&got_columns) || shown.get("records") != Some(&got_records) {
        return Err(fail(format!(
            "returns columns {got_columns} records {got_records}, but the README shows columns {} records {}",
            shown.get("columns").cloned().unwrap_or_default(),
            shown.get("records").cloned().unwrap_or_default()
        )));
    }
    Ok(())
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let arg = |flag: &str| -> Option<String> {
        args.iter().position(|a| a == flag).and_then(|i| args.get(i + 1)).cloned()
    };
    let root = arg("--root").unwrap_or_else(|| ".".into());
    let root = Path::new(&root);

    let mut files = Vec::new();
    walk(root, &mut files);
    files.sort();

    let mut checked = 0usize;
    let mut failed: Vec<(String, usize, String, String)> = Vec::new();
    let mut ignored = 0usize;
    let mut docs_with_cypher = 0usize;
    // Execution: every statement of a block that names a graph, and every
    // quickstart request. `in_scope` counts the statements DX-04 requires run.
    let mut executed = 0usize;
    let mut exec_failed: Vec<(String, usize, String, String)> = Vec::new();
    let mut in_scope = 0usize;
    let mut in_scope_executed = 0usize;
    let mut quickstart_requests = 0usize;

    for file in &files {
        let Ok(text) = std::fs::read_to_string(file) else { continue };
        let rel = file.strip_prefix(root).unwrap_or(file).to_string_lossy().replace('\\', "/");
        let must_execute = MUST_EXECUTE.contains(&rel.as_str());
        let blocks = blocks_in(file, &text);
        if !blocks.is_empty() {
            docs_with_cypher += 1;
        }
        for b in blocks {
            if b.ignored {
                ignored += 1;
                continue;
            }
            let stmts = statements(&b.query);
            let mut parsed = true;
            for stmt in &stmts {
                checked += 1;
                if let Err(e) = parse_query(stmt) {
                    parsed = false;
                    let short: String = e.to_string().lines().next().unwrap_or("").chars().take(90).collect();
                    failed.push((b.file.clone(), b.line, first_line(stmt), short));
                }
            }
            if must_execute {
                in_scope += stmts.len();
            }
            match &b.graph {
                // A parse failure is already reported; running it adds nothing.
                Some(_) if !parsed => {}
                Some(graph) => match execute_block(root, &b, graph) {
                    Ok(_) => {
                        executed += stmts.len();
                        if must_execute {
                            in_scope_executed += stmts.len();
                        }
                    }
                    Err((q, e)) => exec_failed.push((b.file.clone(), b.line, q, e)),
                },
                None if must_execute => exec_failed.push((
                    b.file.clone(),
                    b.line,
                    first_line(stmts.first().map(String::as_str).unwrap_or("")),
                    format!("declares no graph; {rel} examples must run (```cypher graph=<name>, see {DOC_GRAPHS})"),
                )),
                None => {}
            }
        }
        if must_execute {
            let (found, passed, f) = check_quickstart(&file.display().to_string(), &text);
            quickstart_requests += found;
            in_scope += found;
            in_scope_executed += passed;
            executed += passed;
            exec_failed.extend(f);
        }
    }

    println!("CH-DOC-EXEC — Cypher in documentation");
    println!("{}", "=".repeat(78));
    println!("markdown files scanned : {}", files.len());
    println!("files containing cypher: {docs_with_cypher}");
    println!("statements parsed      : {checked}");
    println!("blocks marked ignore   : {ignored}");
    println!("parse failures         : {}", failed.len());
    println!("statements executed    : {executed} (graphs in {DOC_GRAPHS})");
    println!(
        "{:<23}: {in_scope_executed} of {in_scope} executed, including {quickstart_requests} quickstart request(s)",
        MUST_EXECUTE.join(" + ")
    );
    println!("execution failures     : {}", exec_failed.len());
    for (title, list) in [("Parse failures", &failed), ("Execution failures", &exec_failed)] {
        if list.is_empty() {
            continue;
        }
        println!("\n{title}:");
        for (f, l, q, e) in list.iter().take(40) {
            println!("  {f}:{l}\n      {q}\n      {e}");
        }
        if list.len() > 40 {
            println!("  … and {} more", list.len() - 40);
        }
    }

    if let Some(path) = arg("--json") {
        let commit = std::process::Command::new("git")
            .args(["rev-parse", "--short", "HEAD"])
            .output()
            .ok()
            .filter(|o| o.status.success())
            .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
            .unwrap_or_else(|| "unknown".into());
        let esc = |s: &str| s.replace('\\', "\\\\").replace('"', "\\\"");
        let mut cases = String::new();
        let all = failed.iter().map(|c| ("parse", c)).chain(exec_failed.iter().map(|c| ("execute", c)));
        for (kind, (f, l, q, e)) in all.take(200) {
            if !cases.is_empty() {
                cases.push_str(",\n      ");
            }
            let _ = write!(
                cases,
                "{{\"kind\": \"{kind}\", \"file\": \"{}\", \"line\": {l}, \"query\": \"{}\", \"error\": \"{}\"}}",
                esc(f), esc(q), esc(e)
            );
        }
        let ok = failed.is_empty() && exec_failed.is_empty();
        let envelope = format!(
            "{{
  \"suite\": \"doc-exec\",
  \"requirement_ids\": [\"DX-04\"],
  \"run_id\": \"docexec-{commit}\",
  \"engine\": {{\"name\": \"samyama\", \"version\": \"{}\", \"commit\": \"{commit}\"}},
  \"dataset\": {{\"name\": \"repo-markdown\", \"files\": {}, \"files_with_cypher\": {docs_with_cypher}, \"graphs\": \"{DOC_GRAPHS}\"}},
  \"measurements\": {{\"checked\": {checked}, \"failed\": {}, \"ignored\": {ignored},
    \"executed\": {executed}, \"execution_failed\": {},
    \"statements_executed\": {{\"scope\": \"{}\", \"value\": {in_scope_executed}, \"target\": {in_scope}}},
    \"quickstart_requests\": {quickstart_requests},
    \"cases\": [
      {cases}
  ]}},
  \"status\": \"{}\",
  \"artifacts\": [\"examples/doc_check.rs\", \"{DOC_GRAPHS}\"]
}}
",
            env!("CARGO_PKG_VERSION"),
            files.len(),
            failed.len(),
            exec_failed.len(),
            MUST_EXECUTE.join(" + "),
            if ok { "pass" } else { "fail" }
        );
        std::fs::write(&path, envelope).expect("could not write JSON");
        println!("\nwrote {path}");
    }

    if !failed.is_empty() || !exec_failed.is_empty() {
        std::process::exit(1);
    }
}
