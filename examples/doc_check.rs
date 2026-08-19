//! CH-DOC-EXEC — the Cypher in our documentation actually parses (DX-04).
//!
//! Documentation drifts silently. A query in a README keeps its syntax
//! highlighting long after the grammar has moved, and the first person to
//! notice is a new user copying it. This walks the repo's markdown, pulls out
//! every fenced `cypher` block, and parses each statement.
//!
//! Parsing, not executing, is the bar — deliberately. Most documented queries
//! need a specific graph to return anything, and demanding one would mean
//! maintaining a fixture per document, which is how executable-documentation
//! efforts die. A query that does not parse is unambiguously broken; a query
//! that parses and returns nothing may be perfectly fine.
//!
//! Blocks can opt out with `cypher,ignore` — for deliberately invalid examples
//! showing what *not* to write. That marker is counted and reported, so opting
//! out stays visible rather than becoming a quiet way to pass.
//!
//!   cargo run --release --example doc_check
//!   cargo run --release --example doc_check -- --json out.json --root .

use samyama::query::parser::parse_query;
use std::fmt::Write as _;
use std::path::{Path, PathBuf};

struct Block {
    file: String,
    line: usize,
    query: String,
    ignored: bool,
}

/// Fenced blocks tagged `cypher` (optionally `cypher,ignore`).
fn blocks_in(path: &Path, text: &str) -> Vec<Block> {
    let mut out = Vec::new();
    let mut current: Option<(usize, bool, String)> = None;
    for (i, line) in text.lines().enumerate() {
        let trimmed = line.trim_start();
        if let Some((start, ignored, body)) = current.take() {
            if trimmed.starts_with("```") {
                out.push(Block {
                    file: path.display().to_string(),
                    line: start + 1,
                    query: body,
                    ignored,
                });
            } else {
                let mut body = body;
                body.push_str(line);
                body.push('\n');
                current = Some((start, ignored, body));
            }
            continue;
        }
        if let Some(rest) = trimmed.strip_prefix("```") {
            let tag = rest.trim().to_ascii_lowercase();
            if tag == "cypher" || tag.starts_with("cypher,") || tag.starts_with("cypher ") {
                current = Some((i, tag.contains("ignore"), String::new()));
            }
        }
    }
    out
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

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let arg = |flag: &str| -> Option<String> {
        args.iter().position(|a| a == flag).and_then(|i| args.get(i + 1)).cloned()
    };
    let root = arg("--root").unwrap_or_else(|| ".".into());

    let mut files = Vec::new();
    walk(Path::new(&root), &mut files);
    files.sort();

    let mut checked = 0usize;
    let mut failed: Vec<(String, usize, String, String)> = Vec::new();
    let mut ignored = 0usize;
    let mut docs_with_cypher = 0usize;

    for file in &files {
        let Ok(text) = std::fs::read_to_string(file) else { continue };
        let blocks = blocks_in(file, &text);
        if !blocks.is_empty() {
            docs_with_cypher += 1;
        }
        for b in blocks {
            if b.ignored {
                ignored += 1;
                continue;
            }
            for stmt in statements(&b.query) {
                checked += 1;
                if let Err(e) = parse_query(&stmt) {
                    let short: String = e.to_string().lines().next().unwrap_or("").chars().take(90).collect();
                    failed.push((b.file.clone(), b.line, stmt.lines().next().unwrap_or("").chars().take(90).collect(), short));
                }
            }
        }
    }

    println!("CH-DOC-EXEC — Cypher in documentation");
    println!("{}", "=".repeat(78));
    println!("markdown files scanned : {}", files.len());
    println!("files containing cypher: {docs_with_cypher}");
    println!("statements parsed      : {checked}");
    println!("blocks marked ignore   : {ignored}");
    println!("failures               : {}", failed.len());
    if !failed.is_empty() {
        println!("\nFailures:");
        for (f, l, q, e) in failed.iter().take(40) {
            println!("  {f}:{l}\n      {q}\n      {e}");
        }
        if failed.len() > 40 {
            println!("  … and {} more", failed.len() - 40);
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
        let mut cases = String::new();
        for (f, l, q, e) in failed.iter().take(200) {
            if !cases.is_empty() {
                cases.push_str(",\n      ");
            }
            let esc = |s: &str| s.replace('\\', "\\\\").replace('"', "\\\"");
            let _ = write!(
                cases,
                "{{\"file\": \"{}\", \"line\": {l}, \"query\": \"{}\", \"error\": \"{}\"}}",
                esc(f), esc(q), esc(e)
            );
        }
        let envelope = format!(
            "{{
  \"suite\": \"doc-exec\",
  \"requirement_ids\": [\"DX-04\"],
  \"run_id\": \"docexec-{commit}\",
  \"engine\": {{\"name\": \"samyama\", \"version\": \"{}\", \"commit\": \"{commit}\"}},
  \"dataset\": {{\"name\": \"repo-markdown\", \"files\": {}, \"files_with_cypher\": {docs_with_cypher}}},
  \"measurements\": {{\"checked\": {checked}, \"failed\": {}, \"ignored\": {ignored}, \"cases\": [
      {cases}
  ]}},
  \"status\": \"{}\",
  \"artifacts\": [\"examples/doc_check.rs\"]
}}
",
            env!("CARGO_PKG_VERSION"),
            files.len(),
            failed.len(),
            if failed.is_empty() { "pass" } else { "fail" }
        );
        std::fs::write(&path, envelope).expect("could not write JSON");
        println!("\nwrote {path}");
    }

    if !failed.is_empty() {
        std::process::exit(1);
    }
}
