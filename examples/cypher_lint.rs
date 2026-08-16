//! Parse-check every Cypher statement in a file, without needing a database.
//!
//! The KG and demo repositories carry `.cypher` schema and query files that
//! nothing validates (#513). Meanwhile the grammar moved repeatedly this week —
//! chained subscripts, map dot access, leading `FOREACH`, `CALL {}` subqueries.
//! A schema file that stopped parsing would be found by whoever next tried to
//! load that KG, which could be months later and is not how you want to learn.
//!
//! This needs **no data and no engine instance** — it is the parser alone, so it
//! can run on every push in a repository that holds nothing but text.
//!
//!   cargo run --release --example cypher_lint -- FILE...
//!   cargo run --release --example cypher_lint -- --quiet schema/*.cypher
//!
//! Exit code is 0 when every statement parses, 1 otherwise.

use std::path::Path;

use samyama::query::parse_query;

/// Split a file into statements on `;`, dropping `//` line comments.
///
/// Deliberately simple: it does not try to be a lexer. A `;` or `//` inside a
/// string literal would confuse it, which is why a statement that fails here is
/// reported with its text — so a false positive is obvious rather than
/// mysterious.
fn statements(src: &str) -> Vec<(usize, String)> {
    let mut out = Vec::new();
    let mut current = String::new();
    let mut start_line = 1usize;
    let mut line_no = 1usize;

    for line in src.lines() {
        let code = match line.find("//") {
            Some(i) => &line[..i],
            None => line,
        };
        if current.trim().is_empty() && !code.trim().is_empty() {
            start_line = line_no;
        }
        for piece in code.split_inclusive(';') {
            current.push_str(piece);
            if piece.ends_with(';') {
                let stmt = current.trim().trim_end_matches(';').trim().to_string();
                if !stmt.is_empty() {
                    out.push((start_line, stmt));
                }
                current.clear();
                start_line = line_no;
            }
        }
        current.push('\n');
        line_no += 1;
    }
    let tail = current.trim().trim_end_matches(';').trim().to_string();
    if !tail.is_empty() {
        out.push((start_line, tail));
    }
    out
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let quiet = args.iter().any(|a| a == "--quiet");
    // A file with nothing to check is not a file that passed. Three KG schema
    // files are legitimately all-comment prose, so this is a report rather than
    // a failure by default -- but it is never silent, because "ok" on an empty
    // file is how a check quietly stops checking (#449).
    let fail_on_empty = args.iter().any(|a| a == "--fail-on-empty");
    let files: Vec<&String> = args.iter().filter(|a| !a.starts_with("--")).collect();

    if files.is_empty() {
        eprintln!("usage: cypher_lint [--quiet] FILE...");
        eprintln!("       parse-checks every Cypher statement; exits 1 on any failure");
        std::process::exit(2);
    }

    let mut total = 0usize;
    let mut failed = 0usize;
    let mut files_with_failures = 0usize;
    let mut empty_files: Vec<&str> = Vec::new();

    for path in &files {
        let p = Path::new(path.as_str());
        let src = match std::fs::read_to_string(p) {
            Ok(s) => s,
            Err(e) => {
                eprintln!("FAIL  {path}: cannot read: {e}");
                failed += 1;
                files_with_failures += 1;
                continue;
            }
        };

        let stmts = statements(&src);
        let mut file_failures = 0usize;
        for (line, stmt) in &stmts {
            total += 1;
            if let Err(e) = parse_query(stmt) {
                failed += 1;
                file_failures += 1;
                let first = format!("{e}").lines().next().unwrap_or("").trim().to_string();
                eprintln!("FAIL  {path}:{line}");
                eprintln!("      {first}");
                // Echo the statement so a mis-split is visible rather than
                // looking like a grammar bug.
                let preview: String = stmt.chars().take(120).collect();
                eprintln!("      {}{}", preview.replace('\n', " "), if stmt.len() > 120 { " ..." } else { "" });
            }
        }
        if file_failures > 0 {
            files_with_failures += 1;
        } else if stmts.is_empty() {
            empty_files.push(path.as_str());
            println!("none  {path}  (no parseable statements — comments only?)");
        } else if !quiet {
            println!("ok    {path}  ({} statements)", stmts.len());
        }
    }

    println!();
    println!(
        "{} statement(s) in {} file(s): {} parsed, {} failed",
        total,
        files.len(),
        total - failed,
        failed
    );

    if !empty_files.is_empty() {
        println!();
        println!(
            "{} file(s) contained no parseable statements: {}",
            empty_files.len(),
            empty_files.join(", ")
        );
        println!("Those are documentation-only unless something has gone wrong with them.");
        println!("Pass --fail-on-empty to treat that as an error.");
    }

    if failed == 0 && fail_on_empty && !empty_files.is_empty() {
        std::process::exit(1);
    }

    if failed > 0 {
        println!();
        println!("A failure here is one of two things: the grammar no longer accepts");
        println!("something it used to, or the statement was never valid. The echoed");
        println!("text above distinguishes them.");
        std::process::exit(1);
    }
}
