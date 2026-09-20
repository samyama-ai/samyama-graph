//! Point it at your queries; it says which ones this engine accepts (INT-11).
//!
//! `docs/CYPHER_COMPATIBILITY.md` answers "which features exist". That is a
//! different question from "will my queries run", and it is the wrong one for
//! somebody deciding whether to migrate: a matrix of 78 supported features
//! tells you nothing about the twelve queries in your application that use the
//! two that are missing.
//!
//! So this takes a corpus of real queries and reports what is accepted, what
//! is refused, and **why** — grouped by cause, because a migration is planned
//! against causes and not against a list of 400 individual failures.
//!
//! ```bash
//! cargo run --release --example compatibility_report -- --queries my_queries.cypher
//! cargo run --release --example compatibility_report -- --queries q.cypher --json report.json
//! ```
//!
//! Input is one file. Queries are separated by a line containing only `;`, or
//! by a blank line. A line starting with `//` or `#` is a comment. A `.json`
//! file is read as an array of strings instead, which is the shape a query log
//! usually exports in.
//!
//! # What "accepted" means, and what it does not
//!
//! A query is **accepted** when it parses and a plan can be built for it. That
//! is a statement about the language, not about your data and not about the
//! answer: an accepted query can still return the wrong rows, and this tool
//! would not know. It is the strongest thing that can be said without your
//! database, and saying more would be a guess wearing a percentage.
//!
//! Planning happens against an empty store, so a refusal that depends on your
//! schema — a declared index, a registered procedure — is reported as a
//! refusal here and may not be one for you. Those are called out separately
//! rather than folded into the total, because the difference matters to
//! somebody sizing a migration.

use std::collections::BTreeMap;

use samyama::graph::GraphStore;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let Some(path) = arg(&args, "--queries") else {
        eprintln!(
            "usage: compatibility_report --queries <file> [--json <out>] [--fail-on-refused]\n\
             \n\
             The file holds Cypher queries separated by a line containing only `;`, or by\n\
             a blank line. A `.json` file is read as an array of strings."
        );
        std::process::exit(2);
    };
    let json_out = arg(&args, "--json");
    let fail_on_refused = args.iter().any(|a| a == "--fail-on-refused");

    let text = std::fs::read_to_string(&path).unwrap_or_else(|e| {
        eprintln!("cannot read {path}: {e}");
        std::process::exit(2);
    });
    let queries = if path.ends_with(".json") {
        parse_json_array(&text).unwrap_or_else(|| {
            eprintln!("{path} is not a JSON array of strings");
            std::process::exit(2);
        })
    } else {
        samyama::compat::split_queries(&text)
    };

    if queries.is_empty() {
        eprintln!("{path} holds no queries");
        std::process::exit(2);
    }

    let store = GraphStore::new();
    let verdicts = samyama::compat::judge_all(&queries, &store);
    let accepted = verdicts.iter().filter(|v| v.accepted()).count();

    // Grouped by cause. A migration is planned against causes; a list of 400
    // individual failures is the same information in a form nobody can act on.
    let mut by_cause: BTreeMap<(String, String), Vec<&str>> = BTreeMap::new();
    for v in &verdicts {
        if let Some(r) = &v.refusal {
            by_cause
                .entry((r.short_code().to_string(), r.cause()))
                .or_default()
                .push(&v.query);
        }
    }
    let mut causes: Vec<_> = by_cause.into_iter().collect();
    causes.sort_by(|a, b| b.1.len().cmp(&a.1.len()));

    let refused = verdicts.len() - accepted;
    let total = queries.len();
    let pct = 100.0 * accepted as f64 / total as f64;
    println!("Cypher compatibility report — {path}");
    println!("{}", "-".repeat(72));
    println!("  queries read   {total}");
    println!("  accepted       {accepted}  ({pct:.1}%)");
    println!("  refused        {refused}");
    println!();
    if causes.is_empty() {
        println!("  Every query was accepted.");
    } else {
        println!("  Refusals by cause, commonest first:");
        for ((code, cause), qs) in &causes {
            println!("    {:>4}x  {}  {}", qs.len(), code, truncate(cause, 84));
            println!("           e.g. {}", truncate(qs[0], 90));
        }
    }
    println!();
    println!(
        "  \"Accepted\" means it parses and plans. It is not a statement about your data or\n  \
         about the answer: an accepted query can still return the wrong rows, and this\n  \
         tool would not know. Planning runs against an empty store, so a refusal that\n  \
         depends on a declared index or a registered procedure may not be one for you."
    );

    if let Some(out) = json_out {
        let rows: Vec<String> = causes
            .iter()
            .map(|((code, cause), qs)| {
                format!(
                    "{{\"code\": {}, \"cause\": {}, \"count\": {}, \"example\": {}}}",
                    json_string(code),
                    json_string(cause),
                    qs.len(),
                    json_string(qs[0])
                )
            })
            .collect();
        let json = format!(
            "{{\n  \"corpus\": {},\n  \"queries\": {total},\n  \"accepted\": {accepted},\n  \
             \"refused\": {refused},\n  \"accepted_fraction\": {:.4},\n  \"causes\": [{}]\n}}\n",
            json_string(&path),
            accepted as f64 / total as f64,
            rows.join(", ")
        );
        std::fs::write(&out, json).expect("write json");
        eprintln!("[compat] wrote {out}");
    }

    if fail_on_refused && refused > 0 {
        std::process::exit(1);
    }
}






fn truncate(s: &str, n: usize) -> String {
    let flat = s.split_whitespace().collect::<Vec<_>>().join(" ");
    if flat.chars().count() <= n {
        flat
    } else {
        let head: String = flat.chars().take(n - 1).collect();
        format!("{head}…")
    }
}



/// A JSON array of strings, without pulling the query log through serde's
/// derive machinery — the shape is fixed and the file is the user's.
fn parse_json_array(text: &str) -> Option<Vec<String>> {
    let value: serde_json::Value = serde_json::from_str(text).ok()?;
    let items = value.as_array()?;
    Some(
        items
            .iter()
            .filter_map(|v| v.as_str().map(|s| s.to_string()))
            .collect(),
    )
}

fn json_string(s: &str) -> String {
    serde_json::to_string(s).unwrap_or_else(|_| "\"\"".to_string())
}

fn arg(args: &[String], flag: &str) -> Option<String> {
    args.iter()
        .position(|a| a == flag)
        .and_then(|i| args.get(i + 1))
        .cloned()
}
