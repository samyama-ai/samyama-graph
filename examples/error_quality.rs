//! Do our errors carry a code and a span? (LANG-12)
//!
//! LANG-12's H1 target is *"codes + spans on 100%"* — every error carrying a
//! machine-readable code and the offending span of the query text. Nothing
//! measured it, so this does, by provoking errors and inspecting them.
//!
//! Two properties, checked independently:
//!
//! * **code** — a stable machine-readable identifier a client can branch on.
//!   The variant name (`TypeError`, `SemanticError`) is *not* one: it is a
//!   Rust type, it is not published, and it does not distinguish
//!   `TypeError("x is not a node")` from `TypeError("Add requires numeric
//!   operands")` — two different faults a caller would handle differently.
//! * **span** — a position or range in the query text. `pest` produces one
//!   for a grammar error; nothing else does.
//!
//! The corpus is written by fault class rather than by what the engine
//! happens to produce, so the classes we handle badly are visible rather than
//! absent.

use samyama::graph::{GraphStore, Label};
use samyama::query::executor::QueryExecutor;
use samyama::query::parser::parse_query;

/// (class, query) — one representative of each fault a user actually hits.
const CASES: &[(&str, &str)] = &[
    ("syntax", "MATCH (n RETURN n"),
    ("syntax", "RETRUN 1"),
    ("syntax", "MATCH (n) RETURN"),
    ("unknown-function", "RETURN lenght('abc')"),
    ("unknown-procedure", "CALL nosuch.procedure()"),
    // `algo.betweenness` was the case here and it stopped erroring: betweenness
    // centrality was implemented for ALGO-01, so the probe was asserting the
    // absence of something we now ship. A corpus written against today's gaps
    // decays into a corpus asserting yesterday's -- and this one reported it
    // honestly, as a probe that did not error, rather than quietly passing.
    ("unknown-algorithm", "CALL algo.noSuchAlgorithmExists()"),
    ("unbound-variable", "RETURN x"),
    ("unbound-variable", "MATCH (n) RETURN m.name"),
    ("type-error", "RETURN 1 + {a: 1}"),
    // Arithmetic on an *entity*, which stays an error by design: `+` on a
    // string and a list is not one -- Cypher prepends, and it was the wrong
    // case to probe with (it "did not error" correctly, twice).
    ("type-error", "MATCH (n) RETURN n + 1"),
    ("bad-literal", "RETURN '\\uZZ'"),
    ("bad-argument", "RETURN range(1, 10, 0)"),
    ("bad-argument", "RETURN substring('abc')"),
    ("collection-in-pattern", "WITH [1] AS xs MATCH (xs)-->() RETURN 1"),
    // `RETURN count(*) + n` was the case here, and it does not test what it
    // says: it fails because `n` is unbound, which is correct and has nothing
    // to do with aggregation. It reported an aggregate-misuse code identical
    // to unbound-variable's -- a collision that looked like a coding gap and
    // was a corpus bug. Bind the variable so the aggregate is the only fault
    // left.
    ("aggregate-misuse", "MATCH (n) WHERE count(*) > 1 RETURN n"),
    ("write-in-read", "MATCH (n) DELETE n RETURN n"),
];

/// Does the text carry something a client could branch on that is not just
/// English prose? A code is a token like `Neo.ClientError.Statement.SyntaxError`
/// or `SG-1042` — stable, greppable, documented.
/// The code itself, if there is one, so distinctness can be checked.
///
/// Presence is the easy half of LANG-12 and the half that can be satisfied
/// without helping anyone: give every error the same code and 100% of them
/// "carry a code". The requirement says a caller can branch on it, and a
/// constant is not something to branch on -- which is exactly the objection
/// the scorecard already records against using the Rust variant name.
///
/// So the probe reads the code out and reports how many *distinct* ones the
/// fault classes produce. That number is the one that cannot be gamed by
/// adding a prefix.
fn extract_code(msg: &str) -> Option<String> {
    msg.split_whitespace()
        .map(|t| t.trim_matches(|c: char| !c.is_alphanumeric() && c != '.' && c != '-'))
        .find(|t| {
            (t.contains('.') && t.split('.').count() >= 3
                && t.chars().next().is_some_and(|c| c.is_ascii_uppercase()))
                || (t.contains('-') && t.split('-').next().is_some_and(
                    |p| p.len() >= 2 && p.chars().all(|c| c.is_ascii_uppercase())))
        })
        .map(str::to_string)
}

fn has_code(msg: &str) -> bool {
    // Deliberately generous: any bracketed or dotted uppercase token, or an
    // explicit `code` field, counts. Being generous matters — a strict test
    // that found nothing would be indistinguishable from a broken detector.
    msg.split_whitespace().any(|t| {
        let t = t.trim_matches(|c: char| !c.is_alphanumeric() && c != '.' && c != '-');
        (t.contains('.') && t.split('.').count() >= 3
            && t.chars().next().is_some_and(|c| c.is_ascii_uppercase()))
            || (t.contains('-') && t.split('-').next().is_some_and(
                |p| p.len() >= 2 && p.chars().all(|c| c.is_ascii_uppercase())))
    }) || msg.to_lowercase().contains("code:")
}

/// Does it point at a position in the query? A line/column pair, a caret
/// diagram, or an explicit offset.
fn has_span(msg: &str) -> bool {
    let l = msg.to_lowercase();
    l.contains("-->") || l.contains("^") || l.contains("line ") || l.contains("column ")
        || l.contains("offset") || l.contains("position")
}

fn main() {
    let mut store = GraphStore::new();
    let n = store.create_node_with_labels([Label::new("N")]);
    // A real property, so `n.name + [1,2]` is String + List rather than
    // null + List. The first attempt left it unset, and the case "did not
    // error" -- correctly, because `null + anything` is null in Cypher. A
    // probe whose fixture makes the fault untriggerable measures nothing.
    store.set_node_property("default", n, "name",
                            samyama::graph::PropertyValue::String("a".into())).unwrap();

    let mut rows = Vec::new();
    for (class, q) in CASES {
        let msg = match parse_query(q) {
            Err(e) => format!("{e}"),
            Ok(p) => match QueryExecutor::new(&store).execute(&p) {
                Err(e) => format!("{e}"),
                // A query we expected to fail and which succeeded is a
                // finding of its own -- it is the class LANG-03 is about --
                // so it is recorded rather than skipped.
                Ok(_) => String::new(),
            },
        };
        rows.push(serde_json::json!({
            "class": class,
            "query": q,
            "errored": !msg.is_empty(),
            "has_code": !msg.is_empty() && has_code(&msg),
            "code": extract_code(&msg),
            "has_span": !msg.is_empty() && has_span(&msg),
            "message": msg.chars().take(160).collect::<String>(),
        }));
    }

    let errored: Vec<_> = rows.iter().filter(|r| r["errored"] == true).collect();
    let with_code = errored.iter().filter(|r| r["has_code"] == true).count();
    let with_span = errored.iter().filter(|r| r["has_span"] == true).count();
    let did_not_error: Vec<_> = rows.iter()
        .filter(|r| r["errored"] == false)
        .map(|r| r["query"].as_str().unwrap_or("").to_string())
        .collect();

    // Distinctness, per fault class. Two probes of the *same* class sharing a
    // code is right and expected; two different classes sharing one means a
    // caller cannot tell them apart, which is the whole point of the
    // requirement.
    let mut class_codes: std::collections::BTreeMap<String, std::collections::BTreeSet<String>> =
        std::collections::BTreeMap::new();
    for r in &errored {
        if let Some(c) = r["code"].as_str() {
            class_codes
                .entry(r["class"].as_str().unwrap_or("?").to_string())
                .or_default()
                .insert(c.to_string());
        }
    }
    let distinct_codes: std::collections::BTreeSet<&String> =
        class_codes.values().flatten().collect();
    // A class whose code is shared with a different class. Reported by name,
    // because "3 collisions" tells nobody which two faults look alike.
    let mut collisions: Vec<String> = Vec::new();
    for (class, codes) in &class_codes {
        for code in codes {
            let others: Vec<&String> = class_codes
                .iter()
                .filter(|(o, cs)| *o != class && cs.contains(code))
                .map(|(o, _)| o)
                .collect();
            if !others.is_empty() {
                collisions.push(format!("{class} shares {code} with {others:?}"));
            }
        }
    }
    collisions.sort();
    collisions.dedup();

    let json = serde_json::json!({
        "probed": rows.len(),
        "errored": errored.len(),
        "with_code": with_code,
        "with_span": with_span,
        "fault_classes": class_codes.len(),
        "distinct_codes": distinct_codes.len(),
        "code_collisions": collisions,
        "codes_by_class": class_codes,
        "did_not_error": did_not_error,
        "cases": rows,
    });
    let args: Vec<String> = std::env::args().collect();
    let text = serde_json::to_string_pretty(&json).unwrap();
    match args.iter().position(|a| a == "--json").and_then(|i| args.get(i + 1)) {
        Some(p) => std::fs::write(p, &text).unwrap(),
        None => println!("{text}"),
    }
    eprintln!(
        "{} errored; {} carry a code, {} a span; {} distinct codes over {} fault classes \
         (LANG-12 wants 100% of both, and one code per class)",
        errored.len(), with_code, with_span, distinct_codes.len(), class_codes.len()
    );
}
