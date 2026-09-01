//! Which language features actually work (LANG-09, LANG-11, LANG-13, LANG-16,
//! NDS-11).
//!
//! Five requirements filed under CH-TCK that the TCK cannot answer. The TCK
//! measures conformance to *its* scenarios, and none of these are in it:
//! `LOAD CSV` is not a TCK feature, nor is `ANALYZE`, nor index DDL. So all
//! five sat unmeasured under a suite that runs every release — which reads as
//! "not scheduled" and was really "the suite that owns it cannot see it".
//!
//! Each requirement gets several probes rather than one, because "does temporal
//! work" is not a yes/no: `date()` parsing and `datetime + duration` arithmetic
//! and comparison are three different things and a single probe would report
//! whichever one it happened to pick.
//!
//! The corpus is written from **what the requirement asks for**, not from what
//! the engine has. A probe list derived from the implementation would report
//! 100% of whatever we already do.
//!
//!     cargo run --release --example language_features -- --json out.json

use samyama::graph::{GraphStore, Label, PropertyValue};
use samyama::query::executor::{MutQueryExecutor, QueryExecutor};
use samyama::query::parser::parse_query;

/// `(requirement, what it is, query)`.
const PROBES: &[(&str, &str, &str)] = &[
    // LANG-09 — LOAD CSV and a native bulk-ingest clause.
    ("LANG-09", "LOAD CSV from a file URL",
     "LOAD CSV FROM \"file:///tmp/probe.csv\" AS row RETURN row"),
    ("LANG-09", "LOAD CSV WITH HEADERS",
     "LOAD CSV WITH HEADERS FROM \"file:///tmp/probe.csv\" AS row RETURN row.a"),
    ("LANG-09", "LOAD CSV with a field terminator",
     "LOAD CSV FROM \"file:///tmp/probe.csv\" AS row FIELDTERMINATOR \";\" RETURN row"),

    // LANG-11 — CALL … YIELD composed inside a larger query.
    ("LANG-11", "CALL after MATCH, yielding into the same scope",
     "MATCH (n:N) CALL algo.pageRank() YIELD node, score RETURN n.x, score LIMIT 1"),
    ("LANG-11", "CALL then WITH then filter",
     "CALL algo.pageRank() YIELD node, score WITH node, score WHERE score > 0.0 \
      RETURN count(*)"),
    ("LANG-11", "CALL feeding an aggregation",
     "CALL algo.pageRank() YIELD node, score RETURN sum(score) AS total"),

    // LANG-13 — ANALYZE and query hints.
    ("LANG-13", "ANALYZE to refresh statistics", "ANALYZE"),
    ("LANG-13", "USING INDEX hint", "MATCH (n:N) USING INDEX n:N(x) RETURN n"),
    ("LANG-13", "USING SCAN hint", "MATCH (n:N) USING SCAN n:N RETURN n"),

    // LANG-16 — temporal semantics: types, arithmetic, comparison, indexing.
    ("LANG-16", "date literal", "RETURN date(\"2024-01-01\") AS d"),
    ("LANG-16", "datetime literal", "RETURN datetime(\"2024-01-01T00:00:00Z\") AS d"),
    ("LANG-16", "duration literal", "RETURN duration({days: 1}) AS d"),
    ("LANG-16", "datetime + duration arithmetic",
     "RETURN datetime(\"2024-01-01T00:00:00Z\") + duration({days: 1}) AS d"),
    ("LANG-16", "date comparison",
     "RETURN date(\"2024-01-02\") > date(\"2024-01-01\") AS later"),
    ("LANG-16", "duration between two datetimes",
     "RETURN duration.between(date(\"2024-01-01\"), date(\"2024-03-01\")) AS gap"),
    ("LANG-16", "temporal component access",
     "RETURN date(\"2024-05-06\").year AS y"),
    ("LANG-16", "temporal truncation",
     "RETURN date.truncate(\"month\", date(\"2024-05-06\")) AS m"),
    ("LANG-16", "localtime type", "RETURN localtime(\"12:00:00\") AS t"),

    // NDS-11 — declarative DDL for every index kind, and SHOW INDEXES.
    ("NDS-11", "SHOW INDEXES", "SHOW INDEXES"),
    ("NDS-11", "CREATE INDEX (range/btree)", "CREATE INDEX ON :N(x)"),
    ("NDS-11", "CREATE TEXT INDEX", "CREATE TEXT INDEX ON :N(x)"),
    ("NDS-11", "CREATE FULLTEXT INDEX", "CREATE FULLTEXT INDEX ON :N(x)"),
    // The vector form is Neo4j's `FOR (n:L) ON (n.prop)`, not the `ON :L(prop)`
    // shape the plain index uses. Probed in the grammar's own spelling: a probe
    // that guesses a syntax reports the guess, not the engine. NDS-11 asks for
    // *uniform* DDL across kinds, and the two spellings diverging is itself
    // part of what it is asking about -- recorded below as its own probe.
    ("NDS-11", "CREATE VECTOR INDEX (FOR … ON … form)",
     "CREATE VECTOR INDEX vidx FOR (n:N) ON (n.embedding) OPTIONS {dimensions: 3}"),
    ("NDS-11", "CREATE VECTOR INDEX in the plain `ON :L(prop)` form",
     "CREATE VECTOR INDEX ON :N(embedding)"),
    ("NDS-11", "CREATE POINT INDEX", "CREATE POINT INDEX ON :N(x)"),
    // Creates the index first: `DROP` on a fresh store is refused for the
    // absence of the index, not for the absence of the feature, and the
    // per-probe isolation that keeps `CREATE` honest makes `DROP` a false
    // negative unless it sets up its own precondition.
    ("NDS-11", "DROP INDEX", "CREATE INDEX ON :N(x); DROP INDEX ON :N(x)"),
];

/// One labelled node with an indexable property.
fn fixture() -> GraphStore {
    let mut s = GraphStore::new();
    let n = s.create_node_with_labels([Label::new("N")]);
    s.set_node_property("default", n, "x", PropertyValue::Integer(1)).unwrap();
    s
}

fn main() {
    let store = fixture();
    // A real CSV, so `LOAD CSV` fails for the reason that matters rather than
    // because the file is missing. A probe whose fixture makes the feature
    // untriggerable measures the fixture.
    let _ = std::fs::write("/tmp/probe.csv", "a,b\n1,2\n");

    let mut rows = Vec::new();
    for (req, what, cypher) in PROBES {
        // The mutating executor, so DDL is measured on the same footing as a
        // read. Under the read executor every `CREATE INDEX` fails with
        // "requires write access", which is a fact about the probe.
        // A probe may be two statements separated by `;` -- `DROP INDEX` needs
        // its index to exist. Run in order against one store; the first
        // failure is the answer.
        let mut s2 = fixture();
        let mut outcome = String::new();
        for part in cypher.split(';').map(str::trim).filter(|p| !p.is_empty()) {
            outcome = match parse_query(part) {
                Err(e) => format!("parse: {}", e.to_string().lines().next().unwrap_or("")),
                Ok(q) => match MutQueryExecutor::new(&mut s2, "default".to_string()).execute(&q) {
                    Ok(_) => String::new(),
                    Err(e) => format!("exec: {}", e.to_string().lines().next().unwrap_or("")),
                },
            };
            if !outcome.is_empty() { break; }
        }
        let works = outcome.is_empty();
        rows.push(serde_json::json!({
            "requirement": req,
            "feature": what,
            "query": cypher,
            "works": works,
            "error": if works { None } else { Some(outcome.chars().take(160).collect::<String>()) },
        }));
    }

    let mut by_req: std::collections::BTreeMap<&str, (usize, usize, Vec<&str>)> =
        std::collections::BTreeMap::new();
    for (i, (req, what, _)) in PROBES.iter().enumerate() {
        let e = by_req.entry(req).or_insert((0, 0, Vec::new()));
        e.1 += 1;
        if rows[i]["works"] == true { e.0 += 1 } else { e.2.push(what) }
    }

    let json = serde_json::json!({
        "probed": PROBES.len(),
        "working": rows.iter().filter(|r| r["works"] == true).count(),
        "by_requirement": by_req.iter().map(|(k, (ok, total, missing))| {
            (k.to_string(), serde_json::json!({
                "working": ok, "probed": total, "missing": missing,
            }))
        }).collect::<std::collections::BTreeMap<_, _>>(),
        "detail": rows,
    });
    let args: Vec<String> = std::env::args().collect();
    let text = serde_json::to_string_pretty(&json).unwrap();
    match args.iter().position(|a| a == "--json").and_then(|i| args.get(i + 1)) {
        Some(p) => std::fs::write(p, &text).unwrap(),
        None => println!("{text}"),
    }
    for (req, (ok, total, missing)) in &by_req {
        eprintln!("{req}: {ok}/{total}{}", if missing.is_empty() {
            String::new()
        } else {
            format!("  missing: {}", missing.join(", "))
        });
    }
    // Silence the unused-import warning when the read executor is not used.
    let _ = QueryExecutor::new(&store);
}
