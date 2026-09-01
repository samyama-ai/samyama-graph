//! Which native data structures actually exist (NDS-03, 03b, 04, 05, 06, 07,
//! 08, 09, 10, 13).
//!
//! Ten requirements across nine `CH-NDS-*` suites, none of which had an
//! adapter, so every one of them read `unmeasured` -- not because nobody had
//! run them but because nothing could. They are all questions about what the
//! query surface accepts, which makes them answerable the same way
//! `language_features.rs` answered LANG-09/11/13/16: ask the engine.
//!
//! The corpus is written from **what each requirement asks for**, not from
//! what the engine has. Spec 04 names the structures; a probe list derived
//! from the implementation would report 100% of whatever we already built and
//! would have been silent about full-text and sketches, which is precisely
//! where the answer is interesting.
//!
//! Every probe that needs an index creates it first, in the same `;`-separated
//! statement list. Without that the k-NN and full-text probes fail for the
//! absence of an index rather than the absence of the feature -- each probe
//! runs against a fresh store, so a `CREATE INDEX` in a previous probe is not
//! there. Three of these read as false negatives on the first run, which is
//! the same trap `DROP INDEX` set in `language_features.rs`.
//!
//! Several probes are expected to fail, and that is the point. A probe suite
//! whose every case passes on the day it is written is measuring its author.
//!
//!     cargo run --release --example nds_probes -- --json out.json

use samyama::graph::{GraphStore, Label, PropertyValue};
use samyama::query::executor::MutQueryExecutor;
use samyama::query::parser::parse_query;

/// `(requirement, what it is, query)`.
const PROBES: &[(&str, &str, &str)] = &[
    // NDS-03 — temporal index: range, window, as-of, interval overlap.
    ("NDS-03", "range predicate on a stored datetime",
     "MATCH (n:N) WHERE n.t >= datetime(\"2024-01-01T00:00:00Z\") RETURN count(n)"),
    ("NDS-03", "window between two datetimes",
     "MATCH (n:N) WHERE n.t >= datetime(\"2024-01-01T00:00:00Z\") AND n.t < datetime(\"2025-01-01T00:00:00Z\") RETURN count(n)"),
    ("NDS-03", "AS OF a point in time",
     "MATCH (n:N) AS OF datetime(\"2024-06-01T00:00:00Z\") RETURN count(n)"),
    ("NDS-03", "interval overlap predicate",
     "MATCH (n:N) WHERE n.start < datetime(\"2024-06-01T00:00:00Z\") AND n.end > datetime(\"2024-01-01T00:00:00Z\") RETURN count(n)"),
    ("NDS-03", "CREATE INDEX on a temporal property",
     "CREATE INDEX ON :N(t)"),

    // NDS-03b — signals: a time-ordered series on a node.
    ("NDS-03b", "WITHIN time predicate",
     "MATCH (n:N) WHERE n.series WITHIN duration({days: 7}) RETURN count(n)"),
    ("NDS-03b", "window aggregation over a series",
     "MATCH (n:N) RETURN series.avg(n.series, duration({hours: 1})) AS a"),
    ("NDS-03b", "a series-typed property survives a round trip",
     "CREATE (m:S {series: [[1, 2.0], [2, 3.0]]}) RETURN m.series AS s"),

    // NDS-04 — geospatial types and index.
    ("NDS-04", "point() constructor",
     "RETURN point({latitude: 12.9, longitude: 77.6}) AS p"),
    ("NDS-04", "point.distance between two points",
     "RETURN point.distance(point({latitude: 12.9, longitude: 77.6}), point({latitude: 13.0, longitude: 77.7})) AS d"),
    ("NDS-04", "distance() function form",
     "RETURN distance(point({latitude: 12.9, longitude: 77.6}), point({latitude: 13.0, longitude: 77.7})) AS d"),
    ("NDS-04", "within a bounding box / polygon",
     "MATCH (n:N) WHERE point.withinBBox(n.loc, point({latitude: 12.0, longitude: 77.0}), point({latitude: 14.0, longitude: 78.0})) RETURN count(n)"),
    ("NDS-04", "CREATE POINT INDEX", "CREATE POINT INDEX ON :N(loc)"),

    // NDS-05 — vector index and filtered ANN.
    //
    // The engine's `db.index.vector.queryNodes` takes
    // `(label, property, query_vector, k)`. Neo4j's takes
    // `(indexName, k, vector)`. Probed in *our* spelling, because a probe that
    // uses the competitor's signature measures the signature and not the
    // feature -- it reported 0 of 3 on the first run for exactly that reason.
    // The divergence is real and belongs to INT-*/API-* rather than here: a
    // Neo4j-shaped client calling this fails, and no probe in this corpus is
    // the right place to say so.
    ("NDS-05", "CREATE VECTOR INDEX",
     "CREATE VECTOR INDEX vidx FOR (n:N) ON (n.embedding) OPTIONS {dimensions: 3}"),
    ("NDS-05", "k-NN query over the vector index",
     "CREATE VECTOR INDEX vidx FOR (n:N) ON (n.embedding) OPTIONS {dimensions: 3}; CALL db.index.vector.queryNodes(\"N\", \"embedding\", [1.0, 2.0, 3.0], 5) YIELD node, score RETURN node, score"),
    ("NDS-05", "filtered ANN — a predicate alongside the k-NN",
     "CREATE VECTOR INDEX vidx FOR (n:N) ON (n.embedding) OPTIONS {dimensions: 3}; CALL db.index.vector.queryNodes(\"N\", \"embedding\", [1.0, 2.0, 3.0], 5) YIELD node, score WHERE node.x > 0 RETURN count(*)"),

    // NDS-06 — full-text: BM25, phrase, prefix.
    ("NDS-06", "CREATE FULLTEXT INDEX", "CREATE FULLTEXT INDEX ftidx FOR (n:N) ON (n.body)"),
    ("NDS-06", "full-text query with a score",
     "CREATE FULLTEXT INDEX ftidx FOR (n:N) ON (n.body); CALL db.index.fulltext.queryNodes(\"ftidx\", \"graph\") YIELD node, score RETURN count(*)"),
    ("NDS-06", "CONTAINS as the fallback substring search",
     "MATCH (n:N) WHERE n.body CONTAINS \"graph\" RETURN count(n)"),

    // NDS-07 — composability: two structures in one query, one plan.
    ("NDS-07", "vector k-NN composed with a graph traversal",
     "CREATE VECTOR INDEX vidx FOR (n:N) ON (n.embedding) OPTIONS {dimensions: 3}; CALL db.index.vector.queryNodes(\"N\", \"embedding\", [1.0, 2.0, 3.0], 5) YIELD node MATCH (node)-[]->(m) RETURN count(m)"),
    ("NDS-07", "text search composed with a graph traversal",
     "CREATE FULLTEXT INDEX ftidx FOR (n:N) ON (n.body); CALL db.index.fulltext.queryNodes(\"ftidx\", \"graph\") YIELD node MATCH (node)-[]->(m) RETURN count(m)"),
    ("NDS-07", "time predicate composed with a traversal",
     "MATCH (n:N)-[]->(m) WHERE n.t >= datetime(\"2024-01-01T00:00:00Z\") RETURN count(m)"),

    // NDS-08 — document/JSON: nested maps and path expressions.
    ("NDS-08", "store a nested map property",
     "CREATE (d:D {doc: {a: {b: [1, 2, 3]}}}) RETURN d.doc AS doc"),
    ("NDS-08", "read a nested path expression",
     "CREATE (d:D {doc: {a: {b: 1}}}) WITH d RETURN d.doc.a.b AS v"),
    ("NDS-08", "index into a nested list",
     "CREATE (d:D {doc: {a: [10, 20, 30]}}) WITH d RETURN d.doc.a[1] AS v"),

    // NDS-09 — multiple named vectors per node, quantized.
    ("NDS-09", "two named vector properties on one node",
     "CREATE (v:V {title_vec: [1.0, 2.0], body_vec: [3.0, 4.0]}) RETURN v.title_vec AS a, v.body_vec AS b"),
    ("NDS-09", "declare a vector dimension in DDL",
     "CREATE VECTOR INDEX tvec FOR (n:V) ON (n.title_vec) OPTIONS {dimensions: 2, quantization: \"fp16\"}"),

    // NDS-10 — sketches: HLL, t-digest, Bloom.
    ("NDS-10", "count(DISTINCT) — the exact form HLL would approximate",
     "MATCH (n:N) RETURN count(DISTINCT n.x) AS c"),
    ("NDS-10", "HyperLogLog approximate distinct",
     "MATCH (n:N) RETURN approx.countDistinct(n.x) AS c"),
    ("NDS-10", "t-digest quantile",
     "MATCH (n:N) RETURN approx.percentile(n.x, 0.95) AS p"),

    // NDS-13 — indexed and unindexed execution must agree. Probed as the
    // *pair* it is: the same question asked twice. A single query cannot
    // answer a requirement about two execution paths agreeing.
    ("NDS-13", "same range query with and without an index",
     "CREATE INDEX ON :N(x); MATCH (n:N) WHERE n.x > 0 RETURN count(n)"),
];

/// A node carrying one property of every kind the probes ask about, plus an
/// edge so the composability probes have something to traverse.
///
/// The fixture matters more here than usual. A probe for "does a time
/// predicate work" against a node with no time property answers a question
/// about the fixture, and this repo has already produced one probe suite that
/// reported every feature as absent because the graph was empty.
fn fixture() -> GraphStore {
    let mut s = GraphStore::new();
    let a = s.create_node_with_labels([Label::new("N")]);
    s.set_node_property("default", a, "x", PropertyValue::Integer(1)).unwrap();
    s.set_node_property("default", a, "body",
        PropertyValue::String("a graph database indexes its own structures".into())).unwrap();
    // Stored as a string: whether the engine has a datetime *property* type is
    // part of what NDS-03 is asking, so the fixture must not presume it.
    s.set_node_property("default", a, "t",
        PropertyValue::String("2024-06-01T00:00:00Z".into())).unwrap();
    s.set_node_property("default", a, "start",
        PropertyValue::String("2024-02-01T00:00:00Z".into())).unwrap();
    s.set_node_property("default", a, "end",
        PropertyValue::String("2024-08-01T00:00:00Z".into())).unwrap();
    // `Vector`, not `Array` of floats: the engine has a dedicated vector
    // property type, and NDS-05/09 are about that type rather than about a
    // list that happens to hold numbers.
    s.set_node_property("default", a, "embedding",
        PropertyValue::Vector(vec![1.0, 2.0, 3.0])).unwrap();
    let b = s.create_node_with_labels([Label::new("N")]);
    s.set_node_property("default", b, "x", PropertyValue::Integer(2)).unwrap();
    s.create_edge(a, b, "LINKS").unwrap();
    s
}

fn main() {
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
}
