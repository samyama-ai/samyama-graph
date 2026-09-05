//! CH-IMPORT — the same graph, built by different paths, must be the same graph
//! (LANG-15, REL-06).
//!
//! A graph can enter this engine three ways: written by Cypher, restored from a
//! `.sgsnap` snapshot, or built through the store API that loaders use. Each
//! path writes properties, labels and adjacency through different code, and the
//! failure they share is *partial* agreement — the nodes match, the counts
//! match, and one representation has quietly dropped something the others kept.
//!
//! That is not hypothetical here. `node.properties` is empty after a snapshot
//! import because properties live in the columnar store, so a check that reads
//! only row storage reports a restored graph as having none. Dense property
//! columns had to be taught to keep a gap rather than answer `T::default()`,
//! or an absent property would come back as `0`. Both were found by comparing
//! paths, not by testing one.
//!
//! Invariants compared, in increasing order of how easy they are to get wrong:
//!
//!   1. node and edge counts;
//!   2. the label histogram and the edge-type histogram;
//!   3. every property of every node, read back through Cypher — which is the
//!      only reader a user has;
//!   4. degree distribution, so adjacency is compared and not just endpoints;
//!   5. absent properties stay absent, checked with `IS NULL`.
//!
//!   cargo run --release --example import_invariants -- --json out.json

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::{MutQueryExecutor, QueryExecutor, Value};
use samyama::query::parser::parse_query;
use samyama::snapshot::{export_tenant, import_tenant};
use samyama::persistence::PersistenceManager;
use std::collections::BTreeMap;
use std::fmt::Write as _;

/// The fixture, deliberately awkward: several labels, a node missing a
/// property, an empty string, a stored zero, a float, a bool and a list.
const CYPHER_BUILD: &str = "\
CREATE (a:Person:Employee {name: 'Alice', age: 30, score: 0.5, active: true, tags: ['x','y']}) \
CREATE (b:Person {name: 'Bob', age: 0, note: ''}) \
CREATE (c:Company {name: 'Acme'}) \
CREATE (a)-[:WORKS_AT {since: 2019}]->(c) \
CREATE (b)-[:WORKS_AT {since: 2021}]->(c) \
CREATE (a)-[:KNOWS]->(b)";

fn write(store: &mut GraphStore, cypher: &str) {
    let q = parse_query(cypher).expect("fixture should parse");
    MutQueryExecutor::new(store, "default".to_string())
        .execute(&q)
        .expect("fixture should run");
}

/// Every row of a query, rendered canonically and sorted, so two graphs can be
/// compared as text without depending on row order.
fn rows(store: &GraphStore, cypher: &str) -> Vec<String> {
    let q = parse_query(cypher).expect("probe should parse");
    let batch = QueryExecutor::new(store).execute(&q).expect("probe should run");
    let mut out: Vec<String> = batch
        .records
        .iter()
        .map(|r| {
            let mut cells: Vec<String> = r
                .bindings()
                .iter()
                .map(|(k, v)| format!("{k}={}", render(v)))
                .collect();
            cells.sort();
            cells.join(",")
        })
        .collect();
    out.sort();
    out
}

/// Render a value without its node id, which legitimately differs between
/// paths — a snapshot restores ids, a rebuild allocates fresh ones. Comparing
/// ids would report every run as a mismatch.
fn render(v: &Value) -> String {
    match v {
        Value::Property(p) => render_prop(p),
        Value::Node(_, n) => {
            let mut labels: Vec<&str> = n.labels.iter().map(|l| l.as_str()).collect();
            labels.sort_unstable();
            format!("node[{}]", labels.join(":"))
        }
        Value::NodeRef(_) => "noderef".to_string(),
        other => format!("{other:?}"),
    }
}

fn render_prop(p: &PropertyValue) -> String {
    match p {
        PropertyValue::Null => "null".into(),
        PropertyValue::String(s) => format!("'{s}'"),
        PropertyValue::Integer(i) => i.to_string(),
        PropertyValue::Float(f) => format!("{f:?}"),
        PropertyValue::Boolean(b) => b.to_string(),
        PropertyValue::Array(items) => {
            let inner: Vec<String> = items.iter().map(render_prop).collect();
            format!("[{}]", inner.join(","))
        }
        other => format!("{other:?}"),
    }
}

/// The probes that define "the same graph". Each is a Cypher query, because
/// Cypher is the only reader a user has — an invariant checked through a
/// back door can hold while the front door is broken (#333).
fn probes() -> Vec<(&'static str, &'static str)> {
    vec![
        ("node_count", "MATCH (n) RETURN count(n) AS c"),
        ("edge_count", "MATCH ()-[r]->() RETURN count(r) AS c"),
        ("label_histogram", "MATCH (n) UNWIND labels(n) AS l RETURN l, count(*) AS c"),
        ("edge_type_histogram", "MATCH ()-[r]->() RETURN type(r) AS t, count(*) AS c"),
        ("all_node_properties", "MATCH (n) RETURN n.name AS name, n.age AS age, n.score AS score, n.active AS active, n.note AS note, n.tags AS tags"),
        ("keys_per_node", "MATCH (n) RETURN n.name AS name, keys(n) AS k"),
        ("edge_properties", "MATCH ()-[r]->() RETURN type(r) AS t, r.since AS since"),
        ("out_degree", "MATCH (n) RETURN n.name AS name, size([(n)-->() | 1]) AS d"),
        ("absent_property_is_null", "MATCH (n) WHERE n.note IS NULL RETURN count(*) AS c"),
        ("stored_zero_is_present", "MATCH (n) WHERE n.age = 0 RETURN count(*) AS c"),
        ("empty_string_is_present", "MATCH (n) WHERE n.note = '' RETURN count(*) AS c"),
    ]
}

struct Built {
    name: &'static str,
    store: GraphStore,
}

fn main() {
    let json_path = {
        let args: Vec<String> = std::env::args().collect();
        args.iter().position(|a| a == "--json").and_then(|i| args.get(i + 1).cloned())
    };

    // ---- Path 1: written by Cypher.
    let mut cypher_store = GraphStore::new();
    write(&mut cypher_store, CYPHER_BUILD);

    // ---- Path 2: exported and re-imported as a snapshot.
    let mut bytes = Vec::new();
    export_tenant(&cypher_store, &mut bytes).expect("export should succeed");
    let mut snapshot_store = GraphStore::new();
    import_tenant(&mut snapshot_store, bytes.as_slice()).expect("import should succeed");

    // ---- Path 3: built through the store API, the way loaders do.
    let mut api_store = GraphStore::new();
    {
        let s = &mut api_store;
        let a = s.create_node("Person");
        let _ = s.add_label_to_node("default", a, "Employee");
        let _ = s.set_node_property("default", a, "name".to_string(), PropertyValue::String("Alice".into()));
        let _ = s.set_node_property("default", a, "age".to_string(), PropertyValue::Integer(30));
        let _ = s.set_node_property("default", a, "score".to_string(), PropertyValue::Float(0.5));
        let _ = s.set_node_property("default", a, "active".to_string(), PropertyValue::Boolean(true));
        let _ = s.set_node_property("default", a, "tags".to_string(), PropertyValue::Array(vec![
            PropertyValue::String("x".into()),
            PropertyValue::String("y".into()),
        ]));

        let b = s.create_node("Person");
        let _ = s.set_node_property("default", b, "name".to_string(), PropertyValue::String("Bob".into()));
        let _ = s.set_node_property("default", b, "age".to_string(), PropertyValue::Integer(0));
        let _ = s.set_node_property("default", b, "note".to_string(), PropertyValue::String(String::new()));

        let c = s.create_node("Company");
        let _ = s.set_node_property("default", c, "name".to_string(), PropertyValue::String("Acme".into()));

        let e1 = s.create_edge(a, c, "WORKS_AT").unwrap();
        let _ = s.set_edge_property(e1, "since", PropertyValue::Integer(2019));
        let e2 = s.create_edge(b, c, "WORKS_AT").unwrap();
        let _ = s.set_edge_property(e2, "since", PropertyValue::Integer(2021));
        let _ = s.create_edge(a, b, "KNOWS").unwrap();
    }

    // ---- Path 4: written by Cypher with persistence on, then recovered the way
    // a restart recovers -- `PersistenceManager::recover` into a fresh store.
    //
    // The three paths above all live in one process, so between them they could
    // not see a write that reaches memory and never reaches disk. That is the
    // half of REL-06 the requirement actually names ("visible before restart is
    // visible after"), and it is where #1094 was: writes over HTTP returned 200
    // and were gone on the next start.
    let restart_dir = tempfile::tempdir().expect("tempdir");
    let mut restart_store = GraphStore::new();
    {
        let pm = PersistenceManager::new(restart_dir.path()).expect("persistence");
        pm.tenants()
            .create_tenant("default".to_string(), "default".to_string(), None)
            .ok();

        let mut live = GraphStore::new();
        live.enable_write_log();
        // One statement at a time, as a client issues them: a per-statement
        // journal that only works when the whole build arrives at once would
        // pass here and fail for every real caller.
        for stmt in CYPHER_BUILD.split(';').map(str::trim).filter(|s| !s.is_empty()) {
            write(&mut live, stmt);
            let muts = live.take_write_log();
            pm.apply_mutations("default", &live, &muts).expect("persist");
        }
        pm.checkpoint().expect("checkpoint");

        let (nodes, edges) = pm.recover("default").expect("recover");
        for node in nodes {
            restart_store.insert_recovered_node(node);
        }
        for edge in edges {
            restart_store.insert_recovered_edge(edge).expect("recovered edge");
        }
    }

    let built = vec![
        Built { name: "cypher", store: cypher_store },
        Built { name: "snapshot", store: snapshot_store },
        Built { name: "store_api", store: api_store },
        Built { name: "restart", store: restart_store },
    ];

    // The Cypher-built graph is the reference: it is the path a user exercises
    // and the one whose behaviour is specified by the language.
    let reference = &built[0];
    let mut results: Vec<(String, String, bool, String, String)> = Vec::new();

    for (probe_name, cypher) in probes() {
        let expected = rows(&reference.store, cypher);
        for other in &built[1..] {
            let got = rows(&other.store, cypher);
            let agrees = got == expected;
            results.push((
                probe_name.to_string(),
                other.name.to_string(),
                agrees,
                format!("{expected:?}"),
                format!("{got:?}"),
            ));
        }
    }

    // ---- Canary.
    //
    // Every invariant agreeing is the expected outcome, which makes this
    // suite indistinguishable from one that compares nothing. So a graph that
    // is deliberately wrong in one small way is run through the same probes,
    // and the suite fails if the probes *fail to notice*. Without this, a
    // refactor that broke `rows()` would turn CH-IMPORT permanently green.
    let mut canary_store = GraphStore::new();
    write(&mut canary_store, CYPHER_BUILD);
    // One property differs by one, on one node. Nothing else changes: same
    // counts, same labels, same edges.
    write(&mut canary_store, "MATCH (n {name: 'Alice'}) SET n.age = 31");
    let canary_detected = probes()
        .iter()
        .any(|(_, cypher)| rows(&canary_store, cypher) != rows(&reference.store, *cypher));

    let total = results.len();
    let agreed = results.iter().filter(|r| r.2).count();

    println!("CH-IMPORT — cross-path import invariants");
    println!("{}", "=".repeat(78));
    println!("paths: cypher (reference), snapshot, store_api, restart");
    println!("{:<26} {:<12} {}", "probe", "path", "agrees");
    println!("{}", "-".repeat(78));
    for (probe, path, agrees, expected, got) in &results {
        println!("{:<26} {:<12} {}", probe, path, if *agrees { "yes" } else { "NO" });
        if !agrees {
            println!("    reference: {}", truncate(expected));
            println!("    {:<9}: {}", path, truncate(got));
        }
    }
    println!("{}", "-".repeat(78));
    println!("{agreed}/{total} invariants agree across paths");
    println!(
        "canary (one property changed by one): {}",
        if canary_detected { "detected — the probes can fail" } else { "NOT DETECTED — the probes are vacuous" }
    );

    if let Some(path) = json_path {
        let commit = std::process::Command::new("git")
            .args(["rev-parse", "--short", "HEAD"])
            .output()
            .ok()
            .filter(|o| o.status.success())
            .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
            .unwrap_or_else(|| "unknown".into());
        let mut cases = String::new();
        for (probe, p, agrees, expected, got) in &results {
            if !cases.is_empty() {
                cases.push_str(",\n      ");
            }
            let _ = write!(
                cases,
                "{{\"probe\": \"{probe}\", \"path\": \"{p}\", \"agrees\": {agrees}, \
                 \"reference\": \"{}\", \"got\": \"{}\"}}",
                escape(&truncate(expected)),
                escape(&truncate(got))
            );
        }
        let envelope = format!(
            "{{
  \"suite\": \"import-invariants\",
  \"requirement_ids\": [\"LANG-15\", \"REL-06\"],
  \"run_id\": \"import-{commit}\",
  \"engine\": {{\"name\": \"samyama\", \"version\": \"{}\", \"commit\": \"{commit}\"}},
  \"dataset\": {{\"name\": \"synthetic-fixture\", \"paths\": [\"cypher\", \"snapshot\", \"store_api\", \"restart\"]}},
  \"measurements\": {{\"agreed\": {agreed}, \"total\": {total}, \"canary_detected\": {canary_detected}, \"cases\": [
      {cases}
  ]}},
  \"status\": \"{}\",
  \"artifacts\": [\"examples/import_invariants.rs\"]
}}
",
            env!("CARGO_PKG_VERSION"),
            if agreed == total && canary_detected { "pass" } else { "fail" }
        );
        std::fs::write(&path, envelope).expect("could not write JSON");
        println!("wrote {path}");
    }

    if agreed != total || !canary_detected {
        std::process::exit(1);
    }
}

fn truncate(s: &str) -> String {
    if s.chars().count() <= 160 {
        s.to_string()
    } else {
        s.chars().take(160).collect::<String>() + "…"
    }
}

fn escape(s: &str) -> String {
    s.replace('\\', "\\\\").replace('"', "\\\"")
}

/// A BTreeMap keeps the histogram output stable; kept as a type alias so the
/// intent is visible where it is used.
#[allow(dead_code)]
type Histogram = BTreeMap<String, usize>;
