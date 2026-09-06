//! The same query, the same answer, before and after a snapshot round trip.
//!
//! After an import, `node.properties` is **empty by design** — values live in the
//! columnar store (`snapshot_preserves_properties.rs`, #545). Every reader that
//! goes through `node_properties_full()` or the column store is fine; there are
//! ~122 sites in `src/` that read `.properties` directly, and each one of those
//! answers a different question on an imported graph than on a built one.
//!
//! Auditing 122 call sites finds the ones I think to look at. Asking the engine the
//! same question twice finds the ones I do not: this builds a graph, records the
//! answer to a corpus of queries, round-trips it through a snapshot, and asks
//! again. A divergence is a reader that has not been moved.
//!
//! This is the metamorphic shape LANG-05 asks for, applied to storage rather than
//! to query rewriting: the transformation is "persist and reload", which the
//! specification requires to preserve every answer.
//!
//! There are **three** representations, not two, and the ingest path decides which:
//!
//! | path | row copy | columnar |
//! |---|---|---|
//! | `CREATE` / `set_node_property` | yes | yes |
//! | snapshot import | no | yes |
//! | persistence recover | yes | no |
//!
//! `insert_recovered_node` never touches `node_columns`, so a reader that consults
//! only the columns is wrong after a restart in exactly the way a reader that
//! consults only the row was wrong after an import. Both directions are compared
//! here for that reason: fixing one and testing one is how the first bug survived.

use samyama::graph::GraphStore;
use samyama::persistence::PersistenceManager;
use samyama::query::QueryEngine;
use samyama::snapshot::{export_tenant, import_tenant};

const T: &str = "default";

/// A graph with every property type the columnar store treats differently: typed
/// columns (int, float, string, bool) and the `Other` fallback (array, map,
/// temporal), plus a node with no properties and one with a property only it has.
const BUILD: &str = r#"
CREATE (:Person {name: "ada", age: 36, score: 1.5, active: true});
CREATE (:Person {name: "grace", age: 45, score: 2.5, active: false});
CREATE (:Person {name: "alan", age: 41, score: 3.5, active: true, nickname: "prof"});
CREATE (:Empty);
CREATE (:Odd {tags: ["a", "b"], when: date("2026-09-06")});
MATCH (a:Person {name: "ada"}), (b:Person {name: "grace"}) CREATE (a)-[:KNOWS {since: 2020}]->(b);
MATCH (a:Person {name: "grace"}), (b:Person {name: "alan"}) CREATE (a)-[:KNOWS {since: 2021}]->(b);
"#;

/// Every query here must answer identically on both stores. They are chosen to
/// exercise the reader paths separately: whole-node return, single property,
/// property in a predicate, aggregation over a property, `keys()`, `properties()`,
/// ordering by a property, and a traversal carrying an edge property.
const QUERIES: &[&str] = &[
    "MATCH (n:Person) RETURN n.name ORDER BY n.name",
    "MATCH (n:Person) RETURN n ORDER BY n.name",
    "MATCH (n:Person) WHERE n.age > 40 RETURN n.name ORDER BY n.name",
    "MATCH (n:Person) RETURN count(n)",
    "MATCH (n:Person) RETURN sum(n.age)",
    "MATCH (n:Person) RETURN avg(n.score)",
    "MATCH (n:Person) WHERE n.active = true RETURN n.name ORDER BY n.name",
    "MATCH (n:Person {name: \"alan\"}) RETURN keys(n)",
    "MATCH (n:Person {name: \"alan\"}) RETURN properties(n)",
    "MATCH (n:Person {name: \"alan\"}) RETURN n.nickname",
    "MATCH (n:Person) RETURN n.name ORDER BY n.age DESC",
    "MATCH (n:Empty) RETURN keys(n)",
    "MATCH (n:Odd) RETURN n.tags",
    "MATCH (a)-[r:KNOWS]->(b) RETURN a.name, r.since, b.name ORDER BY a.name",
    "MATCH (n) RETURN count(n)",
    "MATCH (n:Person) WHERE n.name STARTS WITH \"a\" RETURN n.name ORDER BY n.name",
    // Shapes added after the first run found one bug in sixteen queries: the
    // corpus is the coverage, so it is worth more than the fix was.
    "MATCH (n:Person) RETURN n.name, n.age, n.score, n.active ORDER BY n.name",
    "MATCH (n) RETURN labels(n) ORDER BY labels(n)",
    "MATCH (n:Person) RETURN min(n.age), max(n.age)",
    "MATCH (n:Person) WHERE n.nickname IS NULL RETURN n.name ORDER BY n.name",
    "MATCH (n:Person) WHERE n.nickname IS NOT NULL RETURN n.name",
    "MATCH (n:Person) RETURN n.name AS who ORDER BY who DESC",
    "MATCH (n:Person) WITH n.age AS a WHERE a > 40 RETURN a ORDER BY a",
    "MATCH (n:Person) RETURN DISTINCT n.active ORDER BY n.active",
    "MATCH (n:Person) RETURN collect(n.name) ORDER BY n.name",
    "MATCH (a)-[r:KNOWS]->(b) RETURN r ORDER BY a.name",
    "MATCH (a)-[r:KNOWS]->(b) RETURN properties(r) ORDER BY a.name",
    "MATCH (a)-[r:KNOWS]->(b) WHERE r.since > 2020 RETURN a.name",
    "MATCH (a:Person)-[:KNOWS*1..2]->(b:Person) RETURN a.name, b.name ORDER BY a.name, b.name",
    "MATCH (n:Person) RETURN n.name ORDER BY n.score",
    "MATCH (n:Person) RETURN count(DISTINCT n.active)",
    "MATCH (n:Odd) RETURN n.when",
    "MATCH (n:Odd) RETURN keys(n)",
    "MATCH (n) WHERE n.name = \"ada\" RETURN n.age",
    "MATCH (n:Person) RETURN sum(n.score) / count(n)",
    "MATCH (n:Person) WHERE n.age IN [36, 45] RETURN n.name ORDER BY n.name",
];

/// Canonicalise the brace-delimited groups a `Debug` rendering produces for maps
/// and property bags, whose iteration order is not part of the answer.
///
/// Without this the check reports `RETURN properties(n)` as a divergence because
/// the same five entries come back in a different order — a false positive that
/// would train a reader to ignore the test.
fn canonical(s: &str) -> String {
    // Node timestamps are struct metadata, not query answers, and a snapshot round
    // trip resets them to 0 — a real gap, filed separately (#1124), and not one this
    // check is about. Masked rather than left in, because a check that fails for a
    // reason it is not testing gets muted.
    let masked = mask_timestamps(s);
    let s = masked.as_str();
    let mut out = String::with_capacity(s.len());
    let mut rest = s;
    while let Some(open) = rest.find('{') {
        out.push_str(&rest[..=open]);
        rest = &rest[open + 1..];
        let Some(close) = rest.find('}') else {
            break;
        };
        let (inner, after) = rest.split_at(close);
        // Only flat groups: a nested brace means the split below would cut a value
        // in half, so it is left alone rather than mangled.
        if inner.contains('{') {
            out.push_str(inner);
        } else {
            let mut parts: Vec<&str> = inner.split(", ").collect();
            parts.sort_unstable();
            out.push_str(&parts.join(", "));
        }
        rest = after;
    }
    out.push_str(rest);
    out
}

/// Replace `created_at: <n>` / `updated_at: <n>` with a placeholder.
fn mask_timestamps(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut rest = s;
    while let Some(pos) = rest.find("_at: ") {
        out.push_str(&rest[..pos + 5]);
        rest = &rest[pos + 5..];
        let end = rest.find(|c: char| !c.is_ascii_digit()).unwrap_or(rest.len());
        out.push_str("<ts>");
        rest = &rest[end..];
    }
    out.push_str(rest);
    out
}

/// A result rendered as a comparable string. Column order and row order are part of
/// the answer, so nothing is sorted there — the queries that need an order say so.
fn answer(engine: &QueryEngine, store: &GraphStore, q: &str) -> String {
    match engine.execute(q, store) {
        Ok(batch) => {
            let mut out = String::new();
            out.push_str(&batch.columns.join("|"));
            for rec in &batch.records {
                out.push('\n');
                let cells: Vec<String> = batch
                    .columns
                    .iter()
                    .map(|c| canonical(&format!("{:?}", rec.get(c))))
                    .collect();
                out.push_str(&cells.join("|"));
            }
            out
        }
        Err(e) => format!("ERR {e}"),
    }
}

/// Build the graph the corpus is written against.
fn built(engine: &QueryEngine) -> GraphStore {
    let mut store = GraphStore::new();
    for stmt in BUILD.split(';').map(str::trim).filter(|s| !s.is_empty()) {
        engine
            .execute_mut(stmt, &mut store, T)
            .unwrap_or_else(|e| panic!("setup failed: {stmt}\n{e}"));
    }
    assert!(store.node_count() >= 5, "setup built nothing to compare");
    store
}

/// Compare every query's answer between two stores, reporting all divergences
/// rather than the first: one failure per run turns a corpus into a queue.
fn compare(engine: &QueryEngine, reference: &GraphStore, other: &GraphStore, what: &str) {
    let mut diffs = Vec::new();
    for q in QUERIES {
        let a = answer(engine, reference, q);
        let b = answer(engine, other, q);
        if a != b {
            diffs.push(format!("--- {q}\n  built: {a}\n  {what}: {b}"));
        }
    }
    assert!(
        diffs.is_empty(),
        "{} of {} queries answer differently after {what}. Each one is a reader \
         that consults one property representation and not the other (#545):\n\n{}",
        diffs.len(),
        QUERIES.len(),
        diffs.join("\n\n")
    );
}

#[test]
fn every_query_answers_the_same_after_a_persistence_restart() {
    let engine = QueryEngine::new();
    let source = built(&engine);

    let dir = tempfile::tempdir().unwrap();
    let pm = PersistenceManager::new(dir.path()).unwrap();
    pm.tenants().create_tenant(T.to_string(), T.to_string(), None).ok();

    // Persisted the way the servers persist, through the mutation journal.
    let mut live = GraphStore::new();
    live.enable_write_log();
    for stmt in BUILD.split(';').map(str::trim).filter(|s| !s.is_empty()) {
        engine.execute_mut(stmt, &mut live, T).unwrap();
        let muts = live.take_write_log();
        pm.apply_mutations(T, &live, &muts).expect("persist");
    }
    pm.checkpoint().expect("checkpoint");

    let (nodes, edges) = pm.recover(T).expect("recover");
    let mut restored = GraphStore::new();
    for n in nodes {
        restored.insert_recovered_node(n);
    }
    for e in edges {
        restored.insert_recovered_edge(e).expect("recovered edge");
    }
    assert_eq!(
        restored.node_count(),
        source.node_count(),
        "the restart lost nodes, so any answer comparison below is moot"
    );

    // The precondition that makes this test mean anything: the restored store must
    // actually hold its properties somewhere different from the built one.
    // `insert_recovered_node` fills the row copy and never touches the columns, so
    // if the columns are populated here the recovery path has changed and this test
    // is comparing a store against itself.
    let a_person = restored
        .get_nodes_by_label(&"Person".into())
        .first()
        .map(|n| n.id)
        .expect("no Person survived the restart");
    let idx = a_person.as_u64() as usize;
    assert!(
        restored.node_columns.get_property_keys(idx).is_empty(),
        "recovery now populates the column store, so this test no longer exercises \
         the row-only representation it was written for — re-derive it rather than \
         deleting this assertion"
    );
    assert!(
        !restored.get_node(a_person).unwrap().properties.is_empty(),
        "recovery populated neither representation"
    );

    compare(&engine, &source, &restored, "a persistence restart");
}

#[test]
fn every_query_answers_the_same_after_a_snapshot_round_trip() {
    let engine = QueryEngine::new();

    let built = built(&engine);

    let mut bytes = Vec::new();
    export_tenant(&built, &mut bytes).expect("export");
    let mut imported = GraphStore::new();
    import_tenant(&mut imported, std::io::Cursor::new(&bytes)).expect("import");
    assert_eq!(
        imported.node_count(),
        built.node_count(),
        "the round trip lost nodes, so any answer comparison below is moot"
    );

    // Same precondition, other direction: after an import the row copy is empty by
    // design and the values are in the columns. If that stops being true this test
    // compares a store against itself and proves nothing.
    let a_person = imported
        .get_nodes_by_label(&"Person".into())
        .first()
        .map(|n| n.id)
        .expect("no Person survived the round trip");
    assert!(
        imported.get_node(a_person).unwrap().properties.is_empty(),
        "import now fills the row copy, so this test no longer exercises the \
         columnar-only representation it was written for"
    );

    let mut diffs = Vec::new();
    for q in QUERIES {
        let before = answer(&engine, &built, q);
        let after = answer(&engine, &imported, q);
        if before != after {
            diffs.push(format!("--- {q}\n  built:    {before}\n  imported: {after}"));
        }
    }

    assert!(
        diffs.is_empty(),
        "{} of {} queries answer differently after a snapshot round trip. \
         Each one is a reader of `Node.properties` that has not moved to the \
         columnar store or `node_properties_full()` (#545):\n\n{}",
        diffs.len(),
        QUERIES.len(),
        diffs.join("\n\n")
    );
}
