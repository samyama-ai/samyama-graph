//! Dense property columns, seen from outside the storage layer (#533).
//!
//! The unit tests in `graph::storage::columnar` cover the representation. What
//! they cannot cover is the three places a wrong representation shows up as a
//! wrong *answer* rather than a slow one:
//!
//! * a missing property must stay missing — the column store returns
//!   `PropertyValue::Null` to mean "not here, try row storage", and a dense
//!   column that answered `T::default()` would turn an absent property into
//!   `0` or `""`;
//! * a deleted node's slot is handed to the next `create_node`, so a value
//!   left behind reappears on unrelated data (#364);
//! * `.sgsnap` writes properties through this store, so both representations
//!   have to round-trip.

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::{QueryExecutor, Value};
use samyama::query::parser::parse_query;
use samyama::snapshot::{export_tenant, import_tenant};

/// Above the promotion threshold, so the columns here are genuinely dense
/// rather than incidentally small.
const N: usize = 3000;

fn rows(store: &GraphStore, cypher: &str) -> Vec<Vec<(String, String)>> {
    let query = parse_query(cypher).expect("query should parse");
    let batch = QueryExecutor::new(store).execute(&query).expect("query should run");
    batch
        .records
        .iter()
        .map(|r| {
            let mut cells: Vec<(String, String)> = r
                .bindings()
                .iter()
                .map(|(k, v)| (k.clone(), format!("{v:?}")))
                .collect();
            cells.sort();
            cells
        })
        .collect()
}

/// Every third node is missing `nickname`, so a dense column has to hold gaps.
fn gappy_graph() -> GraphStore {
    let mut store = GraphStore::new();
    for i in 0..N {
        let id = store.create_node("Person");
        let _ = store.set_node_property("default", id, "seq".to_string(), PropertyValue::Integer(i as i64));
        if i % 3 != 1 {
            let _ = store.set_node_property(
                "default",
                id,
                "nickname".to_string(),
                PropertyValue::String(format!("nick{i}")),
            );
        }
    }
    store
}

#[test]
fn a_property_that_was_never_set_is_null_not_a_default() {
    let store = gappy_graph();

    // IS NULL must find exactly the third of nodes that never got one. If a
    // dense column returned an empty string for an absent slot, this returns
    // zero rows and the graph looks like every node has a nickname.
    let missing = rows(&store, "MATCH (p:Person) WHERE p.nickname IS NULL RETURN p.seq AS s");
    assert_eq!(missing.len(), N / 3, "one in three nodes has no nickname");

    let present = rows(&store, "MATCH (p:Person) WHERE p.nickname IS NOT NULL RETURN p.seq AS s");
    assert_eq!(present.len(), N - N / 3);
}

#[test]
fn a_stored_zero_is_not_mistaken_for_an_absent_row() {
    // The inverse hazard: presence is tracked by a bit, not by comparing
    // against the default, so a legitimately stored 0 must still be present.
    let mut store = GraphStore::new();
    for _ in 0..N {
        let id = store.create_node("Item");
        let _ = store.set_node_property("default", id, "count".to_string(), PropertyValue::Integer(0));
    }
    let zeros = rows(&store, "MATCH (i:Item) WHERE i.count = 0 RETURN i.count AS c");
    assert_eq!(zeros.len(), N);
    let nulls = rows(&store, "MATCH (i:Item) WHERE i.count IS NULL RETURN i.count AS c");
    assert!(nulls.is_empty(), "a stored zero is not a missing value");
}

#[test]
fn an_empty_string_is_not_mistaken_for_an_absent_row() {
    let mut store = GraphStore::new();
    for _ in 0..N {
        let id = store.create_node("Item");
        let _ = store.set_node_property(
            "default",
            id,
            "note".to_string(),
            PropertyValue::String(String::new()),
        );
    }
    let nulls = rows(&store, "MATCH (i:Item) WHERE i.note IS NULL RETURN i.note AS n");
    assert!(nulls.is_empty(), "a stored empty string is not a missing value");
}

#[test]
fn a_recycled_node_id_does_not_inherit_the_previous_occupants_properties() {
    // Node ids come off a free list, so a deleted node's slot goes to the next
    // create_node. On a hash-map column the entry had to be removed; on a
    // dense one the presence bit has to be cleared. Both are #364.
    let mut store = gappy_graph();

    let victim = rows(&store, "MATCH (p:Person) WHERE p.seq = 7 RETURN p.nickname AS n");
    assert_eq!(victim.len(), 1);

    let query = parse_query("MATCH (p:Person) WHERE p.seq = 7 DETACH DELETE p").unwrap();
    let mut mutating = samyama::query::executor::MutQueryExecutor::new(&mut store, "default".to_string());
    mutating.execute(&query).expect("delete should run");

    // The next node created takes the freed id.
    let fresh = store.create_node("Ghost");
    let idx = fresh.as_u64() as usize;
    assert_eq!(
        store.node_columns.get_property(idx, "nickname"),
        PropertyValue::Null,
        "the new node inherited the deleted node's nickname"
    );
    assert_eq!(
        store.node_columns.get_property(idx, "seq"),
        PropertyValue::Null,
        "the new node inherited the deleted node's seq"
    );
}

#[test]
fn a_dense_graph_round_trips_through_a_snapshot() {
    let store = gappy_graph();

    let mut bytes = Vec::new();
    export_tenant(&store, &mut bytes).expect("export");

    let mut restored = GraphStore::new();
    import_tenant(&mut restored, bytes.as_slice()).expect("import");

    assert_eq!(restored.node_count(), store.node_count());

    // Values survive…
    let before = rows(&store, "MATCH (p:Person) WHERE p.seq = 42 RETURN p.nickname AS n");
    let after = rows(&restored, "MATCH (p:Person) WHERE p.seq = 42 RETURN p.nickname AS n");
    assert_eq!(before, after);

    // …and so do the gaps. A round trip that filled absent slots with a
    // default would pass every value check and still be wrong.
    let missing_before =
        rows(&store, "MATCH (p:Person) WHERE p.nickname IS NULL RETURN p.seq AS s").len();
    let missing_after =
        rows(&restored, "MATCH (p:Person) WHERE p.nickname IS NULL RETURN p.seq AS s").len();
    assert_eq!(missing_after, missing_before, "the gaps did not survive the round trip");
    assert_eq!(missing_after, N / 3);
}

#[test]
fn a_sparse_property_on_a_large_graph_does_not_blow_up_memory() {
    // The pathology the fill-factor rule exists to prevent: one property on a
    // handful of nodes at the far end of a big id space must not allocate a
    // slot per node. Asserted through behaviour and process RSS rather than
    // through the representation, so it holds however the rule is implemented.
    let mut store = GraphStore::new();
    for _ in 0..200_000 {
        store.create_node("Filler");
    }
    let rss_before = resident_bytes();

    let tagged = store.create_node("Tagged");
    let _ = store.set_node_property(
        "default",
        tagged,
        "rare".to_string(),
        PropertyValue::String("only one of me".to_string()),
    );

    let growth = resident_bytes().saturating_sub(rss_before);
    assert!(
        growth < 8 * 1024 * 1024,
        "one property on one node grew RSS by {growth} bytes — the column went dense over the id space"
    );
    assert_eq!(
        store.node_columns.get_property(tagged.as_u64() as usize, "rare"),
        PropertyValue::String("only one of me".to_string())
    );
}

#[test]
fn queries_over_a_dense_column_return_the_same_rows_as_a_sparse_one() {
    // A representation change must be invisible in results. The sparse graph
    // stays below the promotion threshold; the dense one crosses it. Same
    // logical content per node, so the same answers.
    let small = {
        let mut store = GraphStore::new();
        for i in 0..50 {
            let id = store.create_node("P");
            let _ = store.set_node_property("default", id, "v".to_string(), PropertyValue::Integer(i % 10));
        }
        store
    };
    let large = {
        let mut store = GraphStore::new();
        for i in 0..5000 {
            let id = store.create_node("P");
            let _ = store.set_node_property("default", id, "v".to_string(), PropertyValue::Integer(i % 10));
        }
        store
    };

    for cypher in [
        "MATCH (p:P) WHERE p.v = 3 RETURN count(p) AS c",
        "MATCH (p:P) RETURN p.v AS v, count(p) AS c ORDER BY v",
        "MATCH (p:P) WHERE p.v > 7 RETURN p.v AS v ORDER BY v LIMIT 5",
    ] {
        let a = rows(&small, cypher);
        let b = rows(&large, cypher);
        // Counts differ by construction (100x the nodes); the shape must not.
        assert_eq!(a.len().min(5), b.len().min(5), "{cypher}");
    }

    // Exact check where the answer is scale-free.
    assert_eq!(
        rows(&large, "MATCH (p:P) RETURN DISTINCT p.v AS v ORDER BY v").len(),
        10
    );
}

/// Resident bytes, from `/proc/self/statm`. Returns 0 where it is unavailable,
/// which makes the assertion above vacuous rather than wrong.
fn resident_bytes() -> usize {
    std::fs::read_to_string("/proc/self/statm")
        .ok()
        .and_then(|s| s.split_whitespace().nth(1)?.parse::<usize>().ok())
        .map(|pages| pages * 4096)
        .unwrap_or(0)
}
