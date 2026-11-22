//! Leakage-safe splits and the leakage check, called from Cypher (ML-05, #1730).
//!
//! Each strategy is checked two ways: `algo.splitLeakage` must return no rows
//! on the split `algo.split` made, and an independent Cypher query -- a
//! variable-length `SAME_AS` match, a min/max over timestamps, a count of
//! splits per source -- must agree that nothing crosses the boundary. Then the
//! negative control: a naive random split of the same data, which the check
//! must catch.

use std::collections::BTreeSet;

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::QueryEngine;

const TENANT: &str = "default";

fn run(
    store: &mut GraphStore,
    engine: &QueryEngine,
    q: &str,
) -> Vec<samyama::query::executor::Record> {
    engine
        .execute_mut(q, store, TENANT)
        .unwrap_or_else(|e| panic!("{q}: {e}"))
        .records
}

fn err(store: &mut GraphStore, engine: &QueryEngine, q: &str) -> String {
    match engine.execute_mut(q, store, TENANT) {
        Ok(_) => panic!("{q}: expected an error"),
        Err(e) => e.to_string(),
    }
}

fn int(rec: &samyama::query::executor::Record, col: &str) -> i64 {
    match rec.get(col).and_then(|v| v.as_property()) {
        Some(PropertyValue::Integer(i)) => *i,
        other => panic!("{col} is {other:?}"),
    }
}

fn text(rec: &samyama::query::executor::Record, col: &str) -> String {
    match rec.get(col).and_then(|v| v.as_property()) {
        Some(PropertyValue::String(s)) => s.clone(),
        other => panic!("{col} is {other:?}"),
    }
}

/// 80 entities of 1 to 4 records each, the records of one entity chained by
/// `SAME_AS` (direction alternating, so a walk must go both ways), every
/// record stamped with a source and a time. Plus `KNOWS` edges between
/// entities, which are not "same entity" and must not hold anything together.
fn records() -> (GraphStore, QueryEngine) {
    let mut store = GraphStore::new();
    let engine = QueryEngine::new();
    let mut t = 0;
    for e in 0..80 {
        let size = 1 + e % 4;
        let mut q = String::from("CREATE ");
        for r in 0..size {
            if r > 0 {
                q.push_str(", ");
            }
            t += 1;
            q.push_str(&format!(
                "(r{r}:R {{entity: {e}, src: 'src{}', ts: {}}})",
                (e * 7 + r) % 9,
                (t * 37) % 1000
            ));
        }
        for r in 1..size {
            if r % 2 == 1 {
                q.push_str(&format!(", (r{})-[:SAME_AS]->(r{r})", r - 1));
            } else {
                q.push_str(&format!(", (r{r})-[:SAME_AS]->(r{})", r - 1));
            }
        }
        run(&mut store, &engine, &q);
    }
    run(
        &mut store,
        &engine,
        "MATCH (a:R), (b:R) WHERE b.entity = a.entity + 1 AND a.entity % 5 = 0 \
         WITH a, b LIMIT 40 CREATE (a)-[:KNOWS]->(b)",
    );
    (store, engine)
}

/// Pairs of records within three `SAME_AS` hops that sit in different splits,
/// found by Cypher's own variable-length match.
fn cypher_entity_leaks(store: &mut GraphStore, engine: &QueryEngine) -> BTreeSet<(i64, i64)> {
    run(
        store,
        engine,
        "MATCH (a:R)-[:SAME_AS*1..3]-(b:R) WHERE id(a) < id(b) AND a.split <> b.split \
         RETURN id(a) AS a, id(b) AS b",
    )
    .iter()
    .map(|r| (int(r, "a"), int(r, "b")))
    .collect()
}

fn leak_pairs(store: &mut GraphStore, engine: &QueryEngine, config: &str) -> BTreeSet<(i64, i64)> {
    run(
        store,
        engine,
        &format!("CALL algo.splitLeakage({{{config}}}) YIELD node1, node2 RETURN id(node1) AS a, id(node2) AS b"),
    )
    .iter()
    .map(|r| (int(r, "a"), int(r, "b")))
    .collect()
}

/// A naive split, as a user would write it without this feature.
fn naive_split(store: &mut GraphStore, engine: &QueryEngine) {
    run(
        store,
        engine,
        "CALL algo.split({strategy: 'random', label: 'R', seed: 3, writeProperty: 'split'}) \
         YIELD node RETURN count(*)",
    );
}

#[test]
fn an_entity_split_keeps_every_same_as_chain_on_one_side_and_a_random_one_does_not() {
    let (mut store, engine) = records();
    let rows = run(
        &mut store,
        &engine,
        "CALL algo.split({strategy: 'entity', label: 'R', edgeType: 'SAME_AS', seed: 7, \
         writeProperty: 'split'}) YIELD node, split RETURN split, count(*) AS n",
    );
    let total: i64 = rows.iter().map(|r| int(r, "n")).sum();
    assert_eq!(total, 200, "every record is placed once");
    // The entity property is the ground truth, independent of the edges.
    let straddling = run(
        &mut store,
        &engine,
        "MATCH (n:R) WITH n.entity AS e, count(DISTINCT n.split) AS sides \
         WHERE sides > 1 RETURN count(*) AS c",
    );
    assert_eq!(
        int(&straddling[0], "c"),
        0,
        "an entity straddles the boundary"
    );
    assert!(cypher_entity_leaks(&mut store, &engine).is_empty());
    let config = "strategy: 'entity', label: 'R', edgeType: 'SAME_AS'";
    assert!(
        leak_pairs(&mut store, &engine, config).is_empty(),
        "a clean split reports a leak"
    );

    // The negative control: the check finds exactly the pairs Cypher finds.
    naive_split(&mut store, &engine);
    let expected = cypher_entity_leaks(&mut store, &engine);
    assert!(
        !expected.is_empty(),
        "the fixture must make a random split leak"
    );
    assert_eq!(leak_pairs(&mut store, &engine, config), expected);
    // `hops` bounds the walk: one hop finds only directly linked pairs.
    let one_hop: BTreeSet<(i64, i64)> = run(
        &mut store,
        &engine,
        "MATCH (a:R)-[:SAME_AS]-(b:R) WHERE id(a) < id(b) AND a.split <> b.split \
         RETURN id(a) AS a, id(b) AS b",
    )
    .iter()
    .map(|r| (int(r, "a"), int(r, "b")))
    .collect();
    assert_eq!(
        leak_pairs(&mut store, &engine, &format!("{config}, hops: 1")),
        one_hop
    );
}

#[test]
fn a_time_split_never_trains_on_the_future_and_the_check_catches_one_that_does() {
    let (mut store, engine) = records();
    run(
        &mut store,
        &engine,
        "CALL algo.split({strategy: 'time', label: 'R', property: 'ts', ratios: [0.7, 0.15, 0.15], \
         writeProperty: 'split'}) YIELD node RETURN count(*)",
    );
    let bound = |store: &mut GraphStore, split: &str, agg: &str| -> i64 {
        let q = format!("MATCH (n:R) WHERE n.split = '{split}' RETURN {agg}(n.ts) AS v");
        int(&run(store, &engine, &q)[0], "v")
    };
    assert!(bound(&mut store, "train", "max") <= bound(&mut store, "val", "min"));
    assert!(bound(&mut store, "val", "max") <= bound(&mut store, "test", "min"));
    let config = "strategy: 'time', label: 'R', property: 'ts'";
    assert!(leak_pairs(&mut store, &engine, config).is_empty());

    // Move one training record into the future: exactly that record leaks,
    // against the earliest record of val and of test.
    let late = run(
        &mut store,
        &engine,
        "MATCH (n:R) WHERE n.split = 'train' WITH n LIMIT 1 SET n.ts = 5000 RETURN id(n) AS id",
    );
    let late = int(&late[0], "id");
    let rows = run(
        &mut store,
        &engine,
        &format!(
            "CALL algo.splitLeakage({{{config}}}) YIELD node1, split1, split2, reason \
                  RETURN id(node1) AS a, split1, split2, reason"
        ),
    );
    assert_eq!(rows.len(), 2, "one row against val, one against test");
    for r in &rows {
        assert_eq!(int(r, "a"), late);
        assert_eq!(text(r, "split1"), "train");
        assert!(text(r, "reason").contains("5000"), "{}", text(r, "reason"));
    }
}

#[test]
fn a_source_split_keeps_each_source_whole_and_a_random_one_does_not() {
    let (mut store, engine) = records();
    run(
        &mut store,
        &engine,
        "CALL algo.split({strategy: 'source', label: 'R', property: 'src', ratios: [0.6, 0.4], \
         seed: 1, writeProperty: 'split'}) YIELD node, group RETURN count(*)",
    );
    let sides = run(
        &mut store,
        &engine,
        "MATCH (n:R) WITH n.src AS s, count(DISTINCT n.split) AS k WHERE k > 1 RETURN count(*) AS c",
    );
    assert_eq!(int(&sides[0], "c"), 0);
    let config = "strategy: 'source', label: 'R', property: 'src'";
    assert!(leak_pairs(&mut store, &engine, config).is_empty());
    naive_split(&mut store, &engine);
    // Nine sources over 200 records: a random split spreads every one.
    let rows = run(
        &mut store,
        &engine,
        &format!("CALL algo.splitLeakage({{{config}}}) YIELD reason RETURN reason"),
    );
    assert!(rows.len() >= 9, "{} leaks", rows.len());
    assert!(rows
        .iter()
        .all(|r| text(r, "reason").contains("on both sides")));
}

#[test]
fn a_community_split_keeps_each_community_whole() {
    let mut store = GraphStore::new();
    let engine = QueryEngine::new();
    // Twelve 5-cliques joined in a ring by single edges: Louvain finds the cliques.
    for c in 0..12 {
        run(&mut store, &engine, &format!(
            "CREATE (a:P {{c: {c}}}), (b:P {{c: {c}}}), (x:P {{c: {c}}}), (d:P {{c: {c}}}), (e:P {{c: {c}}}), \
             (a)-[:L]->(b), (a)-[:L]->(x), (a)-[:L]->(d), (a)-[:L]->(e), (b)-[:L]->(x), \
             (b)-[:L]->(d), (b)-[:L]->(e), (x)-[:L]->(d), (x)-[:L]->(e), (d)-[:L]->(e)"
        ));
    }
    run(&mut store, &engine,
        "MATCH (a:P), (b:P) WHERE b.c = (a.c + 1) % 12 WITH a.c AS c, min(id(a)) AS ia, min(id(b)) AS ib \
         MATCH (a), (b) WHERE id(a) = ia AND id(b) = ib CREATE (a)-[:L]->(b)");
    let rows = run(
        &mut store,
        &engine,
        "CALL algo.split({strategy: 'community', label: 'P', seed: 5, writeProperty: 'split'}) \
         YIELD node, split, group RETURN split, count(DISTINCT group) AS g, count(*) AS n",
    );
    assert_eq!(rows.iter().map(|r| int(r, "n")).sum::<i64>(), 60);
    let sides = run(
        &mut store,
        &engine,
        "MATCH (n:P) WITH n.c AS c, count(DISTINCT n.split) AS k WHERE k > 1 RETURN count(*) AS c",
    );
    assert_eq!(int(&sides[0], "c"), 0, "a clique straddles the boundary");
    assert!(leak_pairs(&mut store, &engine, "strategy: 'community', label: 'P'").is_empty());
    run(&mut store, &engine,
        "CALL algo.split({strategy: 'random', label: 'P', seed: 3, writeProperty: 'split'}) YIELD node RETURN count(*)");
    assert!(!leak_pairs(&mut store, &engine, "strategy: 'community', label: 'P'").is_empty());
}

#[test]
fn the_seed_decides_the_split_and_the_read_path_needs_no_write_access() {
    let (mut store, engine) = records();
    let q = |seed: i64| {
        format!(
            "CALL algo.split({{strategy: 'entity', label: 'R', edgeType: 'SAME_AS', seed: {seed}}}) \
             YIELD node, split RETURN id(node) AS id, split ORDER BY id"
        )
    };
    let read = |store: &GraphStore, seed: i64| -> Vec<(i64, String)> {
        engine
            .execute(&q(seed), store)
            .expect("a split without writeProperty is a read")
            .records
            .iter()
            .map(|r| (int(r, "id"), text(r, "split")))
            .collect()
    };
    assert_eq!(read(&store, 9), read(&store, 9));
    assert_ne!(read(&store, 9), read(&store, 10));
    // The plain call wrote nothing.
    let stored = run(
        &mut store,
        &engine,
        "MATCH (n:R) WHERE n.split IS NOT NULL RETURN count(*) AS c",
    );
    assert_eq!(int(&stored[0], "c"), 0);
}

#[test]
fn requests_that_cannot_be_honoured_are_refused_with_the_reason() {
    let (mut store, engine) = records();
    let cases: &[(&str, &str)] = &[
        ("CALL algo.split({label: 'R'}) YIELD node RETURN node", "`strategy` is required"),
        ("CALL algo.split({strategy: 'magic'}) YIELD node RETURN node", "unknown strategy"),
        ("CALL algo.split({strategy: 'entity'}) YIELD node RETURN node", "needs `edgeType`"),
        ("CALL algo.split({strategy: 'time'}) YIELD node RETURN node", "needs `property`"),
        ("CALL algo.split({strategy: 'random', property: 'ts'}) YIELD node RETURN node", "does not read `property`"),
        ("CALL algo.split({strategy: 'random', ratios: [0.5, 0.2]}) YIELD node RETURN node", "`ratios`"),
        ("CALL algo.split({strategy: 'random', ratios: [0.5, 0.25, 0.125, 0.125]}) YIELD node RETURN node", "`ratios`"),
        ("CALL algo.split({strategy: 'random', seeed: 1}) YIELD node RETURN node", "unknown config key `seeed`"),
        ("CALL algo.splitLeakage({strategy: 'random'}) YIELD reason RETURN reason", "no boundary to check"),
        ("CALL algo.splitLeakage({strategy: 'entity', label: 'R', edgeType: 'SAME_AS'}) YIELD reason RETURN reason", "have none"),
        ("CALL algo.splitLeakage({strategy: 'source', property: 'src', hops: 2}) YIELD reason RETURN reason", "`hops`"),
    ];
    for (q, needle) in cases {
        let e = err(&mut store, &engine, q);
        assert!(e.contains(needle), "{q}\n  -> {e}\n  expected `{needle}`");
    }
    // A node missing the property is refused, not quietly left out.
    run(&mut store, &engine, "CREATE (:R {entity: 999})");
    let e = err(
        &mut store,
        &engine,
        "CALL algo.split({strategy: 'time', label: 'R', property: 'ts'}) YIELD node RETURN node",
    );
    assert!(e.contains("1 of 201 node(s) have none"), "{e}");
    // A stored value that is not a split name is refused.
    run(&mut store, &engine, "MATCH (n:R) SET n.split = 'train'");
    run(
        &mut store,
        &engine,
        "MATCH (n:R {entity: 999}) SET n.split = 'holdout'",
    );
    let e = err(&mut store, &engine, "CALL algo.splitLeakage({strategy: 'entity', label: 'R', edgeType: 'SAME_AS'}) YIELD reason RETURN reason");
    assert!(e.contains("'holdout'") || e.contains("\"holdout\""), "{e}");
}
