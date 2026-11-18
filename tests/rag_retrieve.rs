//! `ai.rag.retrieve` returns the retrieved context with the evidence for each
//! row: the seed it came from, that seed's score, the walk from it, and the
//! node's recorded source (#2098).
//!
//! Every expected row below is derivable by hand from the fixture.

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::record::Value;
use samyama::query::QueryEngine;

// Fixture
//
//   Chunk c1 [1, 0]    best match for [1, 0]
//   Chunk c2 [0.6, 0.8] second
//   Chunk c3 [0, 1]    worst
//
//   edges, in creation (= id) order:
//     c1 -MENTIONS-> e1
//     e1 -RELATED->  e2
//     e2 -RELATED->  e3
//     c2 -MENTIONS-> e1      e1 is c1's: c1 scores higher
//     c2 -MENTIONS-> e4
//     d1 -HAS_CHUNK-> c1     reached against the edge's direction
//
//   c1 carries a source; nothing else does.
fn fixture() -> GraphStore {
    let mut s = GraphStore::new();
    let e = QueryEngine::new();
    for q in [
        "CREATE VECTOR INDEX chunk_emb FOR (n:Chunk) ON (n.emb) OPTIONS {dimensions: 2, similarity: 'cosine'}",
        "CREATE (:Chunk {name: 'c1', emb: [1.0, 0.0], __source_uri: 'https://example.org/a.pdf', \
         __retrieved_at: '2026-11-01', __chunk_id: 'a#3', __span: [120, 480]})",
        "CREATE (:Chunk {name: 'c2', emb: [0.6, 0.8]})",
        "CREATE (:Chunk {name: 'c3', emb: [0.0, 1.0]})",
        "CREATE (:Entity {name: 'e1'}), (:Entity {name: 'e2'}), (:Entity {name: 'e3'}), \
         (:Entity {name: 'e4'}), (:Doc {name: 'd1'})",
        "MATCH (a {name: 'c1'}), (b {name: 'e1'}) CREATE (a)-[:MENTIONS]->(b)",
        "MATCH (a {name: 'e1'}), (b {name: 'e2'}) CREATE (a)-[:RELATED]->(b)",
        "MATCH (a {name: 'e2'}), (b {name: 'e3'}) CREATE (a)-[:RELATED]->(b)",
        "MATCH (a {name: 'c2'}), (b {name: 'e1'}) CREATE (a)-[:MENTIONS]->(b)",
        "MATCH (a {name: 'c2'}), (b {name: 'e4'}) CREATE (a)-[:MENTIONS]->(b)",
        "MATCH (a {name: 'd1'}), (b {name: 'c1'}) CREATE (a)-[:HAS_CHUNK]->(b)",
    ] {
        e.execute_mut(q, &mut s, "default").unwrap_or_else(|err| panic!("{q}: {err}"));
    }
    s
}

fn text(v: Option<&Value>) -> String {
    match v {
        Some(Value::Property(PropertyValue::String(s))) => s.clone(),
        Some(Value::Property(PropertyValue::Integer(i))) => i.to_string(),
        Some(Value::Property(PropertyValue::Boolean(b))) => b.to_string(),
        Some(Value::Property(PropertyValue::Null)) | Some(Value::Null) | None => "null".into(),
        Some(Value::List(items)) => format!(
            "[{}]",
            items
                .iter()
                .map(|i| text(Some(i)))
                .collect::<Vec<_>>()
                .join(",")
        ),
        Some(Value::Property(PropertyValue::Array(items))) => format!(
            "[{}]",
            items
                .iter()
                .map(|i| text(Some(&Value::Property(i.clone()))))
                .collect::<Vec<_>>()
                .join(",")
        ),
        Some(other) => format!("{other:?}"),
    }
}

fn rows(s: &GraphStore, q: &str, cols: &[&str]) -> Vec<Vec<String>> {
    let b = QueryEngine::new()
        .execute(q, s)
        .unwrap_or_else(|e| panic!("{q}: {e}"));
    b.records
        .iter()
        .map(|r| cols.iter().map(|c| text(r.get(c))).collect())
        .collect()
}

fn err(s: &GraphStore, q: &str) -> String {
    match QueryEngine::new().execute(q, s) {
        Ok(_) => panic!("{q} should be refused"),
        Err(e) => e.to_string(),
    }
}

const WALK: &str = "CALL ai.rag.retrieve('Chunk', 'emb', [1.0, 0.0], 2, 2) \
    YIELD node, seed, score, hops, path, source \
    RETURN node.name AS n, seed.name AS s, hops AS h, [x IN nodes(path) | x.name] AS p, \
    [r IN relationships(path) | type(r)] AS t";

#[test]
fn seeds_first_then_each_seeds_walk_best_seed_first() {
    let got = rows(&fixture(), WALK, &["n", "s", "h", "p", "t"]);
    let want: Vec<Vec<String>> = [
        ["c1", "c1", "0", "[c1]", "[]"],
        ["c2", "c2", "0", "[c2]", "[]"],
        // c1's walk, edges in id order: e1 by MENTIONS, d1 against HAS_CHUNK.
        ["e1", "c1", "1", "[c1,e1]", "[MENTIONS]"],
        ["d1", "c1", "1", "[c1,d1]", "[HAS_CHUNK]"],
        ["e2", "c1", "2", "[c1,e1,e2]", "[MENTIONS,RELATED]"],
        // e1 is c1's already; c2 is left only e4. e3 is three hops out.
        ["e4", "c2", "1", "[c2,e4]", "[MENTIONS]"],
    ]
    .iter()
    .map(|r| r.iter().map(|c| c.to_string()).collect())
    .collect();
    assert_eq!(got, want);
}

#[test]
fn the_same_call_gives_the_same_rows() {
    let s = fixture();
    let cols = ["n", "s", "h", "p", "t"];
    let first = rows(&s, WALK, &cols);
    for _ in 0..5 {
        assert_eq!(rows(&s, WALK, &cols), first);
    }
}

#[test]
fn every_row_carries_its_seeds_score() {
    let s = fixture();
    let b = QueryEngine::new()
        .execute(
            "CALL ai.rag.retrieve('Chunk', 'emb', [1.0, 0.0], 2, 2) YIELD node, score \
             RETURN node.name AS n, score AS sc",
            &s,
        )
        .unwrap();
    let score = |name: &str| -> f64 {
        let r = b.records.iter().find(|r| text(r.get("n")) == name).unwrap();
        match r.get("sc") {
            Some(Value::Property(PropertyValue::Float(f))) => *f,
            other => panic!("{name}: {other:?}"),
        }
    };
    assert!(score("c1") > score("c2"), "c1 is the closer match");
    assert_eq!(score("e2"), score("c1"));
    assert_eq!(score("e4"), score("c2"));
}

#[test]
fn source_is_the_recorded_provenance_or_null() {
    let s = fixture();
    let got = rows(
        &s,
        "CALL ai.rag.retrieve('Chunk', 'emb', [1.0, 0.0], 1, 1) YIELD node, source \
         RETURN node.name AS n, source.__source_uri AS uri, source.__retrieved_at AS at, \
         source.__chunk_id AS chunk, source.__span AS span, source.__license AS licence, source IS NULL AS none",
        &["n", "uri", "at", "chunk", "span", "licence", "none"],
    );
    assert_eq!(got.len(), 3, "{got:?}");
    assert_eq!(
        got[0],
        [
            "c1",
            "https://example.org/a.pdf",
            "2026-11-01",
            "a#3",
            "[120,480]",
            "null",
            "false"
        ]
    );
    // Nothing recorded is null, not an empty map: unknown is not "no source".
    for row in &got[1..] {
        assert_eq!(row[6], "true", "{row:?}");
    }
}

#[test]
fn zero_hops_is_the_vector_search_alone() {
    let got = rows(
        &fixture(),
        "CALL ai.rag.retrieve('Chunk', 'emb', [1.0, 0.0], 3, 0) YIELD node, seed, hops \
         RETURN node.name AS n, seed.name AS s, hops AS h",
        &["n", "s", "h"],
    );
    assert_eq!(
        got,
        [["c1", "c1", "0"], ["c2", "c2", "0"], ["c3", "c3", "0"]]
    );
}

#[test]
fn yields_take_aliases_and_a_bare_call_yields_every_column() {
    let s = fixture();
    let got = rows(
        &s,
        "CALL ai.rag.retrieve('Chunk', 'emb', [1.0, 0.0], 1, 0) YIELD node AS chunk, hops AS depth \
         RETURN chunk.name AS n, depth AS d",
        &["n", "d"],
    );
    assert_eq!(got, [["c1", "0"]]);
    let b = QueryEngine::new()
        .execute("CALL ai.rag.retrieve('Chunk', 'emb', [1.0, 0.0], 1, 0)", &s)
        .unwrap();
    assert_eq!(b.records.len(), 1);
    for c in ["node", "seed", "score", "hops", "path", "source"] {
        assert!(b.records[0].get(c).is_some(), "no {c}");
    }
}

#[test]
fn a_bad_call_says_what_is_wrong() {
    let s = fixture();
    let e = err(&s, "CALL ai.rag.retrieve('Chunk', 'emb', [1.0, 0.0], 2)");
    assert!(e.contains("(label, property, queryVector, k, hops)"), "{e}");
    let e = err(&s, "CALL ai.rag.retrieve('Chunk', 'emb', [1.0, 0.0], 2, 4)");
    assert!(e.contains("between 0 and 3"), "{e}");
    let e = err(
        &s,
        "CALL ai.rag.retrieve('Chunk', 'emb', [1.0, 0.0], 2, -1)",
    );
    assert!(e.contains("non-negative"), "{e}");
    let e = err(
        &s,
        "CALL ai.rag.retrieve('Chunk', 'emb', [1.0, 0.0], 2, 1) YIELD nodes",
    );
    assert!(e.contains("there is no `nodes`"), "{e}");
    // The search's own refusals come through unchanged.
    let e = err(&s, "CALL ai.rag.retrieve('Chunk', 'emb', 'x', 2, 1)");
    assert!(e.contains("query vector"), "{e}");
    let e = err(&s, "CALL ai.rag.retreive('Chunk', 'emb', [1.0, 0.0], 2, 1)");
    assert!(e.contains("Did you mean `ai.rag.retrieve`"), "{e}");
}

#[test]
fn explain_shows_the_search_under_the_retrieval() {
    let b = QueryEngine::new()
        .execute(
            "EXPLAIN CALL ai.rag.retrieve('Chunk', 'emb', [1.0, 0.0], 2, 2) YIELD node RETURN node",
            &fixture(),
        )
        .unwrap();
    let plan = format!("{:?}", b.records);
    let rag = plan.find("RagRetrieve").expect("RagRetrieve in the plan");
    let search = plan.find("VectorSearch").expect("VectorSearch in the plan");
    assert!(rag < search, "{plan}");
}
