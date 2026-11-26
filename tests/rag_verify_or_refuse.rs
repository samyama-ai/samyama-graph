//! `ai.rag.verify_or_refuse` checks an answer's claims against the evidence
//! `ai.rag.retrieve` returned, and refuses with a typed reason when they are
//! not grounded (#2234, part C of #2098).
//!
//! The evidence is passed the way a client passes it: retrieved, turned into
//! plain values (ids, numbers, `{nodes, edges}` for a path), and sent back as
//! a parameter.

use std::collections::HashMap;

use samyama::graph::{EdgeId, GraphStore, PropertyValue};
use samyama::query::executor::record::Value;
use samyama::query::QueryEngine;

// Fixture
//
//   Chunk c1 [1, 0]   the only seed for [1, 0]; carries a source
//   Chunk c2 [0, 1]
//
//   edges, in creation (= id) order:
//     c1 -MENTIONS-> Aspirin {dose_mg: 100}
//     Aspirin -TREATS-> Headache
//     c1 -MENTIONS-> Nausea
//     Aspirin -CAUSES-> Nausea          both ends one hop from c1: on no walk
//     c2 -MENTIONS-> Ibuprofen
//     Ibuprofen -TREATS-> Fever         not retrieved from c1
//
// Retrieving [1, 0], k = 1, hops = 2 gives c1 (0 hops), Aspirin and Nausea
// (1 hop, by MENTIONS) and Headache (2 hops, MENTIONS then TREATS).
fn fixture() -> GraphStore {
    let mut s = GraphStore::new();
    let e = QueryEngine::new();
    for q in [
        "CREATE VECTOR INDEX chunk_emb FOR (n:Chunk) ON (n.emb) OPTIONS {dimensions: 2, similarity: 'cosine'}",
        "CREATE (:Chunk {name: 'c1', emb: [1.0, 0.0], __source_uri: 'https://example.org/a.pdf'})",
        "CREATE (:Chunk {name: 'c2', emb: [0.0, 1.0]})",
        "CREATE (:Entity {name: 'Aspirin', dose_mg: 100}), (:Entity {name: 'Headache'}), \
         (:Entity {name: 'Nausea'}), (:Entity {name: 'Ibuprofen'}), (:Entity {name: 'Fever'})",
        "MATCH (a {name: 'c1'}), (b {name: 'Aspirin'}) CREATE (a)-[:MENTIONS]->(b)",
        "MATCH (a {name: 'Aspirin'}), (b {name: 'Headache'}) CREATE (a)-[:TREATS]->(b)",
        "MATCH (a {name: 'c1'}), (b {name: 'Nausea'}) CREATE (a)-[:MENTIONS]->(b)",
        "MATCH (a {name: 'Aspirin'}), (b {name: 'Nausea'}) CREATE (a)-[:CAUSES]->(b)",
        "MATCH (a {name: 'c2'}), (b {name: 'Ibuprofen'}) CREATE (a)-[:MENTIONS]->(b)",
        "MATCH (a {name: 'Ibuprofen'}), (b {name: 'Fever'}) CREATE (a)-[:TREATS]->(b)",
    ] {
        e.execute_mut(q, &mut s, "default")
            .unwrap_or_else(|err| panic!("{q}: {err}"));
    }
    s
}

const RETRIEVE: &str = "CALL ai.rag.retrieve('Chunk', 'emb', [1.0, 0.0], 1, 2) \
    YIELD node, seed, score, path RETURN node, seed, score, path";

fn int(i: u64) -> PropertyValue {
    PropertyValue::Integer(i as i64)
}

fn s(x: &str) -> PropertyValue {
    PropertyValue::String(x.to_string())
}

fn map(entries: &[(&str, PropertyValue)]) -> PropertyValue {
    PropertyValue::Map(
        entries
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect(),
    )
}

fn id_of(v: Option<&Value>) -> u64 {
    match v {
        Some(Value::NodeRef(id)) | Some(Value::Node(id, _)) => id.as_u64(),
        other => panic!("not a node: {other:?}"),
    }
}

/// The retrieval's rows as a client sends them back: `{node, seed, score,
/// path: {nodes, edges}}` of plain ids.
fn rows_envelope(store: &GraphStore) -> PropertyValue {
    let b = QueryEngine::new().execute(RETRIEVE, store).unwrap();
    PropertyValue::Array(
        b.records
            .iter()
            .map(|r| {
                let (nodes, edges) = match r.get("path") {
                    Some(Value::Path { nodes, edges }) => (nodes.clone(), edges.clone()),
                    other => panic!("not a path: {other:?}"),
                };
                let score = match r.get("score") {
                    Some(Value::Property(p)) => p.clone(),
                    other => panic!("no score: {other:?}"),
                };
                map(&[
                    ("node", int(id_of(r.get("node")))),
                    ("seed", int(id_of(r.get("seed")))),
                    ("score", score),
                    (
                        "path",
                        map(&[
                            (
                                "nodes",
                                PropertyValue::Array(
                                    nodes.iter().map(|n| int(n.as_u64())).collect(),
                                ),
                            ),
                            (
                                "edges",
                                PropertyValue::Array(
                                    edges.iter().map(|e| int(e.as_u64())).collect(),
                                ),
                            ),
                        ]),
                    ),
                ])
            })
            .collect(),
    )
}

/// The same retrieval in `samyama-mcp`'s shape, ids as JSON strings, hops as
/// `{src, edge_type, dst}`.
fn mcp_envelope(store: &GraphStore) -> PropertyValue {
    let b = QueryEngine::new().execute(RETRIEVE, store).unwrap();
    let paths = b
        .records
        .iter()
        .map(|r| {
            let edges = match r.get("path") {
                Some(Value::Path { edges, .. }) => edges.clone(),
                other => panic!("not a path: {other:?}"),
            };
            let hops = edges
                .iter()
                .map(|e| {
                    let e = store.get_edge(*e).unwrap();
                    map(&[
                        ("src", s(&e.source.as_u64().to_string())),
                        ("edge_type", s(e.edge_type.as_str())),
                        ("dst", s(&e.target.as_u64().to_string())),
                    ])
                })
                .collect();
            let score = match r.get("score") {
                Some(Value::Property(p)) => p.clone(),
                other => panic!("no score: {other:?}"),
            };
            map(&[
                ("node_id", s(&id_of(r.get("node")).to_string())),
                ("seed_node_id", s(&id_of(r.get("seed")).to_string())),
                ("seed_vector_score", score),
                ("hop_sequence", PropertyValue::Array(hops)),
            ])
        })
        .collect();
    map(&[
        ("sources", PropertyValue::Array(vec![])),
        ("lineage_paths", PropertyValue::Array(paths)),
    ])
}

fn claim(subject: PropertyValue, predicate: &str, object: PropertyValue) -> PropertyValue {
    map(&[
        ("subject", subject),
        ("predicate", s(predicate)),
        ("object", object),
    ])
}

fn claims(cs: &[(&str, &str, PropertyValue)]) -> PropertyValue {
    PropertyValue::Array(
        cs.iter()
            .map(|(a, p, o)| claim(s(a), p, o.clone()))
            .collect(),
    )
}

const VERIFY: &str = "CALL ai.rag.verify_or_refuse($q, $claims, $evidence, $config) \
    YIELD status, verified_claims, unsupported_claims, abstain_reason, confidence, coverage, detail \
    RETURN status, verified_claims, unsupported_claims, abstain_reason, confidence, coverage, detail";

#[derive(Debug, PartialEq)]
struct Verdict {
    status: String,
    reason: Option<String>,
    verified: Vec<HashMap<String, Value>>,
    unsupported: Vec<HashMap<String, Value>>,
    confidence: Option<f64>,
    coverage: Option<f64>,
    detail: String,
}

fn run(
    store: &GraphStore,
    answer: PropertyValue,
    evidence: PropertyValue,
    config: PropertyValue,
) -> Result<Verdict, String> {
    let mut params = HashMap::new();
    params.insert("q".to_string(), s("What does aspirin treat?"));
    params.insert("claims".to_string(), answer);
    params.insert("evidence".to_string(), evidence);
    params.insert("config".to_string(), config);
    let b = QueryEngine::new()
        .execute_with_params(VERIFY, store, &params)
        .map_err(|e| e.to_string())?;
    assert_eq!(b.records.len(), 1, "one verdict");
    let r = &b.records[0];
    let text = |c: &str| match r.get(c) {
        Some(Value::Property(PropertyValue::String(x))) => Some(x.clone()),
        Some(Value::Null) | Some(Value::Property(PropertyValue::Null)) => None,
        other => panic!("{c}: {other:?}"),
    };
    let num = |c: &str| match r.get(c) {
        Some(Value::Property(PropertyValue::Float(f))) => Some(*f),
        Some(Value::Null) | Some(Value::Property(PropertyValue::Null)) => None,
        other => panic!("{c}: {other:?}"),
    };
    let list = |c: &str| -> Vec<HashMap<String, Value>> {
        match r.get(c) {
            Some(Value::List(items)) => items
                .iter()
                .map(|i| match i {
                    Value::Map(m) => m.iter().map(|(k, v)| (k.clone(), v.clone())).collect(),
                    other => panic!("{c}: {other:?}"),
                })
                .collect(),
            other => panic!("{c}: {other:?}"),
        }
    };
    Ok(Verdict {
        status: text("status").unwrap(),
        reason: text("abstain_reason"),
        verified: list("verified_claims"),
        unsupported: list("unsupported_claims"),
        confidence: num("confidence"),
        coverage: num("coverage"),
        detail: text("detail").unwrap(),
    })
}

fn verify(store: &GraphStore, answer: PropertyValue, config: PropertyValue) -> Verdict {
    run(store, answer, rows_envelope(store), config).unwrap()
}

fn get_int(m: &HashMap<String, Value>, k: &str) -> Option<i64> {
    match m.get(k) {
        Some(Value::Property(PropertyValue::Integer(i))) => Some(*i),
        _ => None,
    }
}

fn get_str(m: &HashMap<String, Value>, k: &str) -> Option<String> {
    match m.get(k) {
        Some(Value::Property(PropertyValue::String(x))) => Some(x.clone()),
        _ => None,
    }
}

fn node(store: &GraphStore, name: &str) -> u64 {
    let b = QueryEngine::new()
        .execute(&format!("MATCH (n {{name: '{name}'}}) RETURN n"), store)
        .unwrap();
    id_of(b.records[0].get("n"))
}

fn edge(store: &GraphStore, from: &str, ty: &str) -> u64 {
    let b = QueryEngine::new()
        .execute(
            &format!("MATCH ({{name: '{from}'}})-[r:{ty}]->() RETURN id(r) AS r"),
            store,
        )
        .unwrap();
    match b.records[0].get("r") {
        Some(Value::Property(PropertyValue::Integer(i))) => *i as u64,
        other => panic!("{other:?}"),
    }
}

/// c1's similarity to the query, as the retrieval reports it.
fn seed_score(store: &GraphStore) -> f64 {
    let b = QueryEngine::new().execute(RETRIEVE, store).unwrap();
    match b.records[0].get("score") {
        Some(Value::Property(PropertyValue::Float(f))) => *f,
        other => panic!("{other:?}"),
    }
}

fn none() -> PropertyValue {
    PropertyValue::Null
}

#[test]
fn a_claim_on_a_retrieved_path_is_verified_and_cited() {
    let st = fixture();
    let v = verify(&st, claims(&[("Aspirin", "TREATS", s("Headache"))]), none());
    assert_eq!(v.status, "VERIFIED");
    assert_eq!(v.reason, None);
    assert_eq!(v.coverage, Some(1.0));
    assert!(v.unsupported.is_empty());
    let c = &v.verified[0];
    assert_eq!(get_str(c, "kind").as_deref(), Some("relation"));
    assert_eq!(get_int(c, "seed"), Some(node(&st, "c1") as i64));
    assert_eq!(
        get_int(c, "edge"),
        Some(edge(&st, "Aspirin", "TREATS") as i64)
    );
    // MENTIONS, then TREATS: the cited edge is the walk's second.
    assert_eq!(get_int(c, "hops"), Some(2));
    match c.get("source") {
        Some(Value::Property(PropertyValue::Map(m))) => {
            assert_eq!(m.get("__source_uri"), Some(&s("https://example.org/a.pdf")))
        }
        other => panic!("source: {other:?}"),
    }
    // Two hops at the default decay of 0.5.
    let want = seed_score(&st) * 0.25;
    assert_eq!(v.confidence, Some(want));
}

#[test]
fn a_claim_outside_the_evidence_refuses() {
    let st = fixture();
    // Ibuprofen does treat fever, in the graph, but nothing retrieved says so.
    let v = verify(
        &st,
        claims(&[
            ("Aspirin", "TREATS", s("Headache")),
            ("Ibuprofen", "TREATS", s("Fever")),
        ]),
        none(),
    );
    assert_eq!(v.status, "REFUSED");
    assert_eq!(v.reason.as_deref(), Some("INSUFFICIENT_SUBGRAPH_EVIDENCE"));
    assert_eq!(v.coverage, Some(0.5));
    assert_eq!(v.verified.len(), 1);
    assert_eq!(
        get_str(&v.unsupported[0], "reason").as_deref(),
        Some("NOT_IN_EVIDENCE")
    );
    assert_eq!(
        get_str(&v.unsupported[0], "subject").as_deref(),
        Some("Ibuprofen")
    );
}

#[test]
fn min_coverage_sets_how_much_must_be_supported() {
    let st = fixture();
    let two = claims(&[
        ("Aspirin", "TREATS", s("Headache")),
        ("Ibuprofen", "TREATS", s("Fever")),
    ]);
    let half = map(&[("minCoverage", PropertyValue::Float(0.5))]);
    assert_eq!(verify(&st, two.clone(), half).status, "VERIFIED");
    let most = map(&[("minCoverage", PropertyValue::Float(0.75))]);
    assert_eq!(verify(&st, two, most).status, "REFUSED");
}

#[test]
fn a_property_claim_is_checked_against_the_node() {
    let st = fixture();
    let v = verify(&st, claims(&[("Aspirin", "dose_mg", int(100))]), none());
    assert_eq!(v.status, "VERIFIED");
    assert_eq!(get_str(&v.verified[0], "kind").as_deref(), Some("property"));
    // Numbers compare by value.
    let v = verify(
        &st,
        claims(&[("Aspirin", "dose_mg", PropertyValue::Float(100.0))]),
        none(),
    );
    assert_eq!(v.status, "VERIFIED");
}

#[test]
fn a_contradicted_claim_refuses_whatever_coverage_allows() {
    let st = fixture();
    let v = verify(
        &st,
        claims(&[
            ("Aspirin", "TREATS", s("Headache")),
            ("Aspirin", "dose_mg", int(500)),
        ]),
        map(&[("minCoverage", PropertyValue::Float(0.0))]),
    );
    assert_eq!(v.status, "REFUSED");
    assert_eq!(v.reason.as_deref(), Some("CONTRADICTORY_PATHS"));
    let u = &v.unsupported[0];
    assert_eq!(get_str(u, "reason").as_deref(), Some("CONTRADICTED"));
    assert_eq!(u.get("found"), Some(&Value::Property(int(100))));
}

#[test]
fn direction_matters_unless_switched_off() {
    let st = fixture();
    let backwards = claims(&[("Headache", "TREATS", s("Aspirin"))]);
    assert_eq!(verify(&st, backwards.clone(), none()).status, "REFUSED");
    let either = map(&[("directed", PropertyValue::Boolean(false))]);
    assert_eq!(verify(&st, backwards, either).status, "VERIFIED");
}

#[test]
fn names_match_ignoring_case_and_ids_match_too() {
    let st = fixture();
    let v = verify(
        &st,
        claims(&[("  aspirin", "treats", s("HEADACHE"))]),
        none(),
    );
    assert_eq!(v.status, "VERIFIED");
    let by_id = PropertyValue::Array(vec![claim(
        int(node(&st, "Aspirin")),
        "TREATS",
        int(node(&st, "Headache")),
    )]);
    assert_eq!(verify(&st, by_id, none()).status, "VERIFIED");
}

#[test]
fn an_edge_no_walk_took_counts_only_when_induced() {
    let st = fixture();
    // Aspirin and Nausea are both retrieved, each one hop from c1; the
    // CAUSES edge between them is on neither walk.
    let causes = claims(&[("Aspirin", "CAUSES", s("Nausea"))]);
    let v = verify(&st, causes.clone(), none());
    assert_eq!(v.status, "REFUSED");
    let v = verify(
        &st,
        causes,
        map(&[("induced", PropertyValue::Boolean(true))]),
    );
    assert_eq!(v.status, "VERIFIED");
    let c = &v.verified[0];
    assert_eq!(get_str(c, "kind").as_deref(), Some("induced"));
    assert_eq!(
        get_int(c, "edge"),
        Some(edge(&st, "Aspirin", "CAUSES") as i64)
    );
    // Both ends one hop out, and one more hop for the edge itself.
    assert_eq!(v.confidence, Some(seed_score(&st) * 0.25));
}

#[test]
fn an_envelope_edited_after_retrieval_grounds_nothing() {
    let st = fixture();
    let claim = claims(&[("Ibuprofen", "TREATS", s("Fever"))]);
    let c1 = int(node(&st, "c1"));
    let fever = int(node(&st, "Fever"));
    let ibu = int(node(&st, "Ibuprofen"));
    let treats = int(edge(&st, "Ibuprofen", "TREATS"));
    // A real edge, but it does not join the nodes the walk says it does.
    let wrong_join = map(&[
        ("node", fever.clone()),
        ("seed", c1.clone()),
        ("score", PropertyValue::Float(1.0)),
        (
            "path",
            map(&[
                (
                    "nodes",
                    PropertyValue::Array(vec![c1.clone(), fever.clone()]),
                ),
                ("edges", PropertyValue::Array(vec![treats])),
            ]),
        ),
    ]);
    // An edge that is not in the graph at all.
    let no_edge = map(&[
        ("node_id", fever.clone()),
        ("seed_node_id", ibu.clone()),
        ("seed_vector_score", PropertyValue::Float(1.0)),
        (
            "hop_sequence",
            PropertyValue::Array(vec![map(&[
                ("src", ibu),
                ("edge_type", s("CURES")),
                ("dst", fever),
            ])]),
        ),
    ]);
    let mut items = match rows_envelope(&st) {
        PropertyValue::Array(a) => a,
        _ => unreachable!(),
    };
    let honest = items.len();
    items.push(wrong_join);
    items.push(no_edge);
    let v = run(&st, claim, PropertyValue::Array(items), none()).unwrap();
    assert_eq!(v.status, "REFUSED");
    assert_eq!(v.reason.as_deref(), Some("INSUFFICIENT_SUBGRAPH_EVIDENCE"));
    assert!(
        v.detail
            .contains(&format!("{honest} evidence item(s) used, 2 rejected")),
        "{}",
        v.detail
    );
}

#[test]
fn the_mcp_envelope_gives_the_same_verdict() {
    let st = fixture();
    let answer = claims(&[
        ("Aspirin", "TREATS", s("Headache")),
        ("Aspirin", "dose_mg", int(100)),
        ("Ibuprofen", "TREATS", s("Fever")),
    ]);
    let config = map(&[("minCoverage", PropertyValue::Float(0.5))]);
    let rows = run(&st, answer.clone(), rows_envelope(&st), config.clone()).unwrap();
    let mcp = run(&st, answer, mcp_envelope(&st), config).unwrap();
    assert_eq!(rows.status, "VERIFIED");
    assert_eq!(rows, mcp);
}

#[test]
fn no_evidence_refuses_and_withheld_evidence_says_so() {
    let st = fixture();
    let answer = claims(&[("Aspirin", "TREATS", s("Headache"))]);
    let v = run(&st, answer.clone(), PropertyValue::Array(vec![]), none()).unwrap();
    assert_eq!(v.reason.as_deref(), Some("INSUFFICIENT_SUBGRAPH_EVIDENCE"));

    let withheld = PropertyValue::Array(vec![map(&[("redacted", PropertyValue::Boolean(true))])]);
    let v = run(&st, answer, withheld, none()).unwrap();
    assert_eq!(v.status, "REFUSED");
    assert_eq!(v.reason.as_deref(), Some("POLICY_REDACTED_CONTEXT"));

    // Usable evidence that misses a claim, and a policy withheld some: the
    // missing support may be what was withheld.
    let mut env = match mcp_envelope(&st) {
        PropertyValue::Map(m) => m,
        _ => unreachable!(),
    };
    env.insert("redacted_count".to_string(), int(2));
    let v = run(
        &st,
        claims(&[("Ibuprofen", "TREATS", s("Fever"))]),
        PropertyValue::Map(env),
        none(),
    )
    .unwrap();
    assert_eq!(v.reason.as_deref(), Some("POLICY_REDACTED_CONTEXT"));
}

#[test]
fn a_seed_below_the_similarity_floor_refuses() {
    let st = fixture();
    let answer = claims(&[("Aspirin", "TREATS", s("Headache"))]);
    let score = seed_score(&st);
    let v = verify(
        &st,
        answer.clone(),
        map(&[("minScore", PropertyValue::Float(score + 1.0))]),
    );
    assert_eq!(v.reason.as_deref(), Some("BELOW_SIMILARITY_FLOOR"));
    let v = verify(
        &st,
        answer,
        map(&[("minScore", PropertyValue::Float(score))]),
    );
    assert_eq!(v.status, "VERIFIED");
}

#[test]
fn a_weak_path_refuses_under_min_confidence() {
    let st = fixture();
    let score = seed_score(&st);
    // MENTIONS is one hop from the seed, TREATS two.
    let near = claims(&[("c1", "MENTIONS", s("Aspirin"))]);
    let far = claims(&[("Aspirin", "TREATS", s("Headache"))]);
    let floor = map(&[("minConfidence", PropertyValue::Float(score * 0.4))]);
    assert_eq!(verify(&st, near, floor.clone()).status, "VERIFIED");
    let v = verify(&st, far, floor);
    assert_eq!(v.reason.as_deref(), Some("LOW_PATH_CONFIDENCE"));
    // A gentler decay lets the same path through.
    let gentle = map(&[
        ("minConfidence", PropertyValue::Float(score * 0.4)),
        ("hopDecay", PropertyValue::Float(1.0)),
    ]);
    let far = claims(&[("Aspirin", "TREATS", s("Headache"))]);
    assert_eq!(verify(&st, far, gentle).status, "VERIFIED");
}

#[test]
fn without_claims_the_evidence_alone_is_checked() {
    let st = fixture();
    let v = verify(&st, none(), none());
    assert_eq!(v.status, "VERIFIED");
    assert_eq!(v.coverage, None);
    assert_eq!(v.confidence, Some(seed_score(&st)));
    let v = verify(&st, PropertyValue::Array(vec![]), none());
    assert_eq!(v.status, "VERIFIED");
}

#[test]
fn a_malformed_claim_is_unsupported_not_an_error() {
    let st = fixture();
    let answer = PropertyValue::Array(vec![map(&[("subject", s("Aspirin"))])]);
    let v = verify(&st, answer, none());
    assert_eq!(v.status, "REFUSED");
    assert_eq!(
        get_str(&v.unsupported[0], "reason").as_deref(),
        Some("MALFORMED")
    );
}

#[test]
fn the_same_inputs_give_the_same_verdict() {
    let st = fixture();
    let answer = claims(&[
        ("Aspirin", "TREATS", s("Headache")),
        ("Aspirin", "dose_mg", int(500)),
        ("Ibuprofen", "TREATS", s("Fever")),
        ("Aspirin", "CAUSES", s("Nausea")),
    ]);
    let config = map(&[
        ("induced", PropertyValue::Boolean(true)),
        ("directed", PropertyValue::Boolean(false)),
    ]);
    let first = format!("{:?}", verify(&st, answer.clone(), config.clone()));
    for _ in 0..5 {
        assert_eq!(
            format!("{:?}", verify(&st, answer.clone(), config.clone())),
            first
        );
    }
}

#[test]
fn refusals_name_what_is_wrong() {
    let st = fixture();
    let e = run(
        &st,
        s("Aspirin treats headaches."),
        rows_envelope(&st),
        none(),
    )
    .unwrap_err();
    assert!(e.contains("free text"), "{e}");
    let e = run(
        &st,
        none(),
        rows_envelope(&st),
        map(&[("minCoverge", PropertyValue::Float(0.5))]),
    )
    .unwrap_err();
    assert!(e.contains("`minCoverge`"), "{e}");
    let e = run(
        &st,
        none(),
        rows_envelope(&st),
        map(&[("hopDecay", PropertyValue::Float(2.0))]),
    )
    .unwrap_err();
    assert!(e.contains("hopDecay"), "{e}");
    let e = run(&st, none(), s("not an envelope"), none()).unwrap_err();
    assert!(e.contains("evidence envelope"), "{e}");
    let e = QueryEngine::new()
        .execute(
            "CALL ai.rag.verify_or_refuse('q') YIELD status RETURN status",
            &st,
        )
        .unwrap_err()
        .to_string();
    assert!(e.contains("takes (query, candidate_answer"), "{e}");
    let e = QueryEngine::new()
        .execute(
            "CALL ai.rag.verify_or_refuse('q', null, [], null) YIELD verdict RETURN verdict",
            &st,
        )
        .unwrap_err()
        .to_string();
    assert!(e.contains("there is no `verdict`"), "{e}");
}

#[test]
fn edge_ids_in_the_envelope_are_checked_not_trusted() {
    // An id past the end of the edge table is simply not an edge.
    let st = fixture();
    let c1 = int(node(&st, "c1"));
    let aspirin = int(node(&st, "Aspirin"));
    let item = map(&[
        ("node", aspirin.clone()),
        ("seed", c1.clone()),
        ("score", PropertyValue::Float(1.0)),
        (
            "path",
            map(&[
                ("nodes", PropertyValue::Array(vec![c1, aspirin])),
                (
                    "edges",
                    PropertyValue::Array(vec![int(EdgeId::new(9_999).as_u64())]),
                ),
            ]),
        ),
    ]);
    let v = run(
        &st,
        claims(&[("c1", "MENTIONS", s("Aspirin"))]),
        PropertyValue::Array(vec![item]),
        none(),
    )
    .unwrap();
    assert_eq!(v.reason.as_deref(), Some("INSUFFICIENT_SUBGRAPH_EVIDENCE"));
}
