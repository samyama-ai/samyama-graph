//! Answer-time policy on `ai.rag.retrieve` (#2233, part B of #2098): what a
//! principal's `rag_hide=` rules withhold never reaches the retrieved context,
//! and what was withheld is reported as counts and rules.
//!
//! Every test runs its statements inside `rag_policy::with`, as the listeners
//! do, because one test turns on `require_scope` for this whole binary.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use samyama::auth::{rag_policy_of, Credential};
use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::rag_policy::{self, RagPolicy, Rule};
use samyama::query::executor::record::Value;
use samyama::query::QueryEngine;

// The fixture of tests/rag_retrieve.rs:
//
//   Chunk c1 [1, 0], c2 [0.6, 0.8], c3 [0, 1]
//     c1 -MENTIONS-> e1, e1 -RELATED-> e2, e2 -RELATED-> e3,
//     c2 -MENTIONS-> e1, c2 -MENTIONS-> e4, d1 -HAS_CHUNK-> c1
//   c1 carries a source and a `__chunk_id`.
//
// Unrestricted, `retrieve('Chunk', 'emb', [1, 0], 2, 2)` gives
// c1, c2, e1, d1, e2, e4.
fn fixture() -> GraphStore {
    let mut s = GraphStore::new();
    let e = QueryEngine::new();
    for q in [
        "CREATE VECTOR INDEX chunk_emb FOR (n:Chunk) ON (n.emb) OPTIONS {dimensions: 2, similarity: 'cosine'}",
        "CREATE (:Chunk {name: 'c1', emb: [1.0, 0.0], __source_uri: 'https://example.org/a.pdf', \
         __chunk_id: 'a#3'})",
        "CREATE (:Chunk {name: 'c2', emb: [0.6, 0.8]})",
        "CREATE (:Chunk {name: 'c3', emb: [0.0, 1.0]})",
        "CREATE (:Entity {name: 'e1', ssn: 'x1'}), (:Entity {name: 'e2'}), (:Entity {name: 'e3'}), \
         (:Entity {name: 'e4', ssn: 'x4'}), (:Doc {name: 'd1'})",
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

fn policy(spec: &str) -> Arc<RagPolicy> {
    Arc::new(RagPolicy::parse(spec).unwrap())
}

fn open() -> Arc<RagPolicy> {
    Arc::new(RagPolicy::unrestricted())
}

fn run(s: &GraphStore, p: &Arc<RagPolicy>, q: &str) -> Result<Vec<HashMap<String, Value>>, String> {
    rag_policy::with(p.clone(), || QueryEngine::new().execute(q, s))
        .map(|b| {
            b.records
                .iter()
                .map(|r| {
                    r.bindings()
                        .iter()
                        .map(|(k, v)| (k.to_string(), v.clone()))
                        .collect()
                })
                .collect()
        })
        .map_err(|e| e.to_string())
}

/// Node id to name, read unrestricted.
fn names(s: &GraphStore) -> HashMap<i64, String> {
    run(s, &open(), "MATCH (n) RETURN id(n) AS id, n.name AS name")
        .unwrap()
        .into_iter()
        .map(|r| (int(&r["id"]), string(&r["name"])))
        .collect()
}

fn int(v: &Value) -> i64 {
    match v {
        Value::Property(PropertyValue::Integer(i)) => *i,
        other => panic!("not an integer: {other:?}"),
    }
}

fn string(v: &Value) -> String {
    match v {
        Value::Property(PropertyValue::String(s)) => s.clone(),
        other => panic!("not a string: {other:?}"),
    }
}

fn map(v: &Value) -> HashMap<String, PropertyValue> {
    match v {
        Value::Property(PropertyValue::Map(m)) => m.clone(),
        other => panic!("not a map: {other:?}"),
    }
}

/// `{nodes, edges, properties, rules}` as comparable values.
fn report(v: &Value) -> (i64, i64, i64, BTreeMap<String, i64>) {
    let m = map(v);
    let n = |k: &str| match &m[k] {
        PropertyValue::Integer(i) => *i,
        other => panic!("{k}: {other:?}"),
    };
    let rules = match &m["rules"] {
        PropertyValue::Map(r) => r
            .iter()
            .map(|(k, v)| match v {
                PropertyValue::Integer(i) => (k.clone(), *i),
                other => panic!("{k}: {other:?}"),
            })
            .collect(),
        other => panic!("rules: {other:?}"),
    };
    (n("nodes"), n("edges"), n("properties"), rules)
}

fn rules(pairs: &[(&str, i64)]) -> BTreeMap<String, i64> {
    pairs.iter().map(|(k, v)| (k.to_string(), *v)).collect()
}

const IDS: &str = "CALL ai.rag.retrieve('Chunk', 'emb', [1.0, 0.0], 2, 2) \
    YIELD node_id, seed_id, hops, hop_sequence, labels, properties, source, withheld";

/// `(node, seed, every node id on the walk)` per row, by name.
fn walk(s: &GraphStore, p: &Arc<RagPolicy>) -> Vec<(String, String, Vec<String>)> {
    let names = names(s);
    run(s, p, IDS)
        .unwrap()
        .iter()
        .map(|r| {
            let mut on_walk = Vec::new();
            if let Value::Property(PropertyValue::Array(hops)) = &r["hop_sequence"] {
                for h in hops {
                    if let PropertyValue::Array(t) = h {
                        for end in [&t[0], &t[2]] {
                            if let PropertyValue::Integer(i) = end {
                                on_walk.push(names[i].clone());
                            }
                        }
                    }
                }
            }
            on_walk.sort();
            on_walk.dedup();
            (
                names[&int(&r["node_id"])].clone(),
                names[&int(&r["seed_id"])].clone(),
                on_walk,
            )
        })
        .collect()
}

#[test]
fn with_no_rules_the_context_is_the_unrestricted_one_and_nothing_is_withheld() {
    let s = fixture();
    let got: Vec<String> = walk(&s, &open()).into_iter().map(|(n, _, _)| n).collect();
    assert_eq!(got, ["c1", "c2", "e1", "d1", "e2", "e4"]);
    for r in run(&s, &open(), IDS).unwrap() {
        assert_eq!(report(&r["withheld"]), (0, 0, 0, BTreeMap::new()));
    }
}

#[test]
fn a_withheld_label_is_dropped_and_not_walked_through() {
    let s = fixture();
    let got = walk(&s, &policy("label/Entity"));
    // e1 and e4 are met and withheld; e2 lies only beyond e1, so it is
    // neither in the context nor counted.
    let nodes: Vec<&str> = got.iter().map(|(n, _, _)| n.as_str()).collect();
    assert_eq!(nodes, ["c1", "c2", "d1"]);
    for (_, _, on_walk) in &got {
        assert!(on_walk.iter().all(|n| !n.starts_with('e')), "{got:?}");
    }
    let r = run(&s, &policy("label/Entity"), IDS).unwrap();
    assert_eq!(
        report(&r[0]["withheld"]),
        (2, 0, 0, rules(&[("label/Entity", 2)]))
    );
}

#[test]
fn a_withheld_edge_type_is_not_walked() {
    let s = fixture();
    let p = policy("edge/MENTIONS");
    let got = walk(&s, &p);
    let nodes: Vec<&str> = got.iter().map(|(n, _, _)| n.as_str()).collect();
    assert_eq!(nodes, ["c1", "c2", "d1"]);
    let r = run(&s, &p, IDS).unwrap();
    // c1-e1, c2-e1 and c2-e4: three edges, no nodes.
    assert_eq!(
        report(&r[0]["withheld"]),
        (0, 3, 0, rules(&[("edge/MENTIONS", 3)]))
    );
    // The path that remains is reported in the edge's own direction.
    let d1 = r.iter().find(|row| int(&row["hops"]) == 1).unwrap();
    let names = names(&s);
    match &d1["hop_sequence"] {
        Value::Property(PropertyValue::Array(h)) => match &h[0] {
            PropertyValue::Array(t) => {
                assert_eq!(
                    names[&match t[0] {
                        PropertyValue::Integer(i) => i,
                        _ => panic!(),
                    }],
                    "d1"
                );
                assert_eq!(t[1], PropertyValue::String("HAS_CHUNK".into()));
            }
            other => panic!("{other:?}"),
        },
        other => panic!("{other:?}"),
    }
}

#[test]
fn label_and_edge_rules_combine() {
    // d1 is a Doc and e2 lies past a RELATED edge; everything else stays,
    // from the seed that reached it first.
    let s = fixture();
    let got = walk(&s, &policy("label/Doc,edge/RELATED"));
    let nodes: Vec<(&str, &str)> = got
        .iter()
        .map(|(n, sd, _)| (n.as_str(), sd.as_str()))
        .collect();
    assert_eq!(
        nodes,
        [("c1", "c1"), ("c2", "c2"), ("e1", "c1"), ("e4", "c2")]
    );
}

#[test]
fn withheld_properties_leave_properties_and_source_and_close_the_entity_columns() {
    let s = fixture();
    let p = policy("prop/__chunk_id,prop/Entity.ssn");
    let r = run(&s, &p, IDS).unwrap();
    assert_eq!(r.len(), 6);
    for row in &r {
        let props = map(&row["properties"]);
        assert!(
            !props.contains_key("ssn") && !props.contains_key("__chunk_id"),
            "{props:?}"
        );
        // Only the withheld keys go: names stay.
        assert!(props.contains_key("name"), "{props:?}");
        if let Value::Property(PropertyValue::Map(src)) = &row["source"] {
            assert!(!src.contains_key("__chunk_id"), "{src:?}");
            assert!(src.contains_key("__source_uri"), "{src:?}");
        }
    }
    // e1's and e4's ssn, c1's chunk id.
    assert_eq!(
        report(&r[0]["withheld"]),
        (
            0,
            0,
            3,
            rules(&[("prop/Entity.ssn", 2), ("prop/__chunk_id", 1)])
        )
    );

    // An entity column would render every property from the store.
    for col in ["node", "seed", "path"] {
        let e = run(
            &s,
            &p,
            &format!(
                "CALL ai.rag.retrieve('Chunk', 'emb', [1.0, 0.0], 2, 2) YIELD {col} RETURN {col}"
            ),
        )
        .unwrap_err();
        assert!(e.contains(&format!("`{col}` is closed")), "{e}");
    }
    // A bare call yields every column that is still open.
    let bare = run(
        &s,
        &p,
        "CALL ai.rag.retrieve('Chunk', 'emb', [1.0, 0.0], 2, 2)",
    )
    .unwrap();
    assert!(
        !bare[0].contains_key("node") && !bare[0].contains_key("path"),
        "{:?}",
        bare[0].keys()
    );
    assert!(bare[0].contains_key("properties") && bare[0].contains_key("node_id"));
    // Without property rules the entity columns are open.
    assert!(run(
        &s,
        &policy("label/Doc"),
        "CALL ai.rag.retrieve('Chunk', 'emb', [1.0, 0.0], 2, 2) YIELD node RETURN node.name AS n"
    )
    .is_ok());
}

#[test]
fn nothing_withheld_appears_anywhere_in_the_rows() {
    // Zero leakage, checked against the rendered rows rather than column by
    // column: no withheld id, type or value appears in any of them.
    let s = fixture();
    let names = names(&s);
    let id_of = |n: &str| *names.iter().find(|(_, v)| *v == n).unwrap().0;
    let p = policy("label/Doc,edge/RELATED,prop/ssn");
    // `withheld` names the rules, which is the report, not a leak.
    let rows: Vec<Vec<(String, Value)>> = run(&s, &p, IDS)
        .unwrap()
        .into_iter()
        .map(|r| r.into_iter().filter(|(k, _)| k != "withheld").collect())
        .collect();
    let rendered = format!("{rows:?}");
    assert!(
        !rendered.contains("x1") && !rendered.contains("x4"),
        "{rendered}"
    );
    assert!(
        !rendered.contains("RELATED") && !rendered.contains("\"d1\""),
        "{rendered}"
    );
    for row in run(&s, &p, IDS).unwrap() {
        let id = int(&row["node_id"]);
        assert_ne!(id, id_of("d1"));
        assert_ne!(id, id_of("e2"));
    }
}

#[test]
fn withheld_reports_in_one_row_even_when_the_context_is_empty() {
    let s = fixture();
    let p = policy("label/Chunk");
    assert!(run(&s, &p, IDS).unwrap().is_empty());
    let r = run(
        &s,
        &p,
        "CALL ai.rag.withheld('Chunk', 'emb', [1.0, 0.0], 2, 2)",
    )
    .unwrap();
    assert_eq!(r.len(), 1);
    assert_eq!(int(&r[0]["nodes"]), 2);
    assert_eq!(int(&r[0]["edges"]), 0);
    assert_eq!(int(&r[0]["properties"]), 0);
    assert_eq!(
        map(&r[0]["rules"]).get("label/Chunk"),
        Some(&PropertyValue::Integer(2))
    );
    // Unrestricted, one row of zeros.
    let r = run(
        &s,
        &open(),
        "CALL ai.rag.withheld('Chunk', 'emb', [1.0, 0.0], 2, 2) YIELD nodes AS n",
    )
    .unwrap();
    assert_eq!(r.len(), 1);
    assert_eq!(int(&r[0]["n"]), 0);
    let e = run(
        &s,
        &open(),
        "CALL ai.rag.withheld('Chunk', 'emb', [1.0, 0.0], 2, 2) YIELD node",
    )
    .unwrap_err();
    assert!(e.contains("there is no `node`"), "{e}");
}

#[test]
fn the_same_policy_gives_the_same_rows_and_report() {
    let s = fixture();
    let p = policy("label/Doc,prop/Entity.ssn");
    let first = format!("{:?}", walk(&s, &p));
    let report_first = report(&run(&s, &p, IDS).unwrap()[0]["withheld"]);
    for _ in 0..5 {
        assert_eq!(format!("{:?}", walk(&s, &p)), first);
        assert_eq!(
            report(&run(&s, &p, IDS).unwrap()[0]["withheld"]),
            report_first
        );
    }
}

#[test]
fn the_result_cache_does_not_serve_one_principals_retrieval_to_another() {
    let s = fixture();
    let engine = QueryEngine::new();
    let q =
        "CALL ai.rag.retrieve('Chunk', 'emb', [1.0, 0.0], 2, 2) YIELD node_id RETURN count(*) AS c";
    let count = |p: Arc<RagPolicy>| -> (i64, bool) {
        let (b, hit) = rag_policy::with(p, || engine.execute_cached(q, &s)).unwrap();
        (int(b.records[0].get("c").unwrap()), hit)
    };
    assert_eq!(count(open()), (6, false));
    assert_eq!(count(policy("label/Entity")), (3, false));
    assert_eq!(count(policy("label/Entity")), (3, true));
    assert_eq!(count(open()), (6, true));
}

#[test]
fn rules_come_from_the_credential_line() {
    const D: &str = "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08";
    let c = Credential::parse(&format!(
        "hr:{D}:roles=read:rag_hide=label/Salary, edge/REPORTS_TO,prop/ssn,prop/Person.dob"
    ))
    .unwrap()
    .unwrap();
    let got: Vec<Rule> = c.rag_policy().rules().cloned().collect();
    assert_eq!(
        got,
        [
            Rule::Label("Salary".into()),
            Rule::Edge("REPORTS_TO".into()),
            Rule::Property(None, "ssn".into()),
            Rule::Property(Some("Person".into()), "dob".into()),
        ]
    );
    // Either order with the other fields.
    let c = Credential::parse(&format!("hr:{D}:rag_hide=label/Salary:tenant=default"))
        .unwrap()
        .unwrap();
    assert_eq!(c.tenant.as_deref(), Some("default"));
    assert!(!c.rag_policy().is_empty());
    // No field, no rules; no credential, no rules.
    let c = Credential::parse(&format!("ops:{D}")).unwrap().unwrap();
    assert!(c.rag_hide.is_none() && c.rag_policy().is_empty());
    assert!(rag_policy_of(None).is_empty());
    // A rule that does not parse refuses the line, rather than hiding less.
    for bad in ["lable/Salary", "label/", "Salary", "prop/Person."] {
        let r = Credential::parse(&format!("hr:{D}:rag_hide={bad}")).unwrap();
        assert!(r.is_err(), "{bad} accepted");
    }
}

#[test]
fn once_rules_are_configured_an_unscoped_retrieval_is_refused() {
    let s = fixture();
    rag_policy::require_scope();
    let e = QueryEngine::new()
        .execute(
            "CALL ai.rag.retrieve('Chunk', 'emb', [1.0, 0.0], 2, 2) YIELD node_id",
            &s,
        )
        .unwrap_err()
        .to_string();
    assert!(e.contains("did not say whose rules apply"), "{e}");
    // Inside a scope it runs, also with no rules.
    assert_eq!(run(&s, &open(), IDS).unwrap().len(), 6);
}
