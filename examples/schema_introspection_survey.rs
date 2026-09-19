//! What a model can learn about a graph, and in how many calls (AI-05).
//!
//! AI-05 asks for "a compact, token-budgeted schema description (labels,
//! types, cardinalities, property distributions, sample values, example
//! queries) retrievable in one call". Six named things and a budget, so the
//! measurement is six booleans, a call count, and whether the budget is
//! honoured -- not one verdict.
//!
//! **Coverage is measured against a graph whose schema is known**, not against
//! what the engine reports. A probe that asks the engine what the schema is
//! and then checks the engine agrees is a check that cannot fail: the fixture
//! below is built with an exact set of labels, triples and property keys
//! written down in the source, and the surfaces are scored against that set.
//! Two of those keys are set only on the very last edge of a 100,001-edge
//! type, because that is precisely where a capped scan stops looking (#1348).
//!
//! ```text
//! cargo run --release --example schema_introspection_survey -- --json out.json
//! ```

use std::collections::BTreeSet;

use samyama::graph::{GraphStore, PropertyMap, PropertyValue};
use samyama::query::executor::record::Value;
use samyama::query::QueryEngine;

/// Edges of the bulk shape, so the rare shape sits far past any sampling cap.
const BULK: usize = 100_000;

/// The schema the fixture is built to have. Written down, not discovered.
const EXPECTED_LABELS: &[&str] = &["Person", "Employee", "Company", "Archive", "Ledger"];
const EXPECTED_TRIPLES: &[(&str, &str, &str)] = &[
    ("Person", "WORKS_AT", "Company"),
    ("Employee", "WORKS_AT", "Company"),
    ("Archive", "AUDITS", "Ledger"),
];
/// Node property keys. `nickname` is set on one `Person` in fifty, which is
/// what a one-node property probe misses.
const EXPECTED_NODE_KEYS: &[&str] = &["name", "city", "nickname"];
/// Edge property key, set only on the last edge of the 100,001-edge type.
const EXPECTED_EDGE_KEYS: &[&str] = &["audited_by"];

fn fixture() -> GraphStore {
    let mut store = GraphStore::new();
    let engine = QueryEngine::new();
    let run = |q: &str, s: &mut GraphStore| {
        engine.execute_mut(q, s, "default").expect(q);
    };

    run(
        "UNWIND range(1, 50) AS i CREATE (:Person {name: 'p' + toString(i), \
         city: CASE WHEN i % 3 = 0 THEN 'Pune' ELSE 'Delhi' END})",
        &mut store,
    );
    // One Person in fifty carries `nickname`; a `keys(n) LIMIT 1` probe sees it
    // with probability 1/50.
    run(
        "MATCH (p:Person) WITH p LIMIT 1 SET p.nickname = 'ace'",
        &mut store,
    );
    run("CREATE (:Person:Employee {name: 'dual', city: 'Delhi'})", &mut store);
    run("CREATE (:Company {name: 'Acme'})", &mut store);
    run(
        "MATCH (p:Person), (c:Company) WITH p, c LIMIT 20 CREATE (p)-[:WORKS_AT]->(c)",
        &mut store,
    );
    // The dual-labelled node gets its edge explicitly. Letting the bulk MATCH
    // above pick it up is a coin toss, and a fixture that sometimes contains
    // the shape it is testing for measures the coin rather than the engine.
    run(
        "MATCH (p:Employee), (c:Company) CREATE (p)-[:WORKS_AT]->(c)",
        &mut store,
    );

    // The rare shape: 100,000 (:Archive)-[:AUDITS]->(:Ledger) edges carrying
    // nothing, then one that carries `audited_by`.
    let a = store.create_node("Archive");
    let l = store.create_node("Ledger");
    for _ in 0..BULK {
        store.create_edge(a, l, "AUDITS").expect("bulk");
    }
    let mut props = PropertyMap::new();
    props.insert("audited_by".to_string(), PropertyValue::String("cag".into()));
    store
        .create_edge_with_properties(a, l, "AUDITS", props)
        .expect("rare");
    store
}

fn strings(store: &mut GraphStore, q: &str) -> Vec<Vec<String>> {
    let engine = QueryEngine::new();
    match engine.execute_mut(q, store, "default") {
        Ok(batch) => batch
            .records
            .iter()
            .map(|r| {
                r.bindings()
                    .iter()
                    .map(|(_, v)| match v {
                        Value::Property(PropertyValue::String(s)) => s.clone(),
                        other => format!("{other:?}"),
                    })
                    .collect()
            })
            .collect(),
        Err(_) => Vec::new(),
    }
}

/// The `schema` string, `estimated_tokens` and `complete` from one forLLM call,
/// or `None` if the procedure does not exist.
fn for_llm(store: &mut GraphStore, arg: &str) -> Option<(String, i64, bool)> {
    let engine = QueryEngine::new();
    let q = format!("CALL db.schema.forLLM({arg})");
    let batch = engine.execute_mut(&q, store, "default").ok()?;
    let rec = batch.records.first()?;
    let mut text = String::new();
    let mut tokens = 0i64;
    let mut complete = false;
    for (name, v) in rec.bindings() {
        match (&name[..], v) {
            ("schema", Value::Property(PropertyValue::String(s))) => text = s.clone(),
            ("estimated_tokens", Value::Property(PropertyValue::Integer(n))) => tokens = *n,
            ("complete", Value::Property(PropertyValue::Boolean(b))) => complete = *b,
            _ => {}
        }
    }
    Some((text, tokens, complete))
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let json_out = args
        .iter()
        .position(|a| a == "--json")
        .and_then(|i| args.get(i + 1))
        .cloned();

    let mut store = fixture();

    // --- what each surface recovers, against the written-down schema --------

    let labels: BTreeSet<String> = strings(&mut store, "CALL db.labels() YIELD label RETURN label")
        .into_iter()
        .flatten()
        .collect();
    let keys: BTreeSet<String> = strings(
        &mut store,
        "CALL db.propertyKeys() YIELD propertyKey RETURN propertyKey",
    )
    .into_iter()
    .flatten()
    .collect();
    let triples: BTreeSet<(String, String, String)> = strings(
        &mut store,
        "CALL db.schema.visualization() YIELD source_label, relationship_type, target_label \
         RETURN source_label, relationship_type, target_label",
    )
    .into_iter()
    .filter(|r| r.len() == 3)
    .map(|r| (r[0].clone(), r[1].clone(), r[2].clone()))
    .collect();

    let labels_found: Vec<&str> = EXPECTED_LABELS
        .iter()
        .copied()
        .filter(|l| labels.contains(*l))
        .collect();
    let triples_found: Vec<String> = EXPECTED_TRIPLES
        .iter()
        .filter(|(s, t, d)| {
            triples.contains(&(s.to_string(), t.to_string(), d.to_string()))
        })
        .map(|(s, t, d)| format!("(:{s})-[:{t}]->(:{d})"))
        .collect();
    let all_keys: Vec<&str> = EXPECTED_NODE_KEYS
        .iter()
        .chain(EXPECTED_EDGE_KEYS.iter())
        .copied()
        .collect();
    let keys_found: Vec<&str> = all_keys
        .iter()
        .copied()
        .filter(|k| keys.contains(*k))
        .collect();

    // --- the one-call surface ----------------------------------------------

    let one_call = for_llm(&mut store, "");
    let present = one_call.is_some();
    let text = one_call.as_ref().map(|(t, _, _)| t.clone()).unwrap_or_default();

    // The six things AI-05 names, each checked in the one-call answer.
    let has_labels = EXPECTED_LABELS.iter().any(|l| text.contains(l));
    let has_types = text.contains(":String");
    let has_cardinalities = text.contains("(:Person) 51") || text.contains("(:Person) 50");
    let has_distributions = text.contains("null=") && text.contains("distinct=");
    let has_samples = text.contains("e.g.");
    let has_example_query = text.contains("MATCH (");

    // A budget is honoured only if a small one produces a smaller answer *and*
    // says what it dropped. An answer that silently ignores the budget and an
    // answer that silently truncates fail this the same way.
    let small = for_llm(&mut store, "80");
    let budget_honoured = match (&one_call, &small) {
        (Some((_, big_tokens, _)), Some((s_text, s_tokens, s_complete))) => {
            s_tokens < big_tokens && !s_complete && s_text.contains("truncated")
        }
        _ => false,
    };

    // How many calls it takes to assemble the AI-05 description without the
    // one-call procedure: the three name procedures, the triple procedure, and
    // one sample query per (label, property) a caller would have to ask for.
    let assembled_calls = 4 + EXPECTED_LABELS.len() * EXPECTED_NODE_KEYS.len();

    let doc = serde_json::json!({
        "fixture": {
            "labels": EXPECTED_LABELS,
            "triples": EXPECTED_TRIPLES.iter()
                .map(|(s, t, d)| format!("(:{s})-[:{t}]->(:{d})")).collect::<Vec<_>>(),
            "property_keys": all_keys,
            "bulk_edges_of_one_type": BULK + 1,
            "note": "the schema is written down in the source; the surfaces are \
                     scored against it, not against each other",
        },
        "recovered": {
            "labels": {"found": labels_found, "of": EXPECTED_LABELS.len()},
            "triples": {"found": triples_found, "of": EXPECTED_TRIPLES.len()},
            "property_keys": {"found": keys_found, "of": all_keys.len()},
        },
        "one_call": {
            "procedure": "db.schema.forLLM",
            "present": present,
            "labels": has_labels,
            "types": has_types,
            "cardinalities": has_cardinalities,
            "property_distributions": has_distributions,
            "sample_values": has_samples,
            "example_queries": has_example_query,
            "budget_honoured_and_truncation_announced": budget_honoured,
            "estimated_tokens": one_call.as_ref().map(|(_, t, _)| *t),
        },
        "calls_to_assemble_without_it": assembled_calls,
        // AI-05's H2 column, not attempted here and not implied by anything
        // above: a schema is not a per-question subschema.
        "per_question_subschema_selection": false,
    });

    let s = serde_json::to_string_pretty(&doc).unwrap();
    match json_out {
        Some(p) => {
            std::fs::write(&p, &s).unwrap();
            eprintln!("wrote {p}");
        }
        None => println!("{s}"),
    }
}
