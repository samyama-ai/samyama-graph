//! Model-generated graph state is tagged, excludable and reversible (TRUST-05,
//! AI-10, #1413).
//!
//! `src/agent/enrich.rs` quarantines an LLM answer under `_enrichment` with its
//! confidence and method, and promotes it only once it clears a trust floor.
//! That covers the value *before* it is believed. After promotion there was
//! nothing: `verify` wrote the answer onto the real property and the only trace
//! left was `_enrichment.<prop>.status = "verified"`, a value nested inside a
//! map. A promoted value was indistinguishable from ingested data, edges
//! materialized from a model's list carried nothing at all, and there was no
//! way back.
//!
//! TRUST-05 asks for four things: tagged, confidence-scored, **reversible**,
//! and **excludable with one predicate**. The last two are what these cases
//! pin.
//!
//! # Why one reserved key and not two
//!
//! "One predicate" is the requirement, so a node whose *property* was generated
//! and a node that was *created whole* by a model have to be caught by the same
//! test. Both carry `_generated`, a map: `created` says the artifact itself is
//! model-made, `properties` lists the properties whose current value is. So
//! `WHERE n._generated IS NULL` is the exclusion, for either case, and the same
//! key marks generated edges.
//!
//! The mark goes on the artifact the model made and no further. A node the
//! model drew an edge *from* is not marked: its own data is ingested, and
//! tainting it would make the exclusion predicate hide real rows and make a
//! retraction look like a delete. So a relationship gap marks the edge and any
//! target node it created, not the node it started from.
//!
//! There are no tests in this module's history at all -- not for `quarantine`,
//! `verify`, the trust floor, or materialization -- so the floor and the
//! quarantine are asserted here too rather than assumed.

use samyama::agent::enrich::{
    retract, verify, quarantine, EnrichConfig, EnrichSpec, Materialize, Outcome,
    ENRICHMENT_PROPERTY, GENERATED_PROPERTY,
};
use samyama::graph::{EdgeType, GraphStore, Label, NodeId, PropertyValue};
use samyama::query::executor::{QueryExecutor, Value};
use samyama::query::parser::parse_query;

const TENANT: &str = "default";

fn seed() -> (GraphStore, NodeId) {
    let mut store = GraphStore::new();
    let id = store.create_node(Label::new("Drug"));
    let _ = store.set_node_property(TENANT, id, "name", PropertyValue::String("Semaglutide".into()));
    (store, id)
}

fn scalar_outcome(id: NodeId, property: &str, value: &str, confidence: f64) -> Outcome {
    Outcome {
        node_id: id.0,
        property: property.to_string(),
        value: value.to_string(),
        confidence,
        method: "llm:test".to_string(),
        prompt_hash: "deadbeef".to_string(),
        targets: None,
        materialize: None,
    }
}

fn config(label: &str, property: &str, trust_floor: f64, materialize: Option<Materialize>) -> EnrichConfig {
    let mut cfg = EnrichConfig::default();
    cfg.policies.entry(label.to_string()).or_default().insert(
        property.to_string(),
        EnrichSpec { sources: vec![], trust_floor, materialize },
    );
    cfg
}

/// Run a read query and collect column `r`.
fn ask(store: &GraphStore, cypher: &str) -> Vec<Value> {
    let q = parse_query(cypher).unwrap_or_else(|e| panic!("{cypher}\n  parse: {e:?}"));
    QueryExecutor::new(store)
        .execute(&q)
        .unwrap_or_else(|e| panic!("{cypher}\n  exec: {e:?}"))
        .records
        .iter()
        .map(|rec| rec.get("r").cloned().unwrap_or(Value::Null))
        .collect()
}

fn generated_map(store: &GraphStore, id: NodeId) -> Option<std::collections::HashMap<String, PropertyValue>> {
    match store.node_properties_merged(id).get(GENERATED_PROPERTY) {
        Some(PropertyValue::Map(m)) => Some(m.clone()),
        _ => None,
    }
}

#[test]
fn quarantine_does_not_touch_the_real_property() {
    // The half that already worked, asserted because nothing asserted it.
    let (mut store, id) = seed();
    quarantine(&mut store, &scalar_outcome(id, "mechanism", "GLP-1 agonist", 0.4)).unwrap();
    let props = store.node_properties_merged(id);
    assert!(props.get("mechanism").is_none(), "the real property stays empty");
    assert!(props.get(ENRICHMENT_PROPERTY).is_some(), "the answer is quarantined");
    assert!(props.get(GENERATED_PROPERTY).is_none(), "nothing is generated until it is promoted");
}

#[test]
fn a_value_below_the_trust_floor_is_not_promoted_and_is_not_tagged() {
    let (mut store, id) = seed();
    quarantine(&mut store, &scalar_outcome(id, "mechanism", "GLP-1 agonist", 0.4)).unwrap();
    let rep = verify(&config("Drug", "mechanism", 0.9, None), &mut store, &[id]);
    assert_eq!(rep.promoted, 0);
    assert_eq!(rep.still_pending, 1);
    let props = store.node_properties_merged(id);
    assert!(props.get("mechanism").is_none());
    assert!(
        props.get(GENERATED_PROPERTY).is_none(),
        "a value that was never believed must not be marked as believed"
    );
}

#[test]
fn a_promoted_value_is_tagged_on_the_node_that_carries_it() {
    let (mut store, id) = seed();
    quarantine(&mut store, &scalar_outcome(id, "mechanism", "GLP-1 agonist", 0.4)).unwrap();
    let rep = verify(&config("Drug", "mechanism", 0.0, None), &mut store, &[id]);
    assert_eq!(rep.promoted, 1);

    let props = store.node_properties_merged(id);
    assert_eq!(
        props.get("mechanism"),
        Some(&PropertyValue::String("GLP-1 agonist".into())),
        "the value is promoted"
    );
    let g = generated_map(&store, id).expect("promotion tags the node");
    let Some(PropertyValue::Array(names)) = g.get("properties") else {
        panic!("`properties` lists what was generated, got {g:?}")
    };
    assert_eq!(names, &vec![PropertyValue::String("mechanism".into())]);
    assert_eq!(
        g.get("created"),
        Some(&PropertyValue::Boolean(false)),
        "the node itself was not model-made, only one of its properties"
    );
}

#[test]
fn one_predicate_excludes_every_node_a_model_touched() {
    // The requirement is one predicate, so the node with a generated *property*
    // and the node the model *created* have to answer to the same test.
    let mut store = GraphStore::new();
    let drug = store.create_node(Label::new("Drug"));
    let _ = store.set_node_property(TENANT, drug, "name", PropertyValue::String("Semaglutide".into()));
    let ingested = store.create_node(Label::new("Drug"));
    let _ = store.set_node_property(TENANT, ingested, "name", PropertyValue::String("Metformin".into()));

    quarantine(
        &mut store,
        &Outcome {
            node_id: drug.0,
            property: "treats".to_string(),
            value: String::new(),
            confidence: 0.4,
            method: "llm:test".to_string(),
            prompt_hash: "deadbeef".to_string(),
            targets: Some(vec!["Type 2 Diabetes".into(), "Obesity".into()]),
            materialize: Some(Materialize {
                edge_type: "TREATS".into(),
                target_label: "Indication".into(),
                target_key: "name".into(),
                vocabulary: None,
            }),
        },
    )
    .unwrap();
    let cfg = config(
        "Drug",
        "treats",
        0.0,
        Some(Materialize {
            edge_type: "TREATS".into(),
            target_label: "Indication".into(),
            target_key: "name".into(),
            vocabulary: None,
        }),
    );
    let rep = verify(&cfg, &mut store, &[drug]);
    assert_eq!(rep.edges_materialized, 2);

    // Both drugs survive. Nothing about Semaglutide's own data was written by
    // the model -- what the model produced is the *edges* leaving it, and the
    // indications at the other end. Marking the source node would say its data
    // is model-made when it is not, and would make an exclusion predicate hide
    // ingested rows. The mark goes on the artifact the model made, which is why
    // it goes on the edge.
    let mut kept: Vec<String> = ask(&store, "MATCH (n:Drug) WHERE n._generated IS NULL RETURN n.name AS r")
        .into_iter()
        .map(|v| match v {
            Value::Property(PropertyValue::String(s)) => s,
            other => panic!("{other:?}"),
        })
        .collect();
    kept.sort();
    assert_eq!(kept, vec!["Metformin", "Semaglutide"]);

    let kept = ask(&store, "MATCH (n:Indication) WHERE n._generated IS NULL RETURN n.name AS r");
    assert!(
        kept.is_empty(),
        "both indications were created by the model, so none survives"
    );

    // And the predicate that matters for a relationship gap is the one on the
    // relationship. This is the query a caller writes to see the graph without
    // anything a model asserted.
    let real = ask(
        &store,
        "MATCH (a:Drug)-[r:TREATS]->(b:Indication) WHERE r._generated IS NULL RETURN b.name AS r",
    );
    assert!(real.is_empty(), "every TREATS edge here was drawn by the model");
    let all = ask(&store, "MATCH (a:Drug)-[r:TREATS]->(b:Indication) RETURN b.name AS r");
    assert_eq!(all.len(), 2, "and without the predicate both are there");
}

#[test]
fn a_materialized_target_and_its_edge_are_both_tagged() {
    let (mut store, drug) = seed();
    let mat = Materialize {
        edge_type: "TREATS".into(),
        target_label: "Indication".into(),
        target_key: "name".into(),
        vocabulary: None,
    };
    quarantine(
        &mut store,
        &Outcome {
            node_id: drug.0,
            property: "treats".to_string(),
            value: String::new(),
            confidence: 0.4,
            method: "llm:test".to_string(),
            prompt_hash: "deadbeef".to_string(),
            targets: Some(vec!["Obesity".into()]),
            materialize: Some(mat.clone()),
        },
    )
    .unwrap();
    verify(&config("Drug", "treats", 0.0, Some(mat)), &mut store, &[drug]);

    let target = store
        .nodes_with_label(&Label::new("Indication"))
        .map(|s| s.iter().copied().collect::<Vec<_>>())
        .unwrap_or_default();
    assert_eq!(target.len(), 1);
    let g = generated_map(&store, target[0]).expect("the target node is tagged");
    assert_eq!(
        g.get("created"),
        Some(&PropertyValue::Boolean(true)),
        "the model created this node whole"
    );

    // The edge is the half the model invented that nothing recorded at all.
    let edges = store.edges_between(drug, target[0], Some(&EdgeType::new("TREATS")));
    assert_eq!(edges.len(), 1);
    let ep = store.get_edge(edges[0]).expect("edge").properties.clone();
    assert!(
        matches!(ep.get(GENERATED_PROPERTY), Some(PropertyValue::Map(_))),
        "the edge carries the same key, so one predicate covers it too; got {ep:?}"
    );
}

#[test]
fn retracting_puts_the_graph_back() {
    let (mut store, drug) = seed();
    quarantine(&mut store, &scalar_outcome(drug, "mechanism", "GLP-1 agonist", 0.4)).unwrap();
    let cfg = config("Drug", "mechanism", 0.0, None);
    verify(&cfg, &mut store, &[drug]);
    assert!(store.node_properties_merged(drug).get("mechanism").is_some());

    let rep = retract(&mut store, &[drug]);
    assert_eq!(rep.properties_removed, 1);
    let props = store.node_properties_merged(drug);
    assert!(
        props.get("mechanism").is_none(),
        "the promoted value is gone"
    );
    assert!(props.get(GENERATED_PROPERTY).is_none(), "and so is the tag");
    assert!(
        props.get("name").is_some(),
        "the ingested property is untouched -- retraction is not a delete"
    );

    // The quarantined answer stays, back at pending: retracting withdraws the
    // belief, not the evidence. A later pass with a higher floor can look at it
    // again, and a retraction that destroyed it would make the decision
    // unreviewable.
    let Some(PropertyValue::Map(root)) = props.get(ENRICHMENT_PROPERTY) else {
        panic!("the quarantine entry survives")
    };
    let Some(PropertyValue::Map(e)) = root.get("mechanism") else { panic!("entry") };
    assert_eq!(
        e.get("status"),
        Some(&PropertyValue::String("pending_verification".into()))
    );
}

#[test]
fn retracting_removes_the_edges_and_the_nodes_the_model_created() {
    let mut store = GraphStore::new();
    let drug = store.create_node(Label::new("Drug"));
    let _ = store.set_node_property(TENANT, drug, "name", PropertyValue::String("Semaglutide".into()));
    // An indication that was already in the graph. The model naming it must not
    // put it at risk: retraction removes what the model made, not what it
    // mentioned.
    let known = store.create_node(Label::new("Indication"));
    let _ = store.set_node_property(TENANT, known, "name", PropertyValue::String("Obesity".into()));

    let mat = Materialize {
        edge_type: "TREATS".into(),
        target_label: "Indication".into(),
        target_key: "name".into(),
        vocabulary: None,
    };
    quarantine(
        &mut store,
        &Outcome {
            node_id: drug.0,
            property: "treats".to_string(),
            value: String::new(),
            confidence: 0.4,
            method: "llm:test".to_string(),
            prompt_hash: "deadbeef".to_string(),
            targets: Some(vec!["Obesity".into(), "Type 2 Diabetes".into()]),
            materialize: Some(mat.clone()),
        },
    )
    .unwrap();
    verify(&config("Drug", "treats", 0.0, Some(mat)), &mut store, &[drug]);

    let rep = retract(&mut store, &[drug]);
    assert_eq!(rep.edges_removed, 2, "both materialized edges go");
    assert_eq!(rep.nodes_removed, 1, "only the indication the model invented");

    let left = ask(&store, "MATCH (n:Indication) RETURN n.name AS r");
    assert_eq!(
        left,
        vec![Value::Property(PropertyValue::String("Obesity".into()))],
        "the pre-existing indication is still there"
    );
    assert!(
        store.edges_between(drug, known, Some(&EdgeType::new("TREATS"))).is_empty(),
        "but the edge the model drew to it is gone"
    );
}

#[test]
fn retracting_a_node_a_model_never_touched_does_nothing() {
    let (mut store, id) = seed();
    let rep = retract(&mut store, &[id]);
    assert_eq!(rep.properties_removed, 0);
    assert_eq!(rep.edges_removed, 0);
    assert_eq!(rep.nodes_removed, 0);
    assert!(store.node_properties_merged(id).get("name").is_some());
}
