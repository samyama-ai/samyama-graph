//! Gap detection, the LLM fill step (against a canned server), quarantine,
//! verification and retraction.

use super::*;
use crate::graph::Label;
use crate::nlq::test_http::{dead_base_url, MockHttp};
use crate::persistence::tenant::{LLMProvider, NLQConfig};
use crate::query::executor::record::Record;

fn spec(trust_floor: f64, materialize: Option<Materialize>) -> EnrichSpec {
    EnrichSpec {
        sources: vec![EnrichSource::Llm],
        trust_floor,
        materialize,
    }
}

fn config_with(label: &str, prop: &str, s: EnrichSpec) -> EnrichConfig {
    let mut c = EnrichConfig::default();
    c.policies
        .entry(label.to_string())
        .or_default()
        .insert(prop.to_string(), s);
    c
}

fn mat(edge: &str, target: &str) -> Materialize {
    Materialize {
        edge_type: edge.to_string(),
        target_label: target.to_string(),
        target_key: "name".to_string(),
        vocabulary: None,
    }
}

fn node(store: &mut GraphStore, label: &str, props: &[(&str, PropertyValue)]) -> NodeId {
    let id = store.create_node(label);
    for (k, v) in props {
        store.set_node_property(TENANT, id, *k, v.clone()).unwrap();
    }
    id
}

fn s(v: &str) -> PropertyValue {
    PropertyValue::String(v.to_string())
}

fn prop(store: &GraphStore, id: NodeId, key: &str) -> Option<PropertyValue> {
    store.node_properties_merged(id).get(key).cloned()
}

fn enrichment_entry(
    store: &GraphStore,
    id: NodeId,
    property: &str,
) -> HashMap<String, PropertyValue> {
    match read_enrichment_map(store, id).get(property) {
        Some(PropertyValue::Map(m)) => m.clone(),
        other => panic!("no enrichment entry for {property}: {other:?}"),
    }
}

fn outcome(id: NodeId, property: &str, value: &str, confidence: f64) -> Outcome {
    Outcome {
        node_id: id.as_u64(),
        property: property.to_string(),
        value: value.to_string(),
        confidence,
        method: "llm:test".to_string(),
        prompt_hash: "abc".to_string(),
        targets: None,
        materialize: None,
    }
}

fn rel_outcome(
    id: NodeId,
    property: &str,
    targets: &[&str],
    m: Materialize,
    confidence: f64,
) -> Outcome {
    Outcome {
        node_id: id.as_u64(),
        property: property.to_string(),
        value: targets.join("; "),
        confidence,
        method: "llm:test".to_string(),
        prompt_hash: "h".to_string(),
        targets: Some(targets.iter().map(|t| t.to_string()).collect()),
        materialize: Some(m),
    }
}

fn worker(base: &str) -> EnrichmentWorker {
    let cfg = NLQConfig {
        enabled: true,
        provider: LLMProvider::Ollama,
        model: "m".to_string(),
        api_key: None,
        api_base_url: Some(base.to_string()),
        system_prompt: None,
    };
    EnrichmentWorker::new(NLQClient::new(&cfg).unwrap(), "m1".to_string())
}

fn ollama(answer: &str) -> (u16, String) {
    (200, serde_json::json!({ "response": answer }).to_string())
}

// ------------------------------------------------------------ config / serde

#[test]
fn trust_floor_for_reads_the_declared_spec_only() {
    let c = config_with("Sensor", "description", spec(0.7, None));
    assert_eq!(c.trust_floor_for("Sensor", "description"), Some(0.7));
    assert_eq!(c.trust_floor_for("Sensor", "other"), None);
    assert_eq!(c.trust_floor_for("Pump", "description"), None);
}

#[test]
fn config_deserializes_with_defaults() {
    let c: EnrichConfig = serde_json::from_value(serde_json::json!({
        "policies": { "Sensor": {
            "description": { "sources": ["llm"], "trust_floor": 0.3 },
            "modes": { "sources": ["llm"], "materialize": { "edge_type": "HAS", "target_label": "Mode" } },
            "inert": {}
        } }
    }))
    .unwrap();
    let p = &c.policies["Sensor"];
    assert_eq!(p["description"].sources, vec![EnrichSource::Llm]);
    assert_eq!(p["description"].trust_floor, 0.3);
    let m = p["modes"].materialize.as_ref().unwrap();
    assert_eq!(m.target_key, "name", "target_key defaults to name");
    assert!(m.vocabulary.is_none());
    assert!(p["inert"].sources.is_empty());
    assert_eq!(p["inert"].trust_floor, 0.0);
    assert_eq!(EnrichConfig::default().policies.len(), 0);
}

#[test]
fn global_config_is_a_single_shared_instance() {
    let a = global_config() as *const _;
    let b = global_config() as *const _;
    assert_eq!(a, b);
    // Readable without panicking and starts (or remains) a valid config.
    let _guard = global_config().read().unwrap();
}

// ------------------------------------------------------------ result nodes

#[test]
fn collect_result_nodes_dedups_nodes_and_refs_and_skips_other_values() {
    let mut store = GraphStore::new();
    let a = store.create_node("A");
    let b = store.create_node("B");
    let full_a = store.node_materialized(a).unwrap();

    let mut batch = RecordBatch::new(vec!["x".into(), "y".into(), "z".into()]);
    let mut r1 = Record::new();
    r1.bind("x", Value::Node(a, Box::new(full_a)));
    r1.bind("y", Value::NodeRef(b));
    r1.bind("z", Value::Property(PropertyValue::Integer(3)));
    let mut r2 = Record::new();
    r2.bind("x", Value::NodeRef(a));
    r2.bind("y", Value::Null);
    batch.records = vec![r1, r2];

    assert_eq!(collect_result_nodes(&batch), vec![a, b]);
    assert!(collect_result_nodes(&RecordBatch::new(vec![])).is_empty());
}

// ------------------------------------------------------------ gap detection

#[test]
fn detect_gaps_reports_missing_and_null_properties_on_policy_labels() {
    let mut store = GraphStore::new();
    let missing = node(&mut store, "Sensor", &[("name", s("T1"))]);
    let null = node(
        &mut store,
        "Sensor",
        &[("description", PropertyValue::Null)],
    );
    let present = node(&mut store, "Sensor", &[("description", s("known"))]);
    let other = node(&mut store, "Pump", &[]);

    let c = config_with("Sensor", "description", spec(0.0, Some(mat("HAS", "Mode"))));
    let gaps = detect_gaps(
        &c,
        &store,
        &[missing, null, present, other, NodeId(9_999), missing],
    );
    let ids: Vec<u64> = gaps.iter().map(|g| g.node_id).collect();
    assert_eq!(
        ids,
        vec![missing.as_u64(), null.as_u64()],
        "dup id reported once, unknown id skipped"
    );
    assert!(gaps
        .iter()
        .all(|g| g.label == "Sensor" && g.property == "description"));
    assert_eq!(gaps[0].materialize, Some(mat("HAS", "Mode")));
}

#[test]
fn detect_gaps_ignores_declared_but_inert_specs() {
    let mut store = GraphStore::new();
    let n = node(&mut store, "Sensor", &[]);
    let c = config_with("Sensor", "description", EnrichSpec::default());
    assert!(detect_gaps(&c, &store, &[n]).is_empty());
    assert!(detect_gaps(&EnrichConfig::default(), &store, &[n]).is_empty());
}

// ------------------------------------------------------------ prompts

#[test]
fn build_prompt_lists_context_and_skips_the_reserved_property() {
    let ctx = vec![
        ("name".to_string(), "Chiller 7".to_string()),
        (ENRICHMENT_PROPERTY.to_string(), "{secret}".to_string()),
    ];
    let p = build_prompt("Asset", "description", &ctx);
    assert!(p.contains("- name: Chiller 7"), "{p}");
    assert!(
        !p.contains("{secret}"),
        "quarantine map must not leak into the prompt: {p}"
    );
    assert!(p.contains("`description` of a Asset"), "{p}");
    assert!(p.contains("UNKNOWN"));
}

#[test]
fn build_prompt_with_no_context_says_so() {
    let only_reserved = vec![(ENRICHMENT_PROPERTY.to_string(), "x".to_string())];
    assert!(build_prompt("A", "p", &[]).contains("(no other properties known)"));
    assert!(build_prompt("A", "p", &only_reserved).contains("(no other properties known)"));
    assert!(build_list_prompt("A", &mat("E", "T"), &only_reserved)
        .contains("(no other properties known)"));
}

#[test]
fn build_list_prompt_names_edge_and_target_and_drops_reserved_context() {
    let ctx = vec![
        ("kind".to_string(), "pump".to_string()),
        (ENRICHMENT_PROPERTY.to_string(), "hidden".to_string()),
    ];
    let p = build_list_prompt("Asset", &mat("FAILS_BY", "FailureMode"), &ctx);
    assert!(
        p.contains("list the FailureModes that this Asset relates to via `FAILS_BY`"),
        "{p}"
    );
    assert!(p.contains("- kind: pump"));
    assert!(!p.contains("hidden"));
}

#[test]
fn prompt_hash_is_stable_hex_and_input_sensitive() {
    let a = prompt_hash("abc");
    assert_eq!(a.len(), 16);
    assert!(a.chars().all(|c| c.is_ascii_hexdigit()));
    assert_eq!(a, prompt_hash("abc"));
    assert_ne!(a, prompt_hash("abd"));
}

#[test]
fn parse_list_handles_star_and_bullet_markers_and_case_insensitive_unknown() {
    assert_eq!(
        parse_list("* One\n• Two\n3. Three\nunknown\n   \n"),
        vec!["One", "Two", "Three"]
    );
    assert!(parse_list("").is_empty());
}

// ------------------------------------------------------------ worker.fill

#[tokio::test]
async fn fill_scalar_returns_trimmed_answer_with_provenance() {
    let srv = MockHttp::serve(vec![ollama("  A cooling machine.  ")]);
    let gap = GapEvent {
        node_id: 7,
        label: "Chiller".into(),
        property: "description".into(),
        materialize: None,
    };
    let ctx = vec![("name".to_string(), "Chiller 7".to_string())];
    let out = worker(&srv.base_url)
        .fill(&gap, &ctx)
        .await
        .expect("an answer");
    assert_eq!(out.node_id, 7);
    assert_eq!(out.property, "description");
    assert_eq!(out.value, "A cooling machine.");
    assert_eq!(out.confidence, LLM_DEFAULT_CONFIDENCE);
    assert_eq!(out.method, "llm:m1");
    assert_eq!(
        out.prompt_hash,
        prompt_hash(&build_prompt("Chiller", "description", &ctx))
    );
    assert!(out.targets.is_none() && out.materialize.is_none());
    let sent = srv.requests()[0].json();
    assert_eq!(sent["prompt"], build_prompt("Chiller", "description", &ctx));
}

#[tokio::test]
async fn fill_scalar_declines_on_unknown_or_empty_answer() {
    let srv = MockHttp::serve(vec![ollama("unknown"), ollama("   ")]);
    let w = worker(&srv.base_url);
    let gap = GapEvent {
        node_id: 1,
        label: "A".into(),
        property: "p".into(),
        materialize: None,
    };
    assert!(w.fill(&gap, &[]).await.is_none());
    assert!(w.fill(&gap, &[]).await.is_none());
    assert_eq!(srv.requests().len(), 2);
}

#[tokio::test]
async fn fill_returns_none_when_the_model_call_fails() {
    let w = worker(&dead_base_url());
    let scalar = GapEvent {
        node_id: 1,
        label: "A".into(),
        property: "p".into(),
        materialize: None,
    };
    let list = GapEvent {
        materialize: Some(mat("E", "T")),
        ..scalar.clone()
    };
    assert!(w.fill(&scalar, &[]).await.is_none());
    assert!(w.fill(&list, &[]).await.is_none());
}

#[tokio::test]
async fn fill_relationship_parses_targets() {
    let srv = MockHttp::serve(vec![ollama("- Fouling\n- Leak\n")]);
    let m = mat("FAILS_BY", "FailureMode");
    let gap = GapEvent {
        node_id: 3,
        label: "Pump".into(),
        property: "modes".into(),
        materialize: Some(m.clone()),
    };
    let out = worker(&srv.base_url).fill(&gap, &[]).await.expect("a list");
    assert_eq!(
        out.targets,
        Some(vec!["Fouling".to_string(), "Leak".to_string()])
    );
    assert_eq!(out.value, "Fouling; Leak");
    assert_eq!(out.materialize, Some(m.clone()));
    assert_eq!(out.method, "llm:m1");
    assert_eq!(
        out.prompt_hash,
        prompt_hash(&build_list_prompt("Pump", &m, &[]))
    );
    assert_eq!(srv.requests().len(), 1);
}

#[tokio::test]
async fn fill_relationship_declines_on_unknown_or_blank_list() {
    let srv = MockHttp::serve(vec![ollama(" UNKNOWN "), ollama("-\n\n")]);
    let w = worker(&srv.base_url);
    let gap = GapEvent {
        node_id: 3,
        label: "P".into(),
        property: "m".into(),
        materialize: Some(mat("E", "T")),
    };
    assert!(w.fill(&gap, &[]).await.is_none(), "honest decline");
    assert!(w.fill(&gap, &[]).await.is_none(), "nothing parseable");
    assert_eq!(srv.requests().len(), 2);
}

// ------------------------------------------------------------ quarantine

#[test]
fn quarantine_scalar_writes_pending_entry_and_leaves_property_alone() {
    let mut store = GraphStore::new();
    let n = node(&mut store, "Sensor", &[]);
    quarantine(&mut store, &outcome(n, "description", "hot", 0.4)).unwrap();

    assert_eq!(prop(&store, n, "description"), None);
    let e = enrichment_entry(&store, n, "description");
    assert_eq!(e["value"], s("hot"));
    assert_eq!(e["source"], s("llm"));
    assert_eq!(e["status"], s("pending_verification"));
    assert_eq!(e["confidence"], PropertyValue::Float(0.4));
    assert_eq!(e["method"], s("llm:test"));
    assert_eq!(e["prompt_hash"], s("abc"));
    assert!(!e.contains_key("kind"));
}

#[test]
fn quarantine_keeps_other_entries_and_records_relationship_fields() {
    let mut store = GraphStore::new();
    let n = node(&mut store, "Pump", &[]);
    quarantine(&mut store, &outcome(n, "description", "d", 0.4)).unwrap();
    quarantine(
        &mut store,
        &rel_outcome(n, "modes", &["A", "B"], mat("FAILS_BY", "Mode"), 0.4),
    )
    .unwrap();

    let root = read_enrichment_map(&store, n);
    assert_eq!(root.len(), 2, "second quarantine keeps the first entry");
    let e = enrichment_entry(&store, n, "modes");
    assert_eq!(e["kind"], s("relationship"));
    assert_eq!(e["targets"], PropertyValue::Array(vec![s("A"), s("B")]));
    assert_eq!(e["edge_type"], s("FAILS_BY"));
    assert_eq!(e["target_label"], s("Mode"));
    assert_eq!(e["target_key"], s("name"));
}

#[test]
fn quarantine_on_missing_node_is_an_error() {
    let mut store = GraphStore::new();
    let err = quarantine(&mut store, &outcome(NodeId(424_242), "p", "v", 0.4)).unwrap_err();
    assert!(!err.is_empty());
}

// ------------------------------------------------------------ verify

#[test]
fn verify_promotes_scalar_above_floor_and_marks_it_generated() {
    let mut store = GraphStore::new();
    let n = node(&mut store, "Sensor", &[]);
    let c = config_with("Sensor", "description", spec(0.3, None));
    quarantine(&mut store, &outcome(n, "description", "measures heat", 0.4)).unwrap();

    let rep = verify(&c, &mut store, &[n]);
    assert_eq!(
        (
            rep.nodes_processed,
            rep.promoted,
            rep.still_pending,
            rep.edges_materialized
        ),
        (1, 1, 0, 0)
    );
    assert_eq!(prop(&store, n, "description"), Some(s("measures heat")));
    assert_eq!(
        enrichment_entry(&store, n, "description")["status"],
        s("verified")
    );
    match prop(&store, n, GENERATED_PROPERTY) {
        Some(PropertyValue::Map(m)) => {
            assert_eq!(m["created"], PropertyValue::Boolean(false));
            assert_eq!(
                m["properties"],
                PropertyValue::Array(vec![s("description")])
            );
        }
        other => panic!("expected generated marker, got {other:?}"),
    }

    // A second pass finds nothing pending.
    let again = verify(&c, &mut store, &[n]);
    assert_eq!(
        (again.nodes_processed, again.promoted, again.still_pending),
        (1, 0, 0)
    );
}

#[test]
fn verify_keeps_below_floor_values_pending() {
    let mut store = GraphStore::new();
    let n = node(&mut store, "Sensor", &[]);
    let c = config_with("Sensor", "description", spec(0.9, None));
    quarantine(&mut store, &outcome(n, "description", "maybe", 0.4)).unwrap();

    let rep = verify(&c, &mut store, &[n]);
    assert_eq!((rep.promoted, rep.still_pending), (0, 1));
    assert_eq!(prop(&store, n, "description"), None);
    assert_eq!(
        enrichment_entry(&store, n, "description")["status"],
        s("pending_verification")
    );
}

#[test]
fn verify_without_matching_policy_uses_zero_floor() {
    let mut store = GraphStore::new();
    let n = node(&mut store, "Unlisted", &[]);
    quarantine(&mut store, &outcome(n, "p", "v", 0.0)).unwrap();
    let rep = verify(&EnrichConfig::default(), &mut store, &[n]);
    assert_eq!(rep.promoted, 1);
    assert_eq!(prop(&store, n, "p"), Some(s("v")));
}

#[test]
fn verify_skips_nodes_without_enrichment_and_malformed_entries() {
    let mut store = GraphStore::new();
    let plain = node(&mut store, "Sensor", &[]);
    let odd = node(&mut store, "Sensor", &[]);
    let mut root = HashMap::new();
    root.insert("not_a_map".to_string(), s("x"));
    let mut no_status = HashMap::new();
    no_status.insert("value".to_string(), s("v"));
    root.insert("no_status".to_string(), PropertyValue::Map(no_status));
    let mut no_conf = HashMap::new();
    no_conf.insert("status".to_string(), s("pending_verification"));
    no_conf.insert("value".to_string(), s("fallback"));
    root.insert("no_conf".to_string(), PropertyValue::Map(no_conf));
    let mut rel_missing_fields = HashMap::new();
    rel_missing_fields.insert("status".to_string(), s("pending_verification"));
    rel_missing_fields.insert("kind".to_string(), s("relationship"));
    root.insert(
        "broken_rel".to_string(),
        PropertyValue::Map(rel_missing_fields),
    );
    store
        .set_node_property(TENANT, odd, ENRICHMENT_PROPERTY, PropertyValue::Map(root))
        .unwrap();

    let c = config_with("Sensor", "no_conf", spec(0.0, None));
    let rep = verify(&c, &mut store, &[plain, odd]);
    assert_eq!(
        rep.nodes_processed, 1,
        "the node with no quarantine map is skipped"
    );
    assert_eq!(
        rep.promoted, 1,
        "only the entry with a status and a value is promoted"
    );
    assert_eq!(prop(&store, odd, "no_conf"), Some(s("fallback")));
    assert_eq!(prop(&store, odd, "broken_rel"), None);
    assert_eq!(rep.edges_materialized, 0);
}

#[test]
fn verify_materializes_relationship_merging_existing_targets() {
    let mut store = GraphStore::new();
    let pump = node(&mut store, "Pump", &[]);
    let existing = node(&mut store, "Mode", &[("name", s("Leak"))]);
    let m = mat("FAILS_BY", "Mode");
    let c = config_with("Pump", "modes", spec(0.1, Some(m.clone())));
    quarantine(
        &mut store,
        &rel_outcome(pump, "modes", &["Leak", "Fouling", "Fouling"], m, 0.4),
    )
    .unwrap();

    let rep = verify(&c, &mut store, &[pump]);
    assert_eq!(rep.promoted, 1);
    assert_eq!(rep.edges_materialized, 3);
    assert_eq!(
        enrichment_entry(&store, pump, "modes")["status"],
        s("verified")
    );

    let modes = store.nodes_with_label(&Label::new("Mode")).unwrap().len();
    assert_eq!(modes, 2, "Leak reused, Fouling created once");
    let out = store.get_outgoing_edges(pump);
    assert_eq!(out.len(), 3);
    assert!(out.iter().all(
        |e| e.edge_type.as_str() == "FAILS_BY" && e.properties.contains_key(GENERATED_PROPERTY)
    ));
    assert!(out.iter().any(|e| e.target == existing));
    // The existing target is not marked as generated; the new one is.
    assert_eq!(prop(&store, existing, GENERATED_PROPERTY), None);
    let created = out
        .iter()
        .map(|e| e.target)
        .find(|t| *t != existing)
        .unwrap();
    match prop(&store, created, GENERATED_PROPERTY) {
        Some(PropertyValue::Map(m)) => assert_eq!(m["created"], PropertyValue::Boolean(true)),
        other => panic!("{other:?}"),
    }
    assert_eq!(prop(&store, created, "name"), Some(s("Fouling")));
}

#[test]
fn mark_generated_property_appends_once_and_keeps_created_flag() {
    let mut store = GraphStore::new();
    let n = node(&mut store, "X", &[]);
    store
        .set_node_property(
            TENANT,
            n,
            GENERATED_PROPERTY,
            generated_map(true, vec!["a".into()]),
        )
        .unwrap();
    mark_generated_property(&mut store, n, "b");
    mark_generated_property(&mut store, n, "b");
    let m = read_generated_map(&store, n);
    assert_eq!(m["created"], PropertyValue::Boolean(true));
    assert_eq!(m["properties"], PropertyValue::Array(vec![s("a"), s("b")]));
    assert!(is_generated_whole(&store, n));

    let other = node(&mut store, "X", &[(GENERATED_PROPERTY, s("not a map"))]);
    assert!(read_generated_map(&store, other).is_empty());
    assert!(!is_generated_whole(&store, other));
}

// ------------------------------------------------------------ retract

#[test]
fn retract_undoes_scalar_promotion_and_reopens_quarantine() {
    let mut store = GraphStore::new();
    let n = node(&mut store, "Sensor", &[("name", s("T1"))]);
    let c = config_with("Sensor", "description", spec(0.0, None));
    quarantine(&mut store, &outcome(n, "description", "v", 0.4)).unwrap();
    verify(&c, &mut store, &[n]);
    assert!(prop(&store, n, "description").is_some());

    // A second, still-pending entry stays as it is.
    quarantine(&mut store, &outcome(n, "unit", "C", 0.4)).unwrap();

    let rep = retract(&mut store, &[n]);
    assert_eq!(
        (
            rep.nodes_processed,
            rep.properties_removed,
            rep.edges_removed,
            rep.nodes_removed
        ),
        (1, 1, 0, 0)
    );
    assert_eq!(
        enrichment_entry(&store, n, "unit")["status"],
        s("pending_verification")
    );
    assert_eq!(prop(&store, n, "description"), None);
    assert_eq!(prop(&store, n, GENERATED_PROPERTY), None);
    assert_eq!(
        prop(&store, n, "name"),
        Some(s("T1")),
        "ingested data untouched"
    );
    assert_eq!(
        enrichment_entry(&store, n, "description")["status"],
        s("pending_verification")
    );
}

#[test]
fn retract_removes_generated_edges_and_orphaned_generated_targets_only() {
    let mut store = GraphStore::new();
    let pump = node(&mut store, "Pump", &[]);
    let existing = node(&mut store, "Mode", &[("name", s("Leak"))]);
    let m = mat("FAILS_BY", "Mode");
    let c = config_with("Pump", "modes", spec(0.0, Some(m.clone())));
    quarantine(
        &mut store,
        &rel_outcome(pump, "modes", &["Leak", "Fouling"], m, 0.4),
    )
    .unwrap();
    verify(&c, &mut store, &[pump]);
    // An ordinary, ingested edge that retraction must leave alone.
    let keep = node(&mut store, "Site", &[]);
    store.create_edge(pump, keep, "AT").unwrap();

    let rep = retract(&mut store, &[pump]);
    assert_eq!(rep.nodes_processed, 1);
    assert_eq!(rep.edges_removed, 2);
    assert_eq!(
        rep.nodes_removed, 1,
        "only the model-created Fouling node goes"
    );
    assert!(store.get_node(existing).is_some());
    let remaining = store.get_outgoing_edges(pump);
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].target, keep);
    assert_eq!(
        enrichment_entry(&store, pump, "modes")["status"],
        s("pending_verification")
    );
}

#[test]
fn retract_keeps_a_generated_target_still_referenced_elsewhere() {
    let mut store = GraphStore::new();
    let a = node(&mut store, "Pump", &[]);
    let b = node(&mut store, "Pump", &[]);
    let m = mat("FAILS_BY", "Mode");
    let c = config_with("Pump", "modes", spec(0.0, Some(m.clone())));
    quarantine(
        &mut store,
        &rel_outcome(a, "modes", &["Fouling"], m.clone(), 0.4),
    )
    .unwrap();
    quarantine(&mut store, &rel_outcome(b, "modes", &["Fouling"], m, 0.4)).unwrap();
    verify(&c, &mut store, &[a, b]);
    assert_eq!(
        store.nodes_with_label(&Label::new("Mode")).unwrap().len(),
        1
    );

    let rep = retract(&mut store, &[a]);
    assert_eq!((rep.edges_removed, rep.nodes_removed), (1, 0));
    assert_eq!(
        store.nodes_with_label(&Label::new("Mode")).unwrap().len(),
        1
    );
}

#[test]
fn retract_skips_nodes_the_model_never_touched() {
    let mut store = GraphStore::new();
    let n = node(&mut store, "X", &[("name", s("n"))]);
    let rep = retract(&mut store, &[n, NodeId(77_777)]);
    assert_eq!(rep.nodes_processed, 0);
    assert_eq!(prop(&store, n, "name"), Some(s("n")));
}

// ------------------------------------------------------------ worker_from_env

#[test]
fn worker_from_env_refuses_an_unset_or_unknown_provider() {
    // Read-only check against whatever the environment holds: in CI and on a
    // dev host NLQ_PROVIDER is unset, which must be refused, not defaulted.
    // If someone configured a provider there is nothing to assert here.
    if std::env::var("NLQ_PROVIDER").is_err() {
        let err = worker_from_env()
            .err()
            .expect("unset provider must be refused");
        assert!(err.contains("NLQ_PROVIDER"), "{err}");
    }
}
