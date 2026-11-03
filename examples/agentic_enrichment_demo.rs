//! Agentic Enrichment Demo — Generation-Augmented Knowledge (GAK).
//!
//! Instead of RAG (using a database to help an LLM answer a question), GAK
//! inverts the pattern: the database notices a gap in its own knowledge, asks a
//! model to fill it, and writes the answer back.
//!
//! # What changed, and why (samyama-graph#1413)
//!
//! This demo used to ask Claude for Cypher, filter the reply for lines starting
//! `CREATE` or `MATCH`, and execute them. That is the whole of GAK's hard part
//! skipped: the nodes and edges it wrote were untagged, unscored, unattributed
//! and indistinguishable from ingested data, and there was no way back short of
//! a hand-written `DELETE`. It was also the one path in the repository that did
//! not use `samyama::agent::enrich`, which exists and does the right things.
//!
//! So the model no longer writes Cypher. It answers a *question* — a value, or
//! a list of entity names — and the answer travels the governed route:
//!
//!   detect_gaps → fill → quarantine → verify (trust floor) → retract
//!
//! Nothing the model says reaches a real property until it clears a floor, and
//! everything it does reach carries `_generated`, so one predicate excludes it.
//!
//! # Running it
//!
//! `cargo run --release --example agentic_enrichment_demo` needs the `claude`
//! CLI installed and authenticated.
//!
//! `--offline` runs the identical pipeline over a fixed set of answers instead
//! of calling a model. It exists so the shipped path can be asserted end to end
//! in CI: `tests/gak_demo_writes_are_governed.rs` runs this binary and checks
//! the graph it leaves behind. A demo whose correctness depends on a network
//! call is a demo nothing can check.

use samyama::agent::enrich::{
    detect_gaps, quarantine, retract, verify, EnrichConfig, EnrichSource, EnrichSpec,
    EnrichmentWorker, GapEvent, Materialize, Outcome, GENERATED_PROPERTY,
    LLM_DEFAULT_CONFIDENCE,
};
use samyama::graph::{GraphStore, Label, NodeId, PropertyValue};
use samyama::nlq::client::NLQClient;
use samyama::persistence::tenant::{LLMProvider, NLQConfig};
use samyama::query::executor::{QueryExecutor, Value};
use samyama::query::parser::parse_query;

const TENANT: &str = "default";
const DRUG: &str = "Semaglutide";

/// The floors the demo runs with. `mechanism` sits above the confidence an
/// unsourced model answer carries, so it is the case that must *not* promote.
const FLOOR_PROMOTES: f64 = 0.3;
const FLOOR_REFUSES: f64 = 0.9;

#[tokio::main]
async fn main() {
    let offline = std::env::args().any(|a| a == "--offline");

    println!("================================================================");
    println!("  Samyama Agentic Enrichment Demo");
    println!("  Generation-Augmented Knowledge (GAK)");
    println!("================================================================");
    println!();

    if !offline && !is_claude_available() {
        eprintln!("Error: 'claude' CLI not found.");
        eprintln!("Install Claude Code: https://docs.anthropic.com/en/docs/claude-code");
        eprintln!("Or run with --offline to use fixed answers instead of a model.");
        std::process::exit(1);
    }
    println!(
        "  Source of answers: {}",
        if offline {
            "fixed fixture (--offline)"
        } else {
            "claude CLI, via NLQClient"
        }
    );
    println!();

    // ── Phase 1: a graph with a hole in it ──────────────────────────────────
    println!("--- Phase 1: the graph, and the gap ---");
    let mut store = GraphStore::new();
    let drug = store.create_node(Label::new("Drug"));
    set(&mut store, drug, "name", DRUG);

    // An ingested condition the model will go on to name. It is here so the
    // retraction at the end has something it must *not* delete.
    let ingested = store.create_node(Label::new("Condition"));
    set(&mut store, ingested, "name", "Type 2 Diabetes");
    set(&mut store, ingested, "source", "ingested:icd-10");

    println!("  (:Drug {{name: '{DRUG}'}})  — no manufacturer, no mechanism, no conditions");
    println!("  (:Condition {{name: 'Type 2 Diabetes'}})  — ingested, source icd-10");
    println!();

    let config = policy();
    let surfaced = ask_nodes(&store, &format!("MATCH (d:Drug) WHERE d.name = '{DRUG}' RETURN d"));
    let gaps = detect_gaps(&config, &store, &surfaced);
    println!("  Gaps detected on {} surfaced node(s): {}", surfaced.len(), gaps.len());
    for g in &gaps {
        println!(
            "    {}.{}{}",
            g.label,
            g.property,
            match &g.materialize {
                Some(m) => format!("  → materializes (:{})-[:{}]->", g.label, m.edge_type),
                None => String::new(),
            }
        );
    }
    println!();

    // ── Phase 2: ask the model, and keep the answer out of the graph ────────
    println!("--- Phase 2: fill, then quarantine ---");
    let worker = if offline { None } else { Some(online_worker()) };
    let context = vec![("name".to_string(), DRUG.to_string())];

    let mut outcomes = Vec::new();
    for gap in &gaps {
        let out = match &worker {
            Some(w) => w.fill(gap, &context).await,
            None => fixture_answer(gap),
        };
        match out {
            Some(o) => {
                println!(
                    "  {}.{} = {:?}  (confidence {:.2}, {})",
                    gap.label, gap.property, o.value, o.confidence, o.method
                );
                outcomes.push(o);
            }
            // `fill` returns None when the model answers UNKNOWN. A declined
            // answer is a result, not a failure: the alternative is a guess.
            None => println!("  {}.{} — the model declined", gap.label, gap.property),
        }
    }
    for o in &outcomes {
        if let Err(e) = quarantine(&mut store, o) {
            eprintln!("  quarantine failed: {e}");
            std::process::exit(1);
        }
    }
    println!();
    println!("  Quarantined {} answer(s) under `_enrichment`.", outcomes.len());
    println!("  The real properties are still empty:");
    for o in &outcomes {
        let present = store
            .node_properties_merged(NodeId(o.node_id))
            .contains_key(&o.property);
        println!("    {}: {}", o.property, if present { "SET" } else { "absent" });
    }
    println!();

    // ── Phase 3: promote what clears the floor ──────────────────────────────
    println!("--- Phase 3: verify against the trust floor ---");
    println!("  manufacturer, treats: floor {FLOOR_PROMOTES:.2}   mechanism: floor {FLOOR_REFUSES:.2}");
    println!("  An unsourced model answer carries {LLM_DEFAULT_CONFIDENCE:.2}.");
    let rep = verify(&config, &mut store, &surfaced);
    println!(
        "  promoted {}, edges materialized {}, still pending {}",
        rep.promoted, rep.edges_materialized, rep.still_pending
    );
    println!();

    // ── Phase 4: the state is separable ─────────────────────────────────────
    println!("--- Phase 4: what a query can now tell apart ---");
    let all = count(&store, "MATCH (n) RETURN n AS r");
    let ingested_only = count(
        &store,
        &format!("MATCH (n) WHERE n.{GENERATED_PROPERTY} IS NULL RETURN n AS r"),
    );
    println!("  nodes total                       {all}");
    println!("  nodes with `_generated` absent    {ingested_only}   ← one predicate excludes the rest");
    let marked = generated_nodes(&store);
    for (id, what) in &marked {
        println!("    id {} marked: {}", id.0, what);
    }
    println!();
    println!("  The marker on each, by node rather than by answer — one node can");
    println!("  carry several generated properties, and a refused answer marks none:");
    for (id, _) in &marked {
        if let Some(PropertyValue::Map(m)) = store.node_properties_merged(*id).get(GENERATED_PROPERTY)
        {
            println!("    id {} _generated = {:?}", id.0, sorted_keys(m));
        }
    }
    println!();

    // ── Phase 5: and it is reversible ───────────────────────────────────────
    println!("--- Phase 5: retract ---");
    let touched: Vec<NodeId> = marked.iter().map(|(id, _)| *id).chain(surfaced.clone()).collect();
    let rr = retract(&mut store, &touched);
    println!(
        "  properties removed {}, edges removed {}, nodes removed {}",
        rr.properties_removed, rr.edges_removed, rr.nodes_removed
    );
    let after = count(&store, "MATCH (n) RETURN n AS r");
    let ingested_survived = count(
        &store,
        "MATCH (c:Condition) WHERE c.name = 'Type 2 Diabetes' RETURN c AS r",
    );
    println!("  nodes total                       {after}");
    println!("  the ingested condition survived   {ingested_survived}   ← retraction removes what the model made, not what it named");
    println!();

    // A machine-readable tail so a test can assert the shipped path rather than
    // a reimplementation of it. Printed in both modes; parsed only in --offline.
    println!("GAK-SUMMARY gaps={} filled={} promoted={} pending={} edges={} marked={} \
              nodes_before_retract={} nodes_after_retract={} props_removed={} \
              edges_removed={} nodes_removed={} ingested_survived={}",
        gaps.len(), outcomes.len(), rep.promoted, rep.still_pending, rep.edges_materialized,
        marked.len(), all, after, rr.properties_removed, rr.edges_removed, rr.nodes_removed,
        ingested_survived);
}

/// `Label -> property -> spec`. Two scalar gaps and one relationship gap, so
/// the demo shows a promotion, a refusal and a materialized subgraph.
fn policy() -> EnrichConfig {
    let mut cfg = EnrichConfig::default();
    let drug = cfg.policies.entry("Drug".to_string()).or_default();
    drug.insert(
        "manufacturer".to_string(),
        EnrichSpec {
            sources: vec![EnrichSource::Llm],
            trust_floor: FLOOR_PROMOTES,
            materialize: None,
        },
    );
    drug.insert(
        "mechanism".to_string(),
        EnrichSpec {
            sources: vec![EnrichSource::Llm],
            trust_floor: FLOOR_REFUSES,
            materialize: None,
        },
    );
    drug.insert(
        "treats".to_string(),
        EnrichSpec {
            sources: vec![EnrichSource::Llm],
            trust_floor: FLOOR_PROMOTES,
            materialize: Some(Materialize {
                edge_type: "TREATS".to_string(),
                target_label: "Condition".to_string(),
                target_key: "name".to_string(),
                vocabulary: None,
            }),
        },
    );
    cfg
}

/// The worker the demo uses when a model is available.
///
/// Built here rather than through `enrich::worker_from_env`, which reads
/// `NLQ_PROVIDER` from the environment; this demo is specifically the Claude
/// Code one, and an env var silently selecting a different provider is how the
/// wrong model ends up answering.
fn online_worker() -> EnrichmentWorker {
    let config = NLQConfig {
        enabled: true,
        provider: LLMProvider::ClaudeCode,
        model: String::new(),
        api_key: None,
        api_base_url: None,
        system_prompt: Some(
            "You are a precise pharmacology domain expert. Answer factually and follow \
             the requested output format exactly. Answer UNKNOWN if you are not sure."
                .to_string(),
        ),
    };
    let client = NLQClient::new(&config).unwrap_or_else(|e| {
        eprintln!("Error building the NLQ client: {e}");
        std::process::exit(1);
    });
    EnrichmentWorker::new(client, "claude-code".to_string())
}

/// Fixed answers for `--offline`, in the shape `fill` would have returned.
///
/// These stand in for the model and nothing else: they enter the same
/// `quarantine → verify` path with the same confidence an unsourced model
/// answer gets, so the governance the test asserts is the governance the online
/// run uses. One of the three names a condition that is already in the graph,
/// which is what makes the retraction case meaningful.
fn fixture_answer(gap: &GapEvent) -> Option<Outcome> {
    let (value, targets) = match gap.property.as_str() {
        "manufacturer" => ("Novo Nordisk".to_string(), None),
        "mechanism" => ("GLP-1 receptor agonist".to_string(), None),
        "treats" => {
            let t = vec!["Type 2 Diabetes".to_string(), "Obesity".to_string()];
            (t.join("; "), Some(t))
        }
        _ => return None,
    };
    Some(Outcome {
        node_id: gap.node_id,
        property: gap.property.clone(),
        value,
        confidence: LLM_DEFAULT_CONFIDENCE,
        method: "fixture:offline".to_string(),
        prompt_hash: "offline".to_string(),
        targets,
        materialize: gap.materialize.clone(),
    })
}

fn set(store: &mut GraphStore, id: NodeId, key: &str, value: &str) {
    store
        .set_node_property(TENANT, id, key, PropertyValue::String(value.to_string()))
        .unwrap_or_else(|e| panic!("set {key}: {e:?}"));
}

fn run(store: &GraphStore, cypher: &str) -> Vec<samyama::query::executor::Record> {
    let q = parse_query(cypher).unwrap_or_else(|e| panic!("{cypher}\n  parse: {e:?}"));
    QueryExecutor::new(store)
        .execute(&q)
        .unwrap_or_else(|e| panic!("{cypher}\n  exec: {e:?}"))
        .records
}

fn ask_nodes(store: &GraphStore, cypher: &str) -> Vec<NodeId> {
    run(store, cypher)
        .iter()
        .flat_map(|rec| {
            rec.values().filter_map(|v| match v {
                Value::Node(id, _) | Value::NodeRef(id) => Some(*id),
                _ => None,
            })
        })
        .collect()
}

fn count(store: &GraphStore, cypher: &str) -> usize {
    run(store, cypher).len()
}

/// Every node carrying the reserved marker, with a short description of why.
fn generated_nodes(store: &GraphStore) -> Vec<(NodeId, String)> {
    let mut out = Vec::new();
    for rec in run(store, "MATCH (n) RETURN n AS r") {
        let Some(Value::Node(id, _) | Value::NodeRef(id)) = rec.get("r") else {
            continue;
        };
        if let Some(PropertyValue::Map(m)) = store.node_properties_merged(*id).get(GENERATED_PROPERTY)
        {
            let created = matches!(m.get("created"), Some(PropertyValue::Boolean(true)));
            let props = match m.get("properties") {
                Some(PropertyValue::Array(a)) => a.len(),
                _ => 0,
            };
            out.push((
                *id,
                if created {
                    "created whole by the model".to_string()
                } else {
                    format!("{props} generated property/properties")
                },
            ));
        }
    }
    out
}

fn sorted_keys(m: &std::collections::HashMap<String, PropertyValue>) -> Vec<String> {
    let mut k: Vec<String> = m.keys().cloned().collect();
    k.sort();
    k
}

fn is_claude_available() -> bool {
    std::process::Command::new("claude")
        .arg("--version")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}
