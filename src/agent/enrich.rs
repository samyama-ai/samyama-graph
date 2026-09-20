//! Governed query-time enrichment (GAK) — single-graph OSS port of the enterprise
//! ADR-031 pipeline (epic #696, sub-issues #698–#702).
//!
//! The loop: a **policy** declares which `(Label, property)` pairs are enrichable →
//! a query **surfaces** nodes → [`detect_gaps`] finds declared properties that are
//! null → the [`EnrichmentWorker`] asks the LLM to fill each, **quarantines** the
//! answer under `_enrichment` with provenance (never the real property/graph), and
//! **honest-declines** (`UNKNOWN` ⇒ nothing written) → a later [`verify`] pass
//! promotes a pending value to the real property iff its confidence clears the
//! spec's `trust_floor`. No policy ⇒ nothing is ever enriched (the safe default).
//!
//! #702: a spec may carry a [`Materialize`], making the gap **relationship-shaped** —
//! the worker asks for a *list* of target entities and `verify` materializes
//! `(node)-[:edge_type]->(:target_label {target_key})` nodes + edges (provenance-tagged)
//! instead of promoting a scalar. This is the "subgraph as the missing data layer" form.

use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};

use crate::graph::{EdgeType, GraphStore, Label, NodeId, PropertyValue};
use crate::nlq::client::NLQClient;
use crate::query::{RecordBatch, Value};

/// Reserved node property holding quarantined enrichments (never queried as data).
pub const ENRICHMENT_PROPERTY: &str = "_enrichment";
/// Confidence for an LLM (parametric, unsourced) value — deliberately low.
pub const LLM_DEFAULT_CONFIDENCE: f64 = 0.4;
/// OSS serves a single graph; the store API still takes a tenant id.
const TENANT: &str = "default";

// ─────────────────────────────────────────────────────────── config (#698/#702)

/// Where an enriched value may come from. OSS wires the `Llm` source only.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EnrichSource {
    /// The LLM's parametric knowledge — unsourced, lowest trust.
    Llm,
}

fn default_target_key() -> String {
    "name".to_string()
}

/// A relationship-shaped gap (#702). When present on a spec, the property is filled as a
/// *list* of target entities and materialized as `(node)-[:edge_type]->(:target_label {target_key})`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Materialize {
    pub edge_type: String,
    pub target_label: String,
    #[serde(default = "default_target_key")]
    pub target_key: String,
    /// Optional org-supplied controlled vocabulary (e.g. a sensor-type → failure-mode
    /// taxonomy the org already maintains). When set, the fill prompt prefers these exact
    /// names for types the taxonomy covers and falls back to the LLM's general knowledge
    /// only where the taxonomy is silent — tiered grounding, not a per-node answer key.
    #[serde(default)]
    pub vocabulary: Option<serde_json::Value>,
}

/// Per-`(label, property)` policy. An empty-source spec is declared-but-inert.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct EnrichSpec {
    #[serde(default)]
    pub sources: Vec<EnrichSource>,
    /// Minimum confidence to promote out of quarantine. `0.0` = promote on any pass.
    #[serde(default)]
    pub trust_floor: f64,
    /// If set, this is a relationship gap: fill a list and materialize a subgraph (#702).
    #[serde(default)]
    pub materialize: Option<Materialize>,
}

/// Single-graph enrichment policy: `Label -> property -> spec`. No policy ⇒ inert.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct EnrichConfig {
    #[serde(default)]
    pub policies: HashMap<String, HashMap<String, EnrichSpec>>,
}

impl EnrichConfig {
    fn trust_floor_for(&self, label: &str, property: &str) -> Option<f64> {
        self.policies
            .get(label)
            .and_then(|p| p.get(property))
            .map(|s| s.trust_floor)
    }
}

// ─────────────────────────────────────────────────────────── gap detection (#699)

/// A declared-enrichable property that is missing on a surfaced node.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct GapEvent {
    pub node_id: u64,
    pub label: String,
    pub property: String,
    /// Relationship-shaped when set (#702).
    pub materialize: Option<Materialize>,
}

/// Node ids a query result surfaced (its `Node`/`NodeRef` bindings). Gaps are only
/// detected on nodes the query actually returned — never inferred from an empty result.
pub fn collect_result_nodes(batch: &RecordBatch) -> Vec<NodeId> {
    let mut seen = HashSet::new();
    let mut out = Vec::new();
    for record in &batch.records {
        for value in record.values() {
            let id = match value {
                Value::Node(id, _) => Some(*id),
                Value::NodeRef(id) => Some(*id),
                _ => None,
            };
            if let Some(id) = id {
                if seen.insert(id) {
                    out.push(id);
                }
            }
        }
    }
    out
}

/// Detect declared-enrichable, missing properties on the given nodes.
pub fn detect_gaps(config: &EnrichConfig, store: &GraphStore, node_ids: &[NodeId]) -> Vec<GapEvent> {
    let mut seen: HashSet<(u64, String, String)> = HashSet::new();
    let mut gaps = Vec::new();
    for &id in node_ids {
        let Some(node) = store.get_node(id) else {
            continue;
        };
        let props = store.node_properties_merged(id);
        for (label, specs) in &config.policies {
            if !node.has_label(&Label::new(label.clone())) {
                continue;
            }
            for (property, spec) in specs {
                if spec.sources.is_empty() {
                    continue; // declared but inert
                }
                let missing = props.get(property).map(|v| v.is_null()).unwrap_or(true);
                if missing {
                    let key = (id.as_u64(), label.clone(), property.clone());
                    if seen.insert(key) {
                        gaps.push(GapEvent {
                            node_id: id.as_u64(),
                            label: label.clone(),
                            property: property.clone(),
                            materialize: spec.materialize.clone(),
                        });
                    }
                }
            }
        }
    }
    gaps
}

// ─────────────────────────────────────────────────────────── worker (#700/#702)

/// A filled value ready to be quarantined (produced off the store lock).
#[derive(Debug, Clone)]
pub struct Outcome {
    pub node_id: u64,
    pub property: String,
    pub value: String,
    pub confidence: f64,
    pub method: String,
    pub prompt_hash: String,
    /// Relationship targets + materialize spec, when this is a subgraph gap (#702).
    pub targets: Option<Vec<String>>,
    pub materialize: Option<Materialize>,
}

fn prompt_hash(prompt: &str) -> String {
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    prompt.hash(&mut h);
    format!("{:016x}", h.finish())
}

/// Prompt to complete a scalar `property` of a `label`, with honest-decline for instance facts.
pub fn build_prompt(label: &str, property: &str, context: &[(String, String)]) -> String {
    let mut ctx = String::new();
    for (k, v) in context {
        if k == ENRICHMENT_PROPERTY {
            continue;
        }
        ctx.push_str(&format!("- {}: {}\n", k, v));
    }
    if ctx.is_empty() {
        ctx.push_str("(no other properties known)\n");
    }
    format!(
        "You are completing the `{prop}` of a {label} in a knowledge graph.\n\
         Known properties:\n{ctx}\n\
         If this {label} denotes a well-known general concept or type, give a concise, factual \
         `{prop}` for it (one or two plain-text sentences; no preamble, quotes, or restating the \
         name). Only if `{prop}` is an instance-specific value that cannot be reasonably inferred \
         from general knowledge (e.g. a particular unit's serial number, install date, or bespoke \
         wiring) return exactly UNKNOWN.",
        label = label,
        prop = property,
        ctx = ctx,
    )
}

/// Prompt to list the `edge_type` targets of a `label` (a relationship gap, #702).
pub fn build_list_prompt(label: &str, mat: &Materialize, context: &[(String, String)]) -> String {
    let mut ctx = String::new();
    for (k, v) in context {
        if k == ENRICHMENT_PROPERTY {
            continue;
        }
        ctx.push_str(&format!("- {}: {}\n", k, v));
    }
    if ctx.is_empty() {
        ctx.push_str("(no other properties known)\n");
    }
    // Tiered grounding: prefer the org's own taxonomy where it covers this type, and lean on
    // the LLM's general knowledge only where the taxonomy is silent.
    let vocab_block = match &mat.vocabulary {
        Some(v) => {
            let pretty = serde_json::to_string_pretty(v).unwrap_or_else(|_| v.to_string());
            format!(
                "The organization maintains an approved taxonomy mapping each {label} type to its \
                 {target}s (JSON):\n{vocab}\n\
                 Find the entry whose key best matches this {label}'s type from the known properties \
                 above and output ONLY those exact {target} names, verbatim. If no taxonomy entry \
                 matches this {label}, fall back to general domain knowledge for well-known types.\n",
                label = label,
                target = mat.target_label,
                vocab = pretty,
            )
        }
        None => String::new(),
    };
    format!(
        "In an industrial-asset knowledge graph, list the {target}s that this {label} relates to \
         via `{edge}`.\nKnown properties:\n{ctx}\n{vocab_block}\
         Output the {target}s **one per line**, short canonical names only — no numbering, bullets, \
         prose, or blank lines. Only if this is an instance-specific relationship that cannot be \
         inferred from the taxonomy or general knowledge, return exactly UNKNOWN.",
        target = mat.target_label,
        label = label,
        edge = mat.edge_type,
        ctx = ctx,
        vocab_block = vocab_block,
    )
}

/// Parse a newline list, stripping bullets/numbering; drops `UNKNOWN`/blank lines.
fn parse_list(answer: &str) -> Vec<String> {
    answer
        .lines()
        .map(|l| {
            l.trim()
                .trim_start_matches(|c: char| {
                    c == '-' || c == '*' || c == '•' || c.is_ascii_digit() || c == '.' || c == ')' || c == ' '
                })
                .trim()
                .to_string()
        })
        .filter(|l| !l.is_empty() && !l.eq_ignore_ascii_case("UNKNOWN"))
        .collect()
}

/// The enrichment worker: an LLM client + the model label for provenance.
pub struct EnrichmentWorker {
    client: NLQClient,
    model: String,
}

impl EnrichmentWorker {
    pub fn new(client: NLQClient, model: String) -> Self {
        Self { client, model }
    }

    /// Fill one gap from the LLM. Returns `None` when the model declines (`UNKNOWN`) or returns
    /// nothing — never fabricates. Relationship gaps (#702) yield a list of targets.
    pub async fn fill(&self, gap: &GapEvent, context: &[(String, String)]) -> Option<Outcome> {
        // NLQClient::generate_cypher is a generic chat call (misnamed); reuse it.
        if let Some(mat) = &gap.materialize {
            let prompt = build_list_prompt(&gap.label, mat, context);
            let answer = self.client.generate_cypher(&prompt).await.ok()?;
            if answer.trim().eq_ignore_ascii_case("UNKNOWN") {
                return None; // honest-decline
            }
            let targets = parse_list(&answer);
            if targets.is_empty() {
                return None;
            }
            return Some(Outcome {
                node_id: gap.node_id,
                property: gap.property.clone(),
                value: targets.join("; "),
                confidence: LLM_DEFAULT_CONFIDENCE,
                method: format!("llm:{}", self.model),
                prompt_hash: prompt_hash(&prompt),
                targets: Some(targets),
                materialize: Some(mat.clone()),
            });
        }
        let prompt = build_prompt(&gap.label, &gap.property, context);
        let answer = self.client.generate_cypher(&prompt).await.ok()?;
        let value = answer.trim();
        if value.is_empty() || value.eq_ignore_ascii_case("UNKNOWN") {
            return None; // honest-decline
        }
        Some(Outcome {
            node_id: gap.node_id,
            property: gap.property.clone(),
            value: value.to_string(),
            confidence: LLM_DEFAULT_CONFIDENCE,
            method: format!("llm:{}", self.model),
            prompt_hash: prompt_hash(&prompt),
            targets: None,
            materialize: None,
        })
    }
}

// ─────────────────────────── quarantine + verification (#700/#701/#702)

fn read_enrichment_map(store: &GraphStore, node_id: NodeId) -> HashMap<String, PropertyValue> {
    match store.node_properties_merged(node_id).get(ENRICHMENT_PROPERTY) {
        Some(PropertyValue::Map(m)) => m.clone(),
        _ => HashMap::new(),
    }
}

/// Persist an outcome into `_enrichment.<property>` (quarantined; the real property/graph is untouched).
pub fn quarantine(store: &mut GraphStore, out: &Outcome) -> Result<(), String> {
    let node_id = NodeId(out.node_id);
    let mut root = read_enrichment_map(store, node_id);
    let mut entry: HashMap<String, PropertyValue> = HashMap::new();
    entry.insert("value".into(), PropertyValue::String(out.value.clone()));
    entry.insert("source".into(), PropertyValue::String("llm".into()));
    entry.insert("status".into(), PropertyValue::String("pending_verification".into()));
    entry.insert("confidence".into(), PropertyValue::Float(out.confidence));
    entry.insert("method".into(), PropertyValue::String(out.method.clone()));
    entry.insert("prompt_hash".into(), PropertyValue::String(out.prompt_hash.clone()));
    if let (Some(targets), Some(mat)) = (&out.targets, &out.materialize) {
        let arr: Vec<PropertyValue> = targets.iter().map(|t| PropertyValue::String(t.clone())).collect();
        entry.insert("kind".into(), PropertyValue::String("relationship".into()));
        entry.insert("targets".into(), PropertyValue::Array(arr));
        entry.insert("edge_type".into(), PropertyValue::String(mat.edge_type.clone()));
        entry.insert("target_label".into(), PropertyValue::String(mat.target_label.clone()));
        entry.insert("target_key".into(), PropertyValue::String(mat.target_key.clone()));
    }
    root.insert(out.property.clone(), PropertyValue::Map(entry));
    store
        .set_node_property(TENANT, node_id, ENRICHMENT_PROPERTY, PropertyValue::Map(root))
        .map_err(|e| format!("{:?}", e))
}

/// Materialize `(node)-[:edge]->(:target_label {target_key: name})` for each name, MERGE-ing
/// target nodes by `target_key`; tags new targets `source:"LLM-derived"`. Returns edges created.
fn materialize_edges(store: &mut GraphStore, node_id: NodeId, targets: &[String], mat: &Materialize) -> usize {
    let tlabel = Label::new(mat.target_label.clone());
    // Existing target name -> id (immutable phase).
    let mut existing: HashMap<String, NodeId> = HashMap::new();
    if let Some(ids) = store.nodes_with_label(&tlabel) {
        let ids: Vec<NodeId> = ids.iter().copied().collect();
        for tid in ids {
            if let Some(PropertyValue::String(nm)) = store.node_properties_merged(tid).get(&mat.target_key) {
                existing.insert(nm.clone(), tid);
            }
        }
    }
    let mut created = 0usize;
    for name in targets {
        let tid = if let Some(&id) = existing.get(name) {
            id
        } else {
            let id = store.create_node(tlabel.clone());
            let _ = store.set_node_property(TENANT, id, mat.target_key.clone(), PropertyValue::String(name.clone()));
            let _ = store.set_node_property(TENANT, id, "source", PropertyValue::String("LLM-derived".into()));
            existing.insert(name.clone(), id);
            id
        };
        if store.create_edge(node_id, tid, mat.edge_type.clone()).is_ok() {
            created += 1;
        }
    }
    created
}

/// Report from a verification pass.
#[derive(Debug, Default, Serialize)]
pub struct VerifyReport {
    pub nodes_processed: usize,
    pub promoted: usize,
    pub edges_materialized: usize,
    pub still_pending: usize,
}

/// Verify+promote pending enrichments on the given nodes: scalar values become the real property;
/// relationship values (#702) materialize `(node)-[:edge]->(target)` subgraphs — both gated on `trust_floor`.
pub fn verify(config: &EnrichConfig, store: &mut GraphStore, node_ids: &[NodeId]) -> VerifyReport {
    let mut rep = VerifyReport::default();
    for &id in node_ids {
        let mut root = read_enrichment_map(store, id);
        if root.is_empty() {
            continue;
        }
        rep.nodes_processed += 1;
        let label = store.get_node(id).and_then(|n| {
            config
                .policies
                .keys()
                .find(|l| n.has_label(&Label::new((*l).clone())))
                .cloned()
        });
        let mut scalar_promotions: Vec<(String, String)> = Vec::new();
        let mut rel_promotions: Vec<(String, Vec<String>, Materialize)> = Vec::new();
        for (property, entry) in root.iter() {
            let PropertyValue::Map(e) = entry else { continue };
            let status = match e.get("status") {
                Some(PropertyValue::String(s)) => s.clone(),
                _ => continue,
            };
            if status != "pending_verification" {
                continue;
            }
            let confidence = match e.get("confidence") {
                Some(PropertyValue::Float(f)) => *f,
                _ => 0.0,
            };
            let floor = label
                .as_deref()
                .and_then(|l| config.trust_floor_for(l, property))
                .unwrap_or(0.0);
            if confidence < floor {
                rep.still_pending += 1;
                continue;
            }
            let is_rel = matches!(e.get("kind"), Some(PropertyValue::String(k)) if k == "relationship");
            if is_rel {
                if let (Some(PropertyValue::Array(arr)), Some(PropertyValue::String(edge)),
                        Some(PropertyValue::String(tl)), Some(PropertyValue::String(tk))) =
                    (e.get("targets"), e.get("edge_type"), e.get("target_label"), e.get("target_key"))
                {
                    let targets: Vec<String> = arr
                        .iter()
                        .filter_map(|v| if let PropertyValue::String(s) = v { Some(s.clone()) } else { None })
                        .collect();
                    rel_promotions.push((property.clone(), targets, Materialize {
                        edge_type: edge.clone(), target_label: tl.clone(), target_key: tk.clone(),
                        vocabulary: None,
                    }));
                }
            } else if let Some(PropertyValue::String(v)) = e.get("value") {
                scalar_promotions.push((property.clone(), v.clone()));
            }
        }
        for (property, value) in scalar_promotions {
            if store.set_node_property(TENANT, id, property.clone(), PropertyValue::String(value)).is_ok() {
                if let Some(PropertyValue::Map(e)) = root.get_mut(&property) {
                    e.insert("status".into(), PropertyValue::String("verified".into()));
                }
                rep.promoted += 1;
            }
        }
        for (property, targets, mat) in rel_promotions {
            let n = materialize_edges(store, id, &targets, &mat);
            rep.edges_materialized += n;
            if let Some(PropertyValue::Map(e)) = root.get_mut(&property) {
                e.insert("status".into(), PropertyValue::String("verified".into()));
            }
            rep.promoted += 1;
        }
        let _ = store.set_node_property(TENANT, id, ENRICHMENT_PROPERTY, PropertyValue::Map(root));
    }
    rep
}

/// Build an [`EnrichmentWorker`] from environment config (same knobs as `/api/nlq`).
pub fn worker_from_env() -> Result<EnrichmentWorker, String> {
    use crate::persistence::tenant::{LLMProvider, NLQConfig};
    // Same refusal as `/api/nlq`, and it matters more here: enrichment sends the
    // gap node's actual property **values**, not just schema metadata.
    let provider = LLMProvider::parse(&std::env::var("NLQ_PROVIDER").unwrap_or_default())?;
    let model = std::env::var("NLQ_MODEL").unwrap_or_else(|_| "gpt-4o".to_string());
    let config = NLQConfig {
        enabled: true,
        provider,
        model: model.clone(),
        api_key: std::env::var("OPENAI_API_KEY").ok(),
        api_base_url: std::env::var("NLQ_API_BASE_URL").ok(),
        // Neutral domain prompt — the NLQ client otherwise defaults to "You are a Cypher
        // expert", which biases enrichment answers toward Cypher instead of facts.
        system_prompt: Some(
            "You are a precise industrial asset-management domain expert. Answer factually \
             and follow the requested output format exactly.".to_string(),
        ),
    };
    let client = NLQClient::new(&config).map_err(|e| e.to_string())?;
    Ok(EnrichmentWorker::new(client, model))
}

/// Process-global single-graph enrichment policy (OSS serves one graph). Avoids threading
/// the config through `AppState` + every construction site.
pub fn global_config() -> &'static std::sync::RwLock<EnrichConfig> {
    static G: std::sync::OnceLock<std::sync::RwLock<EnrichConfig>> = std::sync::OnceLock::new();
    G.get_or_init(|| std::sync::RwLock::new(EnrichConfig::default()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mat(vocabulary: Option<serde_json::Value>) -> Materialize {
        Materialize {
            edge_type: "MONITORS".to_string(),
            target_label: "FailureMode".to_string(),
            target_key: "name".to_string(),
            vocabulary,
        }
    }

    #[test]
    fn list_prompt_injects_org_vocabulary_when_set() {
        let vocab = serde_json::json!({ "Supply Temperature": ["Evaporator Water side fouling"] });
        let ctx = vec![("name".to_string(), "Chiller 7 Supply Temperature".to_string())];
        let p = build_list_prompt("Sensor", &mat(Some(vocab)), &ctx);
        // tier 1: org taxonomy is presented with its exact strings
        assert!(p.contains("approved taxonomy"), "tiered vocab instruction missing:\n{p}");
        assert!(p.contains("Evaporator Water side fouling"), "org vocab strings missing:\n{p}");
        // tier 2: LLM fallback where the taxonomy is silent
        assert!(p.contains("fall back to general domain knowledge"), "LLM-fallback tier missing:\n{p}");
    }

    #[test]
    fn list_prompt_omits_vocabulary_block_when_none() {
        let ctx = vec![("name".to_string(), "Chiller 7 Supply Temperature".to_string())];
        let p = build_list_prompt("Sensor", &mat(None), &ctx);
        assert!(!p.contains("approved taxonomy"), "vocab block should be absent when None:\n{p}");
    }

    #[test]
    fn parse_list_strips_bullets_and_drops_unknown() {
        let got = parse_list("- Evaporator Water side fouling\n2) Condenser Water side fouling\nUNKNOWN\n\n");
        assert_eq!(
            got,
            vec![
                "Evaporator Water side fouling".to_string(),
                "Condenser Water side fouling".to_string(),
            ]
        );
    }
}
