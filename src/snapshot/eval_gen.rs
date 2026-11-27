//! Generate an evaluation set from a knowledge graph (ML-14, #1772).
//!
//! An evaluation set is a list of questions, each with a gold answer, a
//! difficulty label, and some questions that deliberately have no answer. Two
//! of 21 KG repositories ship one, each written by hand; KG-08, EVAL-04 and
//! ML-14 all point at the generator that would make the other 19 cheap. This
//! is that generator.
//!
//! ## What it does
//!
//! It reads the graph's own schema -- labels, an identifying property per
//! label, the relationships that actually occur between labels -- and fills a
//! fixed set of question templates from it. Every question is a Cypher query
//! whose values are **bound parameters**, never text spliced into the query
//! (#1156), and every gold answer is what the engine returned for it. A gold
//! answer is therefore correct for this graph by construction, and
//! `samyama verify` re-checks it against any restore.
//!
//! The output is an ordinary question catalog (`samyama.queries/1`, the
//! `.sgqueries` format of #1159), so everything that already reads a catalog
//! -- `verify`, `catalog-gate`, the KG-08 conformance check -- reads this one.
//! Each entry also carries its answer literally (`answer`), because the
//! catalog's row hash proves a restore and cannot score a model's reply.
//!
//! ## Difficulty
//!
//! * **easy** -- one lookup or one aggregate over one label.
//! * **medium** -- one relationship hop, or counting one.
//! * **hard** -- two hops, a ranking, or an aggregate over a hop.
//!
//! ## Unanswerable items
//!
//! Two kinds, both checked to return nothing before they are kept:
//!
//! * a lookup for an identifier the graph does not have;
//! * a relationship type that exists in the graph asked between labels it
//!   never joins.
//!
//! They are not padding. A suite without them cannot tell a system that
//! declines from one that guesses.
//!
//! ## Reproducible
//!
//! The same snapshot and the same seed give the same set, byte for byte: the
//! schema is read in sorted order and every choice comes from a seeded
//! generator written here, so it cannot change under a dependency upgrade. The
//! seed is recorded in the catalog's `generated_by`.
//!
//! ## What it does not do
//!
//! * The questions are templated English, not paraphrased by a model. They are
//!   stiff on purpose: a gold answer is only trustworthy if the question is
//!   unambiguous about which query it means.
//! * A graph too small or too sparse for the count asked yields fewer
//!   questions, and the report says how many and why. Nothing is repeated to
//!   reach the number.

use std::collections::{BTreeMap, BTreeSet};

use crate::graph::{GraphStore, Label, NodeId, PropertyValue};
use crate::query::executor::record::Value;
use crate::query::RecordBatch;
use crate::snapshot::verify::{self, ParamSpec, QueryCatalog, QuerySpec};

/// Questions per suite when no count is given: the mega benchmark's 500.
pub const DEFAULT_COUNT: usize = 500;

/// Share of the suite that is unanswerable, in percent. Ten percent puts 50 in
/// a 500-question suite: enough to measure refusal, few enough that a system
/// answering "nothing" to everything cannot score well.
pub const UNANSWERABLE_PERCENT: usize = 10;

/// Shares of the answerable questions, in percent: easy, medium, hard.
pub const DIFFICULTY_PERCENT: [(Difficulty, usize); 3] = [
    (Difficulty::Easy, 35),
    (Difficulty::Medium, 35),
    (Difficulty::Hard, 30),
];

/// Most rows a gold answer may have. A question whose answer is a list of
/// hundreds is a retrieval task, not a question, and nobody grades it by eye.
pub const MAX_ANSWER_ROWS: usize = 20;

/// Nodes sampled per label to read its properties and relationships.
pub const SCHEMA_SAMPLE: usize = 200;

/// Share of sampled nodes, in percent, that must carry a property for it to be
/// asked about. Below this most generated questions would hit a node without
/// it and be rejected.
pub const PRESENCE_PERCENT: usize = 90;

/// Candidates tried per question asked, before giving up on a shortfall.
pub const ATTEMPTS_PER_QUESTION: usize = 6;

/// Rows a "top" question asks for.
pub const TOP_K: usize = 5;

/// Property names preferred as a node's identifier or display name, in order.
const NAME_LIKE: &[&str] = &["name", "title", "label", "id", "code", "key", "symbol"];

/// How hard a question is.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Difficulty {
    Easy,
    Medium,
    Hard,
}

impl Difficulty {
    pub fn as_str(self) -> &'static str {
        match self {
            Difficulty::Easy => "easy",
            Difficulty::Medium => "medium",
            Difficulty::Hard => "hard",
        }
    }
}

/// How to generate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EvalGenOptions {
    /// Questions wanted, unanswerable ones included.
    pub count: usize,
    /// Seed for every choice. Same snapshot + same seed = same set.
    pub seed: u64,
}

impl Default for EvalGenOptions {
    fn default() -> Self {
        Self {
            count: DEFAULT_COUNT,
            seed: 0,
        }
    }
}

/// What was generated, and what could not be.
#[derive(Debug, Clone, Default, PartialEq, serde::Serialize)]
pub struct EvalGenReport {
    pub requested: usize,
    pub generated: usize,
    /// Answerable questions per difficulty.
    pub by_difficulty: BTreeMap<String, usize>,
    pub unanswerable: usize,
    /// Questions per template.
    pub by_template: BTreeMap<String, usize>,
    /// Candidates tried and not kept: no rows, too many rows, a null answer,
    /// a duplicate, or an "unanswerable" one that turned out to have rows.
    pub rejected: usize,
    /// Why fewer than `requested` were generated, when they were.
    pub shortfall: Vec<String>,
}

/// The generated set.
#[derive(Debug, Clone)]
pub struct EvalSet {
    pub catalog: QueryCatalog,
    pub report: EvalGenReport,
}

/// Generate an evaluation set from `store`.
pub fn generate(store: &GraphStore, opts: &EvalGenOptions) -> Result<EvalSet, String> {
    if opts.count == 0 {
        return Err("asked for 0 questions".to_string());
    }
    let mut rng = SplitMix64::new(opts.seed);
    let schema = Schema::read(store, &mut rng);
    if schema.labels.is_empty() {
        return Err(
            "the graph has no labelled nodes, so there is nothing to ask about".to_string(),
        );
    }

    let unanswerable_quota =
        (opts.count * UNANSWERABLE_PERCENT / 100).max(usize::from(opts.count >= 10));
    let answerable = opts.count - unanswerable_quota;
    let mut quotas: BTreeMap<Bucket, usize> = BTreeMap::new();
    let mut assigned = 0;
    for (i, (d, pct)) in DIFFICULTY_PERCENT.iter().enumerate() {
        // The last bucket takes the remainder, so the quotas sum to the count.
        let n = if i + 1 == DIFFICULTY_PERCENT.len() {
            answerable - assigned
        } else {
            answerable * pct / 100
        };
        assigned += n;
        quotas.insert(Bucket::Answerable(*d), n);
    }
    quotas.insert(Bucket::Unanswerable, unanswerable_quota);

    let mut report = EvalGenReport {
        requested: opts.count,
        ..Default::default()
    };
    let mut kept: Vec<(QuerySpec, serde_json::Value, &'static str)> = Vec::new();
    let mut seen: BTreeSet<(String, String)> = BTreeSet::new();

    for (bucket, quota) in &quotas {
        let mut pool = candidates(&schema, *bucket, &mut rng);
        let mut taken = 0;
        let mut tries = 0;
        let budget = quota.saturating_mul(ATTEMPTS_PER_QUESTION).max(*quota);
        while taken < *quota && tries < budget {
            let Some(c) = pool.next() else { break };
            tries += 1;
            let dedup = (
                c.cypher.clone(),
                c.params
                    .iter()
                    .map(|p| p.sample.to_string())
                    .collect::<Vec<_>>()
                    .join("\u{1f}"),
            );
            if seen.contains(&dedup) {
                report.rejected += 1;
                continue;
            }
            let batch = match verify::run_bound(store, &c.cypher, &c.params) {
                Ok(b) => b,
                Err(_) => {
                    report.rejected += 1;
                    continue;
                }
            };
            let accept = if c.unanswerable {
                batch.records.is_empty()
            } else {
                !batch.records.is_empty()
                    && batch.records.len() <= MAX_ANSWER_ROWS
                    && !first_is_null(&batch)
                    && (!c.ranked || counts_differ(&batch))
            };
            if !accept {
                report.rejected += 1;
                continue;
            }
            seen.insert(dedup);
            taken += 1;
            let id = format!("{}-{:04}", c.template, kept.len() + 1);
            kept.push((
                QuerySpec {
                    id,
                    question: c.question,
                    paraphrases: Vec::new(),
                    difficulty: c.difficulty.as_str().to_string(),
                    cypher: c.cypher,
                    unanswerable: c.unanswerable,
                    params: c.params,
                },
                render(&batch),
                c.template,
            ));
        }
        if taken < *quota {
            report.shortfall.push(format!(
                "{}: {taken} of {quota} -- {}",
                bucket.name(),
                if pool.is_exhausted() {
                    "the graph does not have enough distinct facts of this kind"
                } else {
                    "too many candidates returned nothing usable"
                }
            ));
        }
    }

    let specs: Vec<QuerySpec> = kept.iter().map(|(s, _, _)| s.clone()).collect();
    let mut catalog = verify::build_catalog(store, &specs, &[])?;
    catalog.generated_by = format!("samyama eval-gen {} seed={}", crate::VERSION, opts.seed);
    for (entry, (spec, answer, template)) in catalog.entries.iter_mut().zip(&kept) {
        entry.answer = Some(answer.clone());
        *report.by_template.entry(template.to_string()).or_default() += 1;
        if spec.unanswerable {
            report.unanswerable += 1;
        } else {
            *report
                .by_difficulty
                .entry(spec.difficulty.clone())
                .or_default() += 1;
        }
    }
    report.generated = catalog.entries.len();
    Ok(EvalSet { catalog, report })
}

/// The answer as data: column names and rows, in the query's own order.
fn render(batch: &RecordBatch) -> serde_json::Value {
    let rows: Vec<serde_json::Value> = batch
        .records
        .iter()
        .map(|r| serde_json::Value::Array(batch.columns.iter().map(|c| cell(r.get(c))).collect()))
        .collect();
    serde_json::json!({ "columns": batch.columns, "rows": rows })
}

fn cell(v: Option<&Value>) -> serde_json::Value {
    match v {
        None | Some(Value::Null) => serde_json::Value::Null,
        Some(Value::Property(p)) => crate::snapshot::codec::property_to_json(p),
        Some(other) => serde_json::Value::String(format!("{other:?}")),
    }
}

/// Whether a ranking's counts (its last column) are not all equal.
fn counts_differ(batch: &RecordBatch) -> bool {
    let Some(col) = batch.columns.last() else {
        return false;
    };
    let counts: BTreeSet<String> = batch
        .records
        .iter()
        .map(|r| format!("{:?}", r.get(col)))
        .collect();
    counts.len() > 1
}

/// An answer whose single value is null says nothing ("the average of no
/// values"), so it is not kept as a gold answer.
fn first_is_null(batch: &RecordBatch) -> bool {
    // A missing property comes back as `Property(Null)`, not `Null`: both count.
    batch.records.iter().all(|r| {
        batch.columns.iter().all(|c| {
            matches!(
                r.get(c),
                None | Some(Value::Null) | Some(Value::Property(PropertyValue::Null))
            )
        })
    })
}

// ───────────────────────────────────────────────────────────── the schema

/// What the generator knows about one label.
#[derive(Debug, Clone)]
struct LabelInfo {
    name: String,
    /// An identifying property: on every sampled node, a different value on
    /// each. Questions about one node name it by this.
    key: Option<String>,
    /// What an answer about a node reports: a name-like string, else the key.
    display: Option<String>,
    /// Numeric properties, for aggregates.
    numeric: Vec<String>,
    /// Other properties a lookup can ask for.
    attributes: Vec<String>,
    /// Sampled nodes with their key value, in a seeded order.
    keyed: Vec<(NodeId, PropertyValue)>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct Triple {
    src: String,
    rel: String,
    dst: String,
}

struct Schema {
    labels: BTreeMap<String, LabelInfo>,
    /// Relationships seen between labels, from the sampled nodes.
    triples: Vec<Triple>,
    rel_types: Vec<String>,
}

impl Schema {
    fn read(store: &GraphStore, rng: &mut SplitMix64) -> Schema {
        let mut names: Vec<String> = store
            .all_labels()
            .into_iter()
            .map(|l| l.as_str().to_string())
            .filter(|l| quotable(l))
            .collect();
        names.sort();
        let mut labels = BTreeMap::new();
        let mut triples: BTreeSet<Triple> = BTreeSet::new();
        for name in names {
            let Some(ids) = store.nodes_with_label(&Label::new(&name)) else {
                continue;
            };
            // Sorted first: a HashSet's order differs between runs, and the
            // seed must be the only thing that decides what is chosen.
            let mut ids: Vec<NodeId> = ids.iter().copied().collect();
            ids.sort_unstable();
            rng.shuffle(&mut ids);
            ids.truncate(SCHEMA_SAMPLE);
            if ids.is_empty() {
                continue;
            }
            let props: Vec<_> = ids
                .iter()
                .map(|id| store.node_properties_full(*id))
                .collect();
            let mut presence: BTreeMap<String, (usize, BTreeSet<String>, bool, bool)> =
                BTreeMap::new();
            for p in &props {
                for (k, v) in p {
                    if !quotable(k) {
                        continue;
                    }
                    let e = presence.entry(k.clone()).or_default();
                    e.0 += 1;
                    e.1.insert(format!("{v:?}"));
                    match v {
                        PropertyValue::String(_) => e.2 = true,
                        PropertyValue::Integer(_) | PropertyValue::Float(_) => e.3 = true,
                        _ => {}
                    }
                }
            }
            let enough = |n: usize| n * 100 >= ids.len() * PRESENCE_PERCENT;
            let ranked = |k: &str| {
                NAME_LIKE
                    .iter()
                    .position(|n| k.eq_ignore_ascii_case(n))
                    .unwrap_or(NAME_LIKE.len())
            };
            let mut keys: Vec<&String> = presence
                .iter()
                .filter(|(k, (n, distinct, is_str, is_num))| {
                    *n == ids.len()
                        && distinct.len() == ids.len()
                        && (*is_str ^ *is_num)
                        && key_typed(&props, k)
                })
                .map(|(k, _)| k)
                .collect();
            keys.sort_by_key(|k| (ranked(k), (*k).clone()));
            // Unique in the sample is not unique: confirm over the whole
            // label, so "the X whose key is v" always names one node.
            let all: Vec<NodeId> = store
                .nodes_with_label(&Label::new(&name))
                .map(|s| s.iter().copied().collect())
                .unwrap_or_default();
            let key = keys
                .into_iter()
                .find(|k| unique_over(store, &all, k))
                .cloned();
            let mut displays: Vec<&String> = presence
                .iter()
                .filter(|(_, (n, _, is_str, is_num))| enough(*n) && *is_str && !*is_num)
                .map(|(k, _)| k)
                .collect();
            displays.sort_by_key(|k| (ranked(k), (*k).clone()));
            let display = displays
                .first()
                .map(|k| (*k).clone())
                .or_else(|| key.clone());
            let numeric: Vec<String> = presence
                .iter()
                .filter(|(k, (n, _, is_str, is_num))| {
                    enough(*n) && *is_num && !*is_str && Some(*k) != key.as_ref()
                })
                .map(|(k, _)| k.clone())
                .collect();
            let attributes: Vec<String> = presence
                .iter()
                .filter(|(k, (n, _, is_str, is_num))| {
                    enough(*n) && (*is_str ^ *is_num) && Some(*k) != key.as_ref()
                })
                .map(|(k, _)| k.clone())
                .collect();
            let keyed = match &key {
                Some(k) => ids
                    .iter()
                    .zip(&props)
                    .filter_map(|(id, p)| p.get(k).map(|v| (*id, v.clone())))
                    .collect(),
                None => Vec::new(),
            };
            for id in &ids {
                for e in store.get_outgoing_edges(*id) {
                    let rel = e.edge_type.as_str().to_string();
                    if !quotable(&rel) {
                        continue;
                    }
                    let Some(dst) = store.get_node(e.target) else {
                        continue;
                    };
                    let mut dst_labels: Vec<&str> = dst
                        .labels
                        .iter()
                        .map(|l| l.as_str())
                        .filter(|l| quotable(l))
                        .collect();
                    dst_labels.sort_unstable();
                    for d in dst_labels {
                        triples.insert(Triple {
                            src: name.clone(),
                            rel: rel.clone(),
                            dst: d.to_string(),
                        });
                    }
                }
            }
            labels.insert(
                name.clone(),
                LabelInfo {
                    name,
                    key,
                    display,
                    numeric,
                    attributes,
                    keyed,
                },
            );
        }
        let mut rel_types: Vec<String> = triples
            .iter()
            .map(|t| t.rel.clone())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        rel_types.sort();
        Schema {
            labels,
            triples: triples.into_iter().collect(),
            rel_types,
        }
    }

    fn has_triple(&self, src: &str, rel: &str, dst: &str) -> bool {
        self.triples
            .iter()
            .any(|t| t.src == src && t.rel == rel && t.dst == dst)
    }
}

/// Does every node in `ids` carry `key`, each with a different value?
fn unique_over(store: &GraphStore, ids: &[NodeId], key: &str) -> bool {
    let mut seen = std::collections::HashSet::with_capacity(ids.len());
    ids.iter().all(|id| match store.node_property(*id, key) {
        Some(PropertyValue::String(s)) => seen.insert(s),
        Some(PropertyValue::Integer(i)) => seen.insert(i.to_string()),
        _ => false,
    })
}

/// A key must hold one type on every sampled node, so it can be declared as
/// one parameter type.
fn key_typed(props: &[std::collections::HashMap<String, PropertyValue>], key: &str) -> bool {
    let kinds: BTreeSet<&str> = props
        .iter()
        .filter_map(|p| p.get(key))
        .map(param_kind)
        .collect();
    kinds.len() == 1 && !kinds.contains("other")
}

fn param_kind(v: &PropertyValue) -> &'static str {
    match v {
        PropertyValue::String(_) => "string",
        PropertyValue::Integer(_) => "int",
        _ => "other",
    }
}

/// A name that can be written between backticks. One with a backtick in it
/// would need escaping, and a generated query is not the place to get that
/// wrong; such names are left out.
fn quotable(name: &str) -> bool {
    !name.is_empty() && !name.contains('`') && !name.chars().any(char::is_control)
}

/// "a TREATS" / "an IN_TOURNAMENT", by the first letter.
fn a_or_an(name: &str) -> String {
    let vowel = name
        .chars()
        .next()
        .is_some_and(|c| "AEIOUaeiou".contains(c));
    format!("{} {name}", if vowel { "an" } else { "a" })
}

fn q(name: &str) -> String {
    format!("`{name}`")
}

fn param(value: &PropertyValue) -> Option<ParamSpec> {
    let (kind, sample) = match value {
        PropertyValue::String(s) => ("string", serde_json::Value::String(s.clone())),
        PropertyValue::Integer(i) => ("int", serde_json::Value::from(*i)),
        _ => return None,
    };
    Some(ParamSpec {
        name: "value".to_string(),
        kind: kind.to_string(),
        sample,
        enum_values: None,
    })
}

fn shown(value: &PropertyValue) -> String {
    match value {
        PropertyValue::String(s) => format!("\"{s}\""),
        PropertyValue::Integer(i) => i.to_string(),
        other => format!("{other:?}"),
    }
}

// ───────────────────────────────────────────────────────────── templates

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Bucket {
    Answerable(Difficulty),
    Unanswerable,
}

impl Bucket {
    fn name(self) -> &'static str {
        match self {
            Bucket::Answerable(d) => d.as_str(),
            Bucket::Unanswerable => "unanswerable",
        }
    }
}

#[derive(Debug, Clone)]
struct Candidate {
    template: &'static str,
    difficulty: Difficulty,
    question: String,
    cypher: String,
    params: Vec<ParamSpec>,
    unanswerable: bool,
    /// A ranking: kept only when its counts differ, so the order is decided
    /// by the data and not by the tie-break alone.
    ranked: bool,
}

/// Candidates for one bucket, drawn round-robin across its templates so no
/// one template fills the bucket while others go unused.
struct Pool {
    lanes: Vec<Vec<Candidate>>,
    next_lane: usize,
}

impl Pool {
    fn next(&mut self) -> Option<Candidate> {
        for _ in 0..self.lanes.len() {
            let lane = self.next_lane % self.lanes.len().max(1);
            self.next_lane += 1;
            if let Some(c) = self.lanes[lane].pop() {
                return Some(c);
            }
        }
        None
    }

    fn is_exhausted(&self) -> bool {
        self.lanes.iter().all(Vec::is_empty)
    }
}

/// Keyed nodes of a label, at most `n`, in the label's seeded order.
fn keyed(l: &LabelInfo, n: usize) -> impl Iterator<Item = &(NodeId, PropertyValue)> {
    l.keyed.iter().take(n)
}

fn candidates(s: &Schema, bucket: Bucket, rng: &mut SplitMix64) -> Pool {
    let per_label = SCHEMA_SAMPLE;
    let mut lanes: Vec<Vec<Candidate>> = Vec::new();
    let labels: Vec<&LabelInfo> = s.labels.values().collect();
    match bucket {
        Bucket::Answerable(Difficulty::Easy) => {
            let mut count = Vec::new();
            let mut lookup = Vec::new();
            let mut extreme = Vec::new();
            for l in &labels {
                count.push(Candidate {
                    template: "easy-count",
                    difficulty: Difficulty::Easy,
                    question: format!("How many {} nodes are in the graph?", l.name),
                    cypher: format!("MATCH (n:{}) RETURN count(n) AS answer", q(&l.name)),
                    params: vec![],
                    unanswerable: false,
                    ranked: false,
                });
                for p in &l.numeric {
                    for (agg, word) in [("max", "largest"), ("min", "smallest")] {
                        extreme.push(Candidate {
                            template: "easy-extreme",
                            difficulty: Difficulty::Easy,
                            question: format!("What is the {word} {p} of any {}?", l.name),
                            cypher: format!(
                                "MATCH (n:{}) RETURN {agg}(n.{}) AS answer",
                                q(&l.name),
                                q(p)
                            ),
                            params: vec![],
                            unanswerable: false,
                            ranked: false,
                        });
                    }
                }
                let Some(key) = &l.key else { continue };
                for (_, v) in keyed(l, per_label) {
                    let Some(pv) = param(v) else { continue };
                    for attr in &l.attributes {
                        lookup.push(Candidate {
                            template: "easy-lookup",
                            difficulty: Difficulty::Easy,
                            question: format!(
                                "What is the {attr} of the {} whose {key} is {}?",
                                l.name,
                                shown(v)
                            ),
                            cypher: format!(
                                "MATCH (n:{}) WHERE n.{} = $value RETURN n.{} AS answer",
                                q(&l.name),
                                q(key),
                                q(attr)
                            ),
                            params: vec![pv.clone()],
                            unanswerable: false,
                            ranked: false,
                        });
                    }
                }
            }
            lanes.extend([count, lookup, extreme]);
        }
        Bucket::Answerable(Difficulty::Medium) => {
            let mut out = Vec::new();
            let mut incoming = Vec::new();
            let mut degree = Vec::new();
            let mut pattern = Vec::new();
            for t in &s.triples {
                let (Some(src), Some(dst)) = (s.labels.get(&t.src), s.labels.get(&t.dst)) else {
                    continue;
                };
                pattern.push(Candidate {
                    template: "medium-count-pattern",
                    difficulty: Difficulty::Medium,
                    question: format!(
                        "How many {} have at least one {} relationship to a {}?",
                        src.name, t.rel, dst.name
                    ),
                    cypher: format!(
                        "MATCH (a:{})-[:{}]->(:{}) RETURN count(DISTINCT a) AS answer",
                        q(&src.name),
                        q(&t.rel),
                        q(&dst.name)
                    ),
                    params: vec![],
                    unanswerable: false,
                    ranked: false,
                });
                if let (Some(key), Some(disp)) = (&src.key, &dst.display) {
                    for (_, v) in keyed(src, per_label) {
                        let Some(pv) = param(v) else { continue };
                        out.push(Candidate {
                            template: "medium-one-hop",
                            difficulty: Difficulty::Medium,
                            question: format!(
                                "Which {} (by {disp}) does the {} whose {key} is {} have {} relationship to?",
                                dst.name,
                                src.name,
                                shown(v),
                                a_or_an(&t.rel)
                            ),
                            cypher: format!(
                                "MATCH (a:{})-[:{}]->(b:{}) WHERE a.{} = $value RETURN DISTINCT b.{} AS answer",
                                q(&src.name),
                                q(&t.rel),
                                q(&dst.name),
                                q(key),
                                q(disp)
                            ),
                            params: vec![pv.clone()],
                            unanswerable: false,
                            ranked: false,
                        });
                        degree.push(Candidate {
                            template: "medium-count-neighbours",
                            difficulty: Difficulty::Medium,
                            question: format!(
                                "How many distinct {} does the {} whose {key} is {} have {} relationship to?",
                                dst.name,
                                src.name,
                                shown(v),
                                a_or_an(&t.rel)
                            ),
                            cypher: format!(
                                "MATCH (a:{})-[:{}]->(b:{}) WHERE a.{} = $value RETURN count(DISTINCT b) AS answer",
                                q(&src.name),
                                q(&t.rel),
                                q(&dst.name),
                                q(key)
                            ),
                            params: vec![pv],
                            unanswerable: false,
                            ranked: false,
                        });
                    }
                }
                if let (Some(key), Some(disp)) = (&dst.key, &src.display) {
                    for (_, v) in keyed(dst, per_label) {
                        let Some(pv) = param(v) else { continue };
                        incoming.push(Candidate {
                            template: "medium-incoming",
                            difficulty: Difficulty::Medium,
                            question: format!(
                                "Which {} (by {disp}) have {} relationship to the {} whose {key} is {}?",
                                src.name,
                                a_or_an(&t.rel),
                                dst.name,
                                shown(v)
                            ),
                            cypher: format!(
                                "MATCH (a:{})-[:{}]->(b:{}) WHERE b.{} = $value RETURN DISTINCT a.{} AS answer",
                                q(&src.name),
                                q(&t.rel),
                                q(&dst.name),
                                q(key),
                                q(disp)
                            ),
                            params: vec![pv],
                            unanswerable: false,
                            ranked: false,
                        });
                    }
                }
            }
            lanes.extend([out, incoming, degree, pattern]);
        }
        Bucket::Answerable(Difficulty::Hard) => {
            let mut two_hop = Vec::new();
            let mut top = Vec::new();
            let mut avg = Vec::new();
            for t in &s.triples {
                let (Some(src), Some(dst)) = (s.labels.get(&t.src), s.labels.get(&t.dst)) else {
                    continue;
                };
                if let (Some(disp), Some(key)) = (&src.display, &src.key) {
                    top.push(Candidate {
                        template: "hard-top",
                        difficulty: Difficulty::Hard,
                        question: format!(
                            "Which {TOP_K} {} (by {disp}) have the most {} relationships to a {}, and how many each? \
                             Ties are broken by {key}.",
                            src.name, t.rel, dst.name
                        ),
                        cypher: format!(
                            "MATCH (a:{})-[:{}]->(:{}) WITH a, count(*) AS n \
                             RETURN a.{} AS answer, n ORDER BY n DESC, a.{} ASC LIMIT {TOP_K}",
                            q(&src.name),
                            q(&t.rel),
                            q(&dst.name),
                            q(disp),
                            q(key)
                        ),
                        params: vec![],
                        unanswerable: false,
                        ranked: true,
                    });
                }
                if let Some(key) = &src.key {
                    for num in &dst.numeric {
                        for (_, v) in keyed(src, per_label) {
                            let Some(pv) = param(v) else { continue };
                            avg.push(Candidate {
                                template: "hard-average-over-hop",
                                difficulty: Difficulty::Hard,
                                question: format!(
                                    "What is the average {num} of the {} that the {} whose {key} is {} has {} \
                                     relationship to?",
                                    dst.name,
                                    src.name,
                                    shown(v),
                                    a_or_an(&t.rel)
                                ),
                                cypher: format!(
                                    "MATCH (a:{})-[:{}]->(b:{}) WHERE a.{} = $value RETURN avg(b.{}) AS answer",
                                    q(&src.name),
                                    q(&t.rel),
                                    q(&dst.name),
                                    q(key),
                                    q(num)
                                ),
                                params: vec![pv],
                                unanswerable: false,
                                ranked: false,
                            });
                        }
                    }
                    for t2 in s.triples.iter().filter(|t2| t2.src == t.dst) {
                        let Some(end) = s.labels.get(&t2.dst) else {
                            continue;
                        };
                        let Some(disp) = &end.display else { continue };
                        for (_, v) in keyed(src, per_label) {
                            let Some(pv) = param(v) else { continue };
                            two_hop.push(Candidate {
                                template: "hard-two-hop",
                                difficulty: Difficulty::Hard,
                                question: format!(
                                    "Which {} (by {disp}) are reached from the {} whose {key} is {} by {} \
                                     relationship to a {} and then {} relationship?",
                                    end.name,
                                    src.name,
                                    shown(v),
                                    a_or_an(&t.rel),
                                    dst.name,
                                    a_or_an(&t2.rel)
                                ),
                                cypher: format!(
                                    "MATCH (a:{})-[:{}]->(:{})-[:{}]->(c:{}) WHERE a.{} = $value \
                                     RETURN DISTINCT c.{} AS answer",
                                    q(&src.name),
                                    q(&t.rel),
                                    q(&dst.name),
                                    q(&t2.rel),
                                    q(&end.name),
                                    q(key),
                                    q(disp)
                                ),
                                params: vec![pv],
                                unanswerable: false,
                                ranked: false,
                            });
                        }
                    }
                }
            }
            lanes.extend([two_hop, top, avg]);
        }
        Bucket::Unanswerable => {
            let mut missing_node = Vec::new();
            let mut missing_rel = Vec::new();
            for l in &labels {
                // Ask for something other than the key: the display name when
                // it differs, else any other attribute. (Asking a key for itself
                // would echo the question back.)
                let Some(key) = &l.key else { continue };
                let Some(attr) = l.display.iter().chain(&l.attributes).find(|a| *a != key) else {
                    continue;
                };
                let n = l.keyed.len();
                for (i, (_, v)) in keyed(l, per_label).enumerate() {
                    let other = &l.keyed[(i + n / 2 + 1) % n].1;
                    let Some(absent) = absent_like(v, other, i) else {
                        continue;
                    };
                    let Some(pv) = param(&absent) else { continue };
                    missing_node.push(Candidate {
                        template: "unanswerable-missing-node",
                        difficulty: Difficulty::Easy,
                        question: format!(
                            "What is the {attr} of the {} whose {key} is {}?",
                            l.name,
                            shown(&absent)
                        ),
                        cypher: format!(
                            "MATCH (n:{}) WHERE n.{} = $value RETURN n.{} AS answer",
                            q(&l.name),
                            q(key),
                            q(attr)
                        ),
                        params: vec![pv],
                        unanswerable: true,
                        ranked: false,
                    });
                }
            }
            for src in &labels {
                let Some(key) = &src.key else { continue };
                for dst in &labels {
                    let Some(disp) = &dst.display else { continue };
                    for rel in &s.rel_types {
                        if s.has_triple(&src.name, rel, &dst.name) {
                            continue;
                        }
                        for (_, v) in keyed(src, 3) {
                            let Some(pv) = param(v) else { continue };
                            missing_rel.push(Candidate {
                                template: "unanswerable-missing-relationship",
                                difficulty: Difficulty::Medium,
                                question: format!(
                                    "Which {} (by {disp}) does the {} whose {key} is {} have {} relationship to?",
                                    dst.name,
                                    src.name,
                                    shown(v),
                                    a_or_an(rel)
                                ),
                                cypher: format!(
                                    "MATCH (a:{})-[:{}]->(b:{}) WHERE a.{} = $value RETURN DISTINCT b.{} AS answer",
                                    q(&src.name),
                                    q(rel),
                                    q(&dst.name),
                                    q(key),
                                    q(disp)
                                ),
                                params: vec![pv],
                                unanswerable: true,
                                ranked: false,
                            });
                        }
                    }
                }
            }
            lanes.extend([missing_node, missing_rel]);
        }
    }
    // Each lane in a seeded order; `pop` takes from the end.
    for lane in &mut lanes {
        rng.shuffle(lane);
    }
    lanes.retain(|l| !l.is_empty());
    Pool {
        lanes,
        next_lane: 0,
    }
}

/// A value shaped like the label's real keys that the graph does not have: a
/// near miss rather than an obvious fake, so an unanswerable item cannot be
/// spotted from its wording alone. `other` is another key of the same label.
/// The generator still runs the query and keeps the item only if it returns
/// nothing, so a near miss that happens to exist is never kept.
///
/// * a value with digits has its last run of digits changed
///   (`M-2007-13` -> `M-2007-1013`);
/// * a value of several words keeps its first word and takes `other`'s last
///   (`Lionel Messi` + `Kylian Mbappe` -> `Lionel Mbappe`);
/// * anything else takes the first half of itself and the second of `other`;
/// * an integer is moved past the label's values.
fn absent_like(v: &PropertyValue, other: &PropertyValue, i: usize) -> Option<PropertyValue> {
    match (v, other) {
        (PropertyValue::Integer(n), _) => n
            .checked_add(1_000_000_007 + i as i64)
            .map(PropertyValue::Integer),
        (PropertyValue::String(s), other) => {
            let other = match other {
                PropertyValue::String(o) => o.as_str(),
                _ => "",
            };
            let digits_end = s.rfind(|c: char| c.is_ascii_digit());
            let near = if let Some(end) = digits_end {
                let start = s[..=end]
                    .rfind(|c: char| !c.is_ascii_digit())
                    .map_or(0, |p| p + 1);
                let run: u64 = s[start..=end].parse().ok()?;
                format!(
                    "{}{}{}",
                    &s[..start],
                    run.checked_add(1000 + i as u64)?,
                    &s[end + 1..]
                )
            } else if let (Some((first, _)), Some((_, last))) =
                (s.split_once(' '), other.rsplit_once(' '))
            {
                format!("{first} {last}")
            } else {
                let a: String = s.chars().take(s.chars().count().div_ceil(2)).collect();
                let b: String = other.chars().skip(other.chars().count() / 2).collect();
                format!("{a}{b}")
            };
            (near != *s && !near.is_empty()).then_some(PropertyValue::String(near))
        }
        _ => None,
    }
}

// ───────────────────────────────────────────────────────────── randomness

/// SplitMix64. Written here rather than taken from a crate so that the same
/// seed gives the same suite on every version: a dependency upgrade that
/// changed a generator's output would silently change a published benchmark.
#[derive(Debug, Clone)]
pub struct SplitMix64(u64);

impl SplitMix64 {
    pub fn new(seed: u64) -> Self {
        Self(seed)
    }

    pub fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    /// A value in `0..n`. `n` must be non-zero.
    pub fn below(&mut self, n: usize) -> usize {
        (self.next_u64() % n as u64) as usize
    }

    /// Fisher-Yates.
    pub fn shuffle<T>(&mut self, items: &mut [T]) {
        for i in (1..items.len()).rev() {
            let j = self.below(i + 1);
            items.swap(i, j);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_generator_is_fixed_by_its_seed() {
        // Known values: a change here changes every published suite.
        let mut a = SplitMix64::new(0);
        assert_eq!(a.next_u64(), 0xe220_a839_7b1d_cdaf);
        let mut b = SplitMix64::new(7);
        let mut c = SplitMix64::new(7);
        assert_eq!(
            (0..10).map(|_| b.next_u64()).collect::<Vec<_>>(),
            (0..10).map(|_| c.next_u64()).collect::<Vec<_>>()
        );
    }

    #[test]
    fn a_shuffle_is_a_permutation() {
        let mut v: Vec<u32> = (0..100).collect();
        SplitMix64::new(3).shuffle(&mut v);
        assert_ne!(v, (0..100).collect::<Vec<_>>());
        v.sort_unstable();
        assert_eq!(v, (0..100).collect::<Vec<_>>());
    }

    #[test]
    fn names_with_a_backtick_or_control_character_are_not_used() {
        assert!(quotable("Person") && quotable("has space"));
        assert!(!quotable("a`b") && !quotable("") && !quotable("a\nb"));
        assert_eq!(q("has space"), "`has space`");
    }

    #[test]
    fn an_absent_value_is_a_near_miss_of_the_same_type() {
        let s = |x: &str| PropertyValue::String(x.into());
        assert_eq!(
            absent_like(&s("M-2007-13"), &s("M-1990-2"), 0),
            Some(s("M-2007-1013"))
        );
        assert_eq!(
            absent_like(&s("Lionel Messi"), &s("Kylian Mbappe"), 0),
            Some(s("Lionel Mbappe"))
        );
        assert_eq!(absent_like(&s("alpha"), &s("omega"), 0), Some(s("alpega")));
        assert_eq!(
            absent_like(&PropertyValue::Integer(5), &PropertyValue::Integer(6), 2),
            Some(PropertyValue::Integer(1_000_000_014))
        );
        assert_eq!(
            absent_like(
                &PropertyValue::Integer(i64::MAX),
                &PropertyValue::Integer(0),
                0
            ),
            None
        );
        assert_eq!(
            absent_like(
                &PropertyValue::Boolean(true),
                &PropertyValue::Boolean(false),
                0
            ),
            None
        );
        assert_eq!(
            absent_like(&s(""), &s(""), 0),
            None,
            "nothing to make a near miss of"
        );
    }

    #[test]
    fn the_article_follows_the_first_letter() {
        assert_eq!(a_or_an("IN_TOURNAMENT"), "an IN_TOURNAMENT");
        assert_eq!(a_or_an("TREATS"), "a TREATS");
    }
}
