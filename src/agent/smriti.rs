//! Episodic and semantic agent memory: `remember` and `recall` (#2099,
//! Samyama Smriti Phase 1).
//!
//! ## What is stored
//!
//! ```text
//! (:AgentEpisode {session_id, content, observed_at, embedding?})
//!     -[:MENTIONS]->(:AgentEntity {name})            -- or a domain node, by id
//! (:AgentFact {subject, relation, object, valid_from, valid_to?, status})
//!     -[:ABOUT {role: 'subject'}]->(entity)
//!     -[:ABOUT {role: 'object'}]->(entity)
//!     -[:SUPPORTED_BY]->(:AgentEpisode)
//!     -[:SUPERSEDED]->(:AgentFact)                    -- the fact it closed
//! ```
//!
//! Every memory node carries `tier: 'hot'`, `created_at` and `last_used`
//! (epoch milliseconds), and the episode's scope keys (`session_id`, and
//! `agent_id`, `user_id`, `namespace` when given). The embedding is a node
//! property, so it is persisted with the node and a vector index over it is
//! rebuilt from it on recovery.
//!
//! A fact is a node rather than an edge so that it can carry its own history
//! (`SUPERSEDED`) and provenance (`SUPPORTED_BY`): an edge cannot point at an
//! edge.
//!
//! ## Superseding
//!
//! A fact holds over `[valid_from, valid_to)`. Two facts contradict when they
//! share a subject and a relation, have different objects, **and** the
//! relation is single-valued. A relation is multi-valued unless the caller
//! says otherwise (`single_valued: true` on the relation), so a fact that is
//! one of several true values is never closed. A contradicting fact is not
//! deleted: its `valid_to` is set to the new fact's `valid_from`, its status
//! becomes `superseded`, and `(new)-[:SUPERSEDED]->(old)` records why. A fact
//! that arrives late -- valid from before the active one it contradicts -- is
//! stored already closed, at the later fact's `valid_from`.
//!
//! ## Writes
//!
//! [`remember`] writes through [`PersistedWriter`] as one session
//! transaction for a named tenant (#2241): the episode, its entities and its
//! facts persist whole or not at all. `CALL agent.memory.remember(...)` runs
//! the same body, [`remember_in`], inside the Cypher statement, which the RESP
//! and HTTP write paths already run as one transaction and persist under the
//! statement's tenant -- the path `PersistedWriter` reproduces for writers
//! outside a statement.
//!
//! ## Labels
//!
//! The labels are in [`labels`], in one place. They belong in the single
//! memory label list of #2242 (`memory_labels::MEMORY_LABELS`), so that export
//! leaves them out and the publish gate refuses them; until that list is on
//! `dev`, nothing excludes them.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use crate::graph::{EdgeType, GraphStore, Label, NodeId, PropertyMap, PropertyValue};
use crate::persistence::PersistedWriter;

/// The labels and relationship types this module writes. One place, so that
/// moving them into the shared memory label list (#2242) is one change.
pub mod labels {
    /// One interaction: what was said or observed, in one session.
    pub const EPISODE: &str = "AgentEpisode";
    /// A subject-relation-object statement with a validity interval.
    pub const FACT: &str = "AgentFact";
    /// Something facts are about, when it is not an existing domain node.
    pub const ENTITY: &str = "AgentEntity";
    /// Every label this module writes.
    pub const ALL: &[&str] = &[EPISODE, FACT, ENTITY];

    /// `(:AgentEpisode)-[:MENTIONS]->(entity)`.
    pub const MENTIONS: &str = "MENTIONS";
    /// `(:AgentFact)-[:ABOUT {role}]->(entity)`.
    pub const ABOUT: &str = "ABOUT";
    /// `(:AgentFact)-[:SUPPORTED_BY]->(:AgentEpisode)`.
    pub const SUPPORTED_BY: &str = "SUPPORTED_BY";
    /// `(:AgentFact)-[:SUPERSEDED]->(:AgentFact)`, newer to older.
    pub const SUPERSEDED: &str = "SUPERSEDED";
}

/// A fact's `status`. Only these two are written here; the others in the
/// spec (`candidate`, `disputed`, `stale`) come with consolidation.
pub const STATUS_ACTIVE: &str = "active";
pub const STATUS_SUPERSEDED: &str = "superseded";

/// The deepest `recall` expands from what matched.
pub const MAX_HOPS: usize = 3;
/// The most rows `recall` returns.
pub const MAX_K: usize = 1000;
/// What each hop away from a match multiplies its score by.
const HOP_DECAY: f64 = 0.5;

/// The episode a `remember` records.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct EpisodeInput {
    pub session_id: String,
    pub content: String,
    pub agent_id: Option<String>,
    pub user_id: Option<String>,
    pub namespace: Option<String>,
    /// When it happened, epoch milliseconds; the time of the write when
    /// absent. A relation without its own `valid_from` starts here.
    pub observed_at: Option<i64>,
}

/// An entity the episode mentions.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct EntityInput {
    /// What relations call it. Matched case-insensitively within a namespace.
    pub name: String,
    pub kind: Option<String>,
    /// An existing node the name stands for. Facts about the entity are then
    /// `ABOUT` that node, and no `AgentEntity` is created.
    pub node_id: Option<NodeId>,
}

/// A subject-relation-object statement. `subject` and `object` are entity
/// names; one not listed among the entities is created.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct RelationInput {
    pub subject: String,
    pub relation: String,
    pub object: String,
    /// The relation holds one value at a time, so a new object closes the
    /// old one. Off by default: most relations are multi-valued.
    pub single_valued: bool,
    /// Epoch milliseconds; the episode's `observed_at` when absent.
    pub valid_from: Option<i64>,
    pub confidence: Option<f64>,
}

/// Everything one `remember` writes.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct RememberInput {
    pub episode: EpisodeInput,
    pub entities: Vec<EntityInput>,
    pub relations: Vec<RelationInput>,
    pub embedding: Option<Vec<f32>>,
}

/// What one `remember` wrote.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Remembered {
    pub episode_id: NodeId,
    /// One per entity name, in first-mention order.
    pub entity_ids: Vec<NodeId>,
    /// One per relation, in input order. A relation already held as an
    /// active fact reuses that fact.
    pub fact_ids: Vec<NodeId>,
    /// The active facts this write closed.
    pub superseded: Vec<NodeId>,
}

/// The time now, epoch milliseconds.
pub fn now_millis() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

/// Record an episode, its entities and its facts for `tenant`, as one
/// persisted transaction (#2241). A failure anywhere -- a bad input, a quota,
/// a write that does not reach disk -- leaves nothing behind and is an error.
pub async fn remember(
    writer: &PersistedWriter,
    tenant: &str,
    input: RememberInput,
) -> Result<Remembered, String> {
    let now = now_millis();
    writer
        .write(tenant, |store| remember_in(store, tenant, &input, now))
        .await
        .map_err(|error| error.to_string())
}

/// The writes of [`remember`], for a caller that already holds the store
/// inside a transaction: `CALL agent.memory.remember`, or a
/// [`PersistedWriter::write`] body. `now` is the write's time.
///
/// Inputs are checked before the first node is created; a failure after that
/// is undone by the enclosing transaction.
pub fn remember_in(
    store: &mut GraphStore,
    tenant: &str,
    input: &RememberInput,
    now: i64,
) -> Result<Remembered, String> {
    validate(store, input)?;
    let episode = &input.episode;
    let observed_at = episode.observed_at.unwrap_or(now);
    let namespace = episode.namespace.as_deref();

    let mut scope: Vec<(&str, PropertyValue)> = vec![(
        "session_id",
        PropertyValue::String(episode.session_id.clone()),
    )];
    for (key, value) in [
        ("agent_id", &episode.agent_id),
        ("user_id", &episode.user_id),
        ("namespace", &episode.namespace),
    ] {
        if let Some(value) = value {
            scope.push((key, PropertyValue::String(value.clone())));
        }
    }
    let stamps = [
        ("tier", PropertyValue::String("hot".into())),
        ("created_at", PropertyValue::Integer(now)),
        ("last_used", PropertyValue::Integer(now)),
    ];

    // The episode.
    let mut fields = scope.clone();
    fields.extend(stamps.iter().cloned());
    fields.push(("content", PropertyValue::String(episode.content.clone())));
    fields.push(("observed_at", PropertyValue::Integer(observed_at)));
    if let Some(embedding) = &input.embedding {
        fields.push(("embedding", PropertyValue::Vector(embedding.clone())));
    }
    let episode_id = create(store, tenant, labels::EPISODE, fields)?;

    // The entities: those listed, then any a relation names that is not.
    let mut entity_ids: Vec<NodeId> = Vec::new();
    let mut by_key: HashMap<String, NodeId> = HashMap::new();
    let mentioned = input
        .entities
        .iter()
        .cloned()
        .chain(input.relations.iter().flat_map(|r| {
            [r.subject.clone(), r.object.clone()].map(|name| EntityInput {
                name,
                ..EntityInput::default()
            })
        }));
    for entity in mentioned {
        let key = entity_key(&entity.name);
        if by_key.contains_key(&key) {
            continue;
        }
        let id = match entity.node_id {
            Some(id) => id,
            None => match find_entity(store, namespace, &key) {
                Some(id) => {
                    store
                        .set_node_property(tenant, id, "last_used", PropertyValue::Integer(now))
                        .map_err(|e| format!("could not touch entity `{}`: {e}", entity.name))?;
                    id
                }
                None => {
                    let mut fields = vec![
                        ("name", PropertyValue::String(entity.name.trim().to_owned())),
                        ("key", PropertyValue::String(key.clone())),
                    ];
                    if let Some(ns) = namespace {
                        fields.push(("namespace", PropertyValue::String(ns.to_owned())));
                    }
                    if let Some(kind) = &entity.kind {
                        fields.push(("kind", PropertyValue::String(kind.clone())));
                    }
                    fields.extend(stamps.iter().cloned());
                    create(store, tenant, labels::ENTITY, fields)?
                }
            },
        };
        by_key.insert(key, id);
        entity_ids.push(id);
        store
            .create_edge(episode_id, id, EdgeType::new(labels::MENTIONS))
            .map_err(|e| format!("could not link entity `{}`: {e}", entity.name))?;
    }

    // The facts.
    let mut fact_ids = Vec::with_capacity(input.relations.len());
    let mut superseded = Vec::new();
    for relation in &input.relations {
        let subject_id = by_key[&entity_key(&relation.subject)];
        let object_id = by_key[&entity_key(&relation.object)];
        let valid_from = relation.valid_from.unwrap_or(observed_at);
        let active = active_facts(store, subject_id, &relation.relation);

        // Already known: one more episode supports it.
        if let Some(&(same, _)) = active.iter().find(|(_, object)| *object == object_id) {
            store
                .create_edge(same, episode_id, EdgeType::new(labels::SUPPORTED_BY))
                .map_err(|e| format!("could not link supporting episode: {e}"))?;
            store
                .set_node_property(tenant, same, "last_used", PropertyValue::Integer(now))
                .map_err(|e| format!("could not touch fact: {e}"))?;
            fact_ids.push(same);
            continue;
        }

        // Contradictions, split by which of the two is the newer.
        let mut closes = Vec::new();
        let mut closed_by = Vec::new();
        if relation.single_valued {
            for &(other, _) in &active {
                let other_from = int_property(store, other, "valid_from").unwrap_or(i64::MIN);
                if other_from <= valid_from {
                    closes.push(other);
                } else {
                    closed_by.push((other, other_from));
                }
            }
        }
        let valid_to = closed_by.iter().map(|&(_, from)| from).min();

        let mut fields = scope.clone();
        fields.extend(stamps.iter().cloned());
        fields.extend([
            (
                "subject",
                PropertyValue::String(relation.subject.trim().to_owned()),
            ),
            (
                "relation",
                PropertyValue::String(relation.relation.trim().to_owned()),
            ),
            (
                "object",
                PropertyValue::String(relation.object.trim().to_owned()),
            ),
            (
                "single_valued",
                PropertyValue::Boolean(relation.single_valued),
            ),
            ("valid_from", PropertyValue::Integer(valid_from)),
            (
                "status",
                PropertyValue::String(
                    if valid_to.is_some() {
                        STATUS_SUPERSEDED
                    } else {
                        STATUS_ACTIVE
                    }
                    .into(),
                ),
            ),
        ]);
        if let Some(valid_to) = valid_to {
            fields.push(("valid_to", PropertyValue::Integer(valid_to)));
        }
        if let Some(confidence) = relation.confidence {
            fields.push(("confidence", PropertyValue::Float(confidence)));
        }
        let fact = create(store, tenant, labels::FACT, fields)?;
        for (target, role) in [(subject_id, "subject"), (object_id, "object")] {
            let properties: PropertyMap =
                [("role".to_string(), PropertyValue::String(role.into()))].into();
            store
                .create_edge_with_properties(fact, target, EdgeType::new(labels::ABOUT), properties)
                .map_err(|e| format!("could not link the fact's {role}: {e}"))?;
        }
        store
            .create_edge(fact, episode_id, EdgeType::new(labels::SUPPORTED_BY))
            .map_err(|e| format!("could not link supporting episode: {e}"))?;

        for old in closes {
            store
                .set_node_property(tenant, old, "valid_to", PropertyValue::Integer(valid_from))
                .map_err(|e| format!("could not close fact {old}: {e}"))?;
            store
                .set_node_property(
                    tenant,
                    old,
                    "status",
                    PropertyValue::String(STATUS_SUPERSEDED.into()),
                )
                .map_err(|e| format!("could not close fact {old}: {e}"))?;
            store
                .create_edge(fact, old, EdgeType::new(labels::SUPERSEDED))
                .map_err(|e| format!("could not link superseded fact: {e}"))?;
            superseded.push(old);
        }
        for (newer, _) in closed_by {
            store
                .create_edge(newer, fact, EdgeType::new(labels::SUPERSEDED))
                .map_err(|e| format!("could not link superseding fact: {e}"))?;
        }
        fact_ids.push(fact);
    }

    Ok(Remembered {
        episode_id,
        entity_ids,
        fact_ids,
        superseded,
    })
}

fn validate(store: &GraphStore, input: &RememberInput) -> Result<(), String> {
    let episode = &input.episode;
    if episode.session_id.trim().is_empty() {
        return Err("session_id must not be empty".into());
    }
    if episode.content.trim().is_empty() {
        return Err("the episode's content must not be empty".into());
    }
    if let Some(embedding) = &input.embedding {
        if embedding.is_empty() {
            return Err("the embedding must not be empty; pass null for none".into());
        }
        if embedding.iter().any(|x| !x.is_finite()) {
            return Err("the embedding must hold finite numbers only".into());
        }
    }
    let mut named: HashMap<String, Option<NodeId>> = HashMap::new();
    for entity in &input.entities {
        if entity.name.trim().is_empty() {
            return Err("an entity's name must not be empty".into());
        }
        if let Some(id) = entity.node_id {
            if !store.has_node(id) {
                return Err(format!(
                    "entity `{}` names node {id}, which does not exist",
                    entity.name
                ));
            }
        }
        let key = entity_key(&entity.name);
        if let Some(previous) = named.insert(key, entity.node_id) {
            if previous != entity.node_id {
                return Err(format!(
                    "entity `{}` is listed twice with different nodes",
                    entity.name
                ));
            }
        }
    }
    let mut single: HashMap<(String, String), String> = HashMap::new();
    for relation in &input.relations {
        for (what, value) in [
            ("subject", &relation.subject),
            ("relation", &relation.relation),
            ("object", &relation.object),
        ] {
            if value.trim().is_empty() {
                return Err(format!("a relation's {what} must not be empty"));
            }
        }
        if relation.confidence.is_some_and(|c| !c.is_finite()) {
            return Err("a relation's confidence must be a finite number".into());
        }
        if relation.single_valued {
            let at = (
                entity_key(&relation.subject),
                relation.relation.trim().to_owned(),
            );
            let object = entity_key(&relation.object);
            if let Some(other) = single.insert(at, object.clone()) {
                if other != object {
                    return Err(format!(
                        "`{}` is single-valued and this call gives `{}` two values; \
                         remember them in separate calls, in the order they held",
                        relation.relation, relation.subject
                    ));
                }
            }
        }
    }
    Ok(())
}

fn create(
    store: &mut GraphStore,
    tenant: &str,
    label: &str,
    fields: Vec<(&str, PropertyValue)>,
) -> Result<NodeId, String> {
    store
        .admit_node()
        .map_err(|e| format!("could not store {label}: {e}"))?;
    let id = store.create_node(label);
    for (key, value) in fields {
        store
            .set_node_property(tenant, id, key, value)
            .map_err(|e| format!("could not store {label} field `{key}`: {e}"))?;
    }
    Ok(id)
}

/// How an entity name is matched: trimmed and case-folded.
fn entity_key(name: &str) -> String {
    name.trim().to_lowercase()
}

/// The `AgentEntity` with this key in this namespace.
///
/// Reads the whole label, as `memory::trace_steps` does; a property index on
/// `key` is the fix when the memory benchmark (#2243) shows it.
fn find_entity(store: &GraphStore, namespace: Option<&str>, key: &str) -> Option<NodeId> {
    store
        .nodes_with_label(&Label::new(labels::ENTITY))?
        .iter()
        .copied()
        .filter(|&id| string_property(store, id, "key").as_deref() == Some(key))
        .filter(|&id| string_property(store, id, "namespace").as_deref() == namespace)
        .min()
}

/// `(fact, object)` for each active fact with this subject and relation.
fn active_facts(store: &GraphStore, subject: NodeId, relation: &str) -> Vec<(NodeId, NodeId)> {
    let mut out = Vec::new();
    for (edge, fact, _, edge_type) in store.get_incoming_edge_sources_owned(subject) {
        if edge_type.as_str() != labels::ABOUT || !has_label(store, fact, labels::FACT) {
            continue;
        }
        if string_edge_property(store, edge, "role").as_deref() != Some("subject") {
            continue;
        }
        if string_property(store, fact, "relation").as_deref() != Some(relation.trim())
            || string_property(store, fact, "status").as_deref() != Some(STATUS_ACTIVE)
        {
            continue;
        }
        if let Some(object) = fact_object(store, fact) {
            out.push((fact, object));
        }
    }
    out.sort_unstable();
    out.dedup();
    out
}

fn fact_object(store: &GraphStore, fact: NodeId) -> Option<NodeId> {
    store
        .get_outgoing_edge_targets_owned(fact)
        .into_iter()
        .find(|(edge, _, _, edge_type)| {
            edge_type.as_str() == labels::ABOUT
                && string_edge_property(store, *edge, "role").as_deref() == Some("object")
        })
        .map(|(_, _, target, _)| target)
}

fn has_label(store: &GraphStore, id: NodeId, label: &str) -> bool {
    store
        .get_node(id)
        .is_some_and(|n| n.labels.iter().any(|l| l.as_str() == label))
}

fn string_property(store: &GraphStore, id: NodeId, key: &str) -> Option<String> {
    match store.node_property(id, key) {
        Some(PropertyValue::String(s)) => Some(s),
        _ => None,
    }
}

fn int_property(store: &GraphStore, id: NodeId, key: &str) -> Option<i64> {
    match store.node_property(id, key) {
        Some(PropertyValue::Integer(n)) => Some(n),
        _ => None,
    }
}

fn string_edge_property(store: &GraphStore, id: crate::graph::EdgeId, key: &str) -> Option<String> {
    match store.edge_property(id, key) {
        Some(PropertyValue::String(s)) => Some(s),
        _ => None,
    }
}

// ---------------------------------------------------------------- recall --

/// Which memory a node is, as `recall` reports it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MemoryTier {
    /// An `AgentEpisode`.
    Episodic,
    /// An `AgentFact` or an `AgentEntity`.
    Semantic,
    /// A domain node a fact or an episode is about.
    Domain,
}

impl MemoryTier {
    pub fn name(self) -> &'static str {
        match self {
            Self::Episodic => "episodic",
            Self::Semantic => "semantic",
            Self::Domain => "domain",
        }
    }
}

/// What to recall.
#[derive(Clone, Debug, PartialEq)]
pub struct RecallQuery {
    pub text: Option<String>,
    pub embedding: Option<Vec<f32>>,
    /// Only episodes and facts of these sessions, and entities they mention.
    /// `None` is every session.
    pub sessions: Option<Vec<String>>,
    /// Facts as they stood at this instant (epoch milliseconds), superseded
    /// ones included, and episodes observed by then; `None` is the active
    /// facts only.
    pub as_of: Option<i64>,
    pub k: usize,
    pub max_hops: usize,
}

impl Default for RecallQuery {
    fn default() -> Self {
        Self {
            text: None,
            embedding: None,
            sessions: None,
            as_of: None,
            k: 10,
            max_hops: 1,
        }
    }
}

/// One recalled node.
#[derive(Clone, Debug, PartialEq)]
pub struct Recalled {
    pub node_id: NodeId,
    pub score: f64,
    pub tier: MemoryTier,
    /// The episode it came from: itself for an episode, the first supporting
    /// episode for a fact, the first mentioning one for an entity, and for a
    /// node reached by a hop, that of the node it was reached from.
    pub provenance_episode: Option<NodeId>,
}

/// Find the memory that best matches `query`.
///
/// Every visible episode, fact and entity is scored against the text (the
/// share of query words it contains) and, for an episode with an embedding,
/// against the query embedding (cosine, floored at 0); with both, the two are
/// averaged. Matches are then expanded `max_hops` along memory relationships
/// (`MENTIONS`, `ABOUT`, `SUPPORTED_BY`, `SUPERSEDED`), each hop halving the
/// score, and the best `k` are returned, highest score first.
///
/// Scoring reads every memory node, without an index: correct at any size,
/// and the HNSW and full-text indexes are the follow-up when the memory
/// benchmark (#2243) asks for them.
pub fn recall(store: &GraphStore, query: &RecallQuery) -> Result<Vec<Recalled>, String> {
    let words = query.text.as_deref().map(words).unwrap_or_default();
    let embedding = query.embedding.as_deref().filter(|e| !e.is_empty());
    if words.is_empty() && embedding.is_none() {
        return Err("recall needs query text or a query embedding".into());
    }
    if query.k == 0 || query.k > MAX_K {
        return Err(format!("k must be between 1 and {MAX_K}, got {}", query.k));
    }
    if query.max_hops > MAX_HOPS {
        return Err(format!(
            "max_hops must be at most {MAX_HOPS}, got {}",
            query.max_hops
        ));
    }
    if let Some(e) = embedding {
        if e.iter().any(|x| !x.is_finite()) {
            return Err("the query embedding must hold finite numbers only".into());
        }
    }
    let view = View { store, query };

    // Score what matches directly.
    let mut best: BTreeMap<NodeId, (f64, Option<NodeId>)> = BTreeMap::new();
    for label in labels::ALL {
        let Some(ids) = store.nodes_with_label(&Label::new(*label)) else {
            continue;
        };
        for &id in ids {
            if !view.visible(id) {
                continue;
            }
            let text_score = (!words.is_empty()).then(|| {
                let have = words_of(&view.text(id));
                words.iter().filter(|w| have.contains(*w)).count() as f64 / words.len() as f64
            });
            let vector_score = embedding.and_then(|q| match store.node_property(id, "embedding") {
                Some(PropertyValue::Vector(v)) => cosine(q, &v),
                _ => None,
            });
            let score = match (text_score, vector_score) {
                (Some(t), Some(v)) => (t + v) / 2.0,
                (Some(t), None) => t,
                (None, Some(v)) => v,
                (None, None) => 0.0,
            };
            if score > 0.0 {
                best.insert(id, (score, view.own_provenance(id)));
            }
        }
    }

    // Expand along memory relationships.
    let mut frontier: Vec<NodeId> = best.keys().copied().collect();
    for _ in 0..query.max_hops {
        let mut next = BTreeSet::new();
        for id in frontier {
            let (score, provenance) = best[&id];
            if view.tier(id) == MemoryTier::Domain {
                // A domain node can be reached, never expanded: its degree
                // is the domain's, not memory's.
                continue;
            }
            for neighbour in view.neighbours(id) {
                let candidate = score * HOP_DECAY;
                let provenance = view.own_provenance(neighbour).or(provenance);
                let improves = best.get(&neighbour).is_none_or(|(s, _)| candidate > *s);
                if improves {
                    best.insert(neighbour, (candidate, provenance));
                    next.insert(neighbour);
                }
            }
        }
        frontier = next.into_iter().collect();
    }

    let mut out: Vec<Recalled> = best
        .into_iter()
        .map(|(node_id, (score, provenance_episode))| Recalled {
            node_id,
            score,
            tier: view.tier(node_id),
            provenance_episode,
        })
        .collect();
    out.sort_by(|a, b| b.score.total_cmp(&a.score).then(a.node_id.cmp(&b.node_id)));
    out.truncate(query.k);
    Ok(out)
}

/// What `recall` may see, under its session filter and its instant.
struct View<'a> {
    store: &'a GraphStore,
    query: &'a RecallQuery,
}

impl View<'_> {
    fn tier(&self, id: NodeId) -> MemoryTier {
        if has_label(self.store, id, labels::EPISODE) {
            MemoryTier::Episodic
        } else if has_label(self.store, id, labels::FACT)
            || has_label(self.store, id, labels::ENTITY)
        {
            MemoryTier::Semantic
        } else {
            MemoryTier::Domain
        }
    }

    fn in_session(&self, id: NodeId) -> bool {
        match &self.query.sessions {
            None => true,
            Some(sessions) => {
                string_property(self.store, id, "session_id").is_some_and(|s| sessions.contains(&s))
            }
        }
    }

    fn visible(&self, id: NodeId) -> bool {
        let store = self.store;
        if has_label(store, id, labels::EPISODE) {
            self.in_session(id)
                && self.query.as_of.is_none_or(|t| {
                    int_property(store, id, "observed_at").is_some_and(|at| at <= t)
                })
        } else if has_label(store, id, labels::FACT) {
            if !self.in_session(id) {
                return false;
            }
            let status = string_property(store, id, "status");
            match self.query.as_of {
                None => status.as_deref() == Some(STATUS_ACTIVE),
                Some(t) => {
                    matches!(status.as_deref(), Some(STATUS_ACTIVE | STATUS_SUPERSEDED))
                        && int_property(store, id, "valid_from").is_some_and(|from| from <= t)
                        && int_property(store, id, "valid_to").is_none_or(|to| t < to)
                }
            }
        } else if has_label(store, id, labels::ENTITY) {
            // Entities are shared across sessions: under a session filter one
            // is visible when a visible episode of those sessions mentions it.
            // They have no validity interval of their own -- `as_of` is about
            // when facts held, and an entity is not a claim -- so the same
            // test covers it: an entity no episode up to `as_of` mentions is
            // not visible then.
            (self.query.sessions.is_none() && self.query.as_of.is_none())
                || self.mentioning_episode(id).is_some()
        } else {
            // A domain node is reached only through memory that is visible.
            store.has_node(id)
        }
    }

    /// The first visible episode that mentions `id`.
    fn mentioning_episode(&self, id: NodeId) -> Option<NodeId> {
        self.store
            .get_incoming_edge_sources_owned(id)
            .into_iter()
            .filter(|(_, source, _, t)| {
                t.as_str() == labels::MENTIONS && has_label(self.store, *source, labels::EPISODE)
            })
            .map(|(_, source, _, _)| source)
            .filter(|&source| self.visible(source))
            .min()
    }

    /// The episode a node records or rests on, if it has one of its own.
    fn own_provenance(&self, id: NodeId) -> Option<NodeId> {
        let store = self.store;
        if has_label(store, id, labels::EPISODE) {
            Some(id)
        } else if has_label(store, id, labels::FACT) {
            store
                .get_outgoing_edge_targets_owned(id)
                .into_iter()
                .filter(|(_, _, _, t)| t.as_str() == labels::SUPPORTED_BY)
                .map(|(_, _, target, _)| target)
                .filter(|&episode| self.visible(episode))
                .min()
        } else {
            self.mentioning_episode(id)
        }
    }

    /// The visible nodes one memory relationship away.
    fn neighbours(&self, id: NodeId) -> Vec<NodeId> {
        let memory_edge = |t: &EdgeType| {
            [
                labels::MENTIONS,
                labels::ABOUT,
                labels::SUPPORTED_BY,
                labels::SUPERSEDED,
            ]
            .contains(&t.as_str())
        };
        let mut out: Vec<NodeId> = self
            .store
            .get_outgoing_edge_targets_owned(id)
            .into_iter()
            .filter(|(_, _, _, t)| memory_edge(t))
            .map(|(_, _, target, _)| target)
            .chain(
                self.store
                    .get_incoming_edge_sources_owned(id)
                    .into_iter()
                    .filter(|(_, _, _, t)| memory_edge(t))
                    .map(|(_, source, _, _)| source),
            )
            .filter(|&n| n != id && self.visible(n))
            .collect();
        out.sort_unstable();
        out.dedup();
        out
    }

    /// The text a node is matched on.
    fn text(&self, id: NodeId) -> String {
        let store = self.store;
        let get = |key| string_property(store, id, key).unwrap_or_default();
        if has_label(store, id, labels::EPISODE) {
            get("content")
        } else if has_label(store, id, labels::FACT) {
            format!(
                "{} {} {}",
                get("subject"),
                get("relation").replace('_', " "),
                get("object")
            )
        } else {
            format!("{} {}", get("name"), get("kind"))
        }
    }
}

/// The distinct lower-case words of `text`, in order.
fn words(text: &str) -> Vec<String> {
    let mut seen = HashSet::new();
    text.split(|c: char| !c.is_alphanumeric())
        .filter(|w| !w.is_empty())
        .map(str::to_lowercase)
        .filter(|w| seen.insert(w.clone()))
        .collect()
}

fn words_of(text: &str) -> HashSet<String> {
    words(text).into_iter().collect()
}

/// Cosine similarity floored at 0, or `None` for vectors that cannot be
/// compared.
fn cosine(a: &[f32], b: &[f32]) -> Option<f64> {
    if a.len() != b.len() || a.is_empty() {
        return None;
    }
    let (mut dot, mut na, mut nb) = (0.0f64, 0.0f64, 0.0f64);
    for (x, y) in a.iter().zip(b) {
        let (x, y) = (f64::from(*x), f64::from(*y));
        dot += x * y;
        na += x * x;
        nb += y * y;
    }
    if na == 0.0 || nb == 0.0 {
        return None;
    }
    Some((dot / (na.sqrt() * nb.sqrt())).max(0.0))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lives_in(city: &str, from: i64, single: bool) -> RememberInput {
        RememberInput {
            episode: EpisodeInput {
                session_id: format!("s-{city}"),
                content: format!("Alice moved to {city}"),
                observed_at: Some(from),
                ..EpisodeInput::default()
            },
            entities: vec![EntityInput {
                name: "Alice".into(),
                kind: Some("Person".into()),
                node_id: None,
            }],
            relations: vec![RelationInput {
                subject: "Alice".into(),
                relation: "lives_in".into(),
                object: city.into(),
                single_valued: single,
                ..RelationInput::default()
            }],
            embedding: None,
        }
    }

    #[test]
    fn a_single_valued_fact_closes_the_one_it_contradicts() {
        let mut store = GraphStore::new();
        let paris = remember_in(&mut store, "t", &lives_in("Paris", 1_000, true), 5).unwrap();
        let berlin = remember_in(&mut store, "t", &lives_in("Berlin", 2_000, true), 6).unwrap();
        let old = paris.fact_ids[0];
        assert_eq!(berlin.superseded, vec![old]);
        assert_eq!(int_property(&store, old, "valid_to"), Some(2_000));
        assert_eq!(
            string_property(&store, old, "status").as_deref(),
            Some(STATUS_SUPERSEDED)
        );
        // One Alice, shared by both episodes.
        assert_eq!(paris.entity_ids[0], berlin.entity_ids[0]);
    }

    #[test]
    fn a_multi_valued_fact_is_never_closed() {
        let mut store = GraphStore::new();
        remember_in(&mut store, "t", &lives_in("Paris", 1_000, false), 5).unwrap();
        let second = remember_in(&mut store, "t", &lives_in("Berlin", 2_000, false), 6).unwrap();
        assert!(second.superseded.is_empty());
        let alice = second.entity_ids[0];
        assert_eq!(active_facts(&store, alice, "lives_in").len(), 2);
    }

    #[test]
    fn a_late_fact_is_stored_closed_by_the_newer_one() {
        let mut store = GraphStore::new();
        let berlin = remember_in(&mut store, "t", &lives_in("Berlin", 2_000, true), 5).unwrap();
        let paris = remember_in(&mut store, "t", &lives_in("Paris", 1_000, true), 6).unwrap();
        assert!(paris.superseded.is_empty());
        let late = paris.fact_ids[0];
        assert_eq!(int_property(&store, late, "valid_to"), Some(2_000));
        assert_eq!(
            string_property(&store, berlin.fact_ids[0], "status").as_deref(),
            Some(STATUS_ACTIVE)
        );
    }

    #[test]
    fn a_repeated_fact_gains_support_instead_of_a_copy() {
        let mut store = GraphStore::new();
        let first = remember_in(&mut store, "t", &lives_in("Paris", 1_000, true), 5).unwrap();
        let again = remember_in(&mut store, "t", &lives_in("Paris", 1_500, true), 6).unwrap();
        assert_eq!(first.fact_ids, again.fact_ids);
        assert!(again.superseded.is_empty());
    }

    #[test]
    fn bad_input_is_refused_before_anything_is_written() {
        let mut store = GraphStore::new();
        let mut two_values = lives_in("Paris", 1_000, true);
        two_values.relations.push(RelationInput {
            object: "Rome".into(),
            ..two_values.relations[0].clone()
        });
        assert!(remember_in(&mut store, "t", &two_values, 5)
            .unwrap_err()
            .contains("two values"));
        let mut empty = lives_in("Paris", 1_000, true);
        empty.episode.session_id = " ".into();
        assert!(remember_in(&mut store, "t", &empty, 5).is_err());
        assert_eq!(store.node_count(), 0);
    }

    #[test]
    fn recall_reads_the_present_by_default_and_the_past_as_of() {
        let mut store = GraphStore::new();
        let paris = remember_in(&mut store, "t", &lives_in("Paris", 1_000, true), 5).unwrap();
        let berlin = remember_in(&mut store, "t", &lives_in("Berlin", 2_000, true), 6).unwrap();
        let facts = |as_of| {
            recall(
                &store,
                &RecallQuery {
                    text: Some("Alice lives in".into()),
                    as_of,
                    max_hops: 0,
                    ..RecallQuery::default()
                },
            )
            .unwrap()
            .into_iter()
            .filter(|r| has_label(&store, r.node_id, labels::FACT))
            .map(|r| r.node_id)
            .collect::<Vec<_>>()
        };
        assert_eq!(facts(None), berlin.fact_ids);
        assert_eq!(facts(Some(1_500)), paris.fact_ids);
        assert_eq!(facts(Some(2_500)), berlin.fact_ids);
    }

    #[test]
    fn recall_ranks_by_embedding_and_honours_the_session_filter() {
        let mut store = GraphStore::new();
        let mut a = lives_in("Paris", 1_000, false);
        a.embedding = Some(vec![1.0, 0.0]);
        let mut b = lives_in("Berlin", 1_000, false);
        b.embedding = Some(vec![0.0, 1.0]);
        let a = remember_in(&mut store, "t", &a, 5).unwrap();
        let b = remember_in(&mut store, "t", &b, 5).unwrap();
        let top = recall(
            &store,
            &RecallQuery {
                embedding: Some(vec![0.1, 0.9]),
                k: 1,
                ..RecallQuery::default()
            },
        )
        .unwrap();
        assert_eq!(top[0].node_id, b.episode_id);
        assert_eq!(top[0].tier, MemoryTier::Episodic);

        let only_paris = recall(
            &store,
            &RecallQuery {
                text: Some("Alice".into()),
                sessions: Some(vec!["s-Paris".into()]),
                max_hops: 2,
                k: 100,
                ..RecallQuery::default()
            },
        )
        .unwrap();
        assert!(only_paris.iter().any(|r| r.node_id == a.episode_id));
        assert!(only_paris.iter().all(|r| r.node_id != b.episode_id));
        assert!(only_paris.iter().all(|r| !b.fact_ids.contains(&r.node_id)));
    }

    #[test]
    fn recall_refuses_a_query_with_nothing_to_match() {
        let store = GraphStore::new();
        assert!(recall(&store, &RecallQuery::default()).is_err());
        let too_deep = RecallQuery {
            text: Some("x".into()),
            max_hops: MAX_HOPS + 1,
            ..RecallQuery::default()
        };
        assert!(recall(&store, &too_deep).is_err());
    }
}
