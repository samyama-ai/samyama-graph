//! What a principal's GraphRAG context may hold: answer-time policy for
//! `ai.rag.retrieve` (part B of #2098, #2233; AI-12, TRUST-03).
//!
//! # The rules
//!
//! A credential line may carry a `rag_hide=` field (see [`crate::auth`]):
//!
//! ```text
//! hr-bot:<digest>:roles=read:rag_hide=label/Salary,edge/REPORTS_TO,prop/ssn,prop/Person.dob
//! ```
//!
//! | rule | withholds |
//! |---|---|
//! | `label/L` | every node labelled `L`, whatever its other labels |
//! | `edge/T` | every edge of type `T` |
//! | `prop/k` | property `k` of every node |
//! | `prop/L.k` | property `k` of nodes labelled `L` |
//!
//! Names are case-sensitive, as labels, types and keys are in Cypher.
//!
//! # What "withheld" means
//!
//! - A withheld **node** is dropped from the context, and the walk does not
//!   pass through it. A node reachable only through it is not in the context
//!   either; one reachable another way is, by that way. Every `path` in the
//!   answer is therefore a real path made only of elements the principal may
//!   see. Dropping rather than a "restricted" placeholder: a placeholder still
//!   says something is there and what it connects.
//! - A withheld **edge** is not walked; its two ends may still arrive by
//!   other walks.
//! - A withheld **property** is removed from the row's `properties` and
//!   `source` maps. While any property rule is in force, the entity columns
//!   `node`, `seed` and `path` are refused, because a client renders an entity
//!   with all of its properties read back from the store; the id columns
//!   (`node_id`, `seed_id`, `labels`, `hop_sequence`) say the same thing
//!   without them.
//!
//! What was withheld is reported as counts and the rules that caused them,
//! never as the withheld content: see [`Withheld`].
//!
//! # Scope
//!
//! This governs what `ai.rag.retrieve` assembles into a context, and nothing
//! else. It is not row-level security: a credential that may run Cypher can
//! still `MATCH` a withheld node directly. Its job is that a context builder
//! running under a restricted credential cannot put restricted data into a
//! prompt through retrieval.
//!
//! # How the principal reaches the operator
//!
//! The listeners know the credential; the operator does not take one. Each
//! listener runs a statement inside [`with`], which makes the credential's
//! policy the active one on that thread for the length of the (synchronous)
//! call. When any configured credential carries rules, the server also calls
//! [`require_scope`]: from then on a retrieval run outside [`with`] -- on a
//! path that never said who was asking -- is refused rather than run
//! unfiltered.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use crate::graph::{Edge, GraphStore, Node, NodeId, PropertyValue};

/// One rule of a [`RagPolicy`].
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Rule {
    Label(String),
    Edge(String),
    /// `(label, key)`: the label is `None` for every node.
    Property(Option<String>, String),
}

impl std::fmt::Display for Rule {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Rule::Label(l) => write!(f, "label/{l}"),
            Rule::Edge(t) => write!(f, "edge/{t}"),
            Rule::Property(None, k) => write!(f, "prop/{k}"),
            Rule::Property(Some(l), k) => write!(f, "prop/{l}.{k}"),
        }
    }
}

impl Rule {
    /// `label/L`, `edge/T`, `prop/k` or `prop/L.k`.
    pub fn parse(s: &str) -> Result<Self, String> {
        let s = s.trim();
        let (kind, name) = s.split_once('/').ok_or_else(|| {
            format!("rag_hide rule {s:?} is not kind/name (label/, edge/ or prop/)")
        })?;
        let nonempty = |n: &str| -> Result<String, String> {
            if n.is_empty() {
                Err(format!("rag_hide rule {s:?} names nothing"))
            } else {
                Ok(n.to_string())
            }
        };
        match kind {
            "label" => Ok(Rule::Label(nonempty(name)?)),
            "edge" => Ok(Rule::Edge(nonempty(name)?)),
            "prop" => match name.split_once('.') {
                Some((label, key)) => Ok(Rule::Property(Some(nonempty(label)?), nonempty(key)?)),
                None => Ok(Rule::Property(None, nonempty(name)?)),
            },
            // An unknown kind is an error, not a skipped rule: skipping is how
            // `lable/Salary` becomes a policy that hides nothing.
            other => Err(format!(
                "unknown rag_hide kind {other:?} in {s:?} (expected label, edge or prop)"
            )),
        }
    }
}

/// The rules one principal's retrievals run under. Empty withholds nothing.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RagPolicy {
    rules: BTreeSet<Rule>,
}

impl RagPolicy {
    /// No rules: what a credential without `rag_hide=` and an unauthenticated
    /// server run under.
    pub fn unrestricted() -> Self {
        Self::default()
    }

    /// The comma-separated rules of a `rag_hide=` field.
    pub fn parse(spec: &str) -> Result<Self, String> {
        let rules = spec
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(Rule::parse)
            .collect::<Result<BTreeSet<_>, _>>()?;
        if rules.is_empty() {
            return Err("empty `rag_hide=`".to_string());
        }
        Ok(Self { rules })
    }

    pub fn from_rules(rules: impl IntoIterator<Item = Rule>) -> Self {
        Self {
            rules: rules.into_iter().collect(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.rules.is_empty()
    }

    pub fn rules(&self) -> impl Iterator<Item = &Rule> {
        self.rules.iter()
    }

    /// Whether any property rule is in force, which closes the entity columns.
    pub fn hides_properties(&self) -> bool {
        self.rules.iter().any(|r| matches!(r, Rule::Property(..)))
    }

    /// The rules in canonical order, for a cache key.
    pub fn fingerprint(&self) -> String {
        self.rules
            .iter()
            .map(Rule::to_string)
            .collect::<Vec<_>>()
            .join(",")
    }

    /// The first rule (in canonical order) that withholds `node`.
    pub fn node_rule(&self, node: &Node) -> Option<&Rule> {
        self.rules.iter().find(|r| match r {
            Rule::Label(l) => node.labels.iter().any(|x| x.as_str() == l),
            _ => false,
        })
    }

    /// The rule that withholds `edge`.
    pub fn edge_rule(&self, edge: &Edge) -> Option<&Rule> {
        self.rules.iter().find(|r| match r {
            Rule::Edge(t) => edge.edge_type.as_str() == t,
            _ => false,
        })
    }

    /// The first rule that withholds property `key` of `node`.
    pub fn property_rule(&self, node: &Node, key: &str) -> Option<&Rule> {
        self.rules.iter().find(|r| match r {
            Rule::Property(None, k) => k == key,
            Rule::Property(Some(l), k) => k == key && node.labels.iter().any(|x| x.as_str() == l),
            _ => false,
        })
    }
}

/// What a retrieval withheld: counts and the rules behind them, never the
/// withheld ids, labels' instances or values.
///
/// Counts are of what the walk **met**: a withheld node is not walked
/// through, so whatever lay only beyond it was never looked at and is not
/// counted. Each element is counted once, under the first rule (canonical
/// order) that withholds it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Withheld {
    /// Distinct nodes dropped, vector matches included.
    pub nodes: BTreeSet<NodeId>,
    /// Distinct edges not walked because of their type.
    pub edges: BTreeSet<crate::graph::EdgeId>,
    /// `(node, key)` pairs removed from rows in the context.
    pub properties: usize,
    /// Rule (as written) to the number of elements it withheld.
    pub rules: BTreeMap<String, usize>,
}

impl Withheld {
    pub fn node(&mut self, id: NodeId, rule: &Rule) {
        if self.nodes.insert(id) {
            *self.rules.entry(rule.to_string()).or_default() += 1;
        }
    }

    pub fn edge(&mut self, id: crate::graph::EdgeId, rule: &Rule) {
        if self.edges.insert(id) {
            *self.rules.entry(rule.to_string()).or_default() += 1;
        }
    }

    pub fn property(&mut self, rule: &Rule) {
        self.properties += 1;
        *self.rules.entry(rule.to_string()).or_default() += 1;
    }

    /// `{nodes, edges, properties, rules}` as the `withheld` column holds it.
    pub fn to_map(&self) -> PropertyValue {
        let mut m = std::collections::HashMap::new();
        m.insert(
            "nodes".to_string(),
            PropertyValue::Integer(self.nodes.len() as i64),
        );
        m.insert(
            "edges".to_string(),
            PropertyValue::Integer(self.edges.len() as i64),
        );
        m.insert(
            "properties".to_string(),
            PropertyValue::Integer(self.properties as i64),
        );
        m.insert("rules".to_string(), self.rules_map());
        PropertyValue::Map(m)
    }

    pub fn rules_map(&self) -> PropertyValue {
        PropertyValue::Map(
            self.rules
                .iter()
                .map(|(k, v)| (k.clone(), PropertyValue::Integer(*v as i64)))
                .collect(),
        )
    }
}

/// `node`'s merged properties with the ones `policy` withholds removed,
/// counting each removal in `withheld`.
pub fn visible_properties(
    store: &GraphStore,
    policy: &RagPolicy,
    node: NodeId,
    withheld: &mut Withheld,
) -> crate::graph::PropertyMap {
    let mut props = store.node_properties_merged(node);
    if !policy.hides_properties() {
        return props;
    }
    let Some(n) = store.get_node(node) else {
        return props;
    };
    // Sorted, so the per-rule counts do not depend on hash order.
    let mut keys: Vec<String> = props.keys().cloned().collect();
    keys.sort();
    for key in keys {
        if let Some(rule) = policy.property_rule(n, &key) {
            withheld.property(rule);
            props.remove(&key);
        }
    }
    props
}

thread_local! {
    static ACTIVE: RefCell<Option<Arc<RagPolicy>>> = const { RefCell::new(None) };
}

static REQUIRED: AtomicBool = AtomicBool::new(false);

/// Run `f` with `policy` as this thread's active retrieval policy, restoring
/// whatever was active before, also when `f` panics.
///
/// `f` must not hand its work to another thread: the policy is this
/// thread's. Every query path the listeners use runs its statement
/// synchronously inside one call, so wrapping that call is enough.
pub fn with<R>(policy: Arc<RagPolicy>, f: impl FnOnce() -> R) -> R {
    struct Restore(Option<Arc<RagPolicy>>);
    impl Drop for Restore {
        fn drop(&mut self) {
            let prev = self.0.take();
            ACTIVE.with(|a| *a.borrow_mut() = prev);
        }
    }
    let prev = ACTIVE.with(|a| a.borrow_mut().replace(policy));
    let _restore = Restore(prev);
    f()
}

/// From now on, refuse a retrieval that runs outside [`with`].
///
/// The server calls this when a configured credential carries `rag_hide=`
/// rules: a query path nobody wrapped would otherwise run a restricted
/// principal's retrieval with no rules at all. It cannot be turned off again
/// in the same process; a policy that could be switched off by whatever runs
/// next is not one.
pub fn require_scope() {
    REQUIRED.store(true, Ordering::SeqCst);
}

/// The policy a retrieval on this thread runs under.
///
/// Outside [`with`] that is no rules, unless [`require_scope`] was called,
/// in which case it is an error.
pub fn active() -> Result<Arc<RagPolicy>, String> {
    if let Some(p) = ACTIVE.with(|a| a.borrow().clone()) {
        return Ok(p);
    }
    if REQUIRED.load(Ordering::SeqCst) {
        return Err(
            "ai.rag: this server has answer-time retrieval rules (rag_hide=), and this query \
             path did not say whose rules apply; refusing rather than retrieving unfiltered"
                .to_string(),
        );
    }
    Ok(Arc::new(RagPolicy::unrestricted()))
}

/// The active policy's [`RagPolicy::fingerprint`], or `None` outside
/// [`with`]: part of the result-cache key, so one principal's retrieval is
/// never served to another.
pub fn active_fingerprint() -> Option<String> {
    ACTIVE.with(|a| a.borrow().as_ref().map(|p| p.fingerprint()))
}

/// Whether the walk may enter `node`: its own label rules.
pub(crate) fn node_rule<'p>(
    store: &GraphStore,
    policy: &'p RagPolicy,
    node: NodeId,
) -> Option<&'p Rule> {
    if policy.is_empty() {
        return None;
    }
    store.get_node(node).and_then(|n| policy.node_rule(n))
}
