//! Where a fact came from, under what licence, and what it was derived from.
//!
//! Three requirements rest on this and none of them can be met by a field
//! nobody reads: a row cannot be marked non-redistributable (TRUST-03), an
//! exported row cannot carry its source and licence (ML-09), and there is no
//! derivation path to return (EVAL-10).
//!
//! # Not to be confused with execution provenance
//!
//! Query responses already carry `engine_version` and `snapshot_version`
//! (#1035). That answers *which build, which snapshot* — it is a fact about
//! the run. This answers *which source, under what licence* — a fact about the
//! data, which outlives every run and follows the row into an export.
//!
//! # The convention
//!
//! Five reserved property keys, `__`-prefixed like `__rdf_iri`:
//!
//! | Key | Meaning |
//! |---|---|
//! | `__source_uri` | where the fact came from |
//! | `__source_version` | the version, revision or release of that source |
//! | `__retrieved_at` | when it was fetched |
//! | `__license` | the licence the source grants |
//! | `__redistributable` | whether this row may leave, as a boolean |
//!
//! Properties rather than a parallel store, for one reason: they survive every
//! path a row already takes. A snapshot, an RDF export, a CSV, a GraphML file
//! and a Cypher `RETURN n` all carry properties, and a sidecar table would have
//! to be threaded through each of them and would be forgotten by the sixth.
//!
//! # Absent means unknown, and unknown is not permission
//!
//! [`Redistributable::Unknown`] is a distinct answer from
//! [`Redistributable::No`]. Most rows in an existing graph carry no
//! provenance at all, so treating absence as `Yes` would mark a whole graph
//! redistributable on the strength of nobody having said otherwise, and
//! treating it as `No` would make every export empty and the feature would be
//! turned off within a day.
//!
//! So the policy is the caller's ([`ExportPolicy`]), the default changes
//! nothing about what an export emits, and the count of unknown rows is
//! reported either way. A user who needs the guarantee can tighten it; a user
//! who has not thought about it is told how many rows they have not thought
//! about.

use crate::graph::{GraphStore, NodeId, PropertyValue};
use std::collections::HashSet;

pub const SOURCE_URI: &str = "__source_uri";
pub const SOURCE_VERSION: &str = "__source_version";
pub const RETRIEVED_AT: &str = "__retrieved_at";
pub const LICENSE: &str = "__license";
pub const REDISTRIBUTABLE: &str = "__redistributable";

/// Every reserved key, so an export can strip or carry them as a set rather
/// than as five string literals that drift apart.
pub const KEYS: [&str; 5] = [
    SOURCE_URI,
    SOURCE_VERSION,
    RETRIEVED_AT,
    LICENSE,
    REDISTRIBUTABLE,
];

/// May this row leave?
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum Redistributable {
    Yes,
    No,
    /// Nothing was recorded. Not a synonym for either answer — see the module
    /// docs for why this is a third state and not a default.
    Unknown,
}

/// The provenance recorded on one node.
#[derive(Debug, Clone, Default, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Provenance {
    pub source_uri: Option<String>,
    pub source_version: Option<String>,
    pub retrieved_at: Option<String>,
    pub license: Option<String>,
}

impl Provenance {
    /// True when nothing at all was recorded.
    pub fn is_empty(&self) -> bool {
        self.source_uri.is_none()
            && self.source_version.is_none()
            && self.retrieved_at.is_none()
            && self.license.is_none()
    }

    /// True when the row can be traced back to a retrievable source.
    ///
    /// A `__source_uri` alone is not enough: EVAL-13 makes the same point about
    /// benchmark envelopes, and it is the same point here. "From Wikidata" does
    /// not let anyone fetch what this row was built from; "from Wikidata, dump
    /// 2026-08-01" does.
    pub fn is_reproducible(&self) -> bool {
        self.source_uri.is_some() && self.source_version.is_some()
    }
}

fn text(v: &PropertyValue) -> String {
    match v {
        PropertyValue::String(s) => s.clone(),
        other => other.to_string(),
    }
}

/// Read the provenance recorded on `node`.
///
/// Uses the merged property view: a value written by `set_node_property` lands
/// on the columnar side and is invisible to `node.properties`, which is the
/// trap that made an RDF export produce bare subjects (#554).
pub fn of(store: &GraphStore, node: NodeId) -> Provenance {
    let props = store.node_properties_merged(node);
    let get = |k: &str| props.get(k).map(text);
    Provenance {
        source_uri: get(SOURCE_URI),
        source_version: get(SOURCE_VERSION),
        retrieved_at: get(RETRIEVED_AT),
        license: get(LICENSE),
    }
}

/// Whether `node` may be redistributed.
pub fn redistributable(store: &GraphStore, node: NodeId) -> Redistributable {
    match store.node_properties_merged(node).get(REDISTRIBUTABLE) {
        Some(PropertyValue::Boolean(true)) => Redistributable::Yes,
        Some(PropertyValue::Boolean(false)) => Redistributable::No,
        // A string is accepted because CSV, JSON and RDF imports all deliver
        // booleans as text, and a row that says "false" must not be read as
        // unknown and then emitted.
        Some(PropertyValue::String(s)) => match s.trim().to_ascii_lowercase().as_str() {
            "true" | "yes" | "1" => Redistributable::Yes,
            "false" | "no" | "0" => Redistributable::No,
            _ => Redistributable::Unknown,
        },
        _ => Redistributable::Unknown,
    }
}

/// One step in a derivation.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Derivation {
    pub node: u64,
    /// How far from the node the path started at. The node itself is 0.
    pub depth: usize,
    pub labels: Vec<String>,
    pub provenance: Provenance,
    pub redistributable: Redistributable,
}

/// The edge type a derived fact uses to name what it came from.
pub const DERIVED_FROM: &str = "DERIVED_FROM";

/// The full derivation path of `node`, nearest source first (EVAL-10).
///
/// Follows `DERIVED_FROM` edges outward from the node. Breadth-first, so
/// `depth` means what it says, and cycle-safe: a derivation graph is a claim
/// made by whoever built it, and nothing stops them claiming a cycle.
///
/// The node itself is the first entry at depth 0. A derived fact whose own
/// provenance is empty is still returned — an empty entry says "this step
/// recorded nothing", which is the answer, and omitting it would silently
/// shorten the chain.
pub fn derivation_path(store: &GraphStore, node: NodeId) -> Vec<Derivation> {
    let mut seen: HashSet<u64> = HashSet::new();
    let mut out = Vec::new();
    let mut frontier = vec![node];
    let mut depth = 0usize;

    seen.insert(node.as_u64());
    while !frontier.is_empty() {
        let mut next = Vec::new();
        for id in &frontier {
            let labels = store
                .get_node(*id)
                .map(|n| {
                    let mut l: Vec<String> =
                        n.labels.iter().map(|x| x.as_str().to_string()).collect();
                    // A HashSet, so the order is otherwise arbitrary and two
                    // reports of one graph would differ.
                    l.sort();
                    l
                })
                .unwrap_or_default();
            out.push(Derivation {
                node: id.as_u64(),
                depth,
                labels,
                provenance: of(store, *id),
                redistributable: redistributable(store, *id),
            });

            for e in store.get_outgoing_edges(*id) {
                if e.edge_type.as_str() != DERIVED_FROM {
                    continue;
                }
                if seen.insert(e.target.as_u64()) {
                    next.push(e.target);
                }
            }
        }
        frontier = next;
        depth += 1;
    }
    out
}

/// What an export does with rows it may not be allowed to emit.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ExportPolicy {
    /// Emit everything, and count what was unknown or forbidden.
    ///
    /// The default, deliberately: turning this on must not change what any
    /// existing export produces. A feature that silently empties a user's
    /// export the day it ships is a feature they turn off.
    #[default]
    CountOnly,
    /// Drop rows explicitly marked non-redistributable. Unknown still goes.
    WithholdForbidden,
    /// Drop anything not explicitly marked redistributable.
    ///
    /// The strict reading, and the only one that is actually a guarantee: with
    /// `WithholdForbidden`, a row nobody labelled still leaves.
    RequirePermission,
}

impl ExportPolicy {
    pub fn allows(self, r: Redistributable) -> bool {
        match (self, r) {
            (ExportPolicy::CountOnly, _) => true,
            (ExportPolicy::WithholdForbidden, Redistributable::No) => false,
            (ExportPolicy::WithholdForbidden, _) => true,
            (ExportPolicy::RequirePermission, Redistributable::Yes) => true,
            (ExportPolicy::RequirePermission, _) => false,
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s.trim().to_ascii_lowercase().replace('_', "-").as_str() {
            "count-only" => Some(ExportPolicy::CountOnly),
            "withhold-forbidden" => Some(ExportPolicy::WithholdForbidden),
            "require-permission" => Some(ExportPolicy::RequirePermission),
            _ => None,
        }
    }
}

/// What a licence-aware export saw and did.
#[derive(Debug, Clone, Default, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct LicenceReport {
    pub nodes_considered: usize,
    pub marked_redistributable: usize,
    pub marked_not_redistributable: usize,
    pub unmarked: usize,
    /// Rows the policy withheld. Zero under `CountOnly` by construction.
    pub withheld: usize,
    /// Rows carrying a source URI *and* a version, so a reader can go and
    /// fetch what they were built from.
    pub reproducible: usize,
    /// Distinct licences seen, sorted.
    pub licenses: Vec<String>,
}

/// Which nodes an export may emit under `policy`, and what that cost.
pub fn screen(
    store: &GraphStore,
    nodes: &[NodeId],
    policy: ExportPolicy,
) -> (Vec<NodeId>, LicenceReport) {
    let mut report = LicenceReport {
        nodes_considered: nodes.len(),
        ..Default::default()
    };
    let mut licenses: HashSet<String> = HashSet::new();
    let mut allowed = Vec::with_capacity(nodes.len());

    for id in nodes {
        let r = redistributable(store, *id);
        match r {
            Redistributable::Yes => report.marked_redistributable += 1,
            Redistributable::No => report.marked_not_redistributable += 1,
            Redistributable::Unknown => report.unmarked += 1,
        }
        let p = of(store, *id);
        if p.is_reproducible() {
            report.reproducible += 1;
        }
        if let Some(l) = p.license {
            licenses.insert(l);
        }
        if policy.allows(r) {
            allowed.push(*id);
        } else {
            report.withheld += 1;
        }
    }

    report.licenses = {
        let mut v: Vec<String> = licenses.into_iter().collect();
        v.sort();
        v
    };
    (allowed, report)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unknown_is_a_third_answer_under_every_policy() {
        use ExportPolicy::*;
        use Redistributable::*;
        // The table is the decision, written out, because the interesting
        // cell is the one an `if !forbidden` would get wrong: an unmarked row
        // leaves under WithholdForbidden and does not under RequirePermission.
        assert!(CountOnly.allows(No));
        assert!(CountOnly.allows(Unknown));
        assert!(WithholdForbidden.allows(Unknown));
        assert!(!WithholdForbidden.allows(No));
        assert!(WithholdForbidden.allows(Yes));
        assert!(!RequirePermission.allows(Unknown));
        assert!(!RequirePermission.allows(No));
        assert!(RequirePermission.allows(Yes));
    }

    #[test]
    fn policy_names_round_trip_and_a_typo_is_refused() {
        assert_eq!(
            ExportPolicy::parse("count-only"),
            Some(ExportPolicy::CountOnly)
        );
        assert_eq!(
            ExportPolicy::parse("Require_Permission"),
            Some(ExportPolicy::RequirePermission)
        );
        // Not `None => default`: a misspelt policy that silently became
        // "emit everything" is how a guarantee is lost to a typo.
        assert_eq!(ExportPolicy::parse("require-permision"), None);
        assert_eq!(ExportPolicy::parse(""), None);
    }

    #[test]
    fn a_source_without_a_version_is_not_reproducible() {
        let p = Provenance {
            source_uri: Some("https://wikidata.org".into()),
            ..Default::default()
        };
        assert!(!p.is_empty());
        assert!(!p.is_reproducible(), "'from Wikidata' names no dump");
        let p = Provenance {
            source_version: Some("2026-08-01".into()),
            ..p
        };
        assert!(p.is_reproducible());
    }

    fn node(store: &mut GraphStore, label: &str, props: &[(&str, PropertyValue)]) -> NodeId {
        let id = store.create_node(label);
        for (k, v) in props {
            store.set_node_property("default", id, *k, v.clone()).unwrap();
        }
        id
    }

    fn s(v: &str) -> PropertyValue {
        PropertyValue::String(v.into())
    }

    #[test]
    fn of_reads_every_key_and_renders_non_strings_as_text() {
        let mut g = GraphStore::new();
        let n = node(
            &mut g,
            "F",
            &[
                (SOURCE_URI, s("https://x")),
                (SOURCE_VERSION, PropertyValue::Integer(3)),
                (RETRIEVED_AT, s("2026-01-01")),
                (LICENSE, s("CC-BY")),
            ],
        );
        let p = of(&g, n);
        assert_eq!(p.source_uri.as_deref(), Some("https://x"));
        assert_eq!(p.source_version.as_deref(), Some("3"));
        assert_eq!(p.retrieved_at.as_deref(), Some("2026-01-01"));
        assert_eq!(p.license.as_deref(), Some("CC-BY"));
        let bare = node(&mut g, "F", &[]);
        assert!(of(&g, bare).is_empty());
    }

    #[test]
    fn redistributable_reads_booleans_and_text() {
        let mut g = GraphStore::new();
        let cases = [
            (Some(PropertyValue::Boolean(true)), Redistributable::Yes),
            (Some(PropertyValue::Boolean(false)), Redistributable::No),
            (Some(s(" YES ")), Redistributable::Yes),
            (Some(s("1")), Redistributable::Yes),
            (Some(s("no")), Redistributable::No),
            (Some(s("0")), Redistributable::No),
            (Some(s("maybe")), Redistributable::Unknown),
            (Some(PropertyValue::Integer(1)), Redistributable::Unknown),
            (None, Redistributable::Unknown),
        ];
        for (v, want) in cases {
            let props: Vec<(&str, PropertyValue)> = v.clone().map(|v| (REDISTRIBUTABLE, v)).into_iter().collect();
            let n = node(&mut g, "F", &props);
            assert_eq!(redistributable(&g, n), want, "{v:?}");
        }
    }

    #[test]
    fn derivation_path_is_breadth_first_cycle_safe_and_ignores_other_edges() {
        let mut g = GraphStore::new();
        let fact = node(&mut g, "Fact", &[]);
        let a = node(&mut g, "Source", &[(SOURCE_URI, s("a"))]);
        let b = node(&mut g, "Source", &[]);
        let deep = node(&mut g, "Dump", &[(REDISTRIBUTABLE, PropertyValue::Boolean(false))]);
        let unrelated = node(&mut g, "X", &[]);
        g.create_edge(fact, a, DERIVED_FROM).unwrap();
        g.create_edge(fact, b, DERIVED_FROM).unwrap();
        g.create_edge(a, deep, DERIVED_FROM).unwrap();
        g.create_edge(deep, fact, DERIVED_FROM).unwrap(); // a claimed cycle
        g.create_edge(fact, unrelated, "MENTIONS").unwrap();

        let path = derivation_path(&g, fact);
        let got: Vec<(u64, usize)> = path.iter().map(|d| (d.node, d.depth)).collect();
        assert_eq!(got.len(), 4);
        assert_eq!(got[0], (fact.as_u64(), 0));
        let mut depth1: Vec<u64> = got.iter().filter(|(_, d)| *d == 1).map(|(n, _)| *n).collect();
        depth1.sort();
        assert_eq!(depth1, vec![a.as_u64(), b.as_u64()]);
        assert_eq!(got[3], (deep.as_u64(), 2));
        assert_eq!(path[3].labels, vec!["Dump".to_string()]);
        assert_eq!(path[3].redistributable, Redistributable::No);
        assert!(!got.iter().any(|(n, _)| *n == unrelated.as_u64()));

        // A missing node is still reported, with nothing known about it.
        let ghost = derivation_path(&g, NodeId::new(987_654));
        assert_eq!(ghost.len(), 1);
        assert!(ghost[0].labels.is_empty() && ghost[0].provenance.is_empty());
    }

    #[test]
    fn screen_counts_and_withholds_by_policy() {
        let mut g = GraphStore::new();
        let yes = node(
            &mut g,
            "F",
            &[
                (REDISTRIBUTABLE, PropertyValue::Boolean(true)),
                (SOURCE_URI, s("u")),
                (SOURCE_VERSION, s("v")),
                (LICENSE, s("MIT")),
            ],
        );
        let no = node(&mut g, "F", &[(REDISTRIBUTABLE, s("false")), (LICENSE, s("Proprietary"))]);
        let unknown = node(&mut g, "F", &[(LICENSE, s("MIT"))]);
        let all = [yes, no, unknown];

        let (kept, r) = screen(&g, &all, ExportPolicy::CountOnly);
        assert_eq!(kept, all.to_vec());
        assert_eq!(
            (r.nodes_considered, r.marked_redistributable, r.marked_not_redistributable, r.unmarked, r.withheld, r.reproducible),
            (3, 1, 1, 1, 0, 1)
        );
        assert_eq!(r.licenses, vec!["MIT".to_string(), "Proprietary".to_string()]);

        let (kept, r) = screen(&g, &all, ExportPolicy::WithholdForbidden);
        assert_eq!(kept, vec![yes, unknown]);
        assert_eq!(r.withheld, 1);

        let (kept, r) = screen(&g, &all, ExportPolicy::RequirePermission);
        assert_eq!(kept, vec![yes]);
        assert_eq!(r.withheld, 2);

        let (kept, r) = screen(&g, &[], ExportPolicy::default());
        assert!(kept.is_empty());
        assert_eq!(r, LicenceReport::default());
    }
}
