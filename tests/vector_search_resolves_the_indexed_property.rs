//! `/api/vector-search` asks the index which property holds the vectors (#1481).
//!
//! It defaulted to the literal `"embedding"`. A vector index on any other
//! property — `CREATE VECTOR INDEX vx FOR (n:V) ON (n.emb)` is perfectly
//! ordinary — made the endpoint search a property that does not exist and
//! return `200` with an empty result set. That reads as "nothing is similar",
//! not "wrong property", so it is acted on rather than investigated. An empty
//! result is the worst wrong answer.
//!
//! The manager knew the right answer the whole time: `list_indices()` returns
//! every `(label, property_key)` pair. Nobody asked it.
//!
//! These tests are against `VectorIndexManager`, which is where the resolution
//! gets its facts. The handler's three cases — one index, several, none — are
//! asserted here as the manager-level question each one reduces to, and the
//! end-to-end behaviour was verified against a running server on both binaries
//! before and after the change.

use samyama::vector::{DistanceMetric, VectorIndexManager};

fn manager_with(pairs: &[(&str, &str)]) -> VectorIndexManager {
    let m = VectorIndexManager::new();
    for (label, prop) in pairs {
        m.create_index(label, prop, 3, DistanceMetric::L2)
            .expect("index creation");
    }
    m
}

/// The property is not a guess when there is exactly one index on the label.
#[test]
fn one_index_on_a_label_names_its_own_property() {
    let m = manager_with(&[("Doc", "emb")]);
    let found: Vec<String> = m
        .list_indices()
        .into_iter()
        .filter(|k| k.label == "Doc")
        .map(|k| k.property_key)
        .collect();
    assert_eq!(found, vec!["emb".to_string()],
               "the resolution has exactly one candidate and it is not `embedding`");
}

/// Two indexes is the case a default cannot serve, and where the old code
/// silently searched whichever one happened to be called `embedding`.
#[test]
fn two_indexes_on_a_label_are_both_visible_so_the_caller_can_be_asked() {
    let m = manager_with(&[("Two", "a"), ("Two", "b")]);
    let mut found: Vec<String> = m
        .list_indices()
        .into_iter()
        .filter(|k| k.label == "Two")
        .map(|k| k.property_key)
        .collect();
    found.sort();
    assert_eq!(found, vec!["a".to_string(), "b".to_string()],
               "an ambiguous label must expose every candidate, not pick one");
}

/// A label with no vector index must not become an error: the endpoint still
/// has a no-label path and an empty graph is not a misuse.
#[test]
fn a_label_with_no_index_has_no_candidates() {
    let m = manager_with(&[("Other", "emb")]);
    let found: Vec<String> = m
        .list_indices()
        .into_iter()
        .filter(|k| k.label == "Absent")
        .map(|k| k.property_key)
        .collect();
    assert!(found.is_empty(), "no index on the label means no candidate to resolve");
}

/// Indexes on other labels must not leak into another label's resolution.
#[test]
fn resolution_is_scoped_to_the_label_asked_about() {
    let m = manager_with(&[("A", "emb"), ("B", "other"), ("B", "third")]);
    let a: Vec<String> = m.list_indices().into_iter()
        .filter(|k| k.label == "A").map(|k| k.property_key).collect();
    assert_eq!(a, vec!["emb".to_string()],
               "B's two indexes must not make A ambiguous");
}
