//! The GQL dialect switch: ISO semantics on request, openCypher semantics by default
//! (#1642, #1644).
//!
//! Two defaults differ between the languages and neither can be changed unilaterally:
//!
//!   * **The path mode of an unprefixed variable-length pattern.** openCypher
//!     specifies relationship uniqueness, which is TRAIL. ISO/IEC 39075 says that with
//!     no restrictor the matched object is an unrestricted path -- a WALK.
//!   * **What a bare `*` abbreviates.** openCypher reads it as `{1,}`. GQL and SQL/PGQ
//!     read it as `{0,}`, so a ported query silently gains or loses the zero-length
//!     path: exactly one row per starting node.
//!
//! Flipping either unconditionally would change the answer to every existing Cypher
//! query. So the dialect is selected per query and the Cypher reading stays the
//! default; these tests pin both readings, and pin that an *explicit* keyword wins in
//! either dialect.
//!
//! The fixture is the parallel-edge triangle the conformance suite uses, because it
//! separates the modes: `a =e1,e2=> b -e3-> c -e4-> a`.

use samyama::graph::GraphStore;
use samyama::query::{Dialect, QueryEngine};

const T: &str = "default";

fn fixture() -> GraphStore {
    let engine = QueryEngine::new();
    let mut store = GraphStore::new();
    for n in ["a", "b", "c"] {
        engine
            .execute_mut(&format!("CREATE (:N {{eid: \"{n}\", name: \"{n}\"}})"), &mut store, T)
            .unwrap();
    }
    for (from, to) in [("a", "b"), ("a", "b"), ("b", "c"), ("c", "a")] {
        engine
            .execute_mut(
                &format!(
                    "MATCH (x:N {{eid: \"{from}\"}}), (y:N {{eid: \"{to}\"}}) CREATE (x)-[:E]->(y)"
                ),
                &mut store,
                T,
            )
            .unwrap();
    }
    store
}

fn rows(store: &GraphStore, q: &str, d: Dialect) -> usize {
    QueryEngine::new()
        .execute_with_dialect(q, store, d)
        .unwrap_or_else(|e| panic!("{q} [{d:?}]: {e}"))
        .records
        .len()
}

const BOUNDED: &str = "MATCH (x:N)-[:E*1..4]->(y:N) WHERE x.name = 'a' RETURN x.eid, y.eid";
const BARE_STAR: &str = "MATCH (x:N)-[:E*]->(y:N) WHERE x.name = 'a' RETURN x.eid, y.eid";
const EXPLICIT_ZERO: &str = "MATCH (x:N)-[:E*0..]->(y:N) WHERE x.name = 'a' RETURN x.eid, y.eid";

// ---------------------------------------------------------------------------
// #1642 -- the default path mode
// ---------------------------------------------------------------------------

#[test]
fn cypher_is_unchanged_an_unprefixed_pattern_is_a_trail() {
    // 8: the two parallel edges double every path through b, and no edge repeats.
    assert_eq!(rows(&fixture(), BOUNDED, Dialect::Cypher), 8);
}

#[test]
fn gql_reads_an_unprefixed_pattern_as_a_walk() {
    // 10: the two extra paths re-cross e1 or e2 after going round the triangle.
    assert_eq!(rows(&fixture(), BOUNDED, Dialect::Gql), 10);
}

#[test]
fn an_explicit_restrictor_wins_in_either_dialect() {
    let s = fixture();
    let trail = "MATCH TRAIL (x:N)-[:E*1..4]->(y:N) WHERE x.name = 'a' RETURN x.eid, y.eid";
    let walk = "MATCH WALK (x:N)-[:E*1..4]->(y:N) WHERE x.name = 'a' RETURN x.eid, y.eid";
    assert_eq!(rows(&s, trail, Dialect::Cypher), 8);
    assert_eq!(rows(&s, trail, Dialect::Gql), 8, "GQL must not override an explicit TRAIL");
    assert_eq!(rows(&s, walk, Dialect::Cypher), 10, "Cypher must not override an explicit WALK");
    assert_eq!(rows(&s, walk, Dialect::Gql), 10);
}

#[test]
fn the_dialect_does_not_touch_a_fixed_length_pattern() {
    let s = fixture();
    let q = "MATCH (x:N)-[:E]->(y:N) WHERE x.name = 'a' RETURN x.eid, y.eid";
    assert_eq!(rows(&s, q, Dialect::Cypher), 2);
    assert_eq!(rows(&s, q, Dialect::Gql), 2);
}

// ---------------------------------------------------------------------------
// #1644 -- what a bare `*` abbreviates
// ---------------------------------------------------------------------------

#[test]
fn cypher_reads_a_bare_star_as_one_or_more() {
    // Under TRAIL from `a`: 8 paths, none of them the zero-length one.
    assert_eq!(rows(&fixture(), BARE_STAR, Dialect::Cypher), 8);
}

#[test]
fn gql_reads_a_bare_star_as_zero_or_more() {
    // The same 8, plus the zero-length path a -> a. Under GQL the mode is also WALK,
    // which is unbounded here, so the restrictor is stated to keep the answer finite.
    let s = fixture();
    let q = "MATCH TRAIL (x:N)-[:E*]->(y:N) WHERE x.name = 'a' RETURN x.eid, y.eid";
    assert_eq!(rows(&s, q, Dialect::Cypher), 8);
    assert_eq!(rows(&s, q, Dialect::Gql), 9);
}

#[test]
fn an_explicit_lower_bound_means_the_same_in_both_dialects() {
    // The control. `*0..` and `*1..` are unambiguous, so no dialect may move them.
    let s = fixture();
    let zero = "MATCH TRAIL (x:N)-[:E*0..]->(y:N) WHERE x.name = 'a' RETURN x.eid, y.eid";
    let one = "MATCH TRAIL (x:N)-[:E*1..]->(y:N) WHERE x.name = 'a' RETURN x.eid, y.eid";
    assert_eq!(rows(&s, zero, Dialect::Cypher), 9);
    assert_eq!(rows(&s, zero, Dialect::Gql), 9);
    assert_eq!(rows(&s, one, Dialect::Cypher), 8);
    assert_eq!(rows(&s, one, Dialect::Gql), 8);
    let _ = EXPLICIT_ZERO;
}

#[test]
fn the_two_spellings_agree_within_a_dialect() {
    // The self-contradiction seven of nine measured engines have: `*` and `*0..`
    // answering differently. Within one dialect they must mean one thing.
    let s = fixture();
    let bare = "MATCH TRAIL (x:N)-[:E*]->(y:N) WHERE x.name = 'a' RETURN x.eid, y.eid";
    let zero = "MATCH TRAIL (x:N)-[:E*0..]->(y:N) WHERE x.name = 'a' RETURN x.eid, y.eid";
    let one = "MATCH TRAIL (x:N)-[:E*1..]->(y:N) WHERE x.name = 'a' RETURN x.eid, y.eid";
    assert_eq!(rows(&s, bare, Dialect::Gql), rows(&s, zero, Dialect::Gql));
    assert_eq!(rows(&s, bare, Dialect::Cypher), rows(&s, one, Dialect::Cypher));
}

// ---------------------------------------------------------------------------
// The default must not move
// ---------------------------------------------------------------------------

#[test]
fn execute_without_a_dialect_is_cypher() {
    let s = fixture();
    let plain = QueryEngine::new().execute(BOUNDED, &s).unwrap().records.len();
    assert_eq!(plain, rows(&s, BOUNDED, Dialect::Cypher));
}
