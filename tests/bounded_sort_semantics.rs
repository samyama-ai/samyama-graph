//! A bounded `ORDER BY … LIMIT k` returns what sorting everything would (#568).
//!
//! These began as tests for a rejection cutoff — once the buffer has been
//! trimmed to `k`, the worst key retained bounds what can still qualify, so
//! most rows could be discarded after one comparison. **That optimisation was
//! measured and rejected**: it was 1-8% *slower* in every input order, because
//! the per-row cost of a bounded sort is evaluating the key, not allocating it.
//! Records are moved into the buffer, not cloned, so skipping the move saves
//! little and the extra comparison costs more. The measurement is on #568 and
//! reproducible with `cargo bench --bench sort_topn`.
//!
//! The tests stayed, because bounded `ORDER BY` had no coverage of the cases
//! that actually decide it:
//!
//! * **input order**, since a top-k over data already sorted by the key is a
//!   different path from one over scattered data, and the adversarial case —
//!   every winner arriving in the last batch — is where a wrong bound shows;
//! * **ties**, where rows equal to the cut are still eligible;
//! * **nulls**, which sort *greatest* here (#369), so they lead under DESC and
//!   trail under ASC — asserted against `main` rather than assumed, after an
//!   earlier draft encoded "nulls last" for both directions and failed against
//!   correct output.
//!
//! Every case is checked against the whole input sorted in the test, so the
//! assertion is "same as sorting everything", not a hand-written expectation.
//!
//! The fixture must exceed the input batch size. An earlier draft used 20,000
//! rows against a batch size of 65,536, so the whole input arrived at once;
//! every test passed with the implementation deliberately broken two different
//! ways, because no row was ever compared against anything.

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::{QueryExecutor, Value};
use samyama::query::parser::parse_query;

/// `values` in the order given, so a test controls when good rows arrive.
fn store_of(values: &[Option<i64>]) -> GraphStore {
    let mut store = GraphStore::new();
    for (i, v) in values.iter().enumerate() {
        let id = store.create_node("N");
        let _ = store.set_node_property("default", id, "seq".to_string(), PropertyValue::Integer(i as i64));
        if let Some(v) = v {
            let _ = store.set_node_property("default", id, "v".to_string(), PropertyValue::Integer(*v));
        }
    }
    store
}

fn run(store: &GraphStore, cypher: &str) -> Vec<Option<i64>> {
    let query = parse_query(cypher).expect("query should parse");
    QueryExecutor::new(store)
        .execute(&query)
        .expect("query should run")
        .records
        .iter()
        .map(|r| match r.get("v") {
            Some(Value::Property(PropertyValue::Integer(n))) => Some(*n),
            Some(Value::Property(PropertyValue::Null)) | Some(Value::Null) | None => None,
            other => panic!("{other:?}"),
        })
        .collect()
}

/// What sorting the whole input and taking `k` would give.
///
/// Null sorts **greatest** in this engine (#369), so it comes first under DESC
/// and last under ASC. Verified against `main` rather than assumed: an earlier
/// draft of these tests encoded "nulls last" for both directions and failed
/// against correct output.
fn reference(values: &[Option<i64>], k: usize, descending: bool) -> Vec<Option<i64>> {
    let mut sorted: Vec<Option<i64>> = values.to_vec();
    sorted.sort_by(|a, b| {
        let ord = match (a, b) {
            (Some(x), Some(y)) => x.cmp(y),
            (Some(_), None) => std::cmp::Ordering::Less,
            (None, Some(_)) => std::cmp::Ordering::Greater,
            (None, None) => std::cmp::Ordering::Equal,
        };
        if descending { ord.reverse() } else { ord }
    });
    sorted.into_iter().take(k).collect()
}

/// Enough rows to fill several input batches. See the note at the top of the
/// file: a fixture that fits in one batch tests almost nothing here.
const N: usize = 200_000;

#[test]
fn the_winners_are_found_when_they_arrive_last() {
    // The adversarial case. Rows arrive worst-first, so every row that belongs
    // in the answer arrives after a cutoff has already been set. A threshold
    // that failed to improve, or that rejected on the wrong side, loses them.
    let values: Vec<Option<i64>> = (0..N as i64).map(Some).collect();
    let store = store_of(&values);
    let got = run(&store, "MATCH (n:N) RETURN n.v AS v ORDER BY n.v DESC LIMIT 20");
    assert_eq!(got, reference(&values, 20, true));
    assert_eq!(got[0], Some(N as i64 - 1), "the largest value arrived last: {got:?}");
}

#[test]
fn the_winners_are_found_when_they_arrive_first() {
    // The opposite: the cutoff goes tight almost immediately and nearly every
    // subsequent row is rejected. This is the path the optimisation is for.
    let values: Vec<Option<i64>> = (0..N as i64).rev().map(Some).collect();
    let store = store_of(&values);
    let got = run(&store, "MATCH (n:N) RETURN n.v AS v ORDER BY n.v DESC LIMIT 20");
    assert_eq!(got, reference(&values, 20, true));
}

#[test]
fn a_scattered_order_agrees_with_sorting_everything() {
    let values: Vec<Option<i64>> = (0..N as u64)
        .map(|i| {
            let x = i.wrapping_mul(0x9E37_79B9_7F4A_7C15);
            Some(((x ^ (x >> 31)) % 1_000_000) as i64)
        })
        .collect();
    let store = store_of(&values);
    for (cypher, desc) in [
        ("MATCH (n:N) RETURN n.v AS v ORDER BY n.v DESC LIMIT 20", true),
        ("MATCH (n:N) RETURN n.v AS v ORDER BY n.v ASC LIMIT 20", false),
    ] {
        assert_eq!(run(&store, cypher), reference(&values, 20, desc), "{cypher}");
    }
}

#[test]
fn ties_at_the_threshold_are_not_dropped() {
    // Every row shares one value, so every row ties with the cutoff. Rejecting
    // on `>=` instead of `>` would return nothing after the first trim.
    let values: Vec<Option<i64>> = std::iter::repeat(Some(7)).take(N).collect();
    let store = store_of(&values);
    let got = run(&store, "MATCH (n:N) RETURN n.v AS v ORDER BY n.v DESC LIMIT 20");
    assert_eq!(got.len(), 20, "{got:?}");
    assert!(got.iter().all(|v| *v == Some(7)), "{got:?}");
}

#[test]
fn a_tie_spanning_the_cut_still_returns_k_rows() {
    // Half the rows share the winning value, so the cut falls inside a tie.
    let mut values: Vec<Option<i64>> = std::iter::repeat(Some(100)).take(N / 2).collect();
    values.extend(std::iter::repeat(Some(1)).take(N / 2));
    let store = store_of(&values);
    let got = run(&store, "MATCH (n:N) RETURN n.v AS v ORDER BY n.v DESC LIMIT 20");
    assert_eq!(got.len(), 20);
    assert!(got.iter().all(|v| *v == Some(100)), "{got:?}");
}

#[test]
fn a_handful_of_values_among_many_nulls_is_still_found() {
    // Null sorts greatest, so ASC is the direction where the real values are
    // the answer — and they are three rows in twenty thousand, two of which
    // arrive long after the cutoff is set. A cutoff that compared nulls wrongly
    // would reject them.
    let mut values: Vec<Option<i64>> = std::iter::repeat(None).take(N).collect();
    for (slot, v) in [(N - 1, 5i64), (N / 2, 9), (3, 1)] {
        values[slot] = Some(v);
    }
    let store = store_of(&values);

    let ascending = run(&store, "MATCH (n:N) RETURN n.v AS v ORDER BY n.v ASC LIMIT 5");
    assert_eq!(ascending[..3].to_vec(), vec![Some(1), Some(5), Some(9)], "{ascending:?}");
    assert!(ascending[3].is_none() && ascending[4].is_none(), "nulls fill the rest: {ascending:?}");

    // And the other direction, where nulls win outright.
    let descending = run(&store, "MATCH (n:N) RETURN n.v AS v ORDER BY n.v DESC LIMIT 5");
    assert!(descending.iter().all(|v| v.is_none()), "null sorts greatest: {descending:?}");
}

#[test]
fn a_limit_larger_than_the_input_returns_everything_sorted() {
    let values: Vec<Option<i64>> = (0..50i64).map(Some).collect();
    let store = store_of(&values);
    let got = run(&store, "MATCH (n:N) RETURN n.v AS v ORDER BY n.v DESC LIMIT 500");
    assert_eq!(got, reference(&values, 500, true));
    assert_eq!(got.len(), 50);
}

#[test]
fn skip_is_accounted_for_in_the_bound() {
    // The bound pushed down is SKIP + LIMIT. A cutoff computed from LIMIT
    // alone would discard exactly the rows SKIP is meant to step over.
    let values: Vec<Option<i64>> = (0..N as i64).map(Some).collect();
    let store = store_of(&values);
    let got = run(&store, "MATCH (n:N) RETURN n.v AS v ORDER BY n.v DESC SKIP 10 LIMIT 5");
    let full = reference(&values, 15, true);
    assert_eq!(got, full[10..15].to_vec(), "{got:?}");
}

#[test]
fn an_unbounded_order_by_is_unaffected() {
    // No LIMIT means no bound and no cutoff; every row must survive.
    let values: Vec<Option<i64>> = (0..500i64).map(Some).collect();
    let store = store_of(&values);
    let got = run(&store, "MATCH (n:N) RETURN n.v AS v ORDER BY n.v DESC");
    assert_eq!(got.len(), 500);
    assert_eq!(got, reference(&values, 500, true));
}

#[test]
fn a_two_key_sort_agrees_with_sorting_everything() {
    // The cutoff compares whole key tuples. A comparison that stopped at the
    // first key would reject rows that the second key would have rescued.
    let mut store = GraphStore::new();
    for i in 0..N as i64 {
        let id = store.create_node("N");
        // `major` has only 5 distinct values, so ties on it are constant and
        // `minor` decides — which is exactly where a truncated compare fails.
        let _ = store.set_node_property("default", id, "major".to_string(), PropertyValue::Integer(i % 5));
        let _ = store.set_node_property("default", id, "minor".to_string(), PropertyValue::Integer(i));
    }
    let query = parse_query(
        "MATCH (n:N) RETURN n.major AS a, n.minor AS b ORDER BY n.major DESC, n.minor DESC LIMIT 10",
    )
    .unwrap();
    let batch = QueryExecutor::new(&store).execute(&query).unwrap();
    let got: Vec<(i64, i64)> = batch
        .records
        .iter()
        .map(|r| {
            let g = |k: &str| match r.get(k) {
                Some(Value::Property(PropertyValue::Integer(n))) => *n,
                other => panic!("{other:?}"),
            };
            (g("a"), g("b"))
        })
        .collect();

    let mut expected: Vec<(i64, i64)> = (0..N as i64).map(|i| (i % 5, i)).collect();
    expected.sort_by(|x, y| y.cmp(x));
    assert_eq!(got, expected[..10].to_vec());
}

#[test]
fn a_bounded_sort_does_not_scale_with_the_rows_it_discards() {
    // The property the change is for. A top-20 over 400,000 rows should cost
    // about what a top-20 over 40,000 does plus the scan, not ten times.
    let time = |n: usize| -> f64 {
        let values: Vec<Option<i64>> = (0..n as i64).map(Some).collect();
        let store = store_of(&values);
        let query =
            parse_query("MATCH (n:N) RETURN n.v AS v ORDER BY n.v DESC LIMIT 20").unwrap();
        let _ = QueryExecutor::new(&store).execute(&query).unwrap();
        let started = std::time::Instant::now();
        let out = QueryExecutor::new(&store).execute(&query).unwrap();
        assert_eq!(out.records.len(), 20);
        started.elapsed().as_secs_f64()
    };
    let small = time(40_000);
    let large = time(400_000);
    assert!(
        large < small * 40.0,
        "10x the rows cost {:.1}x the time ({small:.4}s -> {large:.4}s)",
        large / small
    );
}
