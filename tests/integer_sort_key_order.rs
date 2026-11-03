//! `ORDER BY` on integer keys: the order a typed integer sort must reproduce.
//!
//! `SortOperator` gained two things for #1819: a direct arm in its key
//! comparator for two integers, and a typed `(i64, row)` order for a key whose
//! every value is an integer. Both claim to be the same answer as the generic
//! `cypher_order_value` path, so these tests pin that answer — written down
//! here, not read back from the thing under test.
//!
//! Every case is small enough to state the expected order by hand, and the
//! awkward ones are the point: both signs, `i64::MIN`/`MAX`, mixed integer and
//! float (which numbers compare across, so the typed path must **not** take
//! it), null, an absent property, more keys than fit inline, `DESC`, mixed
//! directions, and ties.
//!
//! Ties are asserted on the multiset, never on which tied row survives.
//! `SortOperator`'s own documentation says two runs may disagree about that
//! under `LIMIT`, and a test that pinned it would be pinning latitude the
//! operator deliberately keeps (see `tests/streaming_top_k_aggregate.rs`).

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::{MutQueryExecutor, QueryExecutor, Value};
use samyama::query::parser::parse_query;

fn run(store: &GraphStore, cypher: &str) -> Vec<Vec<Value>> {
    let q = parse_query(cypher).unwrap_or_else(|e| panic!("parse {cypher}: {e}"));
    let out = QueryExecutor::new(store)
        .execute(&q)
        .unwrap_or_else(|e| panic!("execute {cypher}: {e}"));
    out.records
        .iter()
        .map(|r| r.bindings().iter().map(|(_, v)| v.clone()).collect())
        .collect()
}

/// The first column of every row, as `i64` where it is one and `None` where it
/// is null or absent.
fn ints(rows: &[Vec<Value>]) -> Vec<Option<i64>> {
    rows.iter()
        .map(|r| match r.first() {
            Some(Value::Property(PropertyValue::Integer(v))) => Some(*v),
            _ => None,
        })
        .collect()
}

/// The first column as a string, so a mixed-type order can be written down.
fn shown(rows: &[Vec<Value>]) -> Vec<String> {
    rows.iter()
        .map(|r| match r.first() {
            Some(Value::Property(PropertyValue::Integer(v))) => format!("i{v}"),
            Some(Value::Property(PropertyValue::Float(v))) => format!("f{v}"),
            Some(Value::Property(PropertyValue::String(s))) => format!("s{s}"),
            Some(Value::Property(PropertyValue::Null)) | Some(Value::Null) | None => {
                "null".to_string()
            }
            Some(other) => format!("{other:?}"),
        })
        .collect()
}

fn empty() -> GraphStore {
    GraphStore::new()
}

/// Nodes whose `n.k` is the given value, in the order given, plus a `seq` so a
/// second key exists. A `None` leaves the property unset, which is how an
/// absent key reaches the sort.
fn store_with_keys(keys: &[Option<i64>]) -> GraphStore {
    let mut store = GraphStore::new();
    let mut parts = Vec::new();
    for (i, k) in keys.iter().enumerate() {
        match k {
            Some(v) => parts.push(format!("(:T {{k: {v}, seq: {i}}})")),
            None => parts.push(format!("(:T {{seq: {i}}})")),
        }
    }
    let cypher = format!("CREATE {}", parts.join(", "));
    let q = parse_query(&cypher).unwrap();
    MutQueryExecutor::new(&mut store, "default".to_string())
        .execute(&q)
        .unwrap();
    store
}

#[test]
fn integers_of_both_signs_sort_numerically() {
    let store = empty();
    let rows = run(
        &store,
        "UNWIND [3, -1, 0, 9223372036854775807, -9223372036854775808, 2, -7] AS x \
         RETURN x ORDER BY x",
    );
    assert_eq!(
        ints(&rows),
        vec![
            Some(i64::MIN),
            Some(-7),
            Some(-1),
            Some(0),
            Some(2),
            Some(3),
            Some(i64::MAX),
        ]
    );
}

#[test]
fn descending_integers_reverse_the_ascending_order() {
    let store = empty();
    let asc = ints(&run(&store, "UNWIND [3, -1, 0, 2, -7] AS x RETURN x ORDER BY x"));
    let desc = ints(&run(
        &store,
        "UNWIND [3, -1, 0, 2, -7] AS x RETURN x ORDER BY x DESC",
    ));
    let mut expected = asc.clone();
    expected.reverse();
    assert_eq!(desc, expected, "DESC must be ASC reversed when no key ties");
    assert_eq!(asc, vec![Some(-7), Some(-1), Some(0), Some(2), Some(3)]);
}

#[test]
fn mixed_integer_and_float_keys_compare_across_the_two() {
    // 999999 > 6.9 is the rule `PropertyValue::cmp` documents, and the one a
    // typed integer path would break if it took a mixed key.
    let store = empty();
    let rows = run(
        &store,
        "UNWIND [999999, 6.9, 7, 6.8999, -2.5, -2] AS x RETURN x ORDER BY x",
    );
    assert_eq!(
        shown(&rows),
        vec!["f-2.5", "i-2", "f6.8999", "f6.9", "i7", "i999999"]
    );
}

#[test]
fn null_sorts_after_every_integer_ascending_and_first_descending() {
    let store = empty();
    let asc = run(&store, "UNWIND [3, null, -1, null, 0] AS x RETURN x ORDER BY x");
    assert_eq!(shown(&asc), vec!["i-1", "i0", "i3", "null", "null"]);
    let desc = run(
        &store,
        "UNWIND [3, null, -1, null, 0] AS x RETURN x ORDER BY x DESC",
    );
    assert_eq!(shown(&desc), vec!["null", "null", "i3", "i0", "i-1"]);
}

#[test]
fn an_absent_property_is_null_and_sorts_last() {
    let store = store_with_keys(&[Some(5), None, Some(-2), Some(0), None]);
    let rows = run(&store, "MATCH (n:T) RETURN n.k ORDER BY n.k");
    assert_eq!(shown(&rows), vec!["i-2", "i0", "i5", "null", "null"]);
}

#[test]
fn a_string_key_beside_integers_keeps_cyphers_cross_type_order() {
    // String < Boolean < Number < null. A typed integer path must decline this
    // key rather than compare the string as a number.
    let store = empty();
    let rows = run(&store, "UNWIND [3, 'b', -1, 'a'] AS x RETURN x ORDER BY x");
    assert_eq!(shown(&rows), vec!["sa", "sb", "i-1", "i3"]);
}

#[test]
fn two_integer_keys_order_by_the_first_then_the_second() {
    let store = store_with_keys(&[Some(2), Some(1), Some(2), Some(1), Some(3)]);
    let rows = run(&store, "MATCH (n:T) RETURN n.k, n.seq ORDER BY n.k, n.seq");
    let pairs: Vec<(i64, i64)> = rows
        .iter()
        .map(|r| {
            let g = |i: usize| match &r[i] {
                Value::Property(PropertyValue::Integer(v)) => *v,
                other => panic!("not an integer: {other:?}"),
            };
            (g(0), g(1))
        })
        .collect();
    assert_eq!(pairs, vec![(1, 1), (1, 3), (2, 0), (2, 2), (3, 4)]);
}

#[test]
fn two_integer_keys_with_opposite_directions() {
    let store = store_with_keys(&[Some(2), Some(1), Some(2), Some(1), Some(3)]);
    let rows = run(
        &store,
        "MATCH (n:T) RETURN n.k, n.seq ORDER BY n.k ASC, n.seq DESC",
    );
    let pairs: Vec<(i64, i64)> = rows
        .iter()
        .map(|r| {
            let g = |i: usize| match &r[i] {
                Value::Property(PropertyValue::Integer(v)) => *v,
                other => panic!("not an integer: {other:?}"),
            };
            (g(0), g(1))
        })
        .collect();
    assert_eq!(pairs, vec![(1, 3), (1, 1), (2, 2), (2, 0), (3, 4)]);
}

#[test]
fn three_integer_keys_do_not_fit_inline_and_still_order() {
    let store = empty();
    let rows = run(
        &store,
        "UNWIND [[1, 2, 3], [1, 2, 1], [0, 9, 9], [1, 1, 5]] AS t \
         RETURN t[0] AS a, t[1] AS b, t[2] AS c ORDER BY a, b, c",
    );
    let triples: Vec<Vec<i64>> = rows
        .iter()
        .map(|r| {
            r.iter()
                .map(|v| match v {
                    Value::Property(PropertyValue::Integer(x)) => *x,
                    other => panic!("not an integer: {other:?}"),
                })
                .collect()
        })
        .collect();
    assert_eq!(
        triples,
        vec![vec![0, 9, 9], vec![1, 1, 5], vec![1, 2, 1], vec![1, 2, 3]]
    );
}

#[test]
fn ties_keep_every_row_and_group_the_equal_keys_together() {
    let store = empty();
    let rows = run(
        &store,
        "UNWIND [2, 1, 2, 1, 2, 3] AS x RETURN x ORDER BY x",
    );
    // The multiset, not the arrangement within a tie: nothing here distinguishes
    // two rows whose key is 2.
    assert_eq!(
        ints(&rows),
        vec![Some(1), Some(1), Some(2), Some(2), Some(2), Some(3)]
    );
}

#[test]
fn limit_returns_the_k_smallest_integers() {
    // The bounded branch: `limit_hint` reaches the sort and it keeps k rows.
    let store = empty();
    let rows = run(
        &store,
        "UNWIND [5, -3, 11, 0, 7, -9, 2] AS x RETURN x ORDER BY x LIMIT 3",
    );
    assert_eq!(ints(&rows), vec![Some(-9), Some(-3), Some(0)]);
    let rows = run(
        &store,
        "UNWIND [5, -3, 11, 0, 7, -9, 2] AS x RETURN x ORDER BY x DESC LIMIT 3",
    );
    assert_eq!(ints(&rows), vec![Some(11), Some(7), Some(5)]);
}

#[test]
fn skip_and_limit_together_take_a_window_of_the_order() {
    let store = empty();
    let rows = run(
        &store,
        "UNWIND [5, -3, 11, 0, 7, -9, 2] AS x RETURN x ORDER BY x SKIP 2 LIMIT 3",
    );
    assert_eq!(ints(&rows), vec![Some(0), Some(2), Some(5)]);
}

#[test]
fn a_tied_limit_returns_k_rows_from_the_tied_set() {
    // Which of the tied rows survives is not defined; how many do, and that
    // they are all from the tied set, is.
    let store = empty();
    let rows = run(
        &store,
        "UNWIND [4, 4, 4, 4, 9] AS x RETURN x ORDER BY x LIMIT 3",
    );
    assert_eq!(ints(&rows), vec![Some(4), Some(4), Some(4)]);
}

#[test]
fn distinct_above_the_sort_still_orders_every_row() {
    // A `DISTINCT` between the sort and the limit stops the limit reaching the
    // sort as a hard bound, so the sort orders a head up front and the rest
    // only if read. Every row must still come back in order.
    let store = empty();
    let rows = run(
        &store,
        "UNWIND [5, 1, 5, 3, 1, 9, -2] AS x RETURN DISTINCT x ORDER BY x",
    );
    assert_eq!(ints(&rows), vec![Some(-2), Some(1), Some(3), Some(5), Some(9)]);
}

#[test]
fn a_negative_and_a_positive_tie_on_the_first_key() {
    // The second key decides, and the first key spanning zero is where a
    // packed comparison that treated the key as unsigned would show up.
    let store = store_with_keys(&[Some(-1), Some(-1), Some(1), Some(1)]);
    let rows = run(
        &store,
        "MATCH (n:T) RETURN n.k, n.seq ORDER BY n.k, n.seq DESC",
    );
    let pairs: Vec<(i64, i64)> = rows
        .iter()
        .map(|r| {
            let g = |i: usize| match &r[i] {
                Value::Property(PropertyValue::Integer(v)) => *v,
                other => panic!("not an integer: {other:?}"),
            };
            (g(0), g(1))
        })
        .collect();
    assert_eq!(pairs, vec![(-1, 1), (-1, 0), (1, 3), (1, 2)]);
}

#[test]
fn a_boolean_key_outranks_an_integer_and_is_not_an_integer() {
    // `Boolean` sits between `String` and `Number`, and is not an integer, so
    // a typed integer path must decline the key.
    let store = empty();
    let rows = run(&store, "UNWIND [1, true, 0, false] AS x RETURN x ORDER BY x");
    assert_eq!(
        shown(&rows),
        vec!["Property(Boolean(false))", "Property(Boolean(true))", "i0", "i1"]
    );
}

#[test]
fn a_larger_input_orders_the_same_as_a_sorted_copy() {
    // 2,000 rows, past the batch and head thresholds a small fixture never
    // reaches, against an order computed here rather than by the engine.
    let store = empty();
    let values: Vec<i64> = (0..2000)
        .map(|i: i64| ((i * 7919) % 4001) - 2000)
        .collect();
    let list = values
        .iter()
        .map(|v| v.to_string())
        .collect::<Vec<_>>()
        .join(", ");
    let rows = run(&store, &format!("UNWIND [{list}] AS x RETURN x ORDER BY x"));
    let mut expected: Vec<Option<i64>> = values.iter().map(|v| Some(*v)).collect();
    expected.sort();
    assert_eq!(ints(&rows), expected);
}
