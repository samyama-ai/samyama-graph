//! ORDER BY ... LIMIT k returns exactly the first k rows of the full sort.
//!
//! The top-k path keeps only k candidates while reading its input. These
//! checks pin what it must return -- the same rows as sorting everything and
//! taking the first k -- across descending order, SKIP, nulls, two keys, ties
//! at the cutoff and a limit larger than the input, so the way the candidates
//! are kept can change without the answers changing.

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::{MutQueryExecutor, QueryExecutor, Value};
use samyama::query::parser::parse_query;

fn store() -> GraphStore {
    let mut s = GraphStore::new();
    // 1,000 nodes; v is a permutation of 0..999 with no ties, w has ties, and
    // every 7th node has no v at all.
    let q = "UNWIND range(0, 999) AS i \
             CREATE (:N {i: i, v: CASE WHEN i % 7 = 0 THEN null ELSE (i * 7919) % 1000 END, w: i % 10})";
    MutQueryExecutor::new(&mut s, "default".into()).execute(&parse_query(q).unwrap()).unwrap();
    s
}

fn col(s: &GraphStore, q: &str) -> Vec<String> {
    let p = parse_query(q).unwrap_or_else(|e| panic!("`{q}`: {e}"));
    let out = QueryExecutor::new(s).execute(&p).unwrap_or_else(|e| panic!("`{q}`: {e}"));
    out.records
        .iter()
        .map(|r| match r.get("c") {
            Some(Value::Property(PropertyValue::Integer(i))) => i.to_string(),
            Some(Value::Property(PropertyValue::Null)) | Some(Value::Null) | None => "null".into(),
            other => format!("{other:?}"),
        })
        .collect()
}

/// The first `k` rows after `skip` of the same query without SKIP/LIMIT.
fn prefix(s: &GraphStore, order: &str, skip: usize, k: usize) -> Vec<String> {
    let all = col(s, &format!("MATCH (n:N) RETURN n.v AS c ORDER BY {order}"));
    all.into_iter().skip(skip).take(k).collect()
}

#[test]
fn descending_top_k_is_the_full_sort_prefix() {
    let s = store();
    assert_eq!(col(&s, "MATCH (n:N) RETURN n.v AS c ORDER BY c DESC LIMIT 20"), prefix(&s, "c DESC", 0, 20));
}

#[test]
fn ascending_top_k_is_the_full_sort_prefix() {
    let s = store();
    assert_eq!(col(&s, "MATCH (n:N) RETURN n.v AS c ORDER BY c LIMIT 20"), prefix(&s, "c", 0, 20));
}

#[test]
fn skip_then_limit() {
    let s = store();
    assert_eq!(col(&s, "MATCH (n:N) RETURN n.v AS c ORDER BY c DESC SKIP 5 LIMIT 10"), prefix(&s, "c DESC", 5, 10));
}

/// Nulls sort last ascending and first descending.
#[test]
fn nulls_at_either_end() {
    let s = store();
    let desc = col(&s, "MATCH (n:N) RETURN n.v AS c ORDER BY c DESC LIMIT 3");
    assert_eq!(desc, vec!["null", "null", "null"]);
    let asc = col(&s, "MATCH (n:N) WHERE n.i < 30 RETURN n.v AS c ORDER BY c LIMIT 30");
    assert_eq!(asc.last().map(String::as_str), Some("null"));
}

#[test]
fn two_keys() {
    let s = store();
    let q = |lim: &str| format!("MATCH (n:N) WHERE n.v IS NOT NULL RETURN n.i AS c ORDER BY n.w DESC, n.v ASC {lim}");
    let all = col(&s, &q(""));
    assert_eq!(col(&s, &q("LIMIT 15")), all[..15].to_vec());
}

/// With ties at the cutoff, which tied rows come back is not specified; the
/// keys that come back are.
#[test]
fn ties_at_the_cutoff_return_the_right_keys() {
    let s = store();
    let got = col(&s, "MATCH (n:N) RETURN n.w AS c ORDER BY c DESC LIMIT 150");
    assert_eq!(got.len(), 150);
    assert_eq!(got.iter().filter(|k| *k == "9").count(), 100);
    assert_eq!(got.iter().filter(|k| *k == "8").count(), 50);
}

#[test]
fn a_limit_larger_than_the_input_returns_everything_sorted() {
    let s = store();
    assert_eq!(col(&s, "MATCH (n:N) RETURN n.v AS c ORDER BY c DESC LIMIT 5000"), prefix(&s, "c DESC", 0, 1000));
}
