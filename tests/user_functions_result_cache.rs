//! The result cache and the user-function registry (LANG-10, #1674).
//!
//! In a file of its own: the registry's generation is the process's, so a
//! registration by a test running beside this one would also invalidate the
//! cache, and "the second read is a hit" would fail for a reason that is not
//! this test's.

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::Value;
use samyama::query::QueryEngine;
use samyama::udf;

fn int(n: i64) -> Value {
    Value::Property(PropertyValue::Integer(n))
}

fn store(_: &str) -> GraphStore {
    GraphStore::new()
}

#[test]
fn replacing_a_function_is_not_hidden_by_the_result_cache() {
    udf::register_function("t8.version", 0..=0, |_| Ok(PropertyValue::Integer(1))).unwrap();
    let g = store("");
    let engine = QueryEngine::new();
    let q = "RETURN t8.version() AS v";
    let (first, _) = engine.execute_cached(q, &g).unwrap();
    let (_, hit) = engine.execute_cached(q, &g).unwrap();
    assert!(hit, "the second read is a cache hit");
    assert_eq!(first.records[0].get("v"), Some(&int(1)));

    udf::register_function("t8.version", 0..=0, |_| Ok(PropertyValue::Integer(2))).unwrap();
    let (after, hit) = engine.execute_cached(q, &g).unwrap();
    assert!(
        !hit,
        "the registry changed, so the cached answer is not served"
    );
    assert_eq!(after.records[0].get("v"), Some(&int(2)));
}
