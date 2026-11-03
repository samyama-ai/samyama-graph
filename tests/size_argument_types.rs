//! `size()` takes a list or a string, and rejects anything else at compile
//! time (#843).
//!
//! ```cypher
//! MATCH (a), (b), (c) RETURN size((a)-[:REL]->(b))
//! MATCH p = (a)-[*]->(b) RETURN size(p)
//! ```
//!
//! The engine already raised a `TypeError` for the pattern form — **but only
//! when the pattern matched something.** The TCK's scenarios run against an
//! empty graph, so nothing binds, the argument is never evaluated, and the
//! query "succeeds with 0 rows". That is why openCypher requires the error at
//! compile time: a runtime check is not a weaker version of the same thing, it
//! is a different thing that an empty result silently satisfies.
//!
//! So every rejection below is asserted **against an empty store**, which is
//! the condition the runtime check passes and the compile-time one does not.

use samyama::graph::GraphStore;
use samyama::query::executor::{QueryExecutor, Value};
use samyama::query::parser::parse_query;

/// `Err` if the query is refused before it runs.
fn compiles(cypher: &str) -> bool {
    parse_query(cypher).is_ok()
}

/// **Against an empty store**, where a runtime check cannot fire.
#[test]
fn size_of_a_pattern_is_refused_before_it_runs() {
    for pattern in [
        "()--()",
        "()--(a)",
        "(a)-->()",
        "(a)-[:REL]->(b)",
        "(a)<--(a {})",
        "()-[:REL*0..2]->()<-[:REL]-(:A {num: 5})",
        "(a)-[:REL]->(:C)<-[:REL]-(a {num: 5})",
    ] {
        let cypher = format!("MATCH (a), (b), (c) RETURN size({pattern})");
        assert!(!compiles(&cypher), "accepted size({pattern})");
    }
}

/// A path has `length()`, not `size()` — and this one returned a number.
#[test]
fn size_of_a_path_is_refused() {
    assert!(!compiles("MATCH p = (a)-[*]->(b) RETURN size(p)"));
    assert!(!compiles("MATCH p = (a)-->(b) RETURN size(p)"));
    // The same variable name not bound as a path is fine.
    assert!(compiles("WITH [1,2] AS p RETURN size(p)"));
}

/// `EXISTS { ... }` desugars to the same AST node as a bare pattern. The
/// `bare_pattern` flag is the only thing separating them, and widening the
/// check to both is how #798 rejected every `EXISTS` in the codebase.
#[test]
fn exists_subqueries_are_untouched() {
    assert!(compiles("MATCH (a) WHERE EXISTS { MATCH (a)-->(b) } RETURN a"));
    assert!(compiles("MATCH (a) RETURN EXISTS { MATCH (a)-->(b) } AS e"));
}

/// The legitimate arguments still work, and still return the right answers —
/// a check that refuses everything would pass the tests above.
#[test]
fn lists_and_strings_still_work() {
    let store = GraphStore::new();
    for (expr, want) in [("size([1,2,3])", 3), ("size('abc')", 3), ("size(keys({a:1}))", 1)] {
        let cypher = format!("RETURN {expr} AS r");
        let q = parse_query(&cypher).unwrap_or_else(|e| panic!("{cypher} refused: {e:?}"));
        let batch = QueryExecutor::new(&store).execute(&q).expect("runs");
        assert_eq!(
            batch.records.first().and_then(|r| r.get("r")),
            Some(&Value::Property(samyama::graph::PropertyValue::Integer(want))),
            "{expr}"
        );
    }
}
