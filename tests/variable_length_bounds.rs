//! A negative variable-length bound is refused, not a panic (#878).
//!
//! ```text
//! MATCH (a)-[:R*..-2]->(c)
//!   thread panicked: called `Result::unwrap()` on an `Err` value: ParseIntError
//! ```
//!
//! The `integer` grammar rule accepts a leading `-` and the bounds are `usize`,
//! so a negative bound cannot be parsed. `parse_length_pattern` handled that
//! three different ways in three adjacent lines: two `.unwrap()`s that panicked
//! and one `unwrap_or(1)` that silently read `*-2..` as `*1..` and returned
//! rows for a different query.
//!
//! The quiet one is the worse defect; the panic is the louder one. Both are
//! covered, and so are the bounds that must keep working — a fix that rejected
//! every bound would satisfy the first half of this file.

use samyama::query::parser::parse_query;

/// Every position a negative bound can occupy. `parse_query` returning `Err`
/// is the assertion: a panic would fail the test by aborting it.
#[test]
fn a_negative_bound_is_refused() {
    for cypher in [
        "MATCH (a)-[:R*-2]->(c) RETURN c",
        "MATCH (a)-[:R*-2..]->(c) RETURN c",
        "MATCH (a)-[:R*..-2]->(c) RETURN c",
        "MATCH (a)-[:R*-2..-1]->(c) RETURN c",
        "MATCH (a)-[r*-1]->(c) RETURN r",
    ] {
        assert!(parse_query(cypher).is_err(), "accepted `{cypher}`");
    }
}

/// The error names what was wrong, rather than being a generic parse failure —
/// the caller has to be able to see which bound is at fault.
#[test]
fn the_error_names_the_bound() {
    let e = format!("{:?}", parse_query("MATCH (a)-[:R*..-2]->(c) RETURN c").unwrap_err());
    assert!(e.contains("-2"), "error does not mention the offending text: {e}");
    assert!(
        e.contains("non-negative") || e.contains("upper"),
        "error does not say what was expected: {e}"
    );
}

/// **Valid bounds are unaffected.** A fix that rejected everything would pass
/// the tests above.
#[test]
fn valid_bounds_still_parse() {
    for cypher in [
        "MATCH (a)-[:R*]->(c) RETURN c",
        "MATCH (a)-[:R*1]->(c) RETURN c",
        "MATCH (a)-[:R*0]->(c) RETURN c",
        "MATCH (a)-[:R*1..2]->(c) RETURN c",
        "MATCH (a)-[:R*..3]->(c) RETURN c",
        "MATCH (a)-[:R*2..]->(c) RETURN c",
        // Max below min is not a *syntax* error; it simply matches nothing.
        "MATCH (a)-[:R*2..1]->(c) RETURN c",
    ] {
        assert!(parse_query(cypher).is_ok(), "refused `{cypher}`");
    }
}
