//! An unknown function is refused at compile time (#947).
//!
//! ```cypher
//! MATCH (a) RETURN foo(a)
//! ```
//!
//! used to **succeed with zero rows**. A misspelled `lenght(x)` or
//! `strLength(s)` produced an empty result set from a query that reported
//! success, and the reader concluded something about their data. Empty is the
//! answer that survives review: a wrong number gets questioned, an empty
//! result looks like a legitimately empty match.
//!
//! At compile time rather than run time, because the run-time error only fires
//! on a row that reaches the call — over an empty graph it never ran and the
//! query "succeeded".
//!
//! **Deliberately narrow.** The first attempt rejected every name not in the
//! list and turned **345 passing scenarios into errors**: namespaced temporal
//! accessors like `date.realtime` are tolerated at run time, several of them
//! only to propagate null. Over-rejecting a valid query is the worse failure,
//! so a namespaced name is always allowed and this catches the un-namespaced
//! typo — the one that costs a debugging session.

use samyama::query::executor::operator::{is_known_function, KNOWN_FUNCTIONS};
use samyama::query::parser::parse_query;

fn refused(cypher: &str) -> bool {
    parse_query(cypher).is_err()
}

fn accepted(cypher: &str) -> bool {
    parse_query(cypher).is_ok()
}

#[test]
fn an_unknown_function_is_refused() {
    assert!(refused("MATCH (a) RETURN foo(a)"));
    assert!(refused("MATCH (a) RETURN lenght(a.name)"));
    assert!(refused("MATCH (a) WHERE sise(a.list) > 1 RETURN a"));
}

#[test]
fn the_message_names_the_function() {
    let err = format!("{:?}", parse_query("MATCH (a) RETURN lenght(a.name)").unwrap_err());
    assert!(err.contains("lenght"), "{err}");
}

#[test]
fn a_near_miss_of_a_real_name_is_still_refused() {
    // `toLower` and `toLowerCase` are both implemented — the engine takes the
    // alias — so the near miss has to be a name that genuinely is not there.
    assert!(accepted("MATCH (a) RETURN toLower(a.name)"));
    assert!(accepted("MATCH (a) RETURN toLowerCase(a.name)"));
    assert!(refused("MATCH (a) RETURN toLowerCased(a.name)"));
    assert!(accepted("MATCH (a) RETURN length(a.name)"));
    assert!(refused("MATCH (a) RETURN strLength(a.name)"));
}

#[test]
fn every_implemented_function_is_accepted() {
    // The half that matters most. If the list and the dispatcher drift, this
    // is where it shows — rejecting a function that works is far worse than
    // accepting one that does not.
    for name in KNOWN_FUNCTIONS {
        assert!(is_known_function(name), "{name}");
        assert!(is_known_function(&name.to_uppercase()), "{name} uppercased");
    }
}

#[test]
fn function_names_are_case_insensitive() {
    assert!(accepted("MATCH (a) RETURN TOLOWER(a.name)"));
    assert!(accepted("MATCH (a) RETURN ToLower(a.name)"));
}

#[test]
fn aggregates_are_accepted() {
    // Dispatched by the planner, not by `eval_function`, so they are not in
    // KNOWN_FUNCTIONS and would be rejected by a check that forgot them.
    for q in [
        "MATCH (a) RETURN count(a)",
        "MATCH (a) RETURN collect(a.name)",
        "MATCH (a) RETURN sum(a.n), avg(a.n), min(a.n), max(a.n)",
    ] {
        assert!(accepted(q), "{q}");
    }
}

#[test]
fn a_namespaced_name_is_always_allowed() {
    // The 345-scenario lesson, pinned. These are tolerated at run time and
    // must not become compile-time rejections.
    for q in [
        "RETURN date.realtime()",
        "RETURN datetime.statement()",
        "RETURN localtime.transaction()",
    ] {
        assert!(accepted(q), "{q}");
    }
}

#[test]
fn the_quantifiers_and_comprehensions_are_not_function_calls() {
    // `all`/`any`/`none`/`single`, `reduce` and the comprehensions are their
    // own AST nodes and never reach the function check. Asserted because a
    // check that treated them as calls would reject every one.
    for q in [
        "MATCH (a) WHERE all(x IN [1, 2] WHERE x > 0) RETURN a",
        "MATCH (a) WHERE any(x IN [1, 2] WHERE x > 1) RETURN a",
        "MATCH (a) WHERE none(x IN [1] WHERE x > 1) RETURN a",
        "RETURN reduce(acc = 0, x IN [1, 2] | acc + x)",
        "RETURN [x IN [1, 2] | x * 2]",
    ] {
        assert!(accepted(q), "{q}");
    }
}
