//! Error messages that teach (DX-06, #2270): a code, a span, a cause and a
//! suggested repair.
//!
//! The fault classes are the 17 the benchmarks harness probes (`CH-ERR`,
//! `harness/suites/ch_err.py`), each a broken query a user writes. The four
//! parts are checked the way that suite checks them, so this test and the
//! scorecard cannot disagree about what "carries a repair" means:
//!
//! - **code**: `[Samyama.…]`;
//! - **span**: a `--> line:column` pointer;
//! - **cause**: at least 30 characters of explanation before the span, with
//!   the code removed -- not just a restated fault name;
//! - **repair**: the message tells the reader what to write instead.

use samyama::graph::GraphStore;
use samyama::query::QueryEngine;

/// `(class, broken query)`, as in `CH-ERR`'s `FAULTS`.
const FAULTS: &[(&str, &str)] = &[
    ("syntax_missing_label", "MATCH (n:) RETURN n.name"),
    ("syntax_unclosed_paren", "MATCH (n:P RETURN n.name"),
    ("unknown_function", "MATCH (n:P) RETURN nosuchfn(n.name)"),
    ("unbound_variable", "MATCH (n:P) RETURN m.name"),
    ("unknown_procedure", "CALL db.nosuchProc() YIELD x RETURN x"),
    ("wrong_arity", "MATCH (n:P) RETURN toUpper()"),
    ("type_error_operand", "MATCH (n:P) RETURN n.name + n"),
    (
        "aggregate_in_where",
        "MATCH (n:P) WHERE count(n) > 1 RETURN n.name",
    ),
    (
        "undefined_parameter",
        "MATCH (n:P) WHERE n.name = $who RETURN n.name",
    ),
    (
        "order_by_unprojected_column",
        "MATCH (n:P) RETURN n.name ORDER BY nosuch",
    ),
    (
        "dangling_relationship_pattern",
        "MATCH (a:P)-[:KNOWS]->  RETURN a.name",
    ),
    ("property_on_a_scalar", "MATCH (n:P) RETURN n.name.first"),
    ("divide_by_zero", "RETURN 1/0 AS x"),
    ("list_index_not_an_integer", "RETURN [1,2,3]['a'] AS x"),
    (
        "duplicate_result_column",
        "MATCH (n:P) RETURN n.name AS a, n.age AS a",
    ),
    ("missing_label_after_set", "MATCH (n:P) SET n:"),
    ("index_does_not_exist", "DROP INDEX ON :P(nope)"),
];

/// Classes that do not carry all four yet, and which part they lack. A class
/// that gains it fails the test until it is moved off this list.
///
/// `unknown_function` with no near match: there is no published list of the
/// functions to point the reader to, so the message says only "check its
/// spelling". A near match does get "Did you mean `toUpper()`?".
const KNOWN_GAPS: &[(&str, &str)] = &[("unknown_function", "repair")];

/// The same words `CH-ERR` looks for.
const REPAIR_MARKERS: &[&str] = &[
    "did you mean",
    "try ",
    "use ",
    "instead",
    "expected ",
    "project ",
    "suggestion",
    "rewrite",
    "replace ",
    " in a with",
];

fn fixture() -> GraphStore {
    let mut store = GraphStore::new();
    QueryEngine::new()
        .execute_mut(
            "CREATE (a:P {name:'alice', age:30}), (b:P {name:'bob', age:41}), \
             (a)-[:KNOWS {since:2020}]->(b)",
            &mut store,
            "default",
        )
        .expect("fixture");
    store
}

fn parts(message: &str) -> Vec<(&'static str, bool)> {
    let code_end = message
        .starts_with("[Samyama.")
        .then(|| message.find(']'))
        .flatten();
    let without_code = code_end.map_or(message, |i| &message[i + 1..]);
    let cause = without_code
        .split("-->")
        .next()
        .unwrap_or("")
        .trim_matches(|c: char| c == ' ' || c == '\n' || c == ':');
    let lower = message.to_lowercase();
    vec![
        ("code", code_end.is_some()),
        ("span", message.contains("--> ")),
        ("cause", cause.chars().count() >= 30),
        ("repair", REPAIR_MARKERS.iter().any(|m| lower.contains(m))),
    ]
}

#[test]
fn every_fault_class_carries_a_code_span_cause_and_repair() {
    let engine = QueryEngine::new();
    let mut failures = Vec::new();
    for (class, query) in FAULTS {
        let mut store = fixture();
        let message = match engine.execute_mut(query, &mut store, "default") {
            Ok(_) => {
                failures.push(format!("{class}: `{query}` ran; it should be refused"));
                continue;
            }
            Err(e) => e.to_string(),
        };
        let missing: Vec<&str> = parts(&message)
            .into_iter()
            .filter(|(_, ok)| !ok)
            .map(|(name, _)| name)
            .collect();
        let expected: Vec<&str> = KNOWN_GAPS
            .iter()
            .filter(|(c, _)| c == class)
            .map(|(_, part)| *part)
            .collect();
        if missing != expected {
            failures.push(format!(
                "{class}: missing {missing:?}, expected {expected:?}\n{message}\n"
            ));
        }
    }
    assert!(failures.is_empty(), "\n{}", failures.join("\n"));
}

#[test]
fn a_parse_error_states_its_cause_before_the_diagram() {
    let err = QueryEngine::new()
        .execute("MATCH (n:) RETURN n.name", &fixture())
        .unwrap_err()
        .to_string();
    let first = err.lines().next().unwrap_or("");
    assert!(
        first.ends_with("Parse error: expected label at line 1, column 10."),
        "{err}"
    );
    assert!(err.contains("--> 1:10"), "the diagram still follows: {err}");
    assert!(err.contains("= expected label"), "and its last line: {err}");
}

#[test]
fn a_string_index_from_a_variable_is_named_without_a_span() {
    // The value is not in the query text, so there is nothing to point at;
    // a caret on some other `'alice'` would be a guess.
    let err = QueryEngine::new()
        .execute(
            "MATCH (n:P) WITH n.name AS s RETURN [1, 2][s] AS x",
            &fixture(),
        )
        .unwrap_err()
        .to_string();
    assert!(err.contains("not String (`'alice'`)"), "{err}");
    assert!(!err.contains("-->"), "{err}");
}

#[test]
fn a_parse_error_with_many_alternatives_keeps_the_list_below_the_diagram() {
    // Grammar-rule names ("label_check, index_op, member_op, ...") are not a
    // cause a reader can act on; the first line says where, and pest's full
    // list stays on its last line.
    let err = QueryEngine::new()
        .execute("RETURN [1, 2", &fixture())
        .unwrap_err()
        .to_string();
    let first = err.lines().next().unwrap_or("");
    assert!(
        first.ends_with("Parse error: unexpected input at line 1, column 13; what could come there is listed below."),
        "{err}"
    );
    assert!(!first.contains("index_op"), "{err}");
    assert!(err.contains("= expected"), "the list is still there: {err}");
}
