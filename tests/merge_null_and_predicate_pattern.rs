//! Two write/predicate rules the engine was missing (#973).
//!
//! ```cypher
//! MERGE ({num: null})            -- SemanticError: MergeReadOwnWrites
//! MATCH (n) WHERE (n) RETURN n   -- SyntaxError: InvalidArgumentType
//! ```
//!
//! Both used to succeed with zero rows.
//!
//! Cypher stores no null, so `{num: null}` matches nothing *and* the node the
//! MERGE creates does not carry the property either — so the next run fails to
//! find it and creates another. A MERGE that cannot read its own writes.
//!
//! `WHERE (n)` asks whether a node that is already bound exists, which is
//! always true. We answered **no rows** — the opposite — which is its own tell
//! that nothing was evaluating it sensibly.
//!
//! The second rule is deliberately narrow. Applied to every nested variable
//! reference it rejects `WHERE n = m`, `WHERE n:Label` and `WHERE a.x > 1`,
//! because the left side of a comparison is a bare variable too: **23
//! scenarios** broke on the first attempt. It applies only where a boolean is
//! expected — the predicate itself and the operands of AND/OR/XOR/NOT.

use samyama::query::parser::parse_query;

fn refused(cypher: &str) -> bool {
    parse_query(cypher).is_err()
}

fn accepted(cypher: &str) -> bool {
    parse_query(cypher).is_ok()
}

#[test]
fn a_merge_with_a_null_property_is_refused() {
    assert!(refused("MERGE ({num: null})"));
    assert!(refused("MERGE (n:N {a: 1, b: null})"));
    assert!(refused("MERGE (a)-[:R {k: null}]->(b)"));
}

#[test]
fn the_merge_message_names_the_property() {
    let err = format!("{:?}", parse_query("MERGE ({num: null})").unwrap_err());
    assert!(err.contains("num"), "{err}");
}

#[test]
fn an_ordinary_merge_is_unaffected() {
    assert!(accepted("MERGE ({num: 1})"));
    assert!(accepted("MERGE (n:N)"));
    assert!(accepted("MERGE (a:A)-[:R {k: 1}]->(b:B)"));
    // CREATE may set a null; it simply does not store the property.
    assert!(accepted("CREATE ({num: null})"));
}

#[test]
fn a_bare_node_pattern_as_a_predicate_is_refused() {
    assert!(refused("MATCH (n) WHERE (n) RETURN n"));
}

#[test]
fn the_pattern_shorthand_with_a_relationship_is_fine() {
    assert!(accepted("MATCH (n) WHERE (n)-->() RETURN n"));
    assert!(accepted("MATCH (n) WHERE NOT (n)-->() RETURN n"));
    assert!(accepted("MATCH (n) WHERE (n)-[:R]->({k: 1}) RETURN n"));
}

#[test]
fn comparisons_and_label_tests_on_entities_stay_legal() {
    // The 23 scenarios the first attempt broke. Every one of these has a bare
    // entity variable somewhere inside the predicate.
    for q in [
        "MATCH (n), (m) WHERE n = m RETURN n",
        "MATCH (n), (m) WHERE n <> m RETURN n",
        "MATCH (n) WHERE n:Label RETURN n",
        "MATCH (n) WHERE n.x > 1 RETURN n",
        "MATCH (n) WHERE id(n) = 1 RETURN n",
        "MATCH ()-[r]->() WHERE type(r) = 'T' RETURN r",
        "MATCH (n), (m) WHERE n = m AND n.x = 1 RETURN n",
        "MATCH (n) WHERE NOT n.x = 1 RETURN n",
        "MATCH (n) WHERE n IS NULL RETURN n",
    ] {
        assert!(accepted(q), "{q}");
    }
}

#[test]
fn a_boolean_variable_is_still_a_predicate() {
    // The rule keys on the variable binding an *entity*, not on it being a
    // variable, so an ordinary boolean still filters.
    assert!(accepted("UNWIND [true, false] AS flag WITH flag WHERE flag RETURN flag"));
}

#[test]
fn an_entity_inside_a_connective_is_still_refused() {
    // The operands of AND/OR are themselves predicates, so the rule reaches
    // them — which is the whole reason it recurses at all.
    assert!(refused("MATCH (n) WHERE (n) AND n.x = 1 RETURN n"));
    assert!(refused("MATCH (n) WHERE NOT (n) RETURN n"));
}
