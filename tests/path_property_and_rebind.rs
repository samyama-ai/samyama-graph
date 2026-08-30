//! A path has no properties, and `{}` is still a rebinding (#980).
//!
//! ```cypher
//! MATCH r = (n)-[*]->() WHERE r.name = 'apa' RETURN r   -- InvalidArgumentType
//! CREATE (n:Foo) CREATE (n {})-[:OWNS]->(:Dog)          -- VariableAlreadyBound
//! ```
//!
//! Both succeeded with zero rows.
//!
//! `r.name` on a path is not a question a path can answer, and an empty result
//! is indistinguishable from a graph where nothing has that name.
//!
//! `CREATE (n {})` on a bound `n` is a rebinding attempt. The existing rule
//! asked whether the pattern "adds something" and read an **empty** property
//! map as adding nothing, so it accepted the query and created the
//! relationship. Writing braces at all is the tell — a bare re-mention has no
//! property map, which is how `CREATE (a)-[:R]->(b)` stays legal.

use samyama::query::parser::parse_query;

fn refused(cypher: &str) -> bool {
    parse_query(cypher).is_err()
}

fn accepted(cypher: &str) -> bool {
    parse_query(cypher).is_ok()
}

#[test]
fn a_property_read_on_a_path_is_refused() {
    assert!(refused("MATCH (n) MATCH r = (n)-[*]->() WHERE r.name = 'apa' RETURN r"));
    assert!(refused("MATCH p = (a)-->(b) RETURN p.x"));
}

#[test]
fn the_message_names_the_path_and_what_to_use() {
    let err = format!(
        "{:?}",
        parse_query("MATCH p = (a)-->(b) RETURN p.x").unwrap_err()
    );
    assert!(err.contains('p'), "{err}");
    assert!(err.contains("length") || err.contains("nodes"), "{err}");
}

#[test]
fn the_path_functions_are_still_fine() {
    for q in [
        "MATCH p = (a)-->(b) RETURN length(p)",
        "MATCH p = (a)-->(b) RETURN nodes(p)",
        "MATCH p = (a)-->(b) RETURN relationships(p)",
        "MATCH p = (a)-->(b) RETURN p",
    ] {
        assert!(accepted(q), "{q}");
    }
}

#[test]
fn a_property_read_on_a_node_or_relationship_is_unaffected() {
    // The rule keys on the variable binding a *path*, so ordinary reads have
    // to keep working — most of the corpus is these.
    for q in [
        "MATCH (n) RETURN n.x",
        "MATCH ()-[r]->() RETURN r.x",
        "MATCH p = (a)-[r]->(b) RETURN a.x, r.y, b.z",
    ] {
        assert!(accepted(q), "{q}");
    }
}

#[test]
fn an_empty_property_map_on_a_bound_variable_is_refused() {
    assert!(refused("CREATE (n:Foo) CREATE (n {})-[:OWNS]->(:Dog)"));
    assert!(refused("MATCH (n) CREATE (n {})-[:R]->(:X)"));
}

#[test]
fn a_bare_re_mention_stays_legal() {
    // How an edge between existing nodes is written, and the idiom every TCK
    // fixture uses. No property map, so nothing is being rebound.
    assert!(accepted("CREATE (a), (b), (a)-[:R]->(b)"));
    assert!(accepted("MATCH (a), (b) CREATE (a)-[:R]->(b)"));
}

#[test]
fn a_non_empty_property_map_was_already_refused() {
    // The case the old rule did catch, pinned so the widening did not narrow
    // anything by accident.
    assert!(refused("CREATE (n:Foo) CREATE (n {x: 1})-[:R]->(:X)"));
    assert!(refused("CREATE (n:Foo) CREATE (n:Bar)-[:R]->(:X)"));
}
