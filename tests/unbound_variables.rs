//! A name used in an expression must be bound by something in the query (#895).
//!
//! ```cypher
//! MATCH () RETURN foo                          -- returned a null column
//! MATCH (s) WHERE s.name = undefinedVariable   -- returned nothing
//! MERGE (n) ON CREATE SET x.num = 1            -- set a property on nothing
//! ```
//!
//! Every one ran and reported success. A typo in a variable name is the most
//! ordinary mistake there is, and the engine answered it with silence — the
//! result indistinguishable from a query that legitimately matched nothing.
//!
//! The check is deliberately coarse: *does anything in this query bind this
//! name*, not *is it in scope here*. `validate.rs` opens by naming
//! over-rejection as the worse failure, and a false `SyntaxError` breaks a
//! working query while a missed one only leaves today's behaviour. Being
//! coarse is not free — the first attempt, borrowing the collector that
//! answers "what may a later CREATE not redeclare", rejected **126** valid
//! scenarios because that collector deliberately omits path variables.
//! Most of this file is the set of things that must keep parsing.

use samyama::query::parser::parse_query;

fn refused(cypher: &str) -> bool {
    parse_query(cypher).is_err()
}

/// The six the TCK names.
#[test]
fn an_unbound_name_is_refused() {
    assert!(refused("MATCH () RETURN foo"), "RETURN");
    assert!(
        refused("MATCH (s) WHERE s.name = undefinedVariable AND s.age = 10 RETURN s"),
        "WHERE"
    );
    assert!(
        refused("MATCH (a) CREATE (a)-[:KNOWS]->(b {name: missing}) RETURN b"),
        "an inline property map on a created node"
    );
    assert!(refused("MERGE (n) ON CREATE SET x.num = 1"), "ON CREATE");
    assert!(refused("MERGE (n) ON MATCH SET x.num = 1"), "ON MATCH");
    assert!(refused("MATCH (a) SET a.name = missing RETURN a"), "SET");
}

/// Everything that binds, binds. Each of these was a false rejection at some
/// point while the collector was being written.
#[test]
fn every_way_of_binding_a_name_counts() {
    for cypher in [
        // A named path — the omission that cost 126 scenarios.
        "MATCH p = (a)-->(b) RETURN p",
        "MATCH p = (n)-->(b) RETURN nodes(p)",
        "MATCH p = (a)-[*]->(b) RETURN length(p)",
        // More than one UNWIND, before and after a WITH.
        "UNWIND [true, false] AS a UNWIND [true, false] AS b RETURN a, b, (a AND b) AS r",
        "UNWIND range(1, 2) AS row WITH collect(row) AS rows UNWIND rows AS x RETURN x",
        "WITH [1, 2, 3] AS list UNWIND list AS x RETURN *",
        "WITH [1, 2] AS xs, [3, 4] AS ys UNWIND xs AS x UNWIND ys AS y RETURN *",
        // Comprehension and quantifier binders.
        "MATCH (n) RETURN [x IN [1, 2] | x + 1] AS l",
        "MATCH (n) WHERE any(x IN [1, 2] WHERE x > 1) RETURN n",
        "RETURN reduce(acc = 0, x IN [1, 2] | acc + x) AS total",
        "MATCH (n) RETURN [(n)-->(m) | m] AS ms",
        // Aliases, in both directions.
        "MATCH (n) WITH n AS m RETURN m",
        "MATCH (n) RETURN n.name AS name ORDER BY name",
        // Writes.
        "CREATE (a) WITH a CREATE (b) CREATE (a)<-[:T]-(b)",
        "MATCH (n) DETACH DELETE n",
        "MATCH (n) REMOVE n.prop",
        "MATCH (n) REMOVE n:Label",
        "MATCH (n) SET n:Label",
        "MATCH (n) SET n = {a: 1}",
        "FOREACH (i IN [1, 2] | CREATE (:N {v: i}))",
        // A CALL subquery binds inside itself and exports its columns.
        "CALL { RETURN 1 AS n } RETURN n",
        "CALL { MATCH (p:P) RETURN p.n AS n } RETURN n",
        // A parameter is not a variable.
        "MATCH (n) WHERE n.id = $id RETURN n",
        // A MATCH written *after* a WITH binds too. Both of these are ordinary
        // two-stage aggregations, neither covered by the TCK; the engine's own
        // suite is what caught them.
        "MATCH (m:M) WITH m LIMIT 5 MATCH (a:A)-[:R]->(m) WITH m, count(a) AS cnt RETURN m.name, cnt",
        "MATCH (r:R) WHERE r.term CONTAINS 'X' WITH r MATCH (c:C)-[:E]->(r) WITH r, count(c) AS cases RETURN r.term, cases",
    ] {
        assert!(!refused(cypher), "wrongly refused `{cypher}`");
    }
}

/// `RETURN *` is resolved before this check sees it, in **both** AST shapes.
///
/// The parser mirrors a pipeline's RETURN into the by-kind field before star
/// expansion runs, so expanding one left the other holding a variable
/// literally named `*` — and the first thing to read it reported `*` as an
/// undefined name.
#[test]
fn a_star_is_never_seen_as_a_name() {
    for cypher in [
        "MATCH (n) RETURN *",
        "MATCH (n) WITH * RETURN *",
        "WITH [1, 2, 3] AS list UNWIND list AS x RETURN *",
        "CREATE (a) WITH * CREATE (b) RETURN *",
    ] {
        assert!(!refused(cypher), "wrongly refused `{cypher}`");
    }
}
