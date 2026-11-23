//! A clause keyword must be a whole word (#2359): `RETURNtoFloat(a)` used to
//! run as `RETURN toFloat(a)`, and `RETURNa` as `RETURN a`. Pinned both ways:
//! a keyword glued to an identifier is a syntax error, and everything that only
//! looks close -- a keyword before `(` or `*`, a name that starts with a
//! keyword -- still parses and answers as before.

use samyama::graph::GraphStore;
use samyama::query::parser::parse_query;
use samyama::query::QueryEngine;

#[test]
fn a_keyword_glued_to_the_next_word_is_refused() {
    for q in [
        "MATCH (m) WITH count(m) AS a, count(m) AS b RETURNtoFloat(a) / b AS share",
        "MATCH (m) WITH count(m) AS a RETURNa",
        "MATCH (m) RETURNDISTINCT m",
        "MATCH (m) RETURN m ORDERBY m.x",
        "MATCH (m) RETURN m ORDER BYm.x",
        "MATCH (m) RETURN m SKIP1",
        "MATCH (m) RETURN m LIMIT1",
        "MATCH (m) WITHm RETURN m",
        "UNWINDx AS y RETURN y",
        "UNWIND [1] ASy RETURN y",
        "MATCH (m) RETURN m.x ASy",
        "MATCH (m) SETm.x = 1",
        "MATCH (m) DETACHDELETE m",
        "MATCHn RETURN n",
        "OPTIONALMATCH (n) RETURN n",
        "RETURN 1 AS x UNIONALL RETURN 2 AS x",
    ] {
        assert!(parse_query(q).is_err(), "accepted: {q}");
    }
}

#[test]
fn what_only_looks_close_still_parses() {
    let mut s = GraphStore::new();
    let e = QueryEngine::new();
    e.execute_mut("CREATE (:P {x: 1, asset: 'a', returned: true})", &mut s, "default").unwrap();
    for (q, rows) in [
        ("RETURN(1) AS one", 1),
        ("MATCH(n:P) RETURN n.x", 1),
        ("MATCH (n:P) RETURN *", 1),
        ("MATCH (n:P) RETURN DISTINCT n.x", 1),
        ("MATCH (returned:P) RETURN returned.x AS x", 1),
        ("MATCH (n:P) WITH n.asset AS asset RETURN asset", 1),
        ("MATCH (n:P) RETURN n.x AS as_of, n.returned AS setting, n.asset AS skipped", 1),
        ("UNWIND [1, 2] AS created RETURN created ORDER BY created DESC SKIP 0 LIMIT 1", 1),
        ("MATCH (n:P) RETURN n.x ORDER BY n.x ASC", 1),
        ("RETURN 1 AS x UNION ALL RETURN 2 AS x", 2),
        ("WITH 1 AS withdrawn, 2 AS callback RETURN withdrawn + callback AS s", 1),
    ] {
        let b = e.execute(q, &s).unwrap_or_else(|err| panic!("refused: {q}\n{err}"));
        assert_eq!(b.records.len(), rows, "{q}");
    }
}
