//! A malformed `\u` escape is a compile-time error (#989).
//!
//! ```cypher
//! RETURN '\uH'
//! ```
//!
//! returned the string `"uH"`. openCypher asks for `InvalidUnicodeLiteral` at
//! compile time -- a query that should not compile was instead answering with
//! a plausible value nobody wrote.
//!
//! The unknown-escape fallback (`\q` -> `q`) is deliberate and stays: it keeps
//! Windows paths and regex fragments from becoming parse errors. But `\u` is
//! not unknown. openCypher *defines* it as exactly four hex digits, so a
//! malformed `\u` is a broken known escape rather than an unrecognised one,
//! and the two want opposite treatment.

use samyama::graph::GraphStore;
use samyama::query::executor::QueryExecutor;
use samyama::query::parser::parse_query;

fn value(cypher: &str) -> Result<String, String> {
    let store = GraphStore::new();
    let q = parse_query(cypher).map_err(|e| e.to_string())?;
    let r = QueryExecutor::new(&store).execute(&q).map_err(|e| format!("{e:?}"))?;
    let c = r.columns[0].clone();
    Ok(format!("{:?}", r.records[0].get(&c)))
}

#[test]
fn a_non_hex_unicode_escape_is_rejected() {
    let e = value(r#"RETURN '\uH'"#).unwrap_err();
    assert!(e.contains("InvalidUnicodeLiteral"), "got {e}");
}

#[test]
fn a_short_unicode_escape_is_rejected() {
    // `take(4)` yields whatever is left at the end of the input, so the length
    // has to be checked rather than assumed -- `\u00` would otherwise decode
    // as U+0000.
    let e = value(r#"RETURN '\u00'"#).unwrap_err();
    assert!(e.contains("InvalidUnicodeLiteral"), "got {e}");
}

#[test]
fn a_well_formed_unicode_escape_still_decodes() {
    assert!(value(r#"RETURN '\u0041' AS a"#).unwrap().contains("\"A\""));
}

#[test]
fn an_unknown_escape_is_still_tolerated() {
    // The fallback this rule must not swallow.
    assert!(value(r#"RETURN '\q' AS a"#).unwrap().contains("\"q\""));
}

#[test]
fn a_backslash_u_that_is_not_an_escape_needs_escaping() {
    // Deliberate behaviour change, and the one with real blast radius:
    // `'C:\users'` used to yield `C:users` and is now an error, because `\u`
    // followed by `sers` is a malformed unicode escape. That is what
    // openCypher specifies, and what a reference implementation does -- the
    // path has to be written `'C:\\users'`.
    assert!(value(r#"RETURN 'C:\users' AS p"#).is_err());
    assert!(value(r#"RETURN 'C:\\users' AS p"#).unwrap().contains("C:"));
}

#[test]
fn the_escaped_characters_scenario_round_trips() {
    // Literals6[5]. Every pair here is `\\` or `\'`, so no single `\u` arises.
    let got = value(r#"RETURN 'a\\bcn5t\'"\\//\\"\'' AS literal"#).unwrap();
    assert!(got.contains(r#"a\\bcn5t'\"\\//\\\"'"#), "got {got}");
}
