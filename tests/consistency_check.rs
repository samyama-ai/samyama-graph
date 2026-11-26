//! `eval.checkConsistency`: contradictions within a graph (EVAL-08, #1737).
//!
//! Every class is tested twice: a graph with one planted violation, which
//! must be reported, and the same graph without it, which must report
//! nothing. A check that reported everything would pass the first half
//! alone, and one that reported nothing would pass the second.

use samyama::graph::GraphStore;
use samyama::query::executor::Record;
use samyama::query::QueryEngine;

fn store(create: &str) -> GraphStore {
    let mut store = GraphStore::new();
    QueryEngine::new()
        .execute_mut(create, &mut store, "default")
        .expect("fixture");
    store
}

/// Through the read-only path: the check never writes.
fn rows(store: &GraphStore, query: &str) -> Vec<Record> {
    QueryEngine::new()
        .execute(query, store)
        .unwrap_or_else(|e| panic!("`{query}`: {e}"))
        .records
}

fn refused(store: &GraphStore, query: &str) -> String {
    match QueryEngine::new().execute(query, store) {
        Ok(r) => panic!(
            "`{query}` returned {} rows; it should be refused",
            r.records.len()
        ),
        Err(e) => e.to_string(),
    }
}

fn text(r: &Record, column: &str) -> String {
    match r.get(column) {
        Some(samyama::query::executor::Value::Property(samyama::graph::PropertyValue::String(
            s,
        ))) => s.clone(),
        other => panic!("{column}: {other:?}"),
    }
}

/// `(class, violation)` for each row of the check.
fn found(store: &GraphStore, rules: &str) -> Vec<(String, String)> {
    rows(
        store,
        &format!(
            "CALL eval.checkConsistency({rules}) YIELD class, violation RETURN class, violation"
        ),
    )
    .iter()
    .map(|r| (text(r, "class"), text(r, "violation")))
    .collect()
}

/// The names of the nodes each row involves, in order.
fn names(store: &GraphStore, rules: &str) -> Vec<Vec<String>> {
    rows(
        store,
        &format!(
            "CALL eval.checkConsistency({rules}) YIELD nodes \
             RETURN [n IN nodes | n.name] AS names"
        ),
    )
    .iter()
    .map(|r| match r.get("names") {
        Some(samyama::query::executor::Value::Property(samyama::graph::PropertyValue::Array(
            a,
        ))) => a
            .iter()
            .map(|v| match v {
                samyama::graph::PropertyValue::String(s) => s.clone(),
                other => format!("{other:?}"),
            })
            .collect(),
        other => panic!("names: {other:?}"),
    })
    .collect()
}

fn classes(found: &[(String, String)]) -> Vec<&str> {
    found.iter().map(|(c, _)| c.as_str()).collect()
}

#[test]
fn mutually_exclusive_properties() {
    let rules = "{label: 'P', exclusive: [['alive', 'deathDate']]}";
    let bad = store(
        "CREATE (:P {name: 'ann', alive: true}), \
         (:P {name: 'bob', alive: true, deathDate: date('2001-02-03')}), \
         (:Q {name: 'cat', alive: true, deathDate: date('2001-02-03')})",
    );
    let f = found(&bad, rules);
    assert_eq!(
        classes(&f),
        ["mutually_exclusive_properties"],
        "only bob, and not the :Q: {f:?}"
    );
    assert!(
        f[0].1.contains("`alive`") && f[0].1.contains("`deathDate`"),
        "{f:?}"
    );
    assert_eq!(names(&bad, rules), [["bob"]]);

    let good = store(
        "CREATE (:P {name: 'ann', alive: true}), (:P {name: 'bob', deathDate: date('2001-02-03')})",
    );
    assert!(found(&good, rules).is_empty());
}

#[test]
fn mutually_exclusive_labels() {
    let rules = "{exclusiveLabels: [['Alive', 'Dead']]}";
    let bad = store("CREATE (:Alive {name: 'ann'}), (:Alive:Dead {name: 'bob'})");
    assert_eq!(classes(&found(&bad, rules)), ["mutually_exclusive_labels"]);
    assert_eq!(names(&bad, rules), [["bob"]]);

    let good = store("CREATE (:Alive {name: 'ann'}), (:Dead {name: 'bob'})");
    assert!(found(&good, rules).is_empty());
}

#[test]
fn a_cycle_in_an_acyclic_relationship() {
    let rules = "{acyclic: ['BROADER']}";
    // The fixture CH-EVAL-VERIFY uses: a -> b -> c -> a, plus a tail d -> a
    // that reaches the cycle but is not on it.
    let bad = store(
        "CREATE (a:T {name:'a'}), (b:T {name:'b'}), (c:T {name:'c'}), (d:T {name:'d'}), \
         (a)-[:BROADER]->(b), (b)-[:BROADER]->(c), (c)-[:BROADER]->(a), (d)-[:BROADER]->(a)",
    );
    let f = found(&bad, rules);
    assert_eq!(
        classes(&f),
        ["cycle_in_acyclic_relationship"],
        "one cycle, one row: {f:?}"
    );
    assert!(f[0].1.contains("through 3 nodes"), "{f:?}");
    assert_eq!(
        names(&bad, rules),
        [["a", "b", "c"]],
        "d reaches the cycle but is not on it"
    );
    let rels = rows(
        &bad,
        &format!("CALL eval.checkConsistency({rules}) YIELD relationships UNWIND relationships AS r RETURN type(r) AS t"),
    );
    assert_eq!(rels.len(), 3, "the cycle's three edges");

    let good = store(
        "CREATE (a:T {name:'a'}), (b:T {name:'b'}), (c:T {name:'c'}), \
         (a)-[:BROADER]->(b), (b)-[:BROADER]->(c), (a)-[:BROADER]->(c), (c)-[:RELATED]->(a)",
    );
    assert!(
        found(&good, rules).is_empty(),
        "a diamond and a cycle through another type are fine"
    );
}

#[test]
fn a_self_loop_is_a_cycle() {
    let rules = "{acyclic: ['BROADER']}";
    let bad = store(
        "CREATE (a:T {name:'a'}), (b:T {name:'b'}), (a)-[:BROADER]->(a), (a)-[:BROADER]->(b)",
    );
    let f = found(&bad, rules);
    assert_eq!(classes(&f), ["cycle_in_acyclic_relationship"]);
    assert!(f[0].1.contains("to itself"), "{f:?}");
    assert_eq!(names(&bad, rules), [["a"]]);
}

#[test]
fn label_limits_a_cycle_to_relationships_inside_the_label() {
    // The cycle closes through a :U node, so among :T nodes there is none.
    let g = store(
        "CREATE (a:T {name:'a'}), (b:T {name:'b'}), (u:U {name:'u'}), \
         (a)-[:BROADER]->(b), (b)-[:BROADER]->(u), (u)-[:BROADER]->(a)",
    );
    assert!(found(&g, "{label: 'T', acyclic: ['BROADER']}").is_empty());
    assert_eq!(
        classes(&found(&g, "{acyclic: ['BROADER']}")),
        ["cycle_in_acyclic_relationship"]
    );
}

#[test]
fn a_long_chain_is_checked_without_overflowing_the_stack() {
    // Deep enough that a recursive walk would overflow a test thread's stack.
    // Built through the store: a Cypher fixture for 20,000 nodes would
    // measure the fixture's memory, not the check's.
    const N: usize = 20_000;
    let mut chain = GraphStore::new();
    let ids: Vec<_> = (0..N).map(|_| chain.create_node("T")).collect();
    for w in ids.windows(2) {
        chain.create_edge(w[0], w[1], "BROADER").expect("edge");
    }
    assert!(found(&chain, "{acyclic: ['BROADER']}").is_empty());
    chain
        .create_edge(ids[N - 1], ids[0], "BROADER")
        .expect("close it");
    let f = found(&chain, "{acyclic: ['BROADER']}");
    assert_eq!(classes(&f), ["cycle_in_acyclic_relationship"]);
    assert!(f[0].1.contains("through 20000 nodes"), "{}", &f[0].1[..80]);
    assert!(
        f[0].1.ends_with("and 19990 more"),
        "the sentence names ten: {}",
        f[0].1
    );
    let listed = rows(
        &chain,
        "CALL eval.checkConsistency({acyclic: ['BROADER']}) YIELD nodes RETURN size(nodes) AS n",
    );
    assert_eq!(
        listed[0].get("n"),
        Some(&samyama::query::executor::Value::Property(
            samyama::graph::PropertyValue::Integer(N as i64)
        )),
        "the column names all of them"
    );
}

#[test]
fn a_temporal_impossibility() {
    let rules = "{temporal: [['born', 'died']]}";
    let bad = store(
        "CREATE (:P {name: 'ann', born: date('1950-01-01'), died: date('2001-01-01')}), \
         (:P {name: 'bob', born: date('1990-06-01'), died: date('1980-01-01')}), \
         (:P {name: 'cat', born: 1990, died: 1980}), \
         (:P {name: 'dan', born: date('1990-06-01'), died: 'unknown'}), \
         (:P {name: 'eve', born: date('1990-06-01')})",
    );
    let f = found(&bad, rules);
    assert_eq!(
        classes(&f),
        ["temporal_impossibility", "temporal_impossibility"],
        "bob (dates) and cat (numbers); dan cannot be ordered and eve has no end: {f:?}"
    );
    assert!(
        f[0].1
            .contains("`died` (1980-01-01) before `born` (1990-06-01)"),
        "{f:?}"
    );
    assert_eq!(names(&bad, rules), [["bob"], ["cat"]]);

    let same_day =
        store("CREATE (:P {name: 'ann', born: date('1990-06-01'), died: date('1990-06-01')})");
    assert!(
        found(&same_day, rules).is_empty(),
        "the same day is not before"
    );
}

#[test]
fn a_cardinality_violation() {
    let g = store(
        "CREATE (ann:P {name: 'ann'}), (bob:P {name: 'bob'}), (cat:P {name: 'cat'}), \
         (x:City {name: 'x'}), (y:City {name: 'y'}), \
         (ann)-[:BORN_IN]->(x), (bob)-[:BORN_IN]->(x), (bob)-[:BORN_IN]->(y)",
    );
    let at_most_one = "{label: 'P', cardinality: [{type: 'BORN_IN', max: 1}]}";
    let f = found(&g, at_most_one);
    assert_eq!(classes(&f), ["cardinality_violation"]);
    assert!(
        f[0].1
            .contains("has 2 `-[:BORN_IN]->` relationships; the rule allows at most 1"),
        "{f:?}"
    );
    assert_eq!(names(&g, at_most_one), [["bob"]]);

    let exactly_one = "{label: 'P', cardinality: [{type: 'BORN_IN', min: 1, max: 1}]}";
    assert_eq!(names(&g, exactly_one), [["bob"], ["cat"]], "cat has none");

    // Counted the other way: a city where at most one person was born.
    let incoming = "{label: 'City', cardinality: [{type: 'BORN_IN', direction: 'IN', max: 1}]}";
    assert_eq!(names(&g, incoming), [["x"]]);

    let fine = "{label: 'P', cardinality: [{type: 'BORN_IN', max: 2}]}";
    assert!(found(&g, fine).is_empty());
}

#[test]
fn a_duplicate_value() {
    let rules = "{label: 'P', unique: ['passport']}";
    let bad = store(
        "CREATE (:P {name: 'ann', passport: 'X1'}), (:P {name: 'bob', passport: 'X1'}), \
         (:P {name: 'cat', passport: 'Y2'}), (:P {name: 'dan'}), (:P {name: 'eve'}), \
         (:Q {name: 'fay', passport: 'Y2'})",
    );
    let f = found(&bad, rules);
    assert_eq!(
        classes(&f),
        ["duplicate_value"],
        "missing values are not duplicates, :Q is out of scope"
    );
    assert!(
        f[0].1.starts_with("2 nodes share `passport` = 'X1'"),
        "{f:?}"
    );
    assert_eq!(names(&bad, rules), [["ann", "bob"]]);

    // `=` decides what is the same value: 1 = 1.0.
    let numbers = store(
        "CREATE (:P {name: 'ann', n: 1}), (:P {name: 'bob', n: 1.0}), (:P {name: 'cat', n: 2})",
    );
    assert_eq!(names(&numbers, "{unique: ['n']}"), [["ann", "bob"]]);

    let good =
        store("CREATE (:P {name: 'ann', passport: 'X1'}), (:P {name: 'bob', passport: 'X2'})");
    assert!(found(&good, rules).is_empty());
}

#[test]
fn a_missing_required_property() {
    let rules = "{label: 'P', required: ['name']}";
    let bad = store("CREATE (:P {name: 'ann'}), (:P {age: 3}), (:Q {age: 4})");
    let f = found(&bad, rules);
    assert_eq!(classes(&f), ["missing_required_property"], "{f:?}");
    assert!(f[0].1.ends_with("has no `name`"), "{f:?}");

    let good = store("CREATE (:P {name: 'ann'}), (:P {name: 'bob'})");
    assert!(found(&good, rules).is_empty());
}

#[test]
fn several_rules_report_in_a_fixed_order() {
    let g = store(
        "CREATE (a:T {name: 'a', alive: true, deathDate: 1, born: 5, died: 1}), \
         (b:T {name: 'b'}), (a)-[:BROADER]->(b), (b)-[:BROADER]->(a)",
    );
    let rules = "{required: ['alive'], temporal: [['born', 'died']], acyclic: ['BROADER'], \
                 exclusive: [['alive', 'deathDate']]}";
    let first = found(&g, rules);
    assert_eq!(
        classes(&first),
        [
            "mutually_exclusive_properties",
            "cycle_in_acyclic_relationship",
            "temporal_impossibility",
            "missing_required_property"
        ],
        "the documented order, whatever order the map was written in"
    );
    assert_eq!(found(&g, rules), first, "and the same rows every time");
}

#[test]
fn no_rules_check_nothing() {
    let g = store("CREATE (:T {name: 'a'})");
    assert!(found(&g, "{}").is_empty());
    assert!(rows(&g, "CALL eval.checkConsistency() YIELD class RETURN class").is_empty());
}

#[test]
fn the_benchmark_probe_runs() {
    // CH-EVAL-VERIFY's `mutually_exclusive_properties` shape, verbatim, on its
    // acyclic control.
    let g = store(
        "CREATE (a:T {name:'a'}), (b:T {name:'b'}), (c:T {name:'c'}), \
         (a)-[:BROADER]->(b), (b)-[:BROADER]->(c)",
    );
    let r = rows(
        &g,
        "CALL eval.checkConsistency({label:'T'}) YIELD violation RETURN count(*) AS n",
    );
    assert_eq!(r.len(), 1);
}

#[test]
fn rules_come_from_a_parameter_too() {
    let g = store("CREATE (:P {name: 'ann', born: 2000, died: 1990})");
    let mut params = std::collections::HashMap::new();
    let mut rules = std::collections::HashMap::new();
    rules.insert(
        "temporal".to_string(),
        samyama::graph::PropertyValue::Array(vec![samyama::graph::PropertyValue::Array(vec![
            samyama::graph::PropertyValue::String("born".into()),
            samyama::graph::PropertyValue::String("died".into()),
        ])]),
    );
    params.insert(
        "rules".to_string(),
        samyama::graph::PropertyValue::Map(rules),
    );
    let r = QueryEngine::new()
        .execute_with_params(
            "CALL eval.checkConsistency($rules) YIELD class RETURN class",
            &g,
            &params,
        )
        .expect("runs");
    assert_eq!(r.records.len(), 1);
}

#[test]
fn a_mistyped_rule_is_refused_with_the_rules_named() {
    let g = store("CREATE (:T {name: 'a'})");
    let err = refused(
        &g,
        "CALL eval.checkConsistency({temporl: [['a', 'b']]}) YIELD class RETURN class",
    );
    assert!(
        err.contains("Samyama.ClientError.Statement.ArgumentError"),
        "{err}"
    );
    assert!(
        err.contains("there is no rule `temporl`. Did you mean `temporal`?"),
        "{err}"
    );

    for (rules, says) in [
        ("{temporal: ['born', 'died']}", "`temporal` takes pairs"),
        ("{acyclic: 'BROADER'}", "`acyclic` takes a list"),
        (
            "{cardinality: [{type: 'X'}]}",
            "needs a `min`, a `max` or both",
        ),
        (
            "{cardinality: [{type: 'X', max: 1, dir: 'IN'}]}",
            "`cardinality` has no `dir`",
        ),
        (
            "{cardinality: [{type: 'X', min: 3, max: 1}]}",
            "`min` (3) is above its `max` (1)",
        ),
        (
            "{cardinality: [{type: 'X', max: 1, direction: 'UP'}]}",
            "OUT, IN or BOTH",
        ),
        ("'BROADER'", "takes one map of rules"),
    ] {
        let err = refused(
            &g,
            &format!("CALL eval.checkConsistency({rules}) YIELD class RETURN class"),
        );
        assert!(err.contains(says), "{rules}: {err}");
    }

    let err = refused(
        &g,
        "CALL eval.checkConsistency({}) YIELD verdict RETURN verdict",
    );
    assert!(
        err.contains("yields class, violation, nodes, relationships"),
        "{err}"
    );
}

#[test]
fn it_is_a_read() {
    let engine = QueryEngine::new();
    assert!(!engine
        .statement_is_write(
            "CALL eval.checkConsistency({acyclic: ['BROADER']}) YIELD class RETURN class"
        )
        .unwrap());
}
