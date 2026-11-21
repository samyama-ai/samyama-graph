//! Every plan for a query returns the same answer (LANG-06, #1616).
//!
//! `CYPHER plan=N` runs the N-th cheapest candidate the cost-based enumerator
//! produces for each MATCH -- the numbers EXPLAIN prints beside `Alternative
//! plans` -- and `CYPHER planner=cost|legacy` names the planner. Before this
//! no mechanism forced a plan, so the enumerator's alternatives could be read
//! and never run, and plan equivalence could not be tested at all.
//!
//! Here every query of a corpus is run under every plan it has, and each
//! answer is compared with the default planner's as a multiset (a query
//! without ORDER BY may legitimately reorder). The corpus is built to reach
//! what an enumerated plan used to drop: labels on a node reached by an
//! expand, anonymous nodes, inline properties on a later occurrence of a
//! node, two relationships that could be the same one, a parallel edge and a
//! self-loop.

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::{MutQueryExecutor, QueryExecutor, Value};
use samyama::query::parser::parse_query;

fn store() -> GraphStore {
    let mut s = GraphStore::new();
    for q in [
        // CH-PLANEQ's fixture: a chain of 60.
        "UNWIND range(1,60) AS i CREATE (:P {id:i, c: CASE WHEN i%3=0 THEN 'a' ELSE 'b' END})",
        "MATCH (a:P),(b:P) WHERE b.id = a.id + 1 CREATE (a)-[:R]->(b)",
        // People, cities and companies, some people also :Admin.
        "UNWIND range(0,29) AS i CREATE (:Person {pid:i, age: 20 + i % 7})",
        "UNWIND range(0,4) AS i CREATE (:City {cid:i})",
        "UNWIND range(0,3) AS i CREATE (:Company {kid:i})",
        "MATCH (p:Person) WHERE p.pid % 4 = 0 SET p:Admin",
        "MATCH (a:Person),(b:Person) WHERE b.pid = (a.pid * 7 + 3) % 30 AND a <> b CREATE (a)-[:KNOWS]->(b)",
        "MATCH (a:Person),(b:Person) WHERE b.pid = (a.pid + 1) % 30 CREATE (a)-[:KNOWS]->(b)",
        "MATCH (p:Person),(c:City) WHERE c.cid = p.pid % 5 CREATE (p)-[:LIVES_IN]->(c)",
        "MATCH (p:Person),(k:Company) WHERE k.kid = p.pid % 4 CREATE (p)-[:WORKS_AT]->(k)",
        // A parallel KNOWS and a self-loop: what relationship uniqueness is about.
        "MATCH (a:Person {pid: 1}),(b:Person {pid: 2}) CREATE (a)-[:KNOWS]->(b)",
        "MATCH (a:Person {pid: 5}) CREATE (a)-[:KNOWS]->(a)",
        // A company in a city: the label of a node reached by an expand matters.
        "MATCH (k:Company {kid: 0}),(c:City {cid: 0}) CREATE (k)-[:LIVES_IN]->(c)",
        "CREATE INDEX ON :P(id)",
        "CREATE INDEX ON :Person(pid)",
    ] {
        MutQueryExecutor::new(&mut s, "default".into())
            .execute(&parse_query(q).unwrap_or_else(|e| panic!("{q}: {e}")))
            .unwrap_or_else(|e| panic!("{q}: {e}"));
    }
    s
}

const CORPUS: &[&str] = &[
    // CH-PLANEQ's queries.
    "MATCH (p:P) WHERE p.id = 7 RETURN p.id",
    "MATCH (p:P) WHERE p.id > 50 RETURN p.id",
    "MATCH (a:P)-[:R]->(b:P) WHERE a.id = 3 RETURN a.id, b.id",
    "MATCH (p:P) WHERE p.id IN [1,2,3] RETURN p.id",
    "MATCH (p:P) WHERE p.id < 10 AND p.c = 'a' RETURN p.id",
    "MATCH (p:P) WHERE p.id >= 20 RETURN p.c, count(*)",
    "MATCH (a:P)-[:R]->(b:P) WHERE a.id > 55 RETURN a.id, b.id",
    "MATCH (p:P) WHERE NOT p.id = 4 RETURN count(*)",
    // Chains, where the start matters.
    "MATCH (a:P)-[:R]->(b:P)-[:R]->(c:P) WHERE c.id = 10 RETURN a.id, b.id, c.id",
    "MATCH (a:P)-[:R]->(b:P)-[:R]->(c:P)-[:R]->(d:P) WHERE a.id < 5 RETURN a.id, d.id",
    // A label on a node reached by an expand, and on the start.
    "MATCH (p:Person)-[:LIVES_IN]->(c:City) RETURN p.pid, c.cid",
    "MATCH (x)-[:LIVES_IN]->(c:City) RETURN count(*)",
    "MATCH (x:Company)-[:LIVES_IN]->(c) RETURN x.kid, c.cid",
    "MATCH (a:Admin)-[:KNOWS]->(b:Person) RETURN a.pid, b.pid",
    "MATCH (a:Person:Admin)-[:KNOWS]->(b:Admin) RETURN a.pid, b.pid",
    // Anonymous nodes and relationships.
    "MATCH (a:Person)-[:KNOWS]->()-[:KNOWS]->(c:Person) WHERE a.pid = 1 RETURN c.pid",
    "MATCH (:Person {pid: 3})-[:KNOWS]->(b) RETURN b.pid",
    "MATCH (a:Person)-->(c:City) RETURN count(*)",
    // Relationship uniqueness: a parallel edge and a self-loop.
    "MATCH (a:Person)-[:KNOWS]->(b:Person)<-[:KNOWS]-(c:Person) WHERE b.pid = 2 RETURN a.pid, c.pid",
    "MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person) WHERE a.pid = 5 RETURN b.pid, c.pid",
    "MATCH (a:Person)-[r1:KNOWS]->(b:Person), (a)-[r2:KNOWS]->(b) RETURN a.pid, b.pid",
    // A cycle, closed by ExpandInto in some plans.
    "MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person)-[:KNOWS]->(a) RETURN a.pid, b.pid, c.pid",
    "MATCH (a:Person)-[:KNOWS]->(b:Person), (b)-[:KNOWS]->(a) RETURN a.pid, b.pid",
    // Two patterns sharing a node, and an inline property on a later occurrence.
    "MATCH (p:Person)-[:LIVES_IN]->(c:City), (p)-[:WORKS_AT]->(k:Company) RETURN p.pid, c.cid, k.kid",
    "MATCH (p:Person)-[:LIVES_IN]->(c:City), (p {age: 22})-[:WORKS_AT]->(k:Company) RETURN p.pid, k.kid",
    // Several clauses around the MATCH.
    "MATCH (p:Person)-[:LIVES_IN]->(c:City) WITH c, count(p) AS n RETURN c.cid, n",
    "MATCH (p:Person)-[:WORKS_AT]->(k:Company) WHERE p.age > 23 RETURN k.kid, collect(p.pid) AS ps",
    "MATCH (a:Person {pid: 0}) MATCH (a)-[:KNOWS]->(b:Person)-[:LIVES_IN]->(c:City) RETURN b.pid, c.cid",
    "MATCH (c:City) OPTIONAL MATCH (p:Person {age: 26})-[:LIVES_IN]->(c) RETURN c.cid, p.pid",
];

fn run(s: &GraphStore, q: &str) -> Result<Vec<String>, String> {
    let query = parse_query(q).map_err(|e| e.to_string())?;
    let batch = QueryExecutor::new(s).execute(&query).map_err(|e| e.to_string())?;
    let mut rows: Vec<String> = batch
        .records
        .iter()
        .map(|r| {
            batch
                .columns
                .iter()
                .map(|c| match r.get(c) {
                    Some(Value::Property(p)) => format!("{p:?}"),
                    other => panic!("{q}: column {c} is not a scalar: {other:?}"),
                })
                .collect::<Vec<_>>()
                .join("|")
        })
        .collect();
    rows.sort();
    Ok(rows)
}

fn explain(s: &GraphStore, q: &str) -> String {
    let batch = QueryExecutor::new(s).execute(&parse_query(&format!("EXPLAIN {q}")).unwrap()).unwrap();
    match batch.records[0].get("plan") {
        Some(Value::Property(PropertyValue::String(t))) => t.clone(),
        other => panic!("{other:?}"),
    }
}

/// The `K` of EXPLAIN's `Forceable plans: K` line, if it has one.
fn forceable(plan: &str) -> Option<usize> {
    let line = plan.lines().find(|l| l.starts_with("Forceable plans: "))?;
    line["Forceable plans: ".len()..].split_whitespace().next()?.parse().ok()
}

/// The operator tree alone, with the blocks EXPLAIN appends stripped.
fn tree(plan: &str) -> &str {
    plan.split("\n---").next().unwrap()
}

#[test]
fn every_plan_of_every_query_returns_the_default_planners_answer() {
    let s = store();
    let mut queries_with_alternatives = 0;
    let mut plans_run = 0;
    let mut distinct_trees = 0;
    for q in CORPUS {
        let want = run(&s, q).unwrap_or_else(|e| panic!("{q}: {e}"));
        assert_eq!(run(&s, &format!("CYPHER planner=cost {q}")).unwrap(), want, "planner=cost: {q}");
        let mut trees = std::collections::HashSet::new();
        let mut n = 1;
        loop {
            let forced = format!("CYPHER plan={n} {q}");
            match run(&s, &forced) {
                Ok(got) => assert_eq!(got, want, "{forced}"),
                Err(e) => {
                    assert!(n > 1, "{forced}: plan 1 always exists: {e}");
                    assert!(e.contains("cannot honour CYPHER plan="), "{forced}: {e}");
                    break;
                }
            }
            trees.insert(tree(&explain(&s, &forced)).to_string());
            plans_run += 1;
            n += 1;
            assert!(n <= 64, "{q}: more plans than the enumerator keeps");
        }
        // EXPLAIN states the range the loop above found by trial (#2252), so
        // a harness can loop `plan=1..=K` instead of running until refused.
        assert_eq!(forceable(&explain(&s, &format!("CYPHER planner=cost {q}"))), Some(n - 1), "{q}");
        if n > 2 {
            queries_with_alternatives += 1;
        }
        distinct_trees += trees.len();
    }
    // The comparison could not fail if every query had one plan.
    assert!(queries_with_alternatives >= CORPUS.len() / 2, "{queries_with_alternatives} of {}", CORPUS.len());
    assert!(distinct_trees > CORPUS.len(), "{distinct_trees} distinct operator trees over {plans_run} plans");
}

#[test]
fn explain_numbers_the_plans_as_plan_n_takes_them() {
    let s = store();
    let q = "MATCH (a:P)-[:R]->(b:P)-[:R]->(c:P) WHERE c.id = 10 RETURN a.id";
    let cost = explain(&s, &format!("CYPHER planner=cost {q}"));
    assert!(cost.contains("Candidates evaluated: 3"), "{cost}");
    assert!(cost.contains("Chosen plan: #1\n"), "{cost}");
    // Both orders of EXPLAIN and the options.
    for forced in [format!("CYPHER plan=2 EXPLAIN {q}"), format!("EXPLAIN CYPHER plan=2 {q}")] {
        let batch = QueryExecutor::new(&s).execute(&parse_query(&forced).unwrap()).unwrap();
        let Some(Value::Property(PropertyValue::String(text))) = batch.records[0].get("plan") else { panic!() };
        assert!(text.contains("Chosen plan: #2, forced by CYPHER plan=N"), "{text}");
        assert_ne!(tree(text), tree(&cost), "plan 2 is a different tree from plan 1");
    }
    // The default planner prints no candidates; a forced plan leaves none behind.
    run(&s, &format!("CYPHER plan=2 {q}")).unwrap();
    assert!(!explain(&s, q).contains("Candidates evaluated"));
    assert!(!explain(&s, &format!("CYPHER planner=legacy {q}")).contains("Candidates evaluated"));
}

#[test]
fn an_option_that_cannot_be_honoured_is_an_error() {
    let s = store();
    let refused = |q: &str, why: &str| {
        let e = run(&s, q).expect_err(q);
        assert!(e.contains(why), "{q}: {e}");
    };
    let q = "MATCH (a:P)-[:R]->(b:P) RETURN a.id";
    refused(&format!("CYPHER plan=3 {q}"), "this MATCH has 2 candidate plans");
    refused(&format!("CYPHER plan=0 {q}"), "numbered from 1");
    refused(&format!("CYPHER planner=idp {q}"), "planner=cost");
    refused(&format!("CYPHER runtime=slotted {q}"), "one runtime");
    refused(&format!("CYPHER planner=legacy plan=2 {q}"), "planner=legacy");
    refused(&format!("CYPHER plan=1 plan=2 {q}"), "given twice");
    refused(&format!("CYPHER join_order=ab {q}"), "unknown CYPHER option");
    // A pattern the enumerator does not plan has one plan, and says why.
    let varlen = "MATCH (a:P {id: 1})-[:R*1..3]->(b:P) RETURN b.id";
    refused(&format!("CYPHER plan=2 {varlen}"), "variable-length relationship");
    assert_eq!(run(&s, &format!("CYPHER plan=1 {varlen}")).unwrap(), run(&s, varlen).unwrap());
    refused("CYPHER plan=2 MATCH (a:P)-[:R]-(b:P) RETURN a.id", "undirected relationship");
    refused("CYPHER plan=2 MATCH p = (a:P)-[:R]->(b:P) RETURN a.id", "named path");
    // What is accepted: the runtime that exists, and the probe's spellings.
    assert!(run(&s, &format!("CYPHER runtime=volcano {q}")).is_ok());
    assert!(run(&s, "CYPHER planner=cost MATCH (p:P) RETURN p.id").is_ok());
    assert!(run(&s, "cypher PLANNER=Cost MATCH (p:P) RETURN p.id").is_ok());
    // `CYPHER` is a keyword only before options: a label named Cypher is not one.
    assert!(run(&s, "MATCH (c:Cypher) RETURN count(c)").is_ok());
}

/// The plan text EXPLAIN returns for a statement that already says EXPLAIN.
fn explain_text(s: &GraphStore, q: &str) -> String {
    let batch = QueryExecutor::new(s)
        .execute(&parse_query(q).unwrap_or_else(|e| panic!("{q}: {e}")))
        .unwrap_or_else(|e| panic!("{q}: {e}"));
    match batch.records[0].get("plan") {
        Some(Value::Property(PropertyValue::String(t))) => t.clone(),
        other => panic!("{q}: {other:?}"),
    }
}

/// Each of the seven forcing mechanisms CH-PLANEQ probed on 2026-09-26
/// (LANG-06, #2252), by the statement the probe sent. All seven were refused
/// or ignored then. Each is now either honoured, or refused with an error
/// that names what to send instead -- never accepted and ignored.
#[test]
fn the_ch_planeq_forcing_probes() {
    let s = store();

    // using_index: the probe's statement, with the index it names.
    let q = "MATCH (p:P) USING INDEX p:P(id) WHERE p.id = 1 RETURN p.id";
    assert_eq!(run(&s, q).unwrap(), vec!["Integer(1)".to_string()]);
    let plan = explain(&s, q);
    assert!(tree(&plan).contains("IndexScan"), "{plan}");
    assert!(plan.contains("starts its pattern with an IndexScan"), "{plan}");

    // using_scan: a label scan although the index exists.
    let q = "MATCH (p:P) USING SCAN p:P WHERE p.id = 1 RETURN p.id";
    assert_eq!(run(&s, q).unwrap(), vec!["Integer(1)".to_string()]);
    let plan = explain(&s, q);
    assert!(!tree(&plan).contains("IndexScan"), "{plan}");
    assert!(plan.contains("starts its pattern with a label NodeScan"), "{plan}");

    // using_join.
    let q = "MATCH (a:P)-[:R]->(b:P) USING JOIN ON b RETURN count(*)";
    assert_eq!(run(&s, q).unwrap(), vec!["Integer(59)".to_string()]);
    assert!(explain(&s, q).contains("USING JOIN ON b"), "{}", explain(&s, q));

    // cypher_planner_prefix: planned by the enumerator, which says so.
    let q = "CYPHER planner=cost MATCH (p:P) RETURN p.id";
    assert_eq!(run(&s, q).unwrap().len(), 60);
    let plan = explain(&s, q);
    assert!(plan.contains("Candidates evaluated: 1"), "{plan}");
    assert_eq!(forceable(&plan), Some(1), "{plan}");

    // cypher_runtime_prefix: `slotted` is a runtime this engine does not
    // have, refused by name; the one it has is accepted.
    let e = run(&s, "CYPHER runtime=slotted MATCH (p:P) RETURN p.id").unwrap_err();
    assert!(e.contains("one runtime") && e.contains("runtime=volcano"), "{e}");
    assert!(run(&s, "CYPHER runtime=volcano MATCH (p:P) RETURN p.id").is_ok());

    // explain_option_prefix: `EXPLAIN(plan=2)` is `EXPLAIN CYPHER plan=2`.
    // The probe's own pattern has one node and so one plan: plan 2 is
    // refused with the count, not run as plan 1.
    let e = QueryExecutor::new(&s)
        .execute(&parse_query("EXPLAIN(plan=2) MATCH (p:P) RETURN p.id").unwrap())
        .unwrap_err()
        .to_string();
    assert!(e.contains("cannot honour CYPHER plan=2"), "{e}");
    let one = explain_text(&s, "EXPLAIN(plan=1) MATCH (p:P) RETURN p.id");
    assert!(one.contains("Chosen plan: #1\n"), "{one}");
    let chain = "MATCH (a:P)-[:R]->(b:P) RETURN a.id";
    let two = explain_text(&s, &format!("EXPLAIN(plan=2) {chain}"));
    assert!(two.contains("Chosen plan: #2, forced by CYPHER plan=N"), "{two}");
    assert_eq!(tree(&two), tree(&explain(&s, &format!("CYPHER plan=2 {chain}"))));
    assert_eq!(forceable(&two), Some(2), "{two}");
    let opts = parse_query(&format!("PROFILE (planner=cost, plan=2) {chain}")).unwrap();
    assert!(opts.profile);
    assert_eq!(opts.plan_options.plan, Some(2));
    // Checked as the prefix is, and not given twice across the two spellings.
    let e = parse_query(&format!("EXPLAIN (join_order=ab) {chain}")).unwrap_err().to_string();
    assert!(e.contains("unknown CYPHER option"), "{e}");
    let e = parse_query(&format!("CYPHER plan=1 EXPLAIN (plan=2) {chain}")).unwrap_err().to_string();
    assert!(e.contains("given twice"), "{e}");
    let e = parse_query(&format!("CYPHER plan=1 EXPLAIN CYPHER plan=2 {chain}")).unwrap_err().to_string();
    assert!(e.contains("given twice"), "{e}");
    // `(FORMAT JSON)` is still the format, not an option.
    assert!(parse_query(&format!("EXPLAIN (FORMAT JSON) {chain}")).unwrap().plan_json);

    // body_field_plan is an HTTP field: see `src/http/handler_cov_tests.rs`.
}

/// `Forceable plans` counts the query, not its last MATCH: `plan=N` picks the
/// N-th of every MATCH, so a query has as many plans as its MATCH with the
/// fewest (#2252).
#[test]
fn forceable_plans_is_the_fewest_over_the_matches() {
    let s = store();
    // The MATCH planned last has the most candidates, so `Candidates
    // evaluated` alone would overstate the range.
    let q = "MATCH (x:Person)-[:LIVES_IN]->(y:City) \
             MATCH (a:P)-[:R]->(b:P)-[:R]->(c:P) RETURN count(*)";
    let want = run(&s, q).unwrap();
    let plan = explain(&s, &format!("CYPHER planner=cost {q}"));
    let k = forceable(&plan).unwrap_or_else(|| panic!("{plan}"));
    assert!(k >= 2, "{plan}");
    for n in 1..=k {
        assert_eq!(run(&s, &format!("CYPHER plan={n} {q}")).unwrap(), want, "plan={n}");
    }
    let e = run(&s, &format!("CYPHER plan={} {q}", k + 1)).unwrap_err();
    assert!(e.contains(&format!("cannot honour CYPHER plan={}", k + 1)), "{e}");
    // A pattern the enumerator does not plan has the one plan the default
    // planner gives it.
    let plan = explain(&s, "CYPHER planner=cost MATCH (a:P {id: 1})-[:R*1..3]->(b:P) RETURN b.id");
    assert_eq!(forceable(&plan), Some(1), "{plan}");
    // The default planner forces nothing and claims nothing.
    assert_eq!(forceable(&explain(&s, q)), None);
}
