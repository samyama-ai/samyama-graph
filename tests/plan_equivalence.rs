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
