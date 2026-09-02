//! Run the reproducer from each open issue and report what the engine does now.
//!
//! Triage, not a test suite: it answers "is this still true?" for issues whose
//! reproducer is a few lines of Cypher. An issue whose reproducer now behaves
//! as openCypher specifies is a candidate to close; one that still misbehaves
//! stays open with its current output recorded.
//!
//! Every case states the **expected** answer taken from the issue itself, so a
//! reader can disagree with the expectation rather than with a bare verdict.
use samyama::graph::GraphStore;
use samyama::query::executor::{MutQueryExecutor, QueryExecutor};
use samyama::query::parser::parse_query;

/// Run statements against a fresh store; return the last result's rows as text.
fn run(setup: &[&str], q: &str) -> Result<(Vec<String>, usize), String> {
    let mut s = GraphStore::new();
    for stmt in setup {
        let p = parse_query(stmt).map_err(|e| format!("setup parse {stmt:?}: {e:?}"))?;
        MutQueryExecutor::new(&mut s, "default".into())
            .execute(&p)
            .map_err(|e| format!("setup exec {stmt:?}: {e:?}"))?;
    }
    let p = parse_query(q).map_err(|e| format!("parse: {e:?}"))?;
    // Any write clause anywhere means the mutating executor. Keying on a
    // `MATCH` prefix sent `MERGE (a) RETURN ...` to the read-only executor,
    // which answered with a WriteError — and three issues were reported as
    // "still reproduces" on that, which is this harness failing, not the
    // engine. A triage tool that misreports a fixed issue as broken is worse
    // than no triage tool.
    let upper = q.to_uppercase();
    let writes = ["CREATE", "MERGE", "DELETE", "SET ", "REMOVE", "DETACH"]
        .iter()
        .any(|k| upper.contains(k));
    let batch = if writes {
        MutQueryExecutor::new(&mut s, "default".into())
            .execute(&p)
            .map_err(|e| format!("exec: {e:?}"))?
    } else {
        QueryExecutor::new(&s).execute(&p).map_err(|e| format!("exec: {e:?}"))?
    };
    let rows: Vec<String> = batch
        .records
        .iter()
        .map(|r| {
            let mut kv: Vec<String> =
                batch.columns.iter().map(|c| format!("{}={:?}", c, r.get(c))).collect();
            if kv.is_empty() {
                kv.push(format!("{r:?}"));
            }
            kv.join(" ")
        })
        .collect();
    let node_count = s.all_nodes().len();
    Ok((rows, node_count))
}

struct Case {
    issue: u32,
    what: &'static str,
    setup: &'static [&'static str],
    query: &'static str,
    /// What openCypher requires, per the issue.
    expect: &'static str,
    /// Given (rows, node_count), is the issue fixed?
    check: fn(&[String], usize) -> bool,
}

fn main() {
    let cases: Vec<Case> = vec![
        Case { issue: 890, what: "unlabelled MERGE never matches",
            setup: &["CREATE (:X)"], query: "MERGE (a) RETURN count(a) AS n",
            expect: "matches the existing node; the graph still has 1 node",
            check: |_r, n| n == 1 },
        Case { issue: 892, what: "WITH * projects nothing in a clause pipeline",
            setup: &[], query: "CREATE (a) WITH * CREATE (b) CREATE (a)<-[:T]-(b) RETURN 1 AS x",
            expect: "2 nodes (a and b), not 3",
            check: |_r, n| n == 2 },
        Case { issue: 893, what: "MERGE re-creates a variable the row already bound",
            setup: &[], query: "CREATE (a) WITH a MERGE (x) MERGE (y) MERGE (x)-[:T]->(y) RETURN 1 AS x",
            expect: "1 node — the pattern names one",
            check: |_r, n| n == 1 },
        Case { issue: 894, what: "MERGE after MATCH does nothing when an endpoint is unbound",
            setup: &["CREATE (:A {tag: 'first'})"],
            query: "MATCH (a:A) MERGE (a)-[:T]->(b:B) RETURN a, b",
            expect: "1 row, and a :B node is created (2 nodes total)",
            check: |r, n| r.len() == 1 && n == 2 },
        Case { issue: 864, what: "aggregate inside reduce's list expression",
            setup: &["CREATE (:N)", "CREATE (:N)"],
            query: "MATCH (n) RETURN reduce(acc = 0, x IN collect(n) | acc + 1) AS c",
            expect: "2 — collect() lifted out and reduced over",
            check: |r, _n| r.first().is_some_and(|s| s.contains('2')) },
        Case { issue: 871, what: "percentileDisc ignores its percentile argument",
            setup: &["CREATE (:V {x: 1}), (:V {x: 2}), (:V {x: 3}), (:V {x: 4}), (:V {x: 5}), \
                      (:V {x: 6}), (:V {x: 7}), (:V {x: 8}), (:V {x: 9}), (:V {x: 10})"],
            query: "MATCH (v:V) RETURN percentileDisc(v.x, 0.9) AS p",
            expect: "9 or 10 for p90 over 1..10 — not 5, the median",
            check: |r, _n| r.first().is_some_and(|s| s.contains('9') || s.contains("10")) },
        Case { issue: 994, what: "DELETE not applied before the next clause reads",
            setup: &["CREATE (:A {num: 1})", "CREATE (:A {num: 2})"],
            query: "MATCH (a:A) DELETE a MERGE (a2:A) RETURN a2.num AS n",
            expect: "two nulls — the MERGE must not match a deleted node",
            check: |r, _n| r.len() == 2 && r.iter().all(|s| s.to_lowercase().contains("null") || s.contains("None")) },
        Case { issue: 1005, what: "duration() alternative ISO 8601 notation",
            setup: &[], query: "RETURN duration('P2012-02-02T14:37:21.545') AS d",
            expect: "P2012Y2M2DT14H37M21.545S, not PT0S",
            check: |r, _n| r.first().is_some_and(|s| !s.contains("PT0S")) },
        Case { issue: 862, what: "the weekDay accessor",
            setup: &[], query: "RETURN date('2026-09-02').weekDay AS w",
            expect: "a weekday number, not null",
            check: |r, _n| r.first().is_some_and(|s| !s.to_lowercase().contains("null") && !s.contains("None")) },
        Case { issue: 891, what: "DELETE of a non-variable expression",
            setup: &["CREATE (:User)"],
            query: "MATCH (u:User) WITH {key: u} AS nodes DELETE nodes.key RETURN 1 AS x",
            expect: "the node is deleted (0 nodes remain)",
            check: |_r, n| n == 0 },
        // Issues that list more than one reproducer get one case each. Closing
        // an issue on its first example while its second still fails is how a
        // fix comes to be believed wider than it is.
        Case { issue: 890, what: "unlabelled MERGE with a property",
            setup: &["CREATE (:X {p: 1})"], query: "MERGE (a {p: 1}) RETURN count(a) AS n",
            expect: "matches the existing node; still 1 node",
            check: |_r, n| n == 1 },
        Case { issue: 891, what: "DETACH DELETE of a path variable",
            setup: &["CREATE (:A)-[:R]->(:B)"],
            query: "MATCH p = (:A)-[:R]->(:B) DETACH DELETE p RETURN 1 AS x",
            expect: "both nodes deleted (0 remain)",
            check: |_r, n| n == 0 },
        Case { issue: 891, what: "DELETE of a list subscript",
            setup: &["CREATE (:U)"],
            query: "MATCH (u) WITH collect(u) AS us DELETE us[0] RETURN 1 AS x",
            expect: "the node is deleted (0 remain)",
            check: |_r, n| n == 0 },
        Case { issue: 815, what: "extreme years wrap silently",
            setup: &[],
            query: "RETURN localdatetime({year: 1, month: 1, day: 1, hour: 1, minute: 1, second: 1}) AS d",
            expect: "0001-01-01T01:01:01, not a wrapped 1754 date",
            check: |r, _n| r.first().is_some_and(|v| !v.contains("1754")) },
        Case { issue: 987, what: "a failing ORDER BY key sorts by nothing",
            setup: &["CREATE (:N {v: 1})", "CREATE (:N {v: 2})"],
            query: "MATCH (n:N) RETURN n.v AS v ORDER BY n.missing.deeper",
            expect: "an error, not a silent unsorted result",
            check: |_r, _n| false },
        Case { issue: 1013, what: "pca not callable from Cypher",
            setup: &["CREATE (:P {e: [1.0, 2.0]})"],
            query: "CALL algo.pca({dimensions: 1}) YIELD nodeId RETURN count(*) AS n",
            expect: "callable — not Unknown procedure",
            check: |_r, _n| true },
        Case { issue: 1022, what: "algo.pca unreachable from Cypher",
            setup: &["CREATE (:P {e: [1.0, 2.0]})"],
            query: "CALL algo.pca({dimensions: 1}) YIELD nodeId RETURN count(*) AS n",
            expect: "callable — not Unknown procedure",
            check: |_r, _n| true },
    ];

    let mut fixed = Vec::new();
    let mut still_open = Vec::new();
    println!("{:<7}{:<52}{}", "issue", "what", "current behaviour");
    println!("{}", "-".repeat(110));
    for c in &cases {
        match run(c.setup, c.query) {
            Ok((rows, nodes)) => {
                let ok = (c.check)(&rows, nodes);
                let shown = if rows.is_empty() {
                    format!("(0 rows, {nodes} nodes)")
                } else {
                    format!("{} [{} rows, {} nodes]",
                        rows.first().cloned().unwrap_or_default(), rows.len(), nodes)
                };
                println!("{:<7}{:<52}{}", format!("#{}", c.issue), c.what, &shown[..shown.len().min(52)]);
                println!("       expected: {}", c.expect);
                println!("       -> {}\n", if ok { "FIXED — candidate to close" } else { "still reproduces" });
                if ok { fixed.push(c.issue) } else { still_open.push(c.issue) }
            }
            Err(e) => {
                println!("{:<7}{:<52}{}", format!("#{}", c.issue), c.what, &e[..e.len().min(52)]);
                println!("       expected: {}\n       -> still reproduces (errored)\n", c.expect);
                still_open.push(c.issue);
            }
        }
    }
    println!("{}", "=".repeat(110));
    println!("candidates to close: {fixed:?}");
    println!("still reproducing:   {still_open:?}");
}
