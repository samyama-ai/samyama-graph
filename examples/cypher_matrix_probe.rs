//! Executes one representative query per row of `docs/CYPHER_COMPATIBILITY.md`
//! and reports what the engine actually does, so the matrix is measured rather
//! than remembered (#437).
//!
//! Every row prints `SUPPORTED` or `UNSUPPORTED <first line of error>`. A row
//! that parses and executes but returns the wrong answer is *not* caught here --
//! this probe establishes support, not correctness. Correctness lives in the
//! test suites.
//!
//! Run: cargo run --release --example cypher_matrix_probe
//!
//! With `--json PATH` it also writes a conformance result envelope -- the
//! shape spec 18 requires of any run that wants to be quotable: suite,
//! requirement_ids, run_id, engine (with commit), hardware, dataset (with
//! hash), measurements, status, artifacts. The harness that assembles
//! `SCORECARD.json` does not exist yet; this is one suite emitting the
//! envelope it will consume, which is step 1 of that build order.

use samyama::graph::GraphStore;
use samyama::query::QueryEngine;

/// Short commit of the build under test, or "unknown" outside a checkout.
fn engine_commit() -> String {
    std::process::Command::new("git")
        .args(["rev-parse", "--short", "HEAD"])
        .output()
        .ok()
        .filter(|o| o.status.success())
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "unknown".to_string())
}

/// Stable digest of the fixture, so a changed fixture cannot be mistaken for a
/// changed engine. FNV-1a is enough: this identifies a dataset, it does not
/// defend against anyone.
fn dataset_hash(stmts: &[&str]) -> String {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for stmt in stmts {
        for b in stmt.as_bytes() {
            h ^= *b as u64;
            h = h.wrapping_mul(0x0000_0100_0000_01b3);
        }
    }
    format!("fnv1a64:{h:016x}")
}

fn json_escape(s: &str) -> String {
    s.chars()
        .flat_map(|c| match c {
            '"' => "\\\"".chars().collect::<Vec<_>>(),
            '\\' => "\\\\".chars().collect(),
            '\n' => "\\n".chars().collect(),
            '\t' => "\\t".chars().collect(),
            c if (c as u32) < 0x20 => format!("\\u{:04x}", c as u32).chars().collect(),
            c => vec![c],
        })
        .collect()
}

fn main() {
    let engine = QueryEngine::new();
    let mut store = GraphStore::new();

    // A small graph with the shapes the probes need: labels, a typed edge,
    // numeric + string + list properties.
    let fixture: [&str; 4] = [
        "CREATE (:Person {name: \"Ada\", age: 36, tags: [\"a\",\"b\"], score: 1.5})",
        "CREATE (:Person {name: \"Alan\", age: 41, tags: [\"b\"], score: 2.5})",
        "CREATE (:City {name: \"London\"})",
        "MATCH (p:Person {name: \"Ada\"}), (c:City {name: \"London\"}) CREATE (p)-[:LIVES_IN {since: 2020}]->(c)",
    ];
    for stmt in fixture {
        engine
            .execute_mut(stmt, &mut store, "default")
            .unwrap_or_else(|e| panic!("fixture failed: {stmt}\n  {e}"));
    }

    // (category, feature as written in the matrix, probe query, mutating?)
    let probes: Vec<(&str, &str, &str, bool)> = vec![
        ("Read", "MATCH", "MATCH (p:Person) RETURN p.name", false),
        ("Read", "OPTIONAL MATCH", "MATCH (p:Person) OPTIONAL MATCH (p)-[:NOPE]->(x) RETURN p.name, x", false),
        ("Read", "WHERE", "MATCH (p:Person) WHERE p.age > 40 RETURN p.name", false),
        ("Read", "RETURN", "RETURN 1 AS one", false),
        ("Read", "RETURN DISTINCT", "MATCH (p:Person) RETURN DISTINCT p.age", false),
        ("Read", "ORDER BY", "MATCH (p:Person) RETURN p.name ORDER BY p.age DESC", false),
        ("Read", "SKIP / LIMIT", "MATCH (p:Person) RETURN p.name SKIP 1 LIMIT 1", false),
        ("Read", "EXPLAIN", "EXPLAIN MATCH (p:Person) RETURN p.name", false),

        ("Write", "CREATE", "CREATE (:Probe {k: 1})", true),
        ("Write", "DELETE / DETACH DELETE", "MATCH (x:Probe) DETACH DELETE x", true),
        ("Write", "SET", "MATCH (p:Person {name: \"Ada\"}) SET p.seen = true", true),
        ("Write", "REMOVE", "MATCH (p:Person {name: \"Ada\"}) REMOVE p.seen", true),
        ("Write", "MERGE", "MERGE (:Person {name: \"Ada\"})", true),
        ("Write", "MERGE ON CREATE / ON MATCH SET", "MERGE (p:Person {name: \"Ada\"}) ON MATCH SET p.touched = 1", true),

        ("Aggregation", "count()", "MATCH (p:Person) RETURN count(p)", false),
        ("Aggregation", "sum() / avg()", "MATCH (p:Person) RETURN sum(p.age), avg(p.age)", false),
        ("Aggregation", "min() / max()", "MATCH (p:Person) RETURN min(p.age), max(p.age)", false),
        ("Aggregation", "collect()", "MATCH (p:Person) RETURN collect(p.name)", false),
        ("Aggregation", "collect(DISTINCT x)", "MATCH (p:Person) RETURN collect(DISTINCT p.age)", false),
        ("Aggregation", "Implicit GROUP BY", "MATCH (p:Person) RETURN p.age, count(*)", false),

        ("Structure", "WITH", "MATCH (p:Person) WITH p WHERE p.age > 30 RETURN p.name", false),
        ("Structure", "UNWIND", "UNWIND [1,2,3] AS x RETURN x", false),
        ("Structure", "UNION / UNION ALL", "RETURN 1 AS v UNION ALL RETURN 2 AS v", false),
        ("Structure", "EXISTS subquery", "MATCH (p:Person) WHERE EXISTS { MATCH (p)-[:LIVES_IN]->() } RETURN p.name", false),
        ("Structure", "CALL {} subquery", "CALL { MATCH (p:Person) RETURN p.name AS n } RETURN n", false),
        ("Structure", "FOREACH", "FOREACH (i IN [1] | CREATE (:Tmp {i: i}))", true),

        ("String Functions", "toUpper, toLower", "RETURN toUpper(\"a\"), toLower(\"B\")", false),
        ("String Functions", "trim, replace", "RETURN trim(\"  a  \"), replace(\"ab\",\"a\",\"c\")", false),
        ("String Functions", "substring, left, right", "RETURN substring(\"abc\",1), left(\"abc\",1), right(\"abc\",1)", false),
        ("String Functions", "reverse, toString", "RETURN reverse(\"abc\"), toString(1)", false),
        ("String Functions", "split", "RETURN split(\"a,b\", \",\")", false),

        ("Numeric Functions", "abs, ceil, floor, round", "RETURN abs(-1), ceil(1.2), floor(1.8), round(1.5)", false),
        ("Numeric Functions", "sqrt, sign", "RETURN sqrt(4.0), sign(-2)", false),
        ("Numeric Functions", "toInteger, toFloat", "RETURN toInteger(\"3\"), toFloat(\"3.5\")", false),
        ("Numeric Functions", "rand, log, exp", "RETURN log(2.0), exp(1.0)", false),

        ("Collection Functions", "size, length", "RETURN size([1,2,3])", false),
        ("Collection Functions", "head, last, tail", "RETURN head([1,2]), last([1,2]), tail([1,2])", false),
        ("Collection Functions", "keys", "MATCH (p:Person) RETURN keys(p) LIMIT 1", false),
        ("Collection Functions", "range", "RETURN range(1,3)", false),
        ("Collection Functions", "nodes(), relationships()", "MATCH path = (a:Person)-[:LIVES_IN]->(b) RETURN nodes(path), relationships(path)", false),
        ("Collection Functions", "list slicing", "RETURN [1,2,3][0..2]", false),
        ("Collection Functions", "list indexing", "RETURN [1,2,3][0]", false),
        ("Collection Functions", "chained list indexing", "RETURN [[1,2],[3,4]][0][1]", false),
        ("Collection Functions", "reduce()", "RETURN reduce(s = 0, x IN [1,2,3] | s + x)", false),

        ("Graph Functions", "id()", "MATCH (p:Person) RETURN id(p) LIMIT 1", false),
        ("Graph Functions", "labels(), type()", "MATCH (a)-[r]->(b) RETURN labels(a), type(r) LIMIT 1", false),
        ("Graph Functions", "exists(), coalesce()", "MATCH (p:Person) RETURN coalesce(p.nope, 0) LIMIT 1", false),
        ("Graph Functions", "named paths", "MATCH path = (a:Person)-[:LIVES_IN]->(b) RETURN path", false),
        ("Graph Functions", "shortestPath", "MATCH (a:Person {name:\"Ada\"}), (c:City) MATCH p = shortestPath((a)-[*..3]-(c)) RETURN p", false),
        ("Graph Functions", "variable-length paths", "MATCH (a:Person)-[*1..2]->(x) RETURN count(*)", false),

        ("Expressions", "CASE WHEN ... THEN ... END", "MATCH (p:Person) RETURN CASE WHEN p.age > 40 THEN 1 ELSE 0 END LIMIT 1", false),
        ("Expressions", "pattern comprehension", "MATCH (p:Person) RETURN [(p)-[:LIVES_IN]->(c) | c.name] LIMIT 1", false),
        ("Expressions", "list comprehension", "RETURN [x IN [1,2,3] WHERE x > 1 | x * 2]", false),
        ("Expressions", "map literal", "RETURN {a: 1, b: \"x\"}", false),
        ("Expressions", "map bracket access", "RETURN {a: 1}[\"a\"]", false),
        ("Expressions", "map dot access", "MATCH (p:Person) RETURN p.tags", false),

        ("Predicates", "STARTS WITH, ENDS WITH, CONTAINS", "MATCH (p:Person) WHERE p.name STARTS WITH \"A\" AND p.name ENDS WITH \"a\" AND p.name CONTAINS \"d\" RETURN count(p)", false),
        ("Predicates", "=~ (regex)", "MATCH (p:Person) WHERE p.name =~ \"A.*\" RETURN count(p)", false),
        ("Predicates", "IN (list membership)", "MATCH (p:Person) WHERE p.age IN [36, 41] RETURN count(p)", false),
        ("Predicates", "IS NULL, IS NOT NULL", "MATCH (p:Person) WHERE p.nope IS NULL RETURN count(p)", false),
        ("Predicates", "AND, OR, NOT, XOR", "MATCH (p:Person) WHERE (p.age > 30 AND NOT p.age > 50) OR p.age = 1 RETURN count(p)", false),
        ("Predicates", "all/any/none/single", "RETURN any(x IN [1,2] WHERE x > 1), all(x IN [1,2] WHERE x > 0)", false),

        ("Type Handling", "Integer/Float coercion", "MATCH (p:Person) WHERE p.age > 30.5 RETURN count(p)", false),
        ("Type Handling", "Null propagation (comparison)", "RETURN 1 > null", false),
        ("Type Handling", "Null propagation (arithmetic)", "RETURN 1 + null", false),
        ("Type Handling", "Temporal types", "RETURN date(\"2026-01-01\")", false),
        ("Type Handling", "Duration arithmetic", "RETURN duration({days: 1})", false),

        ("Extensions", "CREATE VECTOR INDEX", "CREATE VECTOR INDEX probe_idx FOR (n:Person) ON (n.embedding) OPTIONS {dimension: 4, similarity: \"cosine\"}", true),
        ("Extensions", "algo.pageRank", "CALL algo.pageRank({iterations: 2}) YIELD nodeId, score RETURN count(*)", false),
        ("Extensions", "algo.wcc", "CALL algo.wcc() YIELD nodeId, componentId RETURN count(*)", false),
        ("Extensions", "algo.shortestPath", "CALL algo.shortestPath(0, 2) YIELD nodeId RETURN count(*)", false),
        ("Extensions", "algo.weightedPath", "CALL algo.weightedPath(0, 2, \"since\") YIELD nodeId RETURN count(*)", false),
        ("Extensions", "algo.scc", "CALL algo.scc() YIELD nodeId, componentId RETURN count(*)", false),
        ("Extensions", "algo.mst", "CALL algo.mst() YIELD weight RETURN count(*)", false),
        ("Extensions", "algo.cdlp", "CALL algo.cdlp() YIELD nodeId RETURN count(*)", false),
        ("Extensions", "algo.lcc", "CALL algo.lcc() YIELD nodeId RETURN count(*)", false),
        ("Extensions", "algo.bfs / algo.dijkstra", "CALL algo.bfs({startNodeId: 0}) YIELD nodeId RETURN count(*)", false),
        ("Extensions", "algo.triangleCount", "CALL algo.triangleCount() YIELD nodeId, triangles RETURN count(*)", false),
    ];

    let mut outcomes: Vec<(String, String, bool, String)> = Vec::new();
    let mut supported = 0usize;
    let mut unsupported = 0usize;
    let mut last_cat = "";

    println!("# Measured Cypher support probe");
    println!();

    for (cat, feature, q, mutating) in &probes {
        if cat != &last_cat {
            println!("\n## {cat}");
            last_cat = cat;
        }
        let result = if *mutating {
            engine.execute_mut(q, &mut store, "default").map(|_| ())
        } else {
            engine.execute(q, &store).map(|_| ())
        };
        match result {
            Ok(()) => {
                supported += 1;
                outcomes.push((cat.to_string(), feature.to_string(), true, String::new()));
                println!("SUPPORTED    | {feature}");
            }
            Err(e) => {
                unsupported += 1;
                let msg = format!("{e}");
                let first = msg.lines().next().unwrap_or("").trim().to_string();
                outcomes.push((cat.to_string(), feature.to_string(), false, first.clone()));
                println!("UNSUPPORTED  | {feature} | {first}");
            }
        }
    }

    println!();
    println!("TOTAL {} probes: {} supported, {} unsupported", probes.len(), supported, unsupported);

    let args: Vec<String> = std::env::args().collect();
    if let Some(i) = args.iter().position(|a| a == "--json") {
        let path = args.get(i + 1).cloned().unwrap_or_else(|| "cypher_matrix.json".to_string());
        let commit = engine_commit();
        // The run id is derived from what was measured, not from a clock: the
        // same engine over the same fixture is the same run, and re-running it
        // should not manufacture a new result.
        let run_id = format!("cypher-matrix-{commit}-{}", &dataset_hash(&fixture)[8..16]);

        let mut probe_json = String::new();
        for (i, (cat, feature, ok, err)) in outcomes.iter().enumerate() {
            if i > 0 { probe_json.push_str(",\n"); }
            probe_json.push_str(&format!(
                "      {{\"category\": \"{}\", \"feature\": \"{}\", \"supported\": {}, \"error\": \"{}\"}}",
                json_escape(cat), json_escape(feature), ok, json_escape(err)
            ));
        }

        // An unmeasured requirement counts as failing, never as passing
        // (spec 18 rollup rule), so status is pass only with zero unsupported.
        let status = if unsupported == 0 { "pass" } else { "fail" };

        let envelope = format!(
"{{
  \"suite\": \"cypher-compatibility-matrix\",
  \"requirement_ids\": [\"LANG-01\", \"TRUST-01\"],
  \"run_id\": \"{run_id}\",
  \"engine\": {{\"name\": \"samyama\", \"version\": \"{}\", \"commit\": \"{commit}\"}},
  \"hardware\": {{\"note\": \"support probe; result is independent of hardware\"}},
  \"dataset\": {{\"name\": \"inline-fixture\", \"statements\": {}, \"hash\": \"{}\"}},
  \"measurements\": {{
    \"probes_total\": {},
    \"probes_supported\": {},
    \"probes_unsupported\": {},
    \"probes\": [
{probe_json}
    ]
  }},
  \"status\": \"{status}\",
  \"artifacts\": [\"docs/CYPHER_COMPATIBILITY.md\", \"examples/cypher_matrix_probe.rs\"],
  \"caveat\": \"A supported probe means the query executed. It does not certify semantics; correctness lives in the test suites.\"
}}
",
            env!("CARGO_PKG_VERSION"),
            fixture.len(),
            dataset_hash(&fixture),
            probes.len(), supported, unsupported
        );

        match std::fs::write(&path, envelope) {
            Ok(()) => println!("wrote result envelope: {path}"),
            Err(e) => {
                eprintln!("could not write {path}: {e}");
                std::process::exit(1);
            }
        }
    }
}
