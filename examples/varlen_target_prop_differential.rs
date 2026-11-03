//! Pruning a var-length target inside the operator must not change the answer.
//!
//! `(p)-[:R*1..3]-(f:L {k: v})` now tests `k = v` inside the walk, before a
//! record is built, instead of leaving every endpoint to a `Filter` above
//! (#1063). Writing the same predicate as `WHERE f.k = v` leaves the pattern's
//! properties empty, so it takes the old path — which makes the two forms a
//! differential over the same question.
//!
//! This is the shape that caught 218 disagreements when the shortestPath walk
//! lost its direction reversal. A var-length walk has produced wrong answers
//! four separate times in this file's history (#710, #933, #934, #976), and
//! every one of them looked like a successful query.
use samyama::graph::{GraphStore, Label, PropertyValue};
use samyama::query::executor::{MutQueryExecutor, QueryExecutor};
use samyama::query::parser::parse_query;

/// Deterministic pseudo-random, so a failure is reproducible from the seed.
struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }
}

fn answers(s: &GraphStore, q: &str) -> Result<Vec<String>, String> {
    let p = parse_query(q).map_err(|e| format!("parse: {e:?}"))?;
    let b = QueryExecutor::new(s)
        .execute(&p)
        .map_err(|e| format!("exec: {e:?}"))?;
    let mut rows: Vec<String> = b
        .records
        .iter()
        .map(|r| {
            let k = r.get("k").map(|v| format!("{v:?}")).unwrap_or_default();
            let n = r.get("name").map(|v| format!("{v:?}")).unwrap_or_default();
            format!("k={k},name={n}")
        })
        .collect();
    rows.sort();
    Ok(rows)
}

fn main() {
    let mut rng = Rng(0x5A3C_1234_9E77_0001);
    let mut checked = 0usize;
    let mut disagreements = 0usize;
    let mut non_empty = 0usize;

    for graph_n in 0..40 {
        let nodes = 12 + rng.below(30);
        let mut s = GraphStore::new();
        let mut ids = Vec::new();
        for i in 0..nodes {
            // Two labels and a small value domain, so targets collide and
            // multi-label patterns are exercised.
            let mut labels = vec![Label::new("N")];
            if i % 3 == 0 {
                labels.push(Label::new("M"));
            }
            let id = s.create_node_with_labels(labels);
            s.set_node_property("default", id, "k",
                PropertyValue::Integer((i % 4) as i64)).unwrap();
            // A third of the nodes lack the property entirely: a node without
            // it must not match, and that is exactly where an `is_some_and`
            // and a `map_or(true, ..)` differ.
            if i % 3 != 1 {
                s.set_node_property("default", id, "name",
                    PropertyValue::String(format!("v{}", i % 3))).unwrap();
            }
            ids.push(id);
        }
        let edges = nodes + rng.below(nodes * 2);
        for _ in 0..edges {
            let a = ids[rng.below(nodes)];
            let b = ids[rng.below(nodes)];
            let _ = s.create_edge(a, b, if rng.below(2) == 0 { "R" } else { "S" });
        }
        // Index half the graphs, so both the id-set path and the property
        // compare path are exercised.
        if graph_n % 2 == 0 {
            for stmt in ["CREATE INDEX ON :N(name)", "CREATE INDEX ON :N(k)"] {
                let q = parse_query(stmt).unwrap();
                MutQueryExecutor::new(&mut s, "default".to_string()).execute(&q).unwrap();
            }
        }

        for anchor in [0usize, 1, 2] {
            if anchor >= nodes {
                continue;
            }
            let a_k = anchor % 4;
            for (bounds, dir) in [("*1..2", "-"), ("*1..3", "-"), ("*1..2", "->"),
                                  ("*2..3", "-"), ("*0..2", "-"), ("*1..1", "->")] {
                // An **unlabelled** target is essential, not extra coverage:
                // `resolve_target_ids` declines without a label, so it is the
                // only shape that reaches the `target_props` property compare.
                // With labels only, a deliberate bug in that compare changed
                // nothing and this differential passed 5,760 comparisons while
                // testing a branch it never entered.
                for (labels, prop, val) in [
                    (":N", "name", "\"v0\""),
                    (":N", "k", "1"),
                    (":N:M", "name", "\"v1\""),
                    (":N", "name", "\"absent\""),
                    ("", "name", "\"v0\""),
                    ("", "k", "2"),
                    ("", "name", "\"absent\""),
                ] {
                    for distinct in [true, false] {
                        let d = if distinct { "DISTINCT " } else { "" };
                        let arrow_l = if dir == "->" { "-" } else { "-" };
                        let inline = format!(
                            "MATCH (p:N {{k: {a_k}}}){arrow_l}[{bounds}]{dir}(f{labels} {{{prop}: {val}}}) \
                             RETURN {d}f.k AS k, f.name AS name"
                        );
                        let wherev = format!(
                            "MATCH (p:N {{k: {a_k}}}){arrow_l}[{bounds}]{dir}(f{labels}) \
                             WHERE f.{prop} = {val} RETURN {d}f.k AS k, f.name AS name"
                        );
                        let (a, b) = (answers(&s, &inline), answers(&s, &wherev));
                        match (a, b) {
                            (Ok(a), Ok(b)) => {
                                checked += 1;
                                if !a.is_empty() {
                                    non_empty += 1;
                                }
                                if a != b {
                                    disagreements += 1;
                                    if disagreements <= 3 {
                                        println!("DISAGREEMENT\n  inline: {inline}\n    -> {a:?}\n  where : {wherev}\n    -> {b:?}\n");
                                    }
                                }
                            }
                            // Both forms must be equally acceptable to the
                            // parser/planner; one erroring is itself a finding.
                            (a, b) => {
                                if a.is_err() != b.is_err() {
                                    disagreements += 1;
                                    println!("ONE FORM FAILED\n  inline: {inline}\n    -> {a:?}\n  where : {b:?}\n");
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    println!("{checked} comparisons over 40 random graphs, {non_empty} returning rows");
    println!("{disagreements} disagreements");
    // An all-empty run would agree trivially and prove nothing — the trap this
    // repo has hit three times.
    assert!(non_empty > checked / 10,
        "only {non_empty} of {checked} comparisons returned any rows; this run \
         would agree trivially and cannot detect a dropped row");
    assert_eq!(disagreements, 0, "pruning the target changed the answer");
    println!("\nthe operator's pruning and the filter above agree everywhere");
}
