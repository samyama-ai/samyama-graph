//! Cross-hierarchy conjunctions drive from the most selective axis (#350, #1345).
//!
//! HIER class H4 asks one question over two or three hierarchies at once — "events about
//! anything under T05, on any day in Y2019M01". Before #350 the planner had no plan for
//! that shape: it scanned every fact, walked out to every axis and evaluated `subsumes()`
//! per row, and #1345 measured it 11–49x *slower* than the unindexed traversal. It now
//! enumerates the smallest subtree from the index, walks back into the facts, and tests
//! the remaining axes with `HierarchyOrderTest`.
//!
//! Three properties, each against the benchmark's own generator at a small scale:
//!
//! - **Agreement.** Every corpus query returns the same rows with the index as without.
//!   The unindexed arm is the corpus's `baseline` — a plain variable-length traversal on a
//!   store with no hierarchy declared — so it is ground truth, not a second opinion.
//! - **Work.** The driven plan produces fewer rows, summed over its operators, than both
//!   the unindexed traversal and the scan-and-test plan it replaces, measured by `PROFILE`
//!   in this process. Rows, not milliseconds: CI runs a debug build.
//! - **Shape.** `EXPLAIN` names the driving axis and tests the others.

#[allow(dead_code)]
#[path = "../benches/hier_common/mod.rs"]
mod hier_common;

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::record::Value;
use samyama::query::{QueryEngine, RecordBatch};

const CORPUS: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/benchmarks/hier/queries.json");

/// Small enough for a debug build, large enough that every code the corpus pins exists:
/// three years (Y2019–Y2021), three countries (CO0–CO2), and an ontology four levels deep
/// (the corpus pins `T0531`).
fn scale() -> hier_common::HierScale {
    hier_common::HierScale {
        years: 3,
        days_per_month: 6,
        countries: 3,
        onto_depth: 4,
        onto_fanout: 6,
        threat_layers: 3,
        threat_width: 6,
        events: 1_500,
    }
}

/// The benchmark's two arms: the same graph with and without hierarchy indexes.
fn stores(engine: &QueryEngine) -> (GraphStore, GraphStore) {
    let mut indexed = hier_common::build(&scale()).store;
    let mut baseline = hier_common::build(&scale()).store;
    for decl in hier_common::SETUP_DECLARATIONS {
        for store in [&mut indexed, &mut baseline] {
            engine
                .execute_mut(decl, store, "default")
                .unwrap_or_else(|e| panic!("setup failed: {decl}\n{e}"));
        }
    }
    for decl in hier_common::HIER_DECLARATIONS {
        engine
            .execute_mut(decl, &mut indexed, "default")
            .unwrap_or_else(|e| panic!("declaration failed: {decl}\n{e}"));
    }
    (indexed, baseline)
}

struct CorpusQuery {
    id: String,
    class: String,
    cypher: String,
    baseline: String,
}

/// Every runnable corpus query: skipped ones are covered by `hier_corpus_skips_expire`,
/// and ones reading the HNSW index are not a controlled comparison between two stores
/// (HNSW layers are assigned at random), exactly as the benchmark runner excludes them.
fn corpus() -> Vec<CorpusQuery> {
    let text = std::fs::read_to_string(CORPUS).expect("read corpus");
    let json: serde_json::Value = serde_json::from_str(&text).expect("corpus is JSON");
    json["queries"]
        .as_array()
        .expect("queries array")
        .iter()
        .filter(|q| q["skip"].is_null())
        .map(|q| {
            let cypher = q["cypher"].as_str().expect("cypher").to_string();
            CorpusQuery {
                id: q["id"].as_str().expect("id").to_string(),
                class: q["class"].as_str().expect("class").to_string(),
                baseline: q["baseline"].as_str().unwrap_or(&cypher).to_string(),
                cypher,
            }
        })
        .filter(|q| !q.cypher.contains("db.index.vector."))
        .collect()
}

/// Order-independent rendering of a result, the same one the benchmark gates on.
fn canonical(batch: &RecordBatch) -> String {
    let mut rows: Vec<String> = batch
        .records
        .iter()
        .map(|r| {
            let mut cells: Vec<String> = r
                .bindings()
                .iter()
                .map(|(k, v)| {
                    let v = match v {
                        Value::Property(PropertyValue::Float(f)) => format!("{f:.6}"),
                        Value::Property(p) => format!("{p:?}"),
                        Value::Node(id, _) | Value::NodeRef(id) => format!("n{}", id.as_u64()),
                        other => format!("{other:?}"),
                    };
                    format!("{k}={v}")
                })
                .collect();
            cells.sort();
            cells.join("|")
        })
        .collect();
    rows.sort();
    rows.join(";")
}

fn plan_text_unchecked(engine: &QueryEngine, store: &GraphStore, cypher: &str) -> String {
    let batch = engine
        .execute(cypher, store)
        .unwrap_or_else(|e| panic!("{cypher}\n{e}"));
    match batch.records[0].get("plan") {
        Some(Value::Property(PropertyValue::String(s))) => s.clone(),
        other => panic!("expected a plan string, got {other:?}"),
    }
}

/// [`plan_text_unchecked`], refusing a plan tree that stops early.
///
/// `PhysicalOperator::describe` has a default printing `Unknown` with no
/// children, so an operator that does not implement it truncates the tree —
/// and an assertion that the plan does *not* contain something is then
/// satisfied by the missing half, whatever the planner did (#1826). Checked
/// here, where the text is read, so the hole fails loudly rather than passing.
fn plan_text(engine: &QueryEngine, store: &GraphStore, cypher: &str) -> String {
    let text = plan_text_unchecked(engine, store, cypher);
    assert!(
        samyama::query::executor::undescribed_plan_lines(&text).is_empty(),
        "the plan tree stops at an operator with no describe, so what is below \
         it cannot be asserted about (#1826):\n{text}"
    );
    text
}

/// Rows produced, summed over every operator in a `PROFILE` of `cypher`.
///
/// The total is what the plan materialized on its way to the answer — a scan that reads
/// every fact and a filter that discards most of them count both — which is the work the
/// driving-axis choice exists to avoid.
fn rows_touched(engine: &QueryEngine, store: &GraphStore, cypher: &str) -> u64 {
    let text = plan_text(engine, store, &format!("PROFILE {cypher}"));
    let table = text
        .split("--- Profile (per operator) ---")
        .nth(1)
        .unwrap_or_else(|| panic!("no per-operator table:\n{text}"));
    table
        .lines()
        .skip(2) // the rest of the title line, then the column headers
        .take_while(|l| !l.trim().is_empty())
        .map(|l| {
            let cols: Vec<&str> = l.split_whitespace().collect();
            cols[cols.len() - 2]
                .parse::<u64>()
                .unwrap_or_else(|_| panic!("unparseable profile row: {l}"))
        })
        .sum()
}

#[test]
fn every_hier_query_agrees_with_the_unindexed_traversal() {
    let engine = QueryEngine::new();
    let (indexed, baseline) = stores(&engine);
    let corpus = corpus();
    let h4 = corpus.iter().filter(|q| q.class == "H4").count();
    assert_eq!(h4, 12, "the corpus should carry all twelve H4 queries");

    let mut disagreements = Vec::new();
    let mut nonempty_h4 = 0;
    for q in &corpus {
        let a = engine.execute(&q.cypher, &indexed);
        let b = engine.execute(&q.baseline, &baseline);
        match (a, b) {
            (Ok(a), Ok(b)) => {
                let (ca, cb) = (canonical(&a), canonical(&b));
                if ca != cb {
                    disagreements.push(format!("{}: indexed={ca} baseline={cb}", q.id));
                }
                if q.class == "H4" && !ca.contains("Integer(0)") {
                    nonempty_h4 += 1;
                }
            }
            (a, b) => disagreements.push(format!(
                "{}: indexed ok={} baseline ok={}: {:?} / {:?}",
                q.id,
                a.is_ok(),
                b.is_ok(),
                a.err(),
                b.err()
            )),
        }
    }
    assert!(
        disagreements.is_empty(),
        "{} of {} corpus queries disagree:\n  {}",
        disagreements.len(),
        corpus.len(),
        disagreements.join("\n  ")
    );
    // Agreement on twelve zeros would say nothing about the driven plan.
    assert!(
        nonempty_h4 >= 8,
        "only {nonempty_h4} of 12 H4 queries matched anything at this scale"
    );
}

#[test]
fn explain_names_the_driving_axis_and_tests_the_others() {
    let engine = QueryEngine::new();
    let (indexed, _) = stores(&engine);
    let corpus = corpus();
    let h4_03 = corpus
        .iter()
        .find(|q| q.id == "H4-03")
        .expect("H4-03 in corpus");
    let plan = plan_text(&engine, &indexed, &format!("EXPLAIN {}", h4_03.cypher));
    // Y2019M01 holds 7 nodes, T05 holds 43: the calendar drives.
    assert!(
        plan.contains("HierarchyDescendantScan") && plan.contains("day under"),
        "the calendar axis should drive:\n{plan}"
    );
    assert!(
        plan.contains("HierarchyOrderTest") && plan.contains("t ⊑"),
        "the ontology axis should be an order test:\n{plan}"
    );
    assert!(!plan.contains("NodeScan"), "no fact scan:\n{plan}");

    // Three axes: CO0S0T0 holds 11 nodes against Y2019's 89 and T's 1,555, so the
    // geography drives and the other two are tested.
    let h4_12 = corpus
        .iter()
        .find(|q| q.id == "H4-12")
        .expect("H4-12 in corpus");
    let plan = plan_text(&engine, &indexed, &format!("EXPLAIN {}", h4_12.cypher));
    assert!(
        plan.contains("z under"),
        "the geography axis should drive:\n{plan}"
    );
    assert_eq!(plan.matches("HierarchyOrderTest").count(), 2, "{plan}");
    assert!(!plan.contains("NodeScan"), "no fact scan:\n{plan}");
}

#[test]
fn other_projections_of_the_conjunction_agree_too() {
    // The corpus only counts. The rewrite also produces sums and distinct counts, and
    // reads an edge written from the hierarchy side; each is checked against the same
    // question asked of the unindexed store.
    let engine = QueryEngine::new();
    let (indexed, baseline) = stores(&engine);
    let corpus = corpus();
    let h4_07 = corpus
        .iter()
        .find(|q| q.id == "H4-07")
        .expect("H4-07 in corpus");
    for (from, to) in [
        ("RETURN count(e) AS n", "RETURN sum(e.units) AS n"),
        ("RETURN count(e) AS n", "RETURN count(DISTINCT e) AS n"),
        ("RETURN count(e) AS n", "RETURN count(e)"),
    ] {
        let cypher = h4_07.cypher.replace(from, to);
        let plan = plan_text(&engine, &indexed, &format!("EXPLAIN {cypher}"));
        assert!(
            plan.contains("HierarchyDescendantScan"),
            "not rewritten: {cypher}\n{plan}"
        );
        let a = engine.execute(&cypher, &indexed).unwrap();
        let b = engine
            .execute(&h4_07.baseline.replace(from, to), &baseline)
            .unwrap();
        assert_eq!(canonical(&a), canonical(&b), "{cypher}");
        assert!(
            !canonical(&a).contains("Integer(0)"),
            "{cypher} matched nothing"
        );
    }
}

#[test]
fn the_driven_plan_touches_fewer_rows_than_either_alternative() {
    let engine = QueryEngine::new();
    let (indexed, baseline) = stores(&engine);
    let corpus = corpus();

    let (mut total_driven, mut total_unindexed) = (0u64, 0u64);
    let mut report = Vec::new();
    let mut failures = Vec::new();
    for q in corpus.iter().filter(|q| q.class == "H4") {
        let driven = rows_touched(&engine, &indexed, &q.cypher);
        let unindexed = rows_touched(&engine, &baseline, &q.baseline);
        // The same query with a projection the rewrite does not recognize: the plan every
        // H4 query got before #350, scanning facts and testing each axis per row.
        let scan_and_test = rows_touched(
            &engine,
            &indexed,
            &q.cypher
                .replace("RETURN count(e) AS n", "RETURN count(e) + 0 AS n"),
        );
        let line = format!(
            "{}: driven={driven} unindexed={unindexed} scan_and_test={scan_and_test}",
            q.id
        );
        // Per query: never more work than the traversal, and a fraction of the scan.
        if driven > unindexed || driven * 5 > scan_and_test {
            failures.push(line.clone());
        }
        report.push(line);
        total_driven += driven;
        total_unindexed += unindexed;
    }
    eprintln!("{}", report.join("\n"));
    assert!(
        failures.is_empty(),
        "driven plan did too much work:\n  {}",
        failures.join("\n  ")
    );
    // Over the class, well under the traversal: 9,175 against 23,778 rows when written.
    assert!(
        total_driven * 2 <= total_unindexed,
        "H4 driven plans touched {total_driven} rows against {total_unindexed} unindexed"
    );
}
