//! Measures how wrong the planner's cardinality estimates are, and on what
//! shape of data (#478, PERF-08).
//!
//! `PERF-08` asks for estimates within 2× of actual for 90% of SNB operators
//! and records the baseline as "count-only stats". This quantifies that.
//!
//! The estimator is:
//!
//! ```ignore
//! pub fn estimate_equality_selectivity(&self, label: &Label, property: &str) -> f64 {
//!     self.property_stats.get(...).map(|ps| ps.selectivity).unwrap_or(0.1)
//! }
//! ```
//!
//! with `selectivity = 1.0 / distinct_count`. Two things follow, and this
//! harness measures both:
//!
//!   * it is the **uniform-distribution assumption** — every value of a
//!     property is assumed equally likely;
//!   * the signature takes **no value**, so it cannot vary by value even in
//!     principle. Fixing skew means changing this signature, not just its body.
//!
//! LDBC's generator is deliberately skewed, as is any real social or
//! biomedical graph, so this is the common case rather than an adversarial one.
//!
//!   cargo bench --bench cardinality_accuracy
//!   cargo bench --bench cardinality_accuracy -- --json cardinality.json

use samyama::graph::{GraphStore, Label, PropertyValue};
use std::collections::HashMap;

/// Build `n` nodes whose `prop` values follow the given distribution.
/// Returns the true count of each value.
fn build(n: usize, values: &[(&str, usize)]) -> (GraphStore, HashMap<String, usize>) {
    let mut store = GraphStore::new();
    let mut truth: HashMap<String, usize> = HashMap::new();
    let total_weight: usize = values.iter().map(|(_, w)| *w).sum();

    let mut emitted = 0usize;
    for (value, weight) in values {
        let count = n * weight / total_weight;
        for _ in 0..count {
            let id = store.create_node("Person");
            let _ = store.set_node_property(
                "default",
                id,
                "prop".to_string(),
                PropertyValue::String((*value).to_string()),
            );
            emitted += 1;
        }
        truth.insert((*value).to_string(), count);
    }
    // Top up any rounding shortfall with the first value so the totals line up.
    if emitted < n {
        let (first, _) = values[0];
        for _ in emitted..n {
            let id = store.create_node("Person");
            let _ = store.set_node_property(
                "default",
                id,
                "prop".to_string(),
                PropertyValue::String(first.to_string()),
            );
            *truth.get_mut(first).unwrap() += 1;
        }
    }
    (store, truth)
}

struct Case {
    name: &'static str,
    description: &'static str,
    values: Vec<(&'static str, usize)>,
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let json_path = args
        .iter()
        .position(|a| a == "--json")
        .and_then(|i| args.get(i + 1).cloned());
    let n: usize = args
        .iter()
        .position(|a| a == "--scale")
        .and_then(|i| args.get(i + 1))
        .and_then(|s| s.parse().ok())
        .unwrap_or(20_000);

    let cases = vec![
        Case {
            name: "uniform",
            description: "10 values, equal counts — the distribution the estimator assumes",
            values: (0..10)
                .map(|i| (["v0", "v1", "v2", "v3", "v4", "v5", "v6", "v7", "v8", "v9"][i], 1))
                .collect(),
        },
        Case {
            name: "mild-skew",
            description: "10 values, most common 3x the rarest",
            values: vec![
                ("v0", 3), ("v1", 3), ("v2", 2), ("v3", 2), ("v4", 2),
                ("v5", 1), ("v6", 1), ("v7", 1), ("v8", 1), ("v9", 1),
            ],
        },
        Case {
            name: "heavy-skew",
            description: "one value on ~50% of nodes, long tail — social/biomedical shape",
            values: vec![
                ("v0", 500), ("v1", 200), ("v2", 100), ("v3", 60), ("v4", 40),
                ("v5", 40), ("v6", 30), ("v7", 15), ("v8", 10), ("v9", 5),
            ],
        },
    ];

    println!("Cardinality estimate accuracy — {n} nodes per case");
    println!("{}", "=".repeat(84));

    let mut json_cases: Vec<String> = Vec::new();

    for case in &cases {
        let (store, truth) = build(n, &case.values);
        let stats = store.statistics();
        let label = Label::new("Person");
        let label_count = *stats.label_counts.get(&label).unwrap_or(&0);
        let selectivity = stats.estimate_equality_selectivity(&label, "prop");
        let estimated_rows = label_count as f64 * selectivity;

        println!();
        println!("{} — {}", case.name, case.description);
        println!(
            "  label count {label_count}, estimated selectivity {selectivity:.6} \
-> {estimated_rows:.0} rows for EVERY value"
        );
        println!("  {:<8} {:>10} {:>12} {:>10}", "value", "actual", "estimate", "est/actual");

        let mut ratios: Vec<f64> = Vec::new();
        let mut mcv_ratios: Vec<f64> = Vec::new();
        let mut sorted: Vec<(&String, &usize)> = truth.iter().collect();
        sorted.sort_by(|a, b| b.1.cmp(a.1));
        println!("  {:<8} {:>10} {:>12} {:>10} {:>12} {:>10}", "", "", "uniform", "", "value-aware", "");
        for (value, actual) in &sorted {
            let ratio = if **actual > 0 { estimated_rows / **actual as f64 } else { f64::INFINITY };
            ratios.push(ratio);

            let mcv_sel = stats.estimate_equality_selectivity_for_value(
                &label,
                "prop",
                &PropertyValue::String((*value).clone()),
            );
            let mcv_rows = label_count as f64 * mcv_sel;
            let mcv_ratio = if **actual > 0 { mcv_rows / **actual as f64 } else { f64::INFINITY };
            mcv_ratios.push(mcv_ratio);

            println!(
                "  {:<8} {:>10} {:>12.0} {:>9.2}x {:>12.0} {:>9.2}x",
                value, actual, estimated_rows, ratio, mcv_rows, mcv_ratio
            );
        }

        // The question PERF-08 actually asks.
        let score = |rs: &[f64]| -> (usize, f64) {
            let w = rs.iter().filter(|r| **r >= 0.5 && **r <= 2.0).count();
            let worst = rs.iter().cloned().fold(1.0f64, |a, b| {
                let err = if b >= 1.0 { b } else { 1.0 / b };
                a.max(err)
            });
            (w, worst)
        };
        let (within_2x, worst) = score(&ratios);
        let (mcv_within_2x, mcv_worst) = score(&mcv_ratios);
        println!(
            "  uniform      within 2x: {}/{} ({:.0}%)   worst: {:.1}x",
            within_2x,
            ratios.len(),
            100.0 * within_2x as f64 / ratios.len() as f64,
            worst
        );
        println!(
            "  value-aware  within 2x: {}/{} ({:.0}%)   worst: {:.1}x",
            mcv_within_2x,
            mcv_ratios.len(),
            100.0 * mcv_within_2x as f64 / mcv_ratios.len() as f64,
            mcv_worst
        );

        json_cases.push(format!(
            "{{\"case\": \"{}\", \"values\": {}, \"within_2x_uniform\": {}, \"worst_error_uniform\": {:.2}, \"within_2x_value_aware\": {}, \"worst_error_value_aware\": {:.2}}}",
            case.name,
            ratios.len(),
            within_2x,
            worst,
            mcv_within_2x,
            mcv_worst
        ));
    }

    // ── Range predicates (#478): the fixed third against the histogram ──
    println!();
    println!("Range predicates — {n} nodes, `p op x` over every threshold that keeps >= 0.5% of rows");
    /// A distribution: the value of the `i`-th node.
    type Distribution = Box<dyn Fn(usize) -> i64>;
    let range_cases: Vec<(&str, &str, Distribution)> = vec![
        ("uniform", "0..1000, every value equally often", Box::new(|i| ((i * 7919) % 1000) as i64)),
        ("geometric", "half 0, a quarter 1, an eighth 2, ...", Box::new(move |i| ((n as f64 / (i + 1) as f64).log2().floor()) as i64)),
        ("zipf-like", "value ~ 1000/(rank+1): a long tail of 1000 values", Box::new(|i| (1000.0 / ((i % 2000) as f64 + 1.0)).floor() as i64)),
    ];
    for (name, description, value) in &range_cases {
        let mut store = GraphStore::new();
        let mut values = Vec::with_capacity(n);
        for i in 0..n {
            let id = store.create_node("R");
            let v = value(i);
            values.push(v);
            let _ = store.set_node_property("default", id, "x".to_string(), PropertyValue::Integer(v));
        }
        values.sort_unstable();
        let stats = store.statistics();
        let label = Label::new("R");
        let (lo, hi) = (values[0], values[n - 1]);
        let step = ((hi - lo) / 40).max(1);
        let (mut fixed, mut hist) = (Vec::new(), Vec::new());
        let mut x = lo;
        while x <= hi {
            for op in [samyama::query::ast::BinaryOp::Lt, samyama::query::ast::BinaryOp::Le, samyama::query::ast::BinaryOp::Gt, samyama::query::ast::BinaryOp::Ge] {
                use samyama::query::ast::BinaryOp;
                let actual = values
                    .iter()
                    .filter(|v| match op { BinaryOp::Lt => **v < x, BinaryOp::Le => **v <= x, BinaryOp::Gt => **v > x, _ => **v >= x })
                    .count() as f64;
                if actual < n as f64 * 0.005 {
                    continue;
                }
                fixed.push(n as f64 / 3.0 / actual);
                let sel = stats
                    .estimate_comparison_selectivity(&label, "x", &op, &PropertyValue::Integer(x))
                    .unwrap_or(1.0 / 3.0);
                hist.push(sel * n as f64 / actual);
            }
            x += step;
        }
        let score = |rs: &[f64]| -> (usize, f64) {
            let w = rs.iter().filter(|r| **r >= 0.5 && **r <= 2.0).count();
            let worst = rs.iter().fold(1.0f64, |a, b| a.max(if *b >= 1.0 { *b } else { 1.0 / b }));
            (w, worst)
        };
        let ((fw, fworst), (hw, hworst)) = (score(&fixed), score(&hist));
        println!("  {name} — {description}");
        println!(
            "    fixed 1/3   within 2x: {fw}/{} ({:.0}%)   worst: {fworst:.1}x",
            fixed.len(),
            100.0 * fw as f64 / fixed.len() as f64
        );
        println!(
            "    histogram   within 2x: {hw}/{} ({:.0}%)   worst: {hworst:.1}x",
            hist.len(),
            100.0 * hw as f64 / hist.len() as f64
        );
        json_cases.push(format!(
            "{{\"case\": \"range-{name}\", \"predicates\": {}, \"within_2x_fixed\": {fw}, \"worst_error_fixed\": {fworst:.2}, \"within_2x_histogram\": {hw}, \"worst_error_histogram\": {hworst:.2}}}",
            hist.len()
        ));
    }

    println!();
    println!("{}", "=".repeat(84));
    println!("The estimate is identical for every value of a property, because");
    println!("`estimate_equality_selectivity(label, property)` takes no value. On a");
    println!("uniform distribution that is exactly right; the further the data departs");
    println!("from uniform, the further every estimate is from the truth -- too low for");
    println!("the common values and too high for the rare ones, at the same time.");

    if let Some(path) = json_path {
        let commit = std::process::Command::new("git")
            .args(["rev-parse", "--short", "HEAD"])
            .output()
            .ok()
            .filter(|o| o.status.success())
            .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
            .unwrap_or_else(|| "unknown".into());
        let envelope = format!(
            "{{
  \"suite\": \"cardinality-accuracy\",
  \"requirement_ids\": [\"PERF-08\"],
  \"run_id\": \"cardinality-{commit}-{n}n\",
  \"engine\": {{\"name\": \"samyama\", \"version\": \"{}\", \"commit\": \"{commit}\"}},
  \"hardware\": {{\"note\": \"estimate accuracy is independent of hardware\"}},
  \"dataset\": {{\"name\": \"synthetic-distributions\", \"nodes_per_case\": {n}}},
  \"measurements\": {{\"cases\": [
      {}
  ]}},
  \"status\": \"measured\",
  \"artifacts\": [\"benches/cardinality_accuracy.rs\"],
  \"caveat\": \"Single-property equality and range predicates only. Does not measure join or multi-hop estimates.\"
}}
",
            env!("CARGO_PKG_VERSION"),
            json_cases.join(",\n      ")
        );
        match std::fs::write(&path, envelope) {
            Ok(()) => println!("\nwrote result envelope: {path}"),
            Err(e) => {
                eprintln!("could not write {path}: {e}");
                std::process::exit(1);
            }
        }
    }
}
