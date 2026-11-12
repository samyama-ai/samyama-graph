//! The churn benchmark's own correctness (#1986).
//!
//! `benches/vector_churn_benchmark.rs` reports recall under insert, delete and
//! update. A harness bug there -- a delete aimed at a tag that is not live, a
//! ground truth that still counts deleted vectors, a runbook step read with
//! the wrong fields -- would be a wrong number with nothing to catch it. A
//! bench target cannot run its own tests, so the code it shares lives in
//! `benches/churn_common` and is checked here.
//!
//! Nothing here times anything.

#[path = "../benches/ann_common/mod.rs"]
mod ann_common;

#[path = "../benches/churn_common/mod.rs"]
mod churn_common;

use std::collections::HashSet;

use ann_common::Metric;
use churn_common::*;
use samyama::vector::HnswParams;

fn config() -> Config {
    Config {
        metric: Metric::L2,
        params: HnswParams {
            m: 16,
            ef_construction: 64,
            ef: None,
        },
        k: 10,
        ef: 64,
        fresh_every: 1,
        probe_every: 7,
    }
}

// ------------------------------------------------------------------ runbooks

const RUNBOOK: &str = r#"
random-xs:
  max_pts: 100
  1:
    operation: "insert"
    start: 0
    end: 100
  2:
    operation: "search"
  3:
    operation: "delete"
    start: 0
    end: 40
  4:
    operation: "replace"
    tags_start: 40
    tags_end: 50
    ids_start: 100
    ids_end: 110
  5:
    operation: "search"
  gt_url: "https://example.invalid/gt"
other:
  1:
    operation: "insert"
    start: 0
    end: 5
"#;

#[test]
fn a_runbook_reads_in_step_order() {
    let (name, steps) = parse_runbook(RUNBOOK, Some("random-xs")).unwrap();
    assert_eq!(name, "random-xs");
    assert_eq!(
        steps,
        vec![
            Step::Insert {
                tags: Ids::Range(0, 100),
                rows: Ids::Range(0, 100)
            },
            Step::Search { queries: 0 },
            Step::Delete {
                tags: Ids::Range(0, 40)
            },
            Step::Replace {
                tags: Ids::Range(40, 50),
                rows: Ids::Range(100, 110)
            },
            Step::Search { queries: 0 },
        ]
    );
    assert_eq!(rows_needed(&steps), 110);
}

#[test]
fn a_runbook_with_several_datasets_needs_a_name() {
    let err = parse_runbook(RUNBOOK, None).unwrap_err();
    assert!(err.contains("--runbook-name"), "{err}");
    let err = parse_runbook(RUNBOOK, Some("missing")).unwrap_err();
    assert!(err.contains("random-xs") && err.contains("other"), "{err}");
}

#[test]
fn a_runbook_with_a_bad_step_is_refused() {
    let unknown = "d:\n  1:\n    operation: \"compact\"\n";
    assert!(parse_runbook(unknown, None)
        .unwrap_err()
        .contains("compact"));
    let backwards = "d:\n  1:\n    operation: \"delete\"\n    start: 9\n    end: 3\n";
    assert!(parse_runbook(backwards, None).is_err());
    let uneven = "d:\n  1:\n    operation: \"replace\"\n    tags_start: 0\n    tags_end: 5\n    ids_start: 10\n    ids_end: 12\n";
    assert!(parse_runbook(uneven, None).is_err());
    let missing = "d:\n  1:\n    operation: \"insert\"\n    start: 0\n";
    assert!(parse_runbook(missing, None).unwrap_err().contains("end"));
}

// ---------------------------------------------------------- generated steps

/// Replay the steps on a set of live tags, checking every write is valid.
fn replay(steps: &[Step]) -> Vec<usize> {
    let mut live = HashSet::new();
    let mut ever = HashSet::new();
    let mut sizes = Vec::new();
    for s in steps {
        match s {
            Step::Insert { tags, rows } => {
                assert_eq!(tags.len(), rows.len());
                for t in tags.iter() {
                    assert!(live.insert(t), "insert of live tag {t}");
                    assert!(ever.insert(t), "tag {t} reused");
                }
            }
            Step::Delete { tags } => {
                for t in tags.iter() {
                    assert!(live.remove(&t), "delete of tag {t}, not live");
                }
            }
            Step::Replace { tags, rows } => {
                assert_eq!(tags.len(), rows.len());
                for t in tags.iter() {
                    assert!(live.contains(&t), "replace of tag {t}, not live");
                }
            }
            Step::Search { .. } => sizes.push(live.len()),
        }
    }
    sizes
}

#[test]
fn random_churn_keeps_the_live_count_and_touches_only_live_tags() {
    let steps = churn_steps(500, 6, 50, 3);
    assert_eq!(replay(&steps), vec![500; 7]);
    // Every churned tag consumes one new row.
    assert_eq!(rows_needed(&steps), 500 + 6 * 50);
    let updates: usize = steps
        .iter()
        .map(|s| match s {
            Step::Replace { tags, .. } => tags.len(),
            _ => 0,
        })
        .sum();
    assert_eq!(updates, 6 * 25, "half of each cycle is updated in place");
}

#[test]
fn a_window_deletes_the_oldest() {
    let steps = window_steps(100, 3, 30, 4);
    assert_eq!(replay(&steps), vec![100; 4]);
    assert_eq!(
        steps[2],
        Step::Delete {
            tags: Ids::Range(0, 30)
        }
    );
    assert_eq!(
        steps[5],
        Step::Delete {
            tags: Ids::Range(30, 60)
        }
    );
    assert_eq!(steps.last(), Some(&Step::Search { queries: 3 }));
}

#[test]
fn drift_moves_the_data() {
    let w = synthetic_drift(400, 8, 4, 100, 20, 4.0, 5);
    let mean = |from: usize, to: usize| -> Vec<f32> {
        let mut m = vec![0.0f32; w.data.dim];
        for i in from..to {
            for (x, v) in m.iter_mut().zip(w.data.row(i)) {
                *x += v / (to - from) as f32;
            }
        }
        m
    };
    let (first, last) = (mean(0, 100), mean(700, 800));
    let shift: f32 = first
        .iter()
        .zip(&last)
        .map(|(a, b)| (a - b) * (a - b))
        .sum::<f32>()
        .sqrt();
    // Rows 0..100 and 700..800 sit 7/8 of the run apart, so their centres
    // should be about 3.5 apart; noise and cluster choice move that a little.
    assert!(shift > 2.0, "the distribution moved only {shift}");
    assert_eq!(w.queries.len(), 5);
}

// ----------------------------------------------------------------- the run

#[test]
fn a_small_index_is_searched_exactly_and_scores_one() {
    // At or below 128 live vectors the index answers by linear scan, so the
    // harness's ground truth and the index must agree on every query.
    let w = synthetic_churn(100, 8, 3, 20, 25, 9);
    let reports = run(&w, &config(), |_| {}).unwrap();
    assert_eq!(reports.len(), 4);
    for r in &reports {
        assert!(r.scanned);
        assert_eq!(r.live, 100);
        assert_eq!(r.recall, 1.0, "step {}: truth and index disagree", r.search);
        assert_eq!(r.fresh_recall, Some(1.0));
    }
}

#[test]
fn a_churned_run_reports_every_step() {
    let w = synthetic_churn(1_000, 8, 4, 100, 30, 11);
    let mut seen = 0;
    let reports = run(&w, &config(), |_| seen += 1).unwrap();
    assert_eq!(seen, 5);
    assert_eq!(reports.len(), 5);
    let first = &reports[0];
    assert_eq!(first.inserts.count(), 1_000);
    assert_eq!(first.superseded, 0);
    assert!(!first.scanned, "a clean index searches the graph");
    for r in &reports[1..] {
        assert_eq!(r.live, 1_000);
        assert_eq!(r.inserts.count(), 50);
        assert_eq!(r.deletes.count(), 50);
        assert_eq!(r.updates.count(), 50);
        assert_eq!(r.search_ops.count(), 30);
        assert!(r.probes.count() > 0);
        assert!((0.0..=1.0).contains(&r.recall));
        assert!(r.fresh_recall.is_some());
    }
    // 100 dead entries a cycle against 1,000 live: the index rebuilds itself
    // once a fifth of the graph is dead, so by the fourth cycle it has.
    assert!(reports.last().unwrap().compactions > 0);
    assert!(reports.iter().all(|r| r.dead_share() < 0.2));
}

#[test]
fn a_workload_that_deletes_a_dead_tag_is_refused() {
    let mut w = synthetic_churn(200, 4, 0, 1, 5, 1);
    w.steps.push(Step::Delete {
        tags: Ids::Range(0, 1),
    });
    w.steps.push(Step::Delete {
        tags: Ids::Range(0, 1),
    });
    let err = run(&w, &config(), |_| {}).unwrap_err();
    assert!(err.contains("not live"), "{err}");
}

#[test]
fn recall_counts_set_membership_within_k() {
    let truth = vec![HashSet::from([1u64, 2, 3])];
    assert_eq!(recall(&[vec![3, 2, 1]], &truth, 3), 1.0);
    assert_eq!(recall(&[vec![1, 1, 1]], &truth, 3), 1.0 / 3.0);
    assert_eq!(recall(&[vec![9, 8, 7, 1]], &truth, 3), 0.0);
}
