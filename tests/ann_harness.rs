//! The recall–QPS harness's own correctness (#1983).
//!
//! `benches/ann_benchmark.rs` reports a recall figure that other issues will
//! quote (#1802's NDS-05, #1971's SIFT1M evidence). A harness bug there would
//! be a wrong number with nothing to catch it, and a bench target cannot run
//! its own tests, so the code it shares lives in `benches/ann_common` and is
//! checked here: the file readers, ground truth, recall scoring, percentiles,
//! the Pareto frontier, and both search paths against a real store.
//!
//! Nothing here times anything. The sweep's QPS and latency figures are only
//! checked to be present and consistent with each other.

#[path = "../benches/ann_common/mod.rs"]
mod ann_common;

use ann_common::*;
use std::time::Duration;

fn tmp() -> tempfile::TempDir {
    tempfile::tempdir().expect("tempdir")
}

// ------------------------------------------------------------------ file I/O

#[test]
fn fvecs_and_ivecs_round_trip() {
    let dir = tmp();
    let v = Vectors {
        dim: 3,
        data: vec![1.0, -2.5, 3.0, 0.0, 4.25, -1.0],
    };
    let p = dir.path().join("v.fvecs");
    write_fvecs(&p, &v).unwrap();
    assert_eq!(read_fvecs(&p, None).unwrap(), v);
    // 2 records x (4-byte dim + 3 x 4-byte floats)
    assert_eq!(std::fs::metadata(&p).unwrap().len(), 2 * (4 + 12));

    let n = Neighbours {
        width: 2,
        ids: vec![7, 3, 0, 1],
    };
    let p = dir.path().join("n.ivecs");
    write_ivecs(&p, &n).unwrap();
    assert_eq!(read_ivecs(&p, None).unwrap(), n);
}

#[test]
fn a_limit_reads_only_the_first_records() {
    let dir = tmp();
    let v = Vectors {
        dim: 2,
        data: (0..10).map(|i| i as f32).collect(),
    };
    let p = dir.path().join("v.fvecs");
    write_fvecs(&p, &v).unwrap();
    let head = read_fvecs(&p, Some(2)).unwrap();
    assert_eq!(head.len(), 2);
    assert_eq!(head.data, vec![0.0, 1.0, 2.0, 3.0]);
}

/// The layout is little-endian, whatever the host: SIFT's files are read
/// byte for byte as the TEXMEX page defines them.
#[test]
fn records_are_read_little_endian() {
    let dir = tmp();
    let p = dir.path().join("v.fvecs");
    let mut bytes = 2i32.to_le_bytes().to_vec();
    bytes.extend(1.5f32.to_le_bytes());
    bytes.extend((-0.25f32).to_le_bytes());
    std::fs::write(&p, bytes).unwrap();
    assert_eq!(
        read_fvecs(&p, None).unwrap(),
        Vectors {
            dim: 2,
            data: vec![1.5, -0.25]
        }
    );
}

#[test]
fn a_record_with_a_different_dimension_is_refused() {
    let dir = tmp();
    let p = dir.path().join("v.fvecs");
    let mut bytes = Vec::new();
    for (d, n) in [(2i32, 2usize), (3, 3)] {
        bytes.extend(d.to_le_bytes());
        for _ in 0..n {
            bytes.extend(1.0f32.to_le_bytes());
        }
    }
    std::fs::write(&p, bytes).unwrap();
    let err = read_fvecs(&p, None).unwrap_err();
    assert!(
        err.contains("record 1 has dimension 3, the first had 2"),
        "{err}"
    );
}

#[test]
fn a_truncated_file_is_refused() {
    let dir = tmp();
    let p = dir.path().join("v.fvecs");
    let mut bytes = 4i32.to_le_bytes().to_vec();
    bytes.extend(1.0f32.to_le_bytes()); // one float of the promised four
    std::fs::write(&p, bytes).unwrap();
    let err = read_fvecs(&p, None).unwrap_err();
    assert!(err.contains("cut short"), "{err}");
}

#[test]
fn a_negative_neighbour_id_is_refused() {
    let dir = tmp();
    let p = dir.path().join("n.ivecs");
    let mut bytes = 1i32.to_le_bytes().to_vec();
    bytes.extend((-1i32).to_le_bytes());
    std::fs::write(&p, bytes).unwrap();
    assert!(read_ivecs(&p, None).unwrap_err().contains("negative id"));
}

// ------------------------------------------------------------ ground truth

#[test]
fn ground_truth_is_the_exact_nearest_in_order() {
    // Points on a line; the query sits at 2.2.
    let base = Vectors {
        dim: 1,
        data: vec![0.0, 1.0, 2.0, 3.0, 10.0],
    };
    let q = Vectors {
        dim: 1,
        data: vec![2.2],
    };
    let t = exact_ground_truth(&base, &q, 3, Metric::L2);
    assert_eq!(t.width, 3);
    assert_eq!(t.row(0), &[2, 3, 1]);
}

#[test]
fn ground_truth_follows_the_metric() {
    // [10, 0] is far from [1, 0] by L2 but identical in direction.
    let base = Vectors {
        dim: 2,
        data: vec![10.0, 0.0, 0.9, 0.5],
    };
    let q = Vectors {
        dim: 2,
        data: vec![1.0, 0.0],
    };
    assert_eq!(exact_ground_truth(&base, &q, 1, Metric::L2).row(0), &[1]);
    assert_eq!(
        exact_ground_truth(&base, &q, 1, Metric::Cosine).row(0),
        &[0]
    );
    assert_eq!(exact_ground_truth(&base, &q, 1, Metric::Dot).row(0), &[0]);
}

#[test]
fn ground_truth_wider_than_the_base_set_lists_every_vector() {
    let base = Vectors {
        dim: 1,
        data: vec![5.0, 1.0],
    };
    let q = Vectors {
        dim: 1,
        data: vec![0.0],
    };
    let t = exact_ground_truth(&base, &q, 10, Metric::L2);
    assert_eq!(t.width, 2);
    assert_eq!(t.row(0), &[1, 0]);
}

#[test]
fn metric_names_match_ann_benchmarks() {
    assert_eq!(Metric::parse("euclidean").unwrap(), Metric::L2);
    assert_eq!(Metric::parse("angular").unwrap(), Metric::Cosine);
    assert_eq!(Metric::parse("ip").unwrap(), Metric::Dot);
    assert!(Metric::parse("hamming").is_err());
}

#[test]
fn synthetic_vectors_are_deterministic() {
    let a = synthetic(300, 8, 42);
    assert_eq!(a.len(), 300);
    assert_eq!(a, synthetic(300, 8, 42));
    assert_ne!(a, synthetic(300, 8, 43));
}

// ------------------------------------------------------------------ metrics

#[test]
fn recall_counts_set_membership_within_k() {
    let truth = [1, 2, 3, 4];
    assert_eq!(
        recall_at_k(&[3, 1, 2], &truth, 3),
        1.0,
        "order does not matter"
    );
    assert_eq!(recall_at_k(&[1, 9, 9], &truth, 3), 1.0 / 3.0);
    assert_eq!(
        recall_at_k(&[1], &truth, 3),
        1.0 / 3.0,
        "a short answer is charged for the gap"
    );
    assert_eq!(
        recall_at_k(&[9, 9, 9, 1], &truth, 3),
        0.0,
        "answers past k earn nothing"
    );
    assert_eq!(
        recall_at_k(&[1, 1, 1], &truth, 3),
        1.0 / 3.0,
        "a repeated id counts once"
    );
    assert_eq!(
        recall_at_k(&[4], &truth, 3),
        0.0,
        "only the true first k count"
    );
}

#[test]
fn percentiles_are_nearest_rank() {
    let ms = |n: u64| Duration::from_millis(n);
    let v: Vec<Duration> = (1..=100).map(ms).collect();
    assert_eq!(percentile(&v, 50.0), ms(50));
    assert_eq!(percentile(&v, 99.0), ms(99));
    assert_eq!(percentile(&v, 100.0), ms(100));
    assert_eq!(percentile(&v, 0.0), ms(1));
    assert_eq!(percentile(&[ms(7)], 99.0), ms(7));
    assert_eq!(percentile(&[], 50.0), Duration::ZERO);
}

fn point(ef: usize, recall: f64, qps: f64) -> Point {
    Point {
        ef,
        threads: 1,
        recall,
        qps,
        p50: Duration::ZERO,
        p99: Duration::ZERO,
        mean: Duration::ZERO,
        errors: 0,
    }
}

#[test]
fn the_frontier_drops_dominated_points() {
    let pts = [
        point(10, 0.80, 9000.0),
        point(20, 0.90, 6000.0),
        point(30, 0.85, 5000.0), // beaten by ef=20 on both
        point(40, 0.99, 2000.0),
        point(50, 0.99, 1000.0), // same recall, slower
    ];
    let f: Vec<usize> = pareto_frontier(&pts).iter().map(|p| p.ef).collect();
    assert_eq!(f, vec![10, 20, 40]);
}

#[test]
fn run_point_scores_every_query_and_counts_failures() {
    let queries = Vectors {
        dim: 1,
        data: vec![0.0, 1.0, 2.0, 3.0],
    };
    let truth = Neighbours {
        width: 1,
        ids: vec![0, 1, 2, 3],
    };
    // Right for queries 0 and 1, wrong for 2, an error for 3.
    let search = |q: &[f32], _k: usize, _ef: usize| match q[0] as u32 {
        3 => Err("boom".to_string()),
        2 => Ok(vec![99]),
        i => Ok(vec![i]),
    };
    for threads in [1, 3] {
        let p = run_point(&search, &queries, &truth, 1, 16, threads);
        assert_eq!(p.recall, 0.5, "{threads} threads");
        assert_eq!(p.errors, 1);
        assert_eq!((p.ef, p.threads), (16, threads));
        assert!(p.p50 <= p.p99 && p.qps > 0.0);
    }
}

// ---------------------------------------------------------------- the store

/// Below the exact-search threshold the index scans every vector, so recall
/// must be 1.0 on both paths: any shortfall is the harness, not HNSW.
#[test]
fn both_paths_score_perfect_recall_on_an_exactly_searched_index() {
    let base = synthetic(100, 8, 1);
    let queries = synthetic(20, 8, 2);
    for metric in [Metric::L2, Metric::Cosine] {
        let truth = exact_ground_truth(&base, &queries, 5, metric);
        let loaded = load(
            &base,
            metric,
            Build {
                m: 16,
                ef_construction: 200,
            },
        )
        .unwrap();
        assert_eq!(loaded.position.len(), 100);

        let direct = |q: &[f32], k: usize, ef: usize| direct_search(&loaded, q, k, ef);
        let p = run_point(&direct, &queries, &truth, 5, 40, 2);
        assert_eq!((p.recall, p.errors), (1.0, 0), "direct, {metric:?}");

        let engine = samyama::query::QueryEngine::new();
        let cypher = |q: &[f32], k: usize, ef: usize| cypher_search(&loaded, &engine, q, k, ef);
        let p = run_point(&cypher, &queries, &truth, 5, 40, 1);
        assert_eq!((p.recall, p.errors), (1.0, 0), "cypher, {metric:?}");
    }
}

/// Above the threshold the search is HNSW. A wide search over clustered data
/// should find nearly every true neighbour; the bar is loose because HNSW's
/// layer assignment is random, and this checks the harness end to end, not
/// the index's recall.
#[test]
fn a_wide_hnsw_search_scores_high_recall_and_both_paths_agree() {
    let base = synthetic(1500, 16, 3);
    let queries = synthetic(50, 16, 4);
    let truth = exact_ground_truth(&base, &queries, 10, Metric::L2);
    let loaded = load(
        &base,
        Metric::L2,
        Build {
            m: 16,
            ef_construction: 200,
        },
    )
    .unwrap();

    let direct = |q: &[f32], k: usize, ef: usize| direct_search(&loaded, q, k, ef);
    let wide = run_point(&direct, &queries, &truth, 10, 400, 1);
    assert_eq!(wide.errors, 0);
    assert!(
        wide.recall >= 0.9,
        "recall@10 at ef=400 was {}",
        wide.recall
    );

    let engine = samyama::query::QueryEngine::new();
    for qi in 0..5 {
        let q = queries.row(qi);
        let mut a = direct_search(&loaded, q, 10, 400).unwrap();
        let mut b = cypher_search(&loaded, &engine, q, 10, 400).unwrap();
        a.sort();
        b.sort();
        assert_eq!(a, b, "query {qi}: the Cypher path answered differently");
    }
}

#[test]
fn files_written_by_the_harness_load_into_the_store() {
    let dir = tmp();
    let base = synthetic(50, 4, 5);
    let p = dir.path().join("base.fvecs");
    write_fvecs(&p, &base).unwrap();
    let back = read_fvecs(&p, None).unwrap();
    let loaded = load(
        &back,
        Metric::L2,
        Build {
            m: 16,
            ef_construction: 100,
        },
    )
    .unwrap();
    // A base vector's nearest neighbour is itself.
    assert_eq!(direct_search(&loaded, base.row(7), 1, 40).unwrap(), vec![7]);
}
