//! The scale benchmark's own arithmetic (#1984).
//!
//! `benches/vector_scale_benchmark.rs` prints a memory verdict, an operating
//! point, a cost per million queries and an NDS-05 line, and each is a figure
//! someone will quote. The code that derives them is in
//! `benches/vector_scale_common` and is checked here on tiny inputs. Nothing
//! here times anything.

#[path = "../benches/vector_scale_common/mod.rs"]
mod vector_scale_common;

use vector_scale_common::*;

/// `rows` records of `dim` dimensions in `.fvecs` layout.
fn fvecs_bytes(rows: usize, dim: usize) -> Vec<u8> {
    let mut out = Vec::new();
    for r in 0..rows {
        out.extend_from_slice(&(dim as i32).to_le_bytes());
        for d in 0..dim {
            out.extend_from_slice(&((r * dim + d) as f32).to_le_bytes());
        }
    }
    out
}

// ------------------------------------------------------------------- shape

#[test]
fn fvecs_shape_is_read_from_the_header_and_length() {
    let dir = tempfile::tempdir().unwrap();
    let p = dir.path().join("base.fvecs");
    std::fs::write(&p, fvecs_bytes(5, 3)).unwrap();
    assert_eq!(fvecs_shape(&p).unwrap(), (5, 3));
}

#[test]
fn a_cut_short_fvecs_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let p = dir.path().join("base.fvecs");
    let bytes = fvecs_bytes(2, 4);
    std::fs::write(&p, &bytes[..bytes.len() - 3]).unwrap();
    let err = fvecs_shape(&p).unwrap_err();
    assert!(err.contains("not a whole number"), "{err}");
}

#[test]
fn shape_from_len_counts_records() {
    // 768-d records are 4 + 3072 bytes.
    assert_eq!(shape_from_len(3076 * 1000, 768), Some(1000));
    assert_eq!(shape_from_len(3076 * 1000 + 1, 768), None);
    assert_eq!(shape_from_len(0, 768), Some(0));
}

// ------------------------------------------------------------------ memory

#[test]
fn the_estimate_is_copies_plus_links_plus_overhead() {
    let per = 4 * 768 * 4 + 2 * 16 * LINK_BYTES + NODE_OVERHEAD_BYTES;
    assert_eq!(estimate_bytes(1, 768, 16, 4), per as u128);
    assert_eq!(estimate_bytes(1000, 768, 16, 4), 1000 * per as u128);
}

#[test]
fn ten_million_cohere_vectors_do_not_fit_in_a_64_gib_host() {
    // The issue's own arithmetic: 10M x 768 x 4 B x 3 copies is ~92 GB
    // before the graph. The bench holds a fourth copy, the base set.
    let est = estimate_bytes(10_000_000, 768, 16, BENCH_VECTOR_COPIES);
    assert!(gib(est) > 100.0, "{}", gib(est));
    assert_eq!(memory_fit(est, Some(64 << 30), 0.1), Fit::TooBig);
    assert_eq!(memory_fit(est, Some(256 << 30), 0.1), Fit::Fits);
}

#[test]
fn headroom_is_kept_free() {
    let avail = 1000u64;
    assert_eq!(memory_fit(900, Some(avail), 0.1), Fit::Fits);
    assert_eq!(memory_fit(901, Some(avail), 0.1), Fit::TooBig);
    assert_eq!(memory_fit(1000, Some(avail), 0.0), Fit::Fits);
}

#[test]
fn unknown_memory_is_reported_as_unknown() {
    assert_eq!(memory_fit(1, None, 0.1), Fit::Unknown);
}

#[test]
fn mem_available_is_parsed_from_meminfo() {
    let text =
        "MemTotal:       65536000 kB\nMemFree:         1000 kB\nMemAvailable:   32768000 kB\n";
    assert_eq!(parse_mem_available(text), Some(32_768_000 * 1024));
    assert_eq!(parse_mem_available("MemTotal: 1 kB\n"), None);
}

// ---------------------------------------------------------- operating point

fn sp(ef: usize, r100: f64, r10: f64, qps: f64) -> SweepPoint {
    SweepPoint {
        ef,
        recall_at_100: r100,
        recall_at_10: r10,
        qps,
    }
}

#[test]
fn the_operating_point_is_the_fastest_that_meets_the_target() {
    let pts = [
        sp(100, 0.90, 0.93, 900.0),
        sp(200, 0.96, 0.98, 600.0),
        sp(400, 0.99, 0.99, 300.0),
    ];
    let op = operating_point(&pts, 0.95).unwrap();
    assert_eq!(op.point.ef, 200);
    assert!(op.meets_target);
}

#[test]
fn when_nothing_meets_the_target_the_best_recall_is_used_and_flagged() {
    let pts = [sp(100, 0.80, 0.85, 900.0), sp(200, 0.88, 0.90, 600.0)];
    let op = operating_point(&pts, 0.95).unwrap();
    assert_eq!(op.point.ef, 200);
    assert!(!op.meets_target);
    assert!(operating_point(&[], 0.95).is_none());
}

#[test]
fn max_qps_ignores_points_with_errors() {
    let pts = [
        (1, 500.0, 0),
        (4, 1800.0, 0),
        (8, 2500.0, 3),
        (16, 1700.0, 0),
    ];
    assert_eq!(max_qps(&pts), Some((4, 1800.0)));
    assert_eq!(max_qps(&[(1, 10.0, 1)]), None);
}

// -------------------------------------------------------------------- cost

#[test]
fn dollars_per_million_queries() {
    // 1000 QPS: a million queries take 1000 s; at $3.60/h that is $1.
    assert!((dollars_per_million(1000.0, 3.6) - 1.0).abs() < 1e-12);
    assert!(dollars_per_million(0.0, 3.6).is_infinite());
}

// ------------------------------------------------------------------ NDS-05

#[test]
fn nds05_needs_ten_million_vectors() {
    assert_eq!(nds05(10_000_000, 0.97), Nds05::Met);
    assert_eq!(nds05(10_000_000, 0.94), Nds05::NotMet);
    assert_eq!(
        nds05(1_000_000, 0.99),
        Nds05::NotEvidence { bar_reached: true }
    );
    assert_eq!(
        nds05(1_000_000, 0.50),
        Nds05::NotEvidence { bar_reached: false }
    );
    // The bar is inclusive.
    assert_eq!(nds05(10_000_000, 0.95), Nds05::Met);
}

#[test]
fn the_nds05_line_names_the_vector_count() {
    let line = nds05_line(1_000_000, 0.9712, 256);
    assert!(line.starts_with("NDS-05: NOT EVIDENCE"), "{line}");
    assert!(line.contains("at 1000000 vectors"), "{line}");
    assert!(line.contains("reaches"), "{line}");
    let line = nds05_line(10_000_000, 0.9, 512);
    assert!(line.starts_with("NDS-05: NOT MET"), "{line}");
    assert!(line.contains("at 10000000 vectors (ef=512)"), "{line}");
}
