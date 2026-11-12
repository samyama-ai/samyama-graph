//! The vector cost profile's own arithmetic (#1989).
//!
//! `benches/vector_cost_profile.rs` reports bytes per vector against a
//! theoretical floor and dollars per million queries. Those figures get
//! quoted, and a bench target cannot run its own tests, so the code that
//! derives them lives in `benches/vector_cost_common` and is checked here.
//!
//! Nothing here times anything: the cost is checked as arithmetic on given
//! QPS figures, and memory as ratios between quantizations of one index.

#[path = "../benches/vector_cost_common/mod.rs"]
mod vector_cost_common;

use samyama::graph::NodeId;
use samyama::vector::index::Quantization;
use samyama::vector::{DistanceMetric, VectorIndex};
use std::time::Duration;
use vector_cost_common::*;

#[test]
fn every_profiled_quantization_is_selectable_by_its_row_name() {
    assert!(!QUANTIZATIONS.is_empty());
    for (name, q) in QUANTIZATIONS {
        assert_eq!(Quantization::parse(name), Some(*q), "row `{name}`");
    }
    let mut seen = std::collections::HashSet::new();
    for (name, q) in QUANTIZATIONS {
        assert!(seen.insert(format!("{q:?}")), "`{name}` profiled twice");
    }
}

#[test]
fn theoretical_floor_is_values_plus_layer0_links() {
    // 96 f32 dims and M=16: 384 bytes of values, 32 links of 4 bytes.
    assert_eq!(theoretical_bytes_per_vector(96, 4, 16), 384 + 128);
    assert_eq!(theoretical_bytes_per_vector(768, 2, 16), 1536 + 128);
    assert_eq!(theoretical_bytes_per_vector(1536, 4, 32), 6144 + 256);
}

#[test]
fn operating_point_is_the_fastest_point_meeting_the_target() {
    let sweep = [
        Sweep {
            ef: 10,
            recall: 0.80,
            qps: 9000.0,
        },
        Sweep {
            ef: 40,
            recall: 0.95,
            qps: 5000.0,
        },
        Sweep {
            ef: 80,
            recall: 0.97,
            qps: 6000.0,
        },
        Sweep {
            ef: 160,
            recall: 0.99,
            qps: 2000.0,
        },
    ];
    let p = operating_point(&sweep, 0.95).expect("reached");
    assert_eq!(p.ef, 80);
    assert_eq!(operating_point(&sweep, 0.99).map(|p| p.ef), Some(160));
    assert_eq!(operating_point(&sweep, 0.999), None);
    assert_eq!(operating_point(&[], 0.5), None);
    // A point that answered nothing has no cost to quote.
    assert_eq!(
        operating_point(
            &[Sweep {
                ef: 10,
                recall: 1.0,
                qps: 0.0
            }],
            0.5
        ),
        None
    );
}

#[test]
fn dollars_per_million_queries() {
    // 1,000 QPS answers a million queries in 1,000 s.
    let h = hours_for(1e6, 1000.0);
    assert!((h - 1000.0 / 3600.0).abs() < 1e-12);
    let d = dollars_per_million(1000.0, 3.6);
    assert!((d - 1.0).abs() < 1e-12, "{d}");
    // Twice the throughput, half the cost.
    let ratio = dollars_per_million(500.0, 1.0) / dollars_per_million(1000.0, 1.0);
    assert!((ratio - 2.0).abs() < 1e-12);
    assert!(dollars_per_million(0.0, 1.0).is_infinite());
    assert!((dollars_for(Duration::from_secs(1800), 2.0) - 1.0).abs() < 1e-12);
}

#[test]
fn rss_sampler_keeps_the_maximum_and_reports_unknown_as_unknown() {
    fn unknown() -> Option<u64> {
        None
    }
    fn fixed() -> Option<u64> {
        Some(42)
    }
    let s = RssSampler::start(Duration::from_millis(1), unknown);
    assert_eq!(s.stop(), None);
    let s = RssSampler::start(Duration::from_millis(1), fixed);
    assert_eq!(s.stop(), Some(42));
}

/// The bytes the profile divides by the vector count. Measured on a real
/// index, as ratios: fp16 holds half the vector bytes of f32, and the resident
/// figure is never below the theoretical floor's value part.
#[test]
fn profiled_byte_counts_are_consistent_across_quantizations() {
    let (n, dim) = (300usize, 16usize);
    let vector = |i: usize| -> Vec<f32> {
        (0..dim)
            .map(|d| ((i * 31 + d * 7) % 97) as f32 / 97.0 - 0.5)
            .collect()
    };
    let mut vector_bytes = Vec::new();
    for (name, q) in QUANTIZATIONS {
        let mut index = VectorIndex::with_quantization(dim, DistanceMetric::L2, *q);
        for i in 0..n {
            index.add(NodeId::new(i as u64), &vector(i)).unwrap();
        }
        assert_eq!(index.len(), n, "{name}");
        let values = n * dim * q.bytes_per_value();
        assert!(index.resident_bytes() >= values, "{name}");
        assert!(
            index.resident_bytes() >= index.vector_bytes() + index.graph_bytes(),
            "{name}"
        );
        // The restart the profile times: dump, then load rebuilds every vector.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("index.vec");
        index.dump(&path).unwrap();
        let reloaded =
            VectorIndex::load_with_quantization(&path, dim, DistanceMetric::L2, *q).unwrap();
        assert_eq!(reloaded.len(), n, "{name}");
        assert_eq!(reloaded.vector_bytes(), index.vector_bytes(), "{name}");
        vector_bytes.push((*q, index.vector_bytes()));
    }
    let f32_bytes = vector_bytes.iter().find(|(q, _)| *q == Quantization::None);
    let f16_bytes = vector_bytes.iter().find(|(q, _)| *q == Quantization::Fp16);
    if let (Some((_, full)), Some((_, half))) = (f32_bytes, f16_bytes) {
        assert_eq!(*full, 2 * *half);
    }
}
