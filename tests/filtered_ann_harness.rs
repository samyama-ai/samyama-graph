#[path = "../benches/ann_common/mod.rs"]
mod ann_common;
#[path = "../benches/filtered_ann/mod.rs"]
mod filtered_ann;
use ann_common::Vectors;
use filtered_ann::{oracle, predicate, Correlation};
#[test]
fn correlation_changes_neighbourhood_without_changing_selectivity() {
    let base = Vectors {
        dim: 1,
        data: (0..100).map(|i| i as f32).collect(),
    };
    let positive = predicate(&base, &[0.0], 10, Correlation::Positive, 4);
    let negative = predicate(&base, &[0.0], 10, Correlation::Negative, 4);
    let random = predicate(&base, &[0.0], 10, Correlation::None, 4);
    for mask in [&positive, &negative, &random] {
        assert_eq!(mask.iter().filter(|v| **v).count(), 10);
    }
    assert!(positive[..10].iter().all(|v| *v));
    assert!(negative[90..].iter().all(|v| *v));
    assert_eq!(random, predicate(&base, &[0.0], 10, Correlation::None, 4));
    assert_eq!(oracle(&base, &[0.0], &positive, 3), vec![0, 1, 2]);
    assert_eq!(oracle(&base, &[0.0], &negative, 3), vec![90, 91, 92]);
}
#[test]
fn oracle_handles_short_and_empty_predicate_sets() {
    let base = Vectors {
        dim: 1,
        data: vec![2.0, 1.0, 0.0],
    };
    assert_eq!(oracle(&base, &[0.0], &[true, false, true], 10), vec![2, 0]);
    assert!(oracle(&base, &[0.0], &[false; 3], 10).is_empty());
}
