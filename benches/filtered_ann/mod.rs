//! Deterministic query-dependent predicates with identical cardinality across
//! correlations. Predicate construction and exact oracles are outside timing.
#![allow(dead_code)]
use super::ann_common::{Metric, Vectors};

#[derive(Clone, Copy, Debug)]
pub enum Correlation {
    Positive,
    None,
    Negative,
}
impl Correlation {
    pub fn name(self) -> &'static str {
        match self {
            Self::Positive => "positive",
            Self::None => "none",
            Self::Negative => "negative",
        }
    }
}

pub fn predicate(
    base: &Vectors,
    query: &[f32],
    count: usize,
    correlation: Correlation,
    seed: u64,
) -> Vec<bool> {
    use rand::{seq::SliceRandom, SeedableRng};
    let mut ranked: Vec<usize> = (0..base.len()).collect();
    match correlation {
        Correlation::None => ranked.shuffle(&mut rand::rngs::StdRng::seed_from_u64(seed)),
        Correlation::Positive | Correlation::Negative => {
            let distances: Vec<f32> = (0..base.len())
                .map(|i| Metric::L2.distance(query, base.row(i)))
                .collect();
            ranked.sort_by(|a, b| distances[*a].total_cmp(&distances[*b]).then(a.cmp(b)));
            if matches!(correlation, Correlation::Negative) {
                ranked.reverse();
            }
        }
    }
    let mut mask = vec![false; base.len()];
    for id in ranked.into_iter().take(count) {
        mask[id] = true;
    }
    mask
}

/// Independent f64 oracle; does not call the index's distance kernel.
pub fn oracle(base: &Vectors, query: &[f32], mask: &[bool], k: usize) -> Vec<u32> {
    let mut ranked: Vec<(f64, u32)> = (0..base.len())
        .filter(|i| mask[*i])
        .map(|i| {
            let d = query
                .iter()
                .zip(base.row(i))
                .map(|(a, b)| (*a as f64 - *b as f64).powi(2))
                .sum();
            (d, i as u32)
        })
        .collect();
    ranked.sort_by(|a, b| a.0.total_cmp(&b.0).then(a.1.cmp(&b.1)));
    ranked.into_iter().take(k).map(|(_, i)| i).collect()
}
