//! Simulated binary crossover and polynomial mutation (Deb and Agrawal 1995;
//! Deb and Goyal 1996), shared by the evolutionary multi-objective solvers
//! that use them as published: SPEA2 and MOEA/D.

use ndarray::Array1;
use rand::Rng;

/// Distribution index of simulated binary crossover: larger keeps children
/// closer to their parents.
pub const CROSSOVER_ETA: f64 = 15.0;
/// Distribution index of polynomial mutation.
pub const MUTATION_ETA: f64 = 20.0;
/// Probability that a pair of parents is crossed at all.
pub const CROSSOVER_RATE: f64 = 0.9;
/// Below this, two parent coordinates are treated as equal and not crossed.
const SAME_COORDINATE: f64 = 1e-14;

/// Simulated binary crossover: one child, each coordinate spread around its
/// parents with a polynomial distribution of index [`CROSSOVER_ETA`], then
/// kept in bounds.
pub fn sbx(
    p1: &Array1<f64>,
    p2: &Array1<f64>,
    lower: &Array1<f64>,
    upper: &Array1<f64>,
    rng: &mut impl Rng,
) -> Array1<f64> {
    let mut child = p1.clone();
    if rng.gen::<f64>() > CROSSOVER_RATE {
        return child;
    }
    for j in 0..child.len() {
        if rng.gen_bool(0.5) || (p1[j] - p2[j]).abs() < SAME_COORDINATE {
            continue;
        }
        let u: f64 = rng.gen();
        let beta = if u <= 0.5 {
            (2.0 * u).powf(1.0 / (CROSSOVER_ETA + 1.0))
        } else {
            (1.0 / (2.0 * (1.0 - u))).powf(1.0 / (CROSSOVER_ETA + 1.0))
        };
        let mid = 0.5 * (p1[j] + p2[j]);
        let half = 0.5 * beta * (p1[j] - p2[j]).abs();
        let value = if rng.gen_bool(0.5) {
            mid - half
        } else {
            mid + half
        };
        child[j] = value.clamp(lower[j], upper[j]);
    }
    child
}

/// Polynomial mutation of each coordinate with probability `1 / dim`, kept
/// in bounds.
pub fn polynomial_mutation(
    vars: &mut Array1<f64>,
    lower: &Array1<f64>,
    upper: &Array1<f64>,
    rng: &mut impl Rng,
) {
    let rate = 1.0 / vars.len().max(1) as f64;
    for j in 0..vars.len() {
        if rng.gen::<f64>() >= rate {
            continue;
        }
        let u: f64 = rng.gen();
        let delta = if u < 0.5 {
            (2.0 * u).powf(1.0 / (MUTATION_ETA + 1.0)) - 1.0
        } else {
            1.0 - (2.0 * (1.0 - u)).powf(1.0 / (MUTATION_ETA + 1.0))
        };
        vars[j] = (vars[j] + delta * (upper[j] - lower[j])).clamp(lower[j], upper[j]);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn crossover_and_mutation_stay_in_bounds() {
        let mut rng = crate::common::rng::solver_rng(Some(13));
        let (lo, hi) = (Array1::from_elem(4, -1.0), Array1::from_elem(4, 1.0));
        let (a, b) = (Array1::from_elem(4, -1.0), Array1::from_elem(4, 1.0));
        for _ in 0..1000 {
            let mut c = sbx(&a, &b, &lo, &hi, &mut rng);
            polynomial_mutation(&mut c, &lo, &hi, &mut rng);
            assert!(c.iter().all(|v| (-1.0..=1.0).contains(v)), "{c:?}");
        }
    }

    #[test]
    fn identical_parents_give_their_own_child() {
        let mut rng = crate::common::rng::solver_rng(Some(13));
        let (lo, hi) = (Array1::from_elem(3, 0.0), Array1::from_elem(3, 1.0));
        let p = Array1::from_elem(3, 0.25);
        assert_eq!(sbx(&p, &p, &lo, &hi, &mut rng), p);
    }
}
