//! Multi-objective optimization (MOO) substrate.
//!
//! Free-function utilities reusable by all MO-* solvers (MO-BMR, MO-BWR,
//! MO-BMWR, MO-Rao+DE, SAPHR). Existing solvers (NSGA-II, MOTLBO) inline
//! their own copies; new solvers should call into this module.
//!
//! Provides:
//!   - [`fast_non_dominated_sort`] — Deb-style FNDS, sets `rank` field.
//!   - [`crowding_distance`] — per-rank crowding, sets `crowding_distance` field.
//!   - [`constrained_dominates`] — Deb's constrained-dominance principle.
//!   - [`EliteArchive`] — bounded elite store with crowding-based truncation.
//!   - [`hypervolume_2d`] — exact 2-D hypervolume for Pareto fronts.
//!   - [`hypervolume`] — exact hypervolume for any number of objectives.
//!   - [`igd`] — Inverted Generational Distance against a reference set.
//!   - [`igd_plus`] — IGD+, which does not penalise a point for beating the
//!     reference.
//!   - [`spread`] — how evenly a front is spread, extremes included.
//!   - [`coverage`] — Zitzler's set coverage between two fronts.
//!   - [`quality_metrics`] — all five against a known front (OPT-03).
//!
//! Every objective is minimised throughout.

use crate::common::MultiObjectiveIndividual;

/// Constrained-dominance principle (Deb et al. 2002).
///
/// Returns `true` if (f1, v1) dominates (f2, v2) where `v` is total
/// constraint violation (0 = feasible).
///
/// 1. A feasible solution dominates an infeasible one.
/// 2. Among two infeasibles, the one with smaller violation dominates.
/// 3. Among two feasibles, standard Pareto dominance applies.
pub fn constrained_dominates(f1: &[f64], v1: f64, f2: &[f64], v2: f64) -> bool {
    if v1 == 0.0 && v2 > 0.0 {
        return true;
    }
    if v1 > 0.0 && v2 == 0.0 {
        return false;
    }
    if v1 > 0.0 && v2 > 0.0 {
        return v1 < v2;
    }
    let mut better = false;
    for i in 0..f1.len() {
        if f1[i] > f2[i] {
            return false;
        }
        if f1[i] < f2[i] {
            better = true;
        }
    }
    better
}

/// Fast Non-Dominated Sort. Mutates `population[*].rank` in place.
/// O(M·N²) where M = number of objectives, N = population size.
pub fn fast_non_dominated_sort(population: &mut [MultiObjectiveIndividual]) {
    let n = population.len();
    let mut dominance_counts = vec![0usize; n];
    let mut dominated_sets: Vec<Vec<usize>> = vec![Vec::new(); n];
    let mut fronts: Vec<Vec<usize>> = vec![Vec::new()];

    for i in 0..n {
        for j in 0..n {
            if i == j {
                continue;
            }
            if constrained_dominates(
                &population[i].fitness,
                population[i].constraint_violation,
                &population[j].fitness,
                population[j].constraint_violation,
            ) {
                dominated_sets[i].push(j);
            } else if constrained_dominates(
                &population[j].fitness,
                population[j].constraint_violation,
                &population[i].fitness,
                population[i].constraint_violation,
            ) {
                dominance_counts[i] += 1;
            }
        }
        if dominance_counts[i] == 0 {
            population[i].rank = 0;
            fronts[0].push(i);
        }
    }

    let mut i = 0;
    while !fronts[i].is_empty() {
        let mut next_front = Vec::new();
        for &p in &fronts[i] {
            for &q in &dominated_sets[p] {
                dominance_counts[q] -= 1;
                if dominance_counts[q] == 0 {
                    population[q].rank = i + 1;
                    next_front.push(q);
                }
            }
        }
        i += 1;
        fronts.push(next_front);
    }
}

/// Compute crowding distance for the subset `indices` (one rank).
/// Mutates `population[*].crowding_distance` in place.
pub fn crowding_distance(
    population: &mut [MultiObjectiveIndividual],
    indices: &[usize],
) {
    if indices.is_empty() {
        return;
    }
    let num_objectives = population[indices[0]].fitness.len();
    for &idx in indices {
        population[idx].crowding_distance = 0.0;
    }

    for m in 0..num_objectives {
        let mut sorted = indices.to_vec();
        sorted.sort_by(|&a, &b| {
            crate::common::ascending_nan_last(population[a].fitness[m], population[b].fitness[m])
        });

        let min_val = population[*sorted.first().unwrap()].fitness[m];
        let max_val = population[*sorted.last().unwrap()].fitness[m];
        let range = max_val - min_val;

        population[*sorted.first().unwrap()].crowding_distance = f64::INFINITY;
        population[*sorted.last().unwrap()].crowding_distance = f64::INFINITY;

        if range > 1e-12 {
            for k in 1..(sorted.len() - 1) {
                let prev = population[sorted[k - 1]].fitness[m];
                let next = population[sorted[k + 1]].fitness[m];
                population[sorted[k]].crowding_distance += (next - prev) / range;
            }
        }
    }
}

/// Run FNDS + per-rank crowding distance over the whole population.
pub fn evaluate_population(population: &mut [MultiObjectiveIndividual]) {
    fast_non_dominated_sort(population);
    let mut rank = 0;
    loop {
        let indices: Vec<usize> = population
            .iter()
            .enumerate()
            .filter(|(_, ind)| ind.rank == rank)
            .map(|(i, _)| i)
            .collect();
        if indices.is_empty() {
            break;
        }
        crowding_distance(population, &indices);
        rank += 1;
    }
}

/// Bounded archive of non-dominated elites, truncated by crowding distance.
pub struct EliteArchive {
    pub capacity: usize,
    pub members: Vec<MultiObjectiveIndividual>,
}

impl EliteArchive {
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            members: Vec::new(),
        }
    }

    /// Insert a candidate; archive retains only rank-0 members up to capacity.
    pub fn insert(&mut self, candidate: MultiObjectiveIndividual) {
        self.members.push(candidate);
        evaluate_population(&mut self.members);
        // Keep only rank 0
        self.members.retain(|m| m.rank == 0);
        // If still over capacity, drop lowest crowding distance (most crowded).
        if self.members.len() > self.capacity {
            self.members.sort_by(|a, b| {
                crate::common::descending_nan_last(a.crowding_distance, b.crowding_distance)
            });
            self.members.truncate(self.capacity);
        }
    }
}

/// Exact 2-D hypervolume given a reference point (assumed dominated by every
/// point in `front`). Front entries are 2-vectors of objective values.
/// All objectives are minimized.
pub fn hypervolume_2d(front: &[Vec<f64>], reference: [f64; 2]) -> f64 {
    let mut points: Vec<&Vec<f64>> = front
        .iter()
        .filter(|p| p[0] <= reference[0] && p[1] <= reference[1])
        .collect();
    if points.is_empty() {
        return 0.0;
    }
    points.sort_by(|a, b| a[0].partial_cmp(&b[0]).unwrap_or(std::cmp::Ordering::Equal));

    let mut hv = 0.0;
    let mut prev_y = reference[1];
    for p in points {
        if p[1] < prev_y {
            hv += (reference[0] - p[0]) * (prev_y - p[1]);
            prev_y = p[1];
        }
    }
    hv
}

/// Inverted Generational Distance: average min Euclidean distance from each
/// point in `reference_front` to the nearest point in `approx_front`.
/// Lower is better. Both fronts are M-vectors of objective values.
pub fn igd(approx_front: &[Vec<f64>], reference_front: &[Vec<f64>]) -> f64 {
    if reference_front.is_empty() || approx_front.is_empty() {
        return f64::INFINITY;
    }
    let mut sum = 0.0;
    for r in reference_front {
        let mut d_min = f64::INFINITY;
        for a in approx_front {
            let mut d2 = 0.0;
            for k in 0..r.len() {
                let diff = r[k] - a[k];
                d2 += diff * diff;
            }
            let d = d2.sqrt();
            if d < d_min {
                d_min = d;
            }
        }
        sum += d_min;
    }
    sum / reference_front.len() as f64
}

/// Whether `a` weakly dominates `b`: no worse in any objective.
pub fn weakly_dominates(a: &[f64], b: &[f64]) -> bool {
    a.len() == b.len() && a.iter().zip(b).all(|(x, y)| x <= y)
}

fn distance(a: &[f64], b: &[f64]) -> f64 {
    a.iter()
        .zip(b)
        .map(|(x, y)| (x - y).powi(2))
        .sum::<f64>()
        .sqrt()
}

/// The shortest distance from `point` to any member of `set`.
fn distance_to_set(point: &[f64], set: &[Vec<f64>]) -> f64 {
    set.iter()
        .map(|q| distance(point, q))
        .fold(f64::INFINITY, f64::min)
}

/// Exact hypervolume of `front` against `reference`, for any number of
/// objectives: the volume dominated by the front and bounded by the
/// reference point. Larger is better.
///
/// Points that do not lie strictly inside the reference box contribute
/// nothing and are dropped, as are points of the wrong length. Computed by
/// slicing along the last objective (HSO) down to [`hypervolume_2d`]; the
/// cost grows as `n^(m-1)` in the number of points `n` and objectives `m`,
/// which is immediate for the fronts a solver returns (hundreds of points,
/// two or three objectives) and slow far beyond that.
pub fn hypervolume(front: &[Vec<f64>], reference: &[f64]) -> f64 {
    let inside: Vec<Vec<f64>> = front
        .iter()
        .filter(|p| p.len() == reference.len() && p.iter().zip(reference).all(|(v, r)| v < r))
        .cloned()
        .collect();
    sliced_hypervolume(inside, reference)
}

fn sliced_hypervolume(mut points: Vec<Vec<f64>>, reference: &[f64]) -> f64 {
    let m = reference.len();
    if points.is_empty() || m == 0 {
        return 0.0;
    }
    if m == 1 {
        return reference[0] - points.iter().map(|p| p[0]).fold(f64::INFINITY, f64::min);
    }
    if m == 2 {
        return hypervolume_2d(&points, [reference[0], reference[1]]);
    }
    let last = m - 1;
    points.sort_by(|a, b| a[last].total_cmp(&b[last]));
    let mut volume = 0.0;
    for i in 0..points.len() {
        let top = points.get(i + 1).map_or(reference[last], |next| next[last]);
        let depth = top - points[i][last];
        if depth > 0.0 {
            // Between this point's last objective and the next one's, the
            // dominated region is what the points so far dominate in the
            // remaining objectives.
            let slice: Vec<Vec<f64>> = points[..=i].iter().map(|p| p[..last].to_vec()).collect();
            volume += depth * sliced_hypervolume(slice, &reference[..last]);
        }
    }
    volume
}

/// IGD+ (Ishibuchi et al. 2015): like [`igd`], but a point is only charged
/// for how far it is *worse* than a reference point in each objective, never
/// for being better. So it cannot reward a front for missing the reference in
/// a direction that would have been an improvement. Lower is better; infinite
/// when either front is empty.
pub fn igd_plus(approx_front: &[Vec<f64>], reference_front: &[Vec<f64>]) -> f64 {
    if reference_front.is_empty() || approx_front.is_empty() {
        return f64::INFINITY;
    }
    let shortfall = |z: &[f64], a: &[f64]| {
        a.iter()
            .zip(z)
            .map(|(ak, zk)| (ak - zk).max(0.0).powi(2))
            .sum::<f64>()
            .sqrt()
    };
    reference_front
        .iter()
        .map(|z| {
            approx_front
                .iter()
                .map(|a| shortfall(z, a))
                .fold(f64::INFINITY, f64::min)
        })
        .sum::<f64>()
        / reference_front.len() as f64
}

/// Generalised spread Δ (Zhou, Jin, Zhang, Sendhoff and Tsang 2006): how
/// evenly `approx_front` covers the known front, its extremes included.
///
/// `Δ = (Σ d(eⱼ, S) + Σ |d(x, S) − d̄|) / (Σ d(eⱼ, S) + |S|·d̄)`, where `eⱼ` is
/// the point of the reference front that is best in objective `j`, `d(x, S)`
/// the distance from `x` to its nearest other point in the approximation `S`,
/// and `d̄` the mean of those. 0 is a perfectly even front that reaches every
/// extreme; larger is worse. Deb's two-objective Δ is the case `m = 2` with
/// consecutive distances in place of nearest-neighbour ones. Infinite when
/// either front is empty.
pub fn spread(approx_front: &[Vec<f64>], reference_front: &[Vec<f64>]) -> f64 {
    let (Some(first), false) = (reference_front.first(), approx_front.is_empty()) else {
        return f64::INFINITY;
    };
    let extremes: Vec<&Vec<f64>> = (0..first.len())
        .filter_map(|j| reference_front.iter().min_by(|a, b| a[j].total_cmp(&b[j])))
        .collect();
    let to_extremes: f64 = extremes
        .iter()
        .map(|e| distance_to_set(e, approx_front))
        .sum();
    let nearest: Vec<f64> = approx_front
        .iter()
        .enumerate()
        .map(|(i, x)| {
            approx_front
                .iter()
                .enumerate()
                .filter(|(k, _)| *k != i)
                .map(|(_, y)| distance(x, y))
                .fold(f64::INFINITY, f64::min)
        })
        .map(|d| if d.is_finite() { d } else { 0.0 })
        .collect();
    let mean = nearest.iter().sum::<f64>() / nearest.len() as f64;
    let unevenness: f64 = nearest.iter().map(|d| (d - mean).abs()).sum();
    let denominator = to_extremes + nearest.len() as f64 * mean;
    if denominator == 0.0 {
        0.0
    } else {
        (to_extremes + unevenness) / denominator
    }
}

/// Set coverage C(A, B) (Zitzler and Thiele 1999): the fraction of `b` that
/// some point of `a` weakly dominates, from 0 to 1. Not symmetric: C(A, B)
/// and C(B, A) answer different questions. 0 when `b` is empty, since there
/// is nothing in it to cover.
pub fn coverage(a: &[Vec<f64>], b: &[Vec<f64>]) -> f64 {
    if b.is_empty() {
        return 0.0;
    }
    let covered = b
        .iter()
        .filter(|q| a.iter().any(|p| weakly_dominates(p, q)))
        .count();
    covered as f64 / b.len() as f64
}

/// How far past the reference front's worst value in each objective the
/// hypervolume reference point is placed, as a fraction of that objective's
/// range. The usual choice in the literature; it gives the extremes of the
/// front a non-zero contribution.
pub const HV_REFERENCE_MARGIN: f64 = 0.1;

/// The hypervolume reference point for a known front: its nadir (worst value
/// in each objective), pushed out by [`HV_REFERENCE_MARGIN`] of the range.
/// An objective with no range is pushed out by the margin itself, so the
/// point is still strictly outside the front. Empty for an empty front.
pub fn hv_reference_point(reference_front: &[Vec<f64>]) -> Vec<f64> {
    let Some(first) = reference_front.first() else {
        return Vec::new();
    };
    (0..first.len())
        .map(|j| {
            let (lo, hi) = reference_front
                .iter()
                .fold((f64::INFINITY, f64::NEG_INFINITY), |(lo, hi), p| {
                    (lo.min(p[j]), hi.max(p[j]))
                });
            let range = hi - lo;
            hi + HV_REFERENCE_MARGIN * if range > 0.0 { range } else { 1.0 }
        })
        .collect()
}

/// The five multi-objective quality measures of OPT-03, for one front
/// against a known one.
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct QualityMetrics {
    /// [`hypervolume`] against [`QualityMetrics::reference_point`]. Larger is better.
    pub hypervolume: f64,
    /// [`igd`] to the known front. Lower is better.
    pub igd: f64,
    /// [`igd_plus`] to the known front. Lower is better.
    pub igd_plus: f64,
    /// [`spread`] against the known front's extremes. Lower is better.
    pub spread: f64,
    /// C(known front, front): the share of the front's points that the
    /// known front weakly dominates, so that are provably not optimal.
    /// Lower is better.
    pub coverage: f64,
    /// The point the hypervolume is measured from ([`hv_reference_point`]).
    pub reference_point: Vec<f64>,
    /// Points sampled on the known front.
    pub reference_front_size: usize,
}

/// All five measures for `front` against `reference_front`, or `None` when
/// either is empty or their objective counts differ, where no measure means
/// anything.
pub fn quality_metrics(front: &[Vec<f64>], reference_front: &[Vec<f64>]) -> Option<QualityMetrics> {
    let m = reference_front.first()?.len();
    if front.is_empty() || front.iter().chain(reference_front).any(|p| p.len() != m) {
        return None;
    }
    let reference_point = hv_reference_point(reference_front);
    Some(QualityMetrics {
        hypervolume: hypervolume(front, &reference_point),
        igd: igd(front, reference_front),
        igd_plus: igd_plus(front, reference_front),
        spread: spread(front, reference_front),
        coverage: coverage(reference_front, front),
        reference_point,
        reference_front_size: reference_front.len(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use ndarray::array;

    fn ind(fit: Vec<f64>, viol: f64) -> MultiObjectiveIndividual {
        MultiObjectiveIndividual::new(array![0.0], fit, viol)
    }

    #[test]
    fn dominance_basic() {
        assert!(constrained_dominates(&[1.0, 1.0], 0.0, &[2.0, 2.0], 0.0));
        assert!(!constrained_dominates(&[1.0, 2.0], 0.0, &[2.0, 1.0], 0.0));
    }

    #[test]
    fn dominance_constraints() {
        assert!(constrained_dominates(&[5.0], 0.0, &[1.0], 1.0));
        assert!(constrained_dominates(&[5.0], 0.5, &[1.0], 2.0));
    }

    #[test]
    fn fnds_assigns_ranks() {
        let mut pop = vec![
            ind(vec![1.0, 1.0], 0.0),
            ind(vec![2.0, 2.0], 0.0),
            ind(vec![3.0, 3.0], 0.0),
        ];
        fast_non_dominated_sort(&mut pop);
        assert_eq!(pop[0].rank, 0);
        assert_eq!(pop[1].rank, 1);
        assert_eq!(pop[2].rank, 2);
    }

    #[test]
    fn hv_unit_square() {
        let front = vec![vec![0.0, 1.0], vec![0.5, 0.5], vec![1.0, 0.0]];
        // Reference (2,2): staircase gives 2*1 + 1.5*0.5 + 1*0.5 = 3.25
        let hv = hypervolume_2d(&front, [2.0, 2.0]);
        assert!((hv - 3.25).abs() < 1e-9, "hv = {}", hv);
    }

    #[test]
    fn igd_zero_when_match() {
        let r = vec![vec![0.0, 1.0], vec![1.0, 0.0]];
        let a = r.clone();
        assert!(igd(&a, &r) < 1e-12);
    }

    const EPS: f64 = 1e-9;

    fn close(a: f64, b: f64) -> bool {
        (a - b).abs() < EPS
    }

    #[test]
    fn weak_dominance_includes_equality() {
        assert!(weakly_dominates(&[1.0, 2.0], &[1.0, 2.0]));
        assert!(weakly_dominates(&[1.0, 2.0], &[1.0, 3.0]));
        assert!(!weakly_dominates(&[1.0, 4.0], &[2.0, 3.0]));
        assert!(
            !weakly_dominates(&[1.0], &[1.0, 2.0]),
            "different lengths never dominate"
        );
    }

    #[test]
    fn hypervolume_agrees_with_the_2d_one() {
        let front = vec![vec![0.0, 1.0], vec![0.5, 0.5], vec![1.0, 0.0]];
        assert!(close(
            hypervolume(&front, &[2.0, 2.0]),
            hypervolume_2d(&front, [2.0, 2.0])
        ));
    }

    #[test]
    fn hypervolume_in_three_dimensions_by_hand() {
        // One point at the origin: the whole box, 2 x 3 x 4.
        assert!(close(
            hypervolume(&[vec![0.0, 0.0, 0.0]], &[2.0, 3.0, 4.0]),
            24.0
        ));
        // In a 2x2x2 box, (0,1,1) dominates [0,2]x[1,2]x[1,2] (volume 2) and
        // (1,0,1) dominates [1,2]x[0,2]x[1,2] (volume 2); they overlap in
        // [1,2]x[1,2]x[1,2] (volume 1), so together they dominate 3.
        let front = vec![vec![0.0, 1.0, 1.0], vec![1.0, 0.0, 1.0]];
        assert!(
            close(hypervolume(&front, &[2.0, 2.0, 2.0]), 3.0),
            "{}",
            hypervolume(&front, &[2.0, 2.0, 2.0])
        );
        // A dominated point adds nothing.
        let mut with_dominated = front.clone();
        with_dominated.push(vec![1.5, 1.5, 1.5]);
        assert!(close(hypervolume(&with_dominated, &[2.0, 2.0, 2.0]), 3.0));
    }

    #[test]
    fn hypervolume_ignores_points_outside_the_box_and_handles_one_objective() {
        assert_eq!(hypervolume(&[vec![3.0, 0.0]], &[2.0, 2.0]), 0.0);
        assert_eq!(hypervolume(&[], &[1.0, 1.0]), 0.0);
        assert!(close(hypervolume(&[vec![0.25], vec![0.5]], &[1.0]), 0.75));
    }

    #[test]
    fn igd_plus_does_not_charge_a_point_for_being_better() {
        let reference = vec![vec![1.0, 1.0]];
        // Better in both objectives: IGD charges the distance, IGD+ does not.
        let better = vec![vec![0.0, 0.0]];
        assert!(igd(&better, &reference) > 1.0);
        assert!(close(igd_plus(&better, &reference), 0.0));
        // Worse by 3 and 4: both charge 5.
        let worse = vec![vec![4.0, 5.0]];
        assert!(close(igd_plus(&worse, &reference), 5.0));
        assert!(igd_plus(&[], &reference).is_infinite());
    }

    #[test]
    fn spread_is_zero_for_an_even_front_that_reaches_both_ends() {
        let reference: Vec<Vec<f64>> = (0..=10)
            .map(|i| vec![i as f64 / 10.0, 1.0 - i as f64 / 10.0])
            .collect();
        assert!(close(spread(&reference, &reference), 0.0));
        // Bunched in the middle and missing the ends: clearly worse.
        let bunched = vec![vec![0.45, 0.55], vec![0.5, 0.5], vec![0.6, 0.4]];
        assert!(
            spread(&bunched, &reference) > 0.5,
            "{}",
            spread(&bunched, &reference)
        );
        assert!(spread(&[], &reference).is_infinite());
    }

    #[test]
    fn coverage_counts_what_is_weakly_dominated() {
        let a = vec![vec![1.0, 1.0]];
        let b = vec![
            vec![1.0, 1.0],
            vec![2.0, 2.0],
            vec![0.0, 3.0],
            vec![3.0, 0.0],
        ];
        assert!(close(coverage(&a, &b), 0.5));
        assert!(close(coverage(&b, &a), 1.0), "not symmetric");
        assert_eq!(coverage(&a, &[]), 0.0);
    }

    #[test]
    fn the_reference_point_sits_past_the_nadir_by_the_margin() {
        let front = vec![vec![0.0, 1.0], vec![1.0, 0.0]];
        let r = hv_reference_point(&front);
        assert!(
            close(r[0], 1.0 + HV_REFERENCE_MARGIN) && close(r[1], 1.0 + HV_REFERENCE_MARGIN),
            "{r:?}"
        );
        // An objective with no range still gets a point strictly beyond it.
        let flat = vec![vec![0.0, 2.0], vec![1.0, 2.0]];
        assert!(hv_reference_point(&flat)[1] > 2.0);
        assert!(hv_reference_point(&[]).is_empty());
    }

    #[test]
    fn quality_metrics_of_the_reference_front_itself_are_ideal() {
        let reference: Vec<Vec<f64>> = (0..=20)
            .map(|i| vec![i as f64 / 20.0, 1.0 - i as f64 / 20.0])
            .collect();
        let q = quality_metrics(&reference, &reference).unwrap();
        assert!(close(q.igd, 0.0) && close(q.igd_plus, 0.0) && close(q.spread, 0.0));
        assert!(
            close(q.coverage, 1.0),
            "every point equals a front point, so is weakly dominated"
        );
        assert!(q.hypervolume > 0.0);
        assert_eq!(q.reference_front_size, 21);
        // A worse front scores worse on every measure that has a direction.
        let worse: Vec<Vec<f64>> = reference
            .iter()
            .map(|p| vec![p[0] + 0.2, p[1] + 0.2])
            .collect();
        let w = quality_metrics(&worse, &reference).unwrap();
        assert!(w.hypervolume < q.hypervolume && w.igd > q.igd && w.igd_plus > q.igd_plus);
    }

    #[test]
    fn quality_coverage_is_the_share_of_the_front_the_known_front_beats() {
        // A front strictly behind the known one: every point is beaten by it,
        // and it beats none of the known front. Measured the other way round,
        // C(front, known) would read 0 and call a poor front perfect.
        let reference: Vec<Vec<f64>> = (0..=4)
            .map(|i| vec![i as f64 / 4.0, 1.0 - i as f64 / 4.0])
            .collect();
        let behind: Vec<Vec<f64>> = reference
            .iter()
            .map(|p| vec![p[0] + 0.1, p[1] + 0.1])
            .collect();
        let q = quality_metrics(&behind, &reference).unwrap();
        assert!(close(q.coverage, 1.0), "{}", q.coverage);
        assert!(close(coverage(&behind, &reference), 0.0));
    }

    #[test]
    fn quality_metrics_refuse_what_cannot_be_compared() {
        let reference = vec![vec![0.0, 1.0]];
        assert!(quality_metrics(&[], &reference).is_none());
        assert!(quality_metrics(&reference, &[]).is_none());
        assert!(
            quality_metrics(&[vec![0.0, 1.0, 2.0]], &reference).is_none(),
            "objective counts differ"
        );
    }

    #[test]
    fn elite_archive_caps() {
        let mut arch = EliteArchive::new(2);
        arch.insert(ind(vec![0.0, 1.0], 0.0));
        arch.insert(ind(vec![1.0, 0.0], 0.0));
        arch.insert(ind(vec![0.5, 0.5], 0.0));
        // All three are non-dominated; archive caps at 2 by crowding.
        assert_eq!(arch.members.len(), 2);
    }
}
