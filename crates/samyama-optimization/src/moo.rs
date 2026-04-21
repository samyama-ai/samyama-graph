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
//!   - [`igd`] — Inverted Generational Distance against a reference set.

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
            population[a].fitness[m]
                .partial_cmp(&population[b].fitness[m])
                .unwrap_or(std::cmp::Ordering::Equal)
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
                b.crowding_distance
                    .partial_cmp(&a.crowding_distance)
                    .unwrap_or(std::cmp::Ordering::Equal)
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
