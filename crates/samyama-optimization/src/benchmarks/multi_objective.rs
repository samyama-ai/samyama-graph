//! ZDT and DTLZ multi-objective test suites.
//!
//! References:
//!   - Zitzler, Deb & Thiele 2000, ZDT functions.
//!   - Deb, Thiele, Laumanns & Zitzler 2002, DTLZ scalable test problems.

use crate::common::MultiObjectiveProblem;
use ndarray::Array1;
use std::f64::consts::PI;

pub struct MOProblemSpec {
    pub name: &'static str,
    pub dim: usize,
    pub num_objectives: usize,
    pub lower: f64,
    pub upper: f64,
    /// Approximate hypervolume reference point (one per objective). Used by
    /// `moo::hypervolume_2d` for 2-objective cases. For 3+ objectives,
    /// consumers must supply their own ref-point + HV computation.
    pub hv_ref: Vec<f64>,
}

pub struct ZDT {
    pub variant: u8, // 1..=6
    pub dim: usize,
}

impl MultiObjectiveProblem for ZDT {
    fn dim(&self) -> usize { self.dim }
    fn num_objectives(&self) -> usize { 2 }
    fn bounds(&self) -> (Array1<f64>, Array1<f64>) {
        match self.variant {
            5 => unreachable!("ZDT5 is binary-coded; not supported in this continuous suite"),
            _ => (Array1::zeros(self.dim), Array1::ones(self.dim)),
        }
    }
    fn objectives(&self, x: &Array1<f64>) -> Vec<f64> {
        let n = self.dim as f64;
        match self.variant {
            1 => {
                let f1 = x[0];
                let g = 1.0 + 9.0 * x.iter().skip(1).sum::<f64>() / (n - 1.0);
                vec![f1, g * (1.0 - (f1 / g).sqrt())]
            }
            2 => {
                let f1 = x[0];
                let g = 1.0 + 9.0 * x.iter().skip(1).sum::<f64>() / (n - 1.0);
                vec![f1, g * (1.0 - (f1 / g).powi(2))]
            }
            3 => {
                let f1 = x[0];
                let g = 1.0 + 9.0 * x.iter().skip(1).sum::<f64>() / (n - 1.0);
                let f2 = g * (1.0 - (f1 / g).sqrt() - (f1 / g) * (10.0 * PI * f1).sin());
                vec![f1, f2]
            }
            4 => {
                let f1 = x[0];
                let g = 1.0
                    + 10.0 * (n - 1.0)
                    + x.iter()
                        .skip(1)
                        .map(|&v| v * v - 10.0 * (4.0 * PI * v).cos())
                        .sum::<f64>();
                vec![f1, g * (1.0 - (f1 / g).sqrt())]
            }
            6 => {
                let f1 = 1.0 - (-4.0 * x[0]).exp() * (6.0 * PI * x[0]).sin().powi(6);
                let g = 1.0
                    + 9.0 * (x.iter().skip(1).sum::<f64>() / (n - 1.0)).powf(0.25);
                vec![f1, g * (1.0 - (f1 / g).powi(2))]
            }
            _ => panic!("ZDT variant {} not supported", self.variant),
        }
    }
}

pub struct DTLZ {
    pub variant: u8, // 1..=7
    pub dim: usize,
    pub m: usize,    // number of objectives
}

impl MultiObjectiveProblem for DTLZ {
    fn dim(&self) -> usize { self.dim }
    fn num_objectives(&self) -> usize { self.m }
    fn bounds(&self) -> (Array1<f64>, Array1<f64>) {
        (Array1::zeros(self.dim), Array1::ones(self.dim))
    }
    fn objectives(&self, x: &Array1<f64>) -> Vec<f64> {
        let m = self.m;
        let k = self.dim - m + 1;
        let xm = &x.as_slice().unwrap()[self.dim - k..];

        match self.variant {
            1 => {
                let g = 100.0 * (k as f64
                    + xm.iter()
                        .map(|&v| (v - 0.5).powi(2) - (20.0 * PI * (v - 0.5)).cos())
                        .sum::<f64>());
                let mut f = vec![0.5 * (1.0 + g); m];
                for i in 0..m {
                    for j in 0..(m - 1 - i) { f[i] *= x[j]; }
                    if i > 0 { f[i] *= 1.0 - x[m - 1 - i]; }
                }
                f
            }
            2 | 3 | 4 => {
                // DTLZ2/3/4 share spherical PF; only g and the meta-variable transformation differ.
                let g = match self.variant {
                    2 | 4 => xm.iter().map(|&v| (v - 0.5).powi(2)).sum::<f64>(),
                    3 => 100.0 * (k as f64
                        + xm.iter()
                            .map(|&v| (v - 0.5).powi(2) - (20.0 * PI * (v - 0.5)).cos())
                            .sum::<f64>()),
                    _ => unreachable!(),
                };
                let alpha = if self.variant == 4 { 100.0 } else { 1.0 };
                let theta: Vec<f64> = (0..m - 1)
                    .map(|j| 0.5 * PI * x[j].powf(alpha))
                    .collect();
                let mut f = vec![1.0 + g; m];
                for i in 0..m {
                    for j in 0..(m - 1 - i) { f[i] *= theta[j].cos(); }
                    if i > 0 { f[i] *= theta[m - 1 - i].sin(); }
                }
                f
            }
            5 => {
                let g = xm.iter().map(|&v| (v - 0.5).powi(2)).sum::<f64>();
                let mut theta = vec![0.0; m - 1];
                theta[0] = 0.5 * PI * x[0];
                let t = PI / (4.0 * (1.0 + g));
                for j in 1..m - 1 {
                    theta[j] = t * (1.0 + 2.0 * g * x[j]);
                }
                let mut f = vec![1.0 + g; m];
                for i in 0..m {
                    for j in 0..(m - 1 - i) { f[i] *= theta[j].cos(); }
                    if i > 0 { f[i] *= theta[m - 1 - i].sin(); }
                }
                f
            }
            6 => {
                let g = xm.iter().map(|&v| v.powf(0.1)).sum::<f64>();
                let mut theta = vec![0.0; m - 1];
                theta[0] = 0.5 * PI * x[0];
                let t = PI / (4.0 * (1.0 + g));
                for j in 1..m - 1 {
                    theta[j] = t * (1.0 + 2.0 * g * x[j]);
                }
                let mut f = vec![1.0 + g; m];
                for i in 0..m {
                    for j in 0..(m - 1 - i) { f[i] *= theta[j].cos(); }
                    if i > 0 { f[i] *= theta[m - 1 - i].sin(); }
                }
                f
            }
            7 => {
                let g = 1.0 + 9.0 / k as f64 * xm.iter().sum::<f64>();
                let mut f = vec![0.0; m];
                for i in 0..m - 1 { f[i] = x[i]; }
                let h: f64 = (0..m - 1)
                    .map(|i| f[i] / (1.0 + g) * (1.0 + (3.0 * PI * f[i]).sin()))
                    .sum();
                f[m - 1] = (1.0 + g) * (m as f64 - h);
                f
            }
            _ => panic!("DTLZ variant {} not supported", self.variant),
        }
    }
}

/// Points sampled along the position variable when tracing a ZDT front.
pub const ZDT_FRONT_SAMPLES: usize = 500;

/// Points sampled per position variable when tracing a DTLZ front; with `m`
/// objectives there are `m - 1` position variables, so `30^(m-1)` points
/// (900 at three objectives) before dominated ones are removed.
pub const DTLZ_FRONT_SAMPLES_PER_AXIS: usize = 30;

/// The points of `points` no other point dominates, with exact duplicates
/// removed. Quadratic; for the few thousand points of a sampled front.
pub fn non_dominated(points: Vec<Vec<f64>>) -> Vec<Vec<f64>> {
    let dominated = |p: &Vec<f64>, q: &Vec<f64>| {
        q.iter().zip(p).all(|(a, b)| a <= b) && q.iter().zip(p).any(|(a, b)| a < b)
    };
    let mut kept: Vec<Vec<f64>> = Vec::new();
    for (i, p) in points.iter().enumerate() {
        let beaten = points
            .iter()
            .enumerate()
            .any(|(k, q)| k != i && dominated(p, q));
        if !beaten && !kept.contains(p) {
            kept.push(p.clone());
        }
    }
    kept
}

/// `samples` evenly spaced values from 0 to 1 inclusive.
fn unit_grid(samples: usize) -> Vec<f64> {
    match samples {
        0 => Vec::new(),
        1 => vec![0.0],
        n => (0..n).map(|i| i as f64 / (n - 1) as f64).collect(),
    }
}

impl ZDT {
    /// The known Pareto front, sampled.
    ///
    /// Traced with the problem's own `objectives`, not a copied formula: every
    /// ZDT problem is on its front exactly when the tail variables are 0 (then
    /// g = 1), so sweeping `x[0]` over [0, 1] with the tail at 0 walks the
    /// front. Dominated samples are dropped, which leaves ZDT3's five pieces
    /// and ZDT6's front starting at f1 = 0.28 (where its curve turns back).
    pub fn reference_front(&self, samples: usize) -> Vec<Vec<f64>> {
        let mut x = Array1::zeros(self.dim);
        let points = unit_grid(samples)
            .into_iter()
            .map(|x0| {
                x[0] = x0;
                self.objectives(&x)
            })
            .collect();
        non_dominated(points)
    }
}

impl DTLZ {
    /// The value every distance variable takes on the known front: where `g`
    /// is 0 (DTLZ1-5) or least (DTLZ6, DTLZ7).
    fn optimal_distance_value(&self) -> f64 {
        match self.variant {
            6 | 7 => 0.0,
            _ => 0.5,
        }
    }

    /// The known Pareto front, sampled.
    ///
    /// Traced with the problem's own `objectives` over a grid of the `m - 1`
    /// position variables, the distance variables held at their optimum, and
    /// dominated samples dropped. DTLZ4 shares DTLZ2's spherical front; its
    /// mapping `x^100` would put almost every grid point at one end of it, so
    /// DTLZ2's sampling is used instead. DTLZ5 and DTLZ6 have a degenerate,
    /// curve-shaped front; the grid's duplicates on it are removed.
    pub fn reference_front(&self, samples_per_axis: usize) -> Vec<Vec<f64>> {
        if self.variant == 4 {
            return DTLZ {
                variant: 2,
                dim: self.dim,
                m: self.m,
            }
            .reference_front(samples_per_axis);
        }
        let positions = self.m - 1;
        let grid = unit_grid(samples_per_axis);
        let mut x = Array1::from_elem(self.dim, self.optimal_distance_value());
        let mut points = Vec::new();
        let mut index = vec![0usize; positions];
        if grid.is_empty() {
            return points;
        }
        loop {
            for (j, &i) in index.iter().enumerate() {
                x[j] = grid[i];
            }
            points.push(self.objectives(&x));
            // Advance the odometer over the position variables.
            let mut j = 0;
            loop {
                if j == positions {
                    return non_dominated(points);
                }
                index[j] += 1;
                if index[j] < grid.len() {
                    break;
                }
                index[j] = 0;
                j += 1;
            }
        }
    }
}

/// Returns the full MO test suite: ZDT1-4, ZDT6, DTLZ1-7 at m=3.
pub fn moo_suite(zdt_dim: usize, dtlz_dim: usize, dtlz_m: usize) -> Vec<MOProblemSpec> {
    let mut v = Vec::new();
    for variant in [1u8, 2, 3, 4, 6] {
        v.push(MOProblemSpec {
            name: match variant { 1 => "ZDT1", 2 => "ZDT2", 3 => "ZDT3", 4 => "ZDT4", 6 => "ZDT6", _ => "ZDT?" },
            dim: zdt_dim,
            num_objectives: 2,
            lower: 0.0,
            upper: 1.0,
            hv_ref: vec![1.1, 1.1],
        });
    }
    for variant in 1u8..=7 {
        v.push(MOProblemSpec {
            name: match variant {
                1 => "DTLZ1", 2 => "DTLZ2", 3 => "DTLZ3", 4 => "DTLZ4",
                5 => "DTLZ5", 6 => "DTLZ6", 7 => "DTLZ7", _ => "DTLZ?",
            },
            dim: dtlz_dim,
            num_objectives: dtlz_m,
            lower: 0.0,
            upper: 1.0,
            hv_ref: vec![1.1; dtlz_m],
        });
    }
    v
}

/// Constructor matching name -> Box<dyn MultiObjectiveProblem>.
pub fn build_mo_problem(spec: &MOProblemSpec) -> Box<dyn MultiObjectiveProblem> {
    if let Some(rest) = spec.name.strip_prefix("ZDT") {
        let variant: u8 = rest.parse().expect("ZDT variant");
        return Box::new(ZDT { variant, dim: spec.dim });
    }
    if let Some(rest) = spec.name.strip_prefix("DTLZ") {
        let variant: u8 = rest.parse().expect("DTLZ variant");
        return Box::new(DTLZ { variant, dim: spec.dim, m: spec.num_objectives });
    }
    panic!("unknown MO problem {}", spec.name)
}

#[cfg(test)]
mod reference_front_tests {
    use super::*;

    /// How close a sampled point must be to its front's equation.
    const TOLERANCE: f64 = 1e-9;
    const ZDT_DIM: usize = 30;
    const DTLZ_M: usize = 3;
    /// k = 5 distance variables for DTLZ1, 10 for the rest: the usual sizes.
    const DTLZ1_DIM: usize = DTLZ_M - 1 + 5;
    const DTLZ_DIM: usize = DTLZ_M - 1 + 10;

    fn zdt(variant: u8) -> Vec<Vec<f64>> {
        ZDT {
            variant,
            dim: ZDT_DIM,
        }
        .reference_front(ZDT_FRONT_SAMPLES)
    }

    fn dtlz(variant: u8, dim: usize) -> Vec<Vec<f64>> {
        DTLZ {
            variant,
            dim,
            m: DTLZ_M,
        }
        .reference_front(DTLZ_FRONT_SAMPLES_PER_AXIS)
    }

    #[test]
    fn zdt1_and_zdt4_lie_on_f2_equals_one_minus_root_f1() {
        for variant in [1, 4] {
            let front = zdt(variant);
            assert_eq!(
                front.len(),
                ZDT_FRONT_SAMPLES,
                "ZDT{variant}: every sample is optimal"
            );
            for p in &front {
                assert!(
                    (p[1] - (1.0 - p[0].sqrt())).abs() < TOLERANCE,
                    "ZDT{variant}: {p:?}"
                );
            }
        }
    }

    #[test]
    fn zdt2_lies_on_f2_equals_one_minus_f1_squared() {
        for p in zdt(2) {
            assert!((p[1] - (1.0 - p[0] * p[0])).abs() < TOLERANCE, "{p:?}");
        }
    }

    #[test]
    fn zdt3_is_disconnected_so_its_dominated_samples_are_dropped() {
        let front = zdt(3);
        assert!(
            front.len() < ZDT_FRONT_SAMPLES,
            "ZDT3's front is five pieces"
        );
        assert!(
            front.iter().any(|p| p[1] < 0.0),
            "ZDT3's front dips below zero"
        );
    }

    #[test]
    fn zdt6_starts_where_its_curve_turns_back() {
        let front = zdt(6);
        let smallest = front.iter().map(|p| p[0]).fold(f64::INFINITY, f64::min);
        // The literature's 0.2807753191; the grid gets within one step of it.
        assert!((smallest - 0.280_775_319_1).abs() < 1e-2, "{smallest}");
        for p in &front {
            assert!((p[1] - (1.0 - p[0] * p[0])).abs() < TOLERANCE, "{p:?}");
        }
    }

    #[test]
    fn dtlz1_lies_on_the_plane_summing_to_a_half() {
        for p in dtlz(1, DTLZ1_DIM) {
            assert!((p.iter().sum::<f64>() - 0.5).abs() < TOLERANCE, "{p:?}");
        }
    }

    #[test]
    fn dtlz2_3_and_4_lie_on_the_unit_sphere() {
        for variant in [2, 3, 4] {
            let front = dtlz(variant, DTLZ_DIM);
            assert!(
                front.len() > DTLZ_FRONT_SAMPLES_PER_AXIS,
                "DTLZ{variant}: a surface, not a curve"
            );
            for p in &front {
                let r: f64 = p.iter().map(|v| v * v).sum();
                assert!((r - 1.0).abs() < TOLERANCE, "DTLZ{variant}: {p:?}");
            }
        }
    }

    #[test]
    fn dtlz5_and_6_are_a_curve_on_the_sphere() {
        for variant in [5, 6] {
            let front = dtlz(variant, DTLZ_DIM);
            assert!(
                front.len() <= DTLZ_FRONT_SAMPLES_PER_AXIS,
                "DTLZ{variant}: a curve"
            );
            for p in &front {
                let r: f64 = p.iter().map(|v| v * v).sum();
                assert!((r - 1.0).abs() < TOLERANCE, "DTLZ{variant}: {p:?}");
                assert!(
                    (p[0] - p[1]).abs() < TOLERANCE,
                    "DTLZ{variant}: f1 = f2 on the front"
                );
            }
        }
    }

    #[test]
    fn dtlz7_is_disconnected_so_its_dominated_samples_are_dropped() {
        let all = DTLZ_FRONT_SAMPLES_PER_AXIS.pow(DTLZ_M as u32 - 1);
        let front = dtlz(7, DTLZ_DIM);
        assert!(
            !front.is_empty() && front.len() < all,
            "{} of {all}",
            front.len()
        );
    }

    #[test]
    fn every_sampled_front_is_mutually_non_dominated() {
        let fronts = [
            zdt(1),
            zdt(3),
            zdt(6),
            dtlz(1, DTLZ1_DIM),
            dtlz(7, DTLZ_DIM),
        ];
        for front in fronts {
            assert_eq!(non_dominated(front.clone()).len(), front.len());
        }
    }

    #[test]
    fn non_dominated_drops_dominated_points_and_duplicates() {
        let kept = non_dominated(vec![
            vec![1.0, 1.0],
            vec![2.0, 2.0],
            vec![0.0, 3.0],
            vec![1.0, 1.0],
        ]);
        assert_eq!(kept, vec![vec![1.0, 1.0], vec![0.0, 3.0]]);
    }

    #[test]
    fn a_grid_of_zero_or_one_sample_is_handled() {
        assert!(ZDT {
            variant: 1,
            dim: ZDT_DIM
        }
        .reference_front(0)
        .is_empty());
        assert_eq!(
            ZDT {
                variant: 1,
                dim: ZDT_DIM
            }
            .reference_front(1)
            .len(),
            1
        );
        assert!(DTLZ {
            variant: 2,
            dim: DTLZ_DIM,
            m: DTLZ_M
        }
        .reference_front(0)
        .is_empty());
    }
}
