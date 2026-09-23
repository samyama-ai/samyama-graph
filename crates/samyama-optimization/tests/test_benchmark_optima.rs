//! Verify each benchmark function reaches its documented global minimum
//! when evaluated at the known optimal point.

use ndarray::Array1;
use samyama_optimization::benchmarks::single_objective::*;

fn approx(a: f64, b: f64, eps: f64) -> bool {
    (a - b).abs() < eps
}

#[test]
fn sphere_at_origin() {
    let x = Array1::zeros(30);
    assert!(approx(sphere(&x), 0.0, 1e-12));
}

#[test]
fn rastrigin_at_origin() {
    let x = Array1::zeros(30);
    assert!(approx(rastrigin(&x), 0.0, 1e-12));
}

#[test]
fn ackley_at_origin() {
    let x = Array1::zeros(30);
    assert!(approx(ackley(&x), 0.0, 1e-12));
}

#[test]
fn rosenbrock_at_ones() {
    let x = Array1::ones(30);
    assert!(approx(rosenbrock(&x), 0.0, 1e-12));
}

#[test]
fn griewank_at_origin() {
    let x = Array1::zeros(30);
    assert!(approx(griewank(&x), 0.0, 1e-12));
}

#[test]
fn schwefel_at_optimum() {
    let x = Array1::from_elem(30, 420.9687);
    // Schwefel optimum is approximate; tolerance reflects the constant truncation.
    assert!(approx(schwefel(&x), 0.0, 1e-2), "schwefel = {}", schwefel(&x));
}

#[test]
fn levy_at_ones() {
    let x = Array1::ones(30);
    assert!(approx(levy(&x), 0.0, 1e-12));
}

#[test]
fn zakharov_at_origin() {
    let x = Array1::zeros(30);
    assert!(approx(zakharov(&x), 0.0, 1e-12));
}

#[test]
fn dixon_price_at_known_optimum() {
    // x_i = 2^(-(2^i - 2)/2^i); for i=0 -> x_0 = 1; for i=1 -> 2^(-0.5); etc.
    let dim = 5;
    let x = Array1::from_iter((0..dim).map(|i| {
        let p = (2.0_f64.powi(i as i32 + 1) - 2.0) / 2.0_f64.powi(i as i32 + 1);
        2.0_f64.powf(-p)
    }));
    assert!(dixon_price(&x).abs() < 1e-10, "dixon_price = {}", dixon_price(&x));
}

#[test]
fn styblinski_tang_at_optimum() {
    let dim = 30;
    let x = Array1::from_elem(dim, -2.903534);
    let expected = -39.16599 * dim as f64;
    let got = styblinski_tang(&x);
    assert!((got - expected).abs() < 1e-2, "got {} expected {}", got, expected);
}

// ───────────────────────────── the eighteen added for OPT-02's pairwise test
//
// Each is evaluated at its closed-form optimum. This is the check that makes
// the `global_minimum` field in `so_suite` mean something: the survey reports
// `median - global_minimum`, so a wrong constant shifts every solver's score
// on that function by the same amount and never looks wrong.

#[test]
fn schwefel_1_2_at_origin() {
    assert!(approx(schwefel_1_2(&Array1::zeros(30)), 0.0, 1e-12));
}

#[test]
fn schwefel_2_21_at_origin() {
    assert!(approx(schwefel_2_21(&Array1::zeros(30)), 0.0, 1e-12));
    // The max, not the sum: one large coordinate is the whole value.
    let mut x = Array1::zeros(30);
    x[7] = -4.0;
    assert!(approx(schwefel_2_21(&x), 4.0, 1e-12));
}

#[test]
fn schwefel_2_22_at_origin() {
    assert!(approx(schwefel_2_22(&Array1::zeros(30)), 0.0, 1e-12));
}

#[test]
fn step_at_origin_and_across_its_flat_cell() {
    assert!(approx(step(&Array1::zeros(30)), 0.0, 1e-12));
    // Flat over [-0.5, 0.5): the property a gradient cannot see.
    assert!(approx(step(&Array1::from_elem(30, 0.4)), 0.0, 1e-12));
    assert!(approx(step(&Array1::from_elem(30, 0.6)), 30.0, 1e-12));
}

#[test]
fn sum_squares_at_origin() {
    assert!(approx(sum_squares(&Array1::zeros(30)), 0.0, 1e-12));
    // Weighted by index: all-ones is 1 + 2 + ... + 30.
    assert!(approx(sum_squares(&Array1::ones(30)), 465.0, 1e-12));
}

#[test]
fn sum_of_different_powers_at_origin() {
    assert!(approx(sum_of_different_powers(&Array1::zeros(30)), 0.0, 1e-12));
}

#[test]
fn trid_at_its_known_optimum() {
    // x_i = i(n+1-i), minimum -n(n+4)(n-1)/6. The one function here whose
    // optimum is neither a constant vector nor the origin.
    for dim in [5usize, 10, 30] {
        let n = dim as f64;
        let x = Array1::from_iter((1..=dim).map(|i| {
            let i = i as f64;
            i * (n + 1.0 - i)
        }));
        let got = trid(&x);
        let want = trid_minimum(dim);
        assert!(
            (got - want).abs() < 1e-6 * want.abs().max(1.0),
            "trid dim {dim}: got {got}, want {want}"
        );
    }
}

#[test]
fn alpine1_at_origin() {
    assert!(approx(alpine1(&Array1::zeros(30)), 0.0, 1e-12));
}

#[test]
fn happy_cat_at_minus_one_not_at_the_origin() {
    assert!(approx(happy_cat(&Array1::from_elem(30, -1.0)), 0.0, 1e-12));
    // Stated as a separate assertion because it is the reason this function is
    // in the suite: a solver drawn to the centre of its bounds does badly here.
    assert!(
        happy_cat(&Array1::zeros(30)) > 0.5,
        "the origin must not be the optimum: {}",
        happy_cat(&Array1::zeros(30))
    );
}

#[test]
fn hgbat_at_minus_one_not_at_the_origin() {
    assert!(approx(hgbat(&Array1::from_elem(30, -1.0)), 0.0, 1e-12));
    assert!(hgbat(&Array1::zeros(30)) > 0.4);
}

#[test]
fn bent_cigar_and_discus_are_conditioned_opposite_ways() {
    assert!(approx(bent_cigar(&Array1::zeros(30)), 0.0, 1e-12));
    assert!(approx(discus(&Array1::zeros(30)), 0.0, 1e-12));

    // One unit along the first axis, then one unit along the second.
    let mut first = Array1::zeros(30);
    first[0] = 1.0;
    let mut second = Array1::zeros(30);
    second[1] = 1.0;
    assert!(approx(bent_cigar(&first), 1.0, 1e-9));
    assert!(approx(bent_cigar(&second), 1e6, 1e-3));
    assert!(approx(discus(&first), 1e6, 1e-3));
    assert!(approx(discus(&second), 1.0, 1e-9));
}

#[test]
fn high_conditioned_elliptic_spans_six_orders_of_magnitude() {
    assert!(approx(high_conditioned_elliptic(&Array1::zeros(30)), 0.0, 1e-12));
    let mut first = Array1::zeros(30);
    first[0] = 1.0;
    let mut last = Array1::zeros(30);
    last[29] = 1.0;
    assert!(approx(high_conditioned_elliptic(&first), 1.0, 1e-9));
    assert!(approx(high_conditioned_elliptic(&last), 1e6, 1e-3));
}

#[test]
fn salomon_at_origin() {
    assert!(approx(salomon(&Array1::zeros(30)), 0.0, 1e-12));
}

#[test]
fn qing_at_the_square_roots() {
    // x_i = sqrt(i), so every coordinate has a different target.
    let dim = 30;
    let x = Array1::from_iter((1..=dim).map(|i| (i as f64).sqrt()));
    assert!(qing(&x).abs() < 1e-18, "qing = {}", qing(&x));
    // The origin is nowhere near it, unlike most of the suite: at x = 0 the
    // value is sum(i^2) for i = 1..30, which is 30*31*61/6 = 9455.
    assert!(approx(qing(&Array1::zeros(dim)), 9455.0, 1e-9));
}

#[test]
fn chung_reynolds_at_origin() {
    assert!(approx(chung_reynolds(&Array1::zeros(30)), 0.0, 1e-12));
}

#[test]
fn exponential_bottoms_out_at_minus_one() {
    assert!(approx(exponential(&Array1::zeros(30)), -1.0, 1e-12));
    // Bounded below, which is why its global_minimum is not 0.
    assert!(exponential(&Array1::from_elem(30, 0.5)) > -1.0);
}

#[test]
fn periodic_bottoms_out_at_zero_point_nine() {
    // 1 + 0 - 0.1 = 0.9. A function whose optimum is not zero catches a survey
    // that subtracts the wrong constant.
    assert!(approx(periodic(&Array1::zeros(30)), 0.9, 1e-12));
}

#[test]
fn so_suite_has_twenty_eight_functions() {
    // The count is load-bearing, not cosmetic. OPT-02's pairwise Wilcoxon test
    // ranks over functions and its exact p-value cannot fall below 2/2^n, so
    // at n = 10 no pair of solvers could be declared different at any
    // corrected threshold. At n = 28 the floor is 7.5e-9.
    let s = so_suite(30);
    assert_eq!(s.len(), 28);

    let names: std::collections::HashSet<&str> = s.iter().map(|p| p.name).collect();
    assert_eq!(names.len(), s.len(), "duplicate function name in the suite");

    let floor = 2.0 / 2f64.powi(s.len() as i32);
    let holm_first_threshold = 0.05 / 325.0; // 26 solvers -> 325 pairs
    assert!(
        floor <= holm_first_threshold,
        "the suite is too small for the pairwise test to reject anything: \
         smallest achievable p {floor:.3e} > Holm threshold {holm_first_threshold:.3e}"
    );
}

#[test]
fn every_suite_entry_evaluates_at_its_declared_bounds() {
    // Not an optimum check: a guard that no spec names bounds its own function
    // cannot be evaluated over. `sum_of_different_powers` raises |x| to the
    // 30th power, `chung_reynolds` squares a sum of squares -- both overflow
    // to infinity on a domain chosen carelessly, and a solver would then rank
    // every candidate equal.
    for spec in so_suite(10) {
        for corner in [spec.lower, spec.upper, 0.0] {
            let x = Array1::from_elem(spec.dim, corner);
            let v = (spec.func)(&x);
            assert!(
                v.is_finite(),
                "{} at {corner} is {v}, which no ranking can order",
                spec.name
            );
        }
        let at_opt = spec.global_minimum;
        assert!(at_opt.is_finite(), "{}: global_minimum is not finite", spec.name);
    }
}
