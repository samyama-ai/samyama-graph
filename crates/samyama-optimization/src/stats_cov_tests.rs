use super::*;

#[test]
fn friedman_with_perfectly_separated_ranks_reports_infinite_f_and_zero_p() {
    // Every block ranks the treatments identically: chi = n(k-1), so the
    // Iman-Davenport divisor is zero.
    let blocks = vec![vec![1.0, 2.0, 3.0]; 4];
    let r = friedman(&blocks).unwrap();
    assert!((r.chi_square - 8.0).abs() < 1e-9, "{}", r.chi_square);
    assert_eq!(r.average_ranks, vec![1.0, 2.0, 3.0]);
    assert!(r.iman_davenport_f.is_infinite());
    assert_eq!(r.iman_davenport_p, 0.0);
}

#[test]
fn friedman_rejects_undefined_inputs() {
    assert!(friedman(&[vec![1.0, 2.0]]).is_none());
    assert!(friedman(&[vec![1.0], vec![2.0]]).is_none());
    assert!(friedman(&[vec![1.0, 2.0], vec![1.0]]).is_none());
}

#[test]
fn normal_approximation_with_zero_variance_returns_p_one() {
    assert_eq!(normal_two_sided_p(0.0, 0, &[]), 1.0);
}

#[test]
fn normal_sf_is_symmetric_about_zero() {
    assert!((normal_sf(0.0) - 0.5).abs() < 1e-12);
    // Negative arguments go through erfc's reflection.
    let lo = normal_sf(-1.0);
    let hi = normal_sf(1.0);
    assert!((lo + hi - 1.0).abs() < 1e-12, "{} + {}", lo, hi);
    assert!((hi - 0.158_655_253_931_457).abs() < 1e-9, "{}", hi);
}

#[test]
fn chi_square_and_f_tails_are_one_at_or_below_zero() {
    assert_eq!(chi_square_sf(0.0, 3.0), 1.0);
    assert_eq!(chi_square_sf(-2.0, 3.0), 1.0);
    assert_eq!(f_sf(0.0, 2.0, 5.0), 1.0);
    assert_eq!(f_sf(-1.0, 2.0, 5.0), 1.0);
}

#[test]
fn chi_square_with_half_degree_of_freedom_uses_gamma_reflection() {
    // df = 0.5 puts a = 0.25 < 0.5, which evaluates ln_gamma by reflection.
    // Q(0.25, 0.5) = 0.153513595808322, from an independent series using
    // Python's math.lgamma.
    let p = chi_square_sf(1.0, 0.5);
    assert!((p - 0.153_513_595_808_322).abs() < 1e-9, "{}", p);
}

#[test]
fn ln_gamma_matches_known_values_on_both_sides_of_one_half() {
    // Gamma(0.25) = 3.6256099082219083
    assert!((ln_gamma(0.25) - 3.625_609_908_221_908f64.ln()).abs() < 1e-10);
    // Gamma(5) = 24
    assert!((ln_gamma(5.0) - 24f64.ln()).abs() < 1e-10);
}

#[test]
fn gamma_q_is_nan_outside_its_domain_and_one_at_zero() {
    assert!(gamma_q(0.0, 1.0).is_nan());
    assert!(gamma_q(1.0, -1.0).is_nan());
    assert_eq!(gamma_q(2.0, 0.0), 1.0);
    // Zero degrees of freedom reaches the same guard through the public API.
    assert!(chi_square_sf(1.0, 0.0).is_nan());
}

#[test]
fn incomplete_beta_is_clamped_at_the_ends_of_the_unit_interval() {
    assert_eq!(betainc_regularized(0.0, 2.0, 3.0), 0.0);
    assert_eq!(betainc_regularized(-0.5, 2.0, 3.0), 0.0);
    assert_eq!(betainc_regularized(1.0, 2.0, 3.0), 1.0);
    assert_eq!(betainc_regularized(1.5, 2.0, 3.0), 1.0);
    // I_0.5(a, a) = 0.5 by symmetry.
    assert!((betainc_regularized(0.5, 3.0, 3.0) - 0.5).abs() < 1e-12);
}
