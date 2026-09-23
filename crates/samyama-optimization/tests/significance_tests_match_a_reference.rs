//! The Friedman and Wilcoxon implementations agree with SciPy (OPT-02).
//!
//! A statistic that is subtly wrong looks exactly like a statistic that is
//! right: it is a plausible number in a plausible range, and nothing in the
//! output says otherwise. The only useful check is against an implementation
//! that was not written here, so every expected value below was produced by
//! SciPy 1.18 (`scipy.stats.friedmanchisquare`, `scipy.stats.wilcoxon`,
//! `scipy.stats.chi2.sf`, `scipy.stats.f.sf`, `scipy.stats.norm.sf`) and is
//! recorded with the call that produced it.
//!
//! SciPy is not run here. Requiring Python in `cargo test` would make the
//! engine's test suite depend on a scientific stack; the values are pinned
//! instead, which is the same trade every conformance corpus makes.
//!
//! The cases are chosen for the things that are easy to get wrong rather than
//! for coverage: ties (which the survey's old ranking ignored entirely), zero
//! differences, and the boundary where the exact test gives way to the normal
//! approximation.

use samyama_optimization::stats::{
    average_ranks, chi_square_sf, f_sf, friedman, holm_adjust, normal_sf, wilcoxon_signed_rank,
    WilcoxonMethod,
};

/// Relative closeness, so a p-value of 1e-12 is held to the same standard as
/// one of 0.5. An absolute tolerance would pass anything in the tail.
#[track_caller]
fn close(got: f64, want: f64, rel: f64) {
    let denom = want.abs().max(f64::MIN_POSITIVE);
    let err = (got - want).abs() / denom;
    assert!(
        err <= rel,
        "got {got:.15e}, want {want:.15e} (relative error {err:.3e} > {rel:.1e})"
    );
}

#[test]
fn ties_share_the_average_of_the_positions_they_span() {
    // The defect this replaces: `examples/optimization_survey.rs` sorted and
    // assigned `position + 1`, so two solvers that both reached the global
    // minimum were given ranks 1 and 2 on the strength of comparison order.
    assert_eq!(average_ranks(&[1.0, 2.0, 3.0]), vec![1.0, 2.0, 3.0]);
    assert_eq!(average_ranks(&[3.0, 1.0, 2.0]), vec![3.0, 1.0, 2.0]);
    assert_eq!(average_ranks(&[1.0, 1.0, 3.0]), vec![1.5, 1.5, 3.0]);
    assert_eq!(average_ranks(&[5.0, 5.0, 5.0]), vec![2.0, 2.0, 2.0]);
    assert_eq!(average_ranks(&[2.0, 1.0, 1.0, 2.0]), vec![3.5, 1.5, 1.5, 3.5]);
}

#[test]
fn friedman_without_ties_matches_scipy() {
    // scipy.stats.friedmanchisquare(*zip(*blocks)) -> 4.333333333333, p=0.1145588439927
    let blocks = vec![
        vec![1.0, 2.0, 3.0],
        vec![2.0, 3.0, 1.0],
        vec![1.0, 3.0, 2.0],
        vec![1.0, 2.0, 3.0],
        vec![2.0, 1.0, 3.0],
        vec![1.0, 2.0, 3.0],
    ];
    let r = friedman(&blocks).expect("6 blocks, 3 treatments is a defined test");
    close(r.chi_square, 4.333333333333, 1e-10);
    close(r.chi_square_p, 1.145588439927e-01, 1e-10);
    assert_eq!(r.chi_square_df, 2);
    assert_eq!((r.k, r.n), (3, 6));

    // Iman-Davenport, derived from the same statistic:
    //   F = (n-1)chi / (n(k-1) - chi); scipy.stats.f.sf(F, 2, 10)
    close(r.iman_davenport_f, 2.826086956522, 1e-10);
    close(r.iman_davenport_p, 1.064453455763e-01, 1e-10);
    assert_eq!((r.f_df1, r.f_df2), (2, 10));
}

#[test]
fn friedman_applies_the_tie_correction() {
    // scipy.stats.friedmanchisquare -> 0.4, p=0.818730753078
    // Every block here has a tie, so an implementation that skipped the
    // correction would report a *smaller* statistic and a larger p-value --
    // the direction that quietly concludes "no difference".
    let blocks = vec![
        vec![1.0, 1.0, 3.0],
        vec![2.0, 2.0, 2.0],
        vec![1.0, 3.0, 3.0],
        vec![4.0, 2.0, 2.0],
        vec![2.0, 1.0, 1.0],
        vec![3.0, 3.0, 1.0],
    ];
    let r = friedman(&blocks).expect("defined");
    close(r.chi_square, 0.4, 1e-10);
    close(r.chi_square_p, 8.187307530780e-01, 1e-10);
}

#[test]
fn friedman_refuses_an_input_it_is_not_defined_on() {
    assert!(friedman(&[]).is_none(), "no blocks");
    assert!(friedman(&[vec![1.0, 2.0]]).is_none(), "one block");
    assert!(
        friedman(&[vec![1.0], vec![2.0]]).is_none(),
        "one treatment: there is nothing to compare"
    );
    assert!(
        friedman(&[vec![1.0, 2.0], vec![1.0, 2.0, 3.0]]).is_none(),
        "ragged blocks -- a solver missing on one function is not a rank of zero"
    );
}

#[test]
// `3.14` in the fixture below is a measurement from the published example, not
// an approximation of pi. Rounding it to `std::f64::consts::PI` to satisfy the
// lint would change the data the SciPy expectation was computed from, which is
// the one thing this file may not do.
#[allow(clippy::approx_constant)]
fn wilcoxon_is_exact_when_it_can_be() {
    // scipy.stats.wilcoxon(x, y, method='exact') -> statistic=5.0, p=0.0390625
    // The p-value is a dyadic rational because the null distribution is a count
    // over 2^9 sign assignments; an approximation would not land on it.
    let x = [1.83, 0.50, 1.62, 2.48, 1.68, 1.88, 1.55, 3.06, 1.30];
    let y = [0.878, 0.647, 0.598, 2.05, 1.06, 1.29, 1.06, 3.14, 1.29];
    let r = wilcoxon_signed_rank(&x, &y).expect("9 non-zero differences");
    assert_eq!(r.method, WilcoxonMethod::Exact);
    assert_eq!(r.n, 9);
    close(r.statistic, 5.0, 1e-12);
    close(r.p_two_sided, 3.906250000000e-02, 1e-12);
    close(r.w_plus + r.w_minus, 45.0, 1e-12); // ranks 1..9 sum to 45
}

#[test]
fn wilcoxon_falls_back_to_the_normal_approximation_when_absolute_differences_tie() {
    // diffs = [-1, 1, -2, 2, -2, 2, -2, 2]: the exact distribution assumes the
    // ranks are 1..n, which ties break.
    // scipy.stats.wilcoxon(x, y, method='approx', correction=False)
    //   -> statistic=18.0, p=1.0
    let x = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0];
    let y = [2.0, 1.0, 5.0, 2.0, 7.0, 4.0, 9.0, 6.0];
    let r = wilcoxon_signed_rank(&x, &y).expect("8 non-zero differences");
    assert_eq!(
        r.method,
        WilcoxonMethod::Normal,
        "ties among |d| must not be scored against the exact distribution"
    );
    close(r.statistic, 18.0, 1e-12);
    close(r.p_two_sided, 1.0, 1e-12);
}

#[test]
fn wilcoxon_drops_zero_differences_and_says_so_by_its_method() {
    // Two pairs are equal (index 0 and index 4), so they contribute nothing
    // and n falls from 10 to 8. |d| also ties at 0.8, so this is the normal
    // approximation on both counts.
    // scipy.stats.wilcoxon(x, y, method='approx', correction=False)
    //   -> statistic=13.5, p=0.5281061253471
    let x = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0];
    let y = [1.0, 3.0, 2.5, 4.7, 5.0, 6.9, 6.2, 8.8, 9.1, 9.0];
    let r = wilcoxon_signed_rank(&x, &y).expect("8 non-zero differences");
    assert_eq!(r.n, 8, "the tied pairs are dropped, not counted as agreement");
    assert_eq!(r.method, WilcoxonMethod::Normal);
    close(r.statistic, 13.5, 1e-12);
    close(r.p_two_sided, 5.281061253471e-01, 1e-11);
}

#[test]
fn wilcoxon_refuses_when_there_is_nothing_to_rank() {
    let a = [1.0, 2.0, 3.0];
    assert!(
        wilcoxon_signed_rank(&a, &a).is_none(),
        "two identical solvers give no test. Returning p = 1 would read as \
         'measured, no significant difference' when the truth is that the \
         comparison never happened."
    );
    assert!(wilcoxon_signed_rank(&[], &[]).is_none());
    assert!(
        wilcoxon_signed_rank(&[1.0, 2.0], &[1.0]).is_none(),
        "unequal lengths are not a paired sample"
    );
}

#[test]
fn holm_adjusts_by_the_step_down_ladder_and_stays_monotone() {
    // Worked by hand, because SciPy has no Holm (only Benjamini-Hochberg) and
    // statsmodels is not a dependency. m = 4, sorted p = .005 .01 .03 .04:
    //   .005 x 4 = .020
    //   .010 x 3 = .030
    //   .030 x 2 = .060
    //   .040 x 1 = .040 -> raised to .060 by the running maximum
    // The last step is the part worth pinning: without the monotone
    // adjustment, .04 would come out *smaller* than the .03 above it, and a
    // reader taking the table at α = 0.05 would reject the weaker result while
    // failing to reject the stronger one.
    let got = holm_adjust(&[0.01, 0.04, 0.005, 0.03]);
    let want = [0.030, 0.060, 0.020, 0.060];
    for (g, w) in got.iter().zip(&want) {
        close(*g, *w, 1e-12);
    }

    // Capped at 1, never above.
    let got = holm_adjust(&[0.5, 0.6, 0.7]);
    assert!(got.iter().all(|p| *p <= 1.0), "an adjusted p-value is still a p-value: {got:?}");

    // A single test is its own family and is not inflated.
    close(holm_adjust(&[0.02])[0], 0.02, 1e-12);
    assert!(holm_adjust(&[]).is_empty());
}

#[test]
fn holm_is_the_reason_a_pairwise_table_can_be_read() {
    // 465 pairs is what 31 solvers gives. A raw table at α = 0.05 rejects
    // about 23 of them under a true null; Holm rejects none of these, which is
    // the whole point of reporting the adjusted column beside the raw one.
    let raw: Vec<f64> = (0..465).map(|i| (i as f64 + 0.5) / 465.0).collect();
    let adj = holm_adjust(&raw);
    let raw_hits = raw.iter().filter(|p| **p < 0.05).count();
    let adj_hits = adj.iter().filter(|p| **p < 0.05).count();
    assert_eq!(raw_hits, 23);
    assert_eq!(adj_hits, 0);
}

#[test]
fn the_distribution_functions_match_scipy() {
    // scipy.stats.chi2.sf
    close(chi_square_sf(10.0, 3.0), 1.85661354630433e-02, 1e-12);
    close(chi_square_sf(0.5, 1.0), 4.79500122186953e-01, 1e-12);
    // Deep in the tail, where a series that stops early quietly returns zero.
    close(chi_square_sf(120.0, 30.0), 1.02027585414700e-12, 1e-10);

    // scipy.stats.f.sf
    close(f_sf(3.0, 5.0, 20.0), 3.52013374526666e-02, 1e-12);
    // The shape the survey actually produces: 31 solvers over 10 functions.
    close(f_sf(1.2, 30.0, 270.0), 2.24502277565637e-01, 1e-12);

    // scipy.stats.norm.sf
    close(normal_sf(1.96), 2.49978951482204e-02, 1e-12);
    close(normal_sf(5.0), 2.86651571879193e-07, 1e-11);
    close(normal_sf(0.0), 0.5, 1e-14);
}
