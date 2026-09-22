//! Significance tests for comparing solvers across a benchmark suite (OPT-02).
//!
//! OPT-02 asks for "mean/median/std/best/worst per function; Wilcoxon
//! signed-rank pairwise and Friedman rank across algorithms; significance
//! stated". The per-function descriptives were already computed and the
//! Friedman *ranks* were reported; the tests were not, and the survey said so
//! rather than letting the rank ordering imply one. A rank says which solver
//! placed where. It says nothing about whether the ordering would survive a
//! reshuffle, which is the entire question when thirty-one solvers are ranked
//! on ten functions.
//!
//! # Why the ranks here are not the ranks that were there
//!
//! The survey assigned rank `position + 1` after sorting. That is wrong the
//! moment two solvers tie, which on these functions is common: several solvers
//! reach the global minimum exactly, and competition ranking would hand one of
//! them rank 1 and the other rank 2 on the strength of float comparison order.
//! Ties take the average of the positions they span, which is what the
//! Friedman statistic is defined over and what every reference implementation
//! does.
//!
//! # What is exact and what is approximate
//!
//! - **Friedman** is the standard chi-square approximation with the tie
//!   correction, reported beside the **Iman–Davenport F**, which is less
//!   conservative and is the form the comparison literature prefers. Both are
//!   approximations; with ten blocks and thirty-one treatments they disagree
//!   enough to be worth printing together rather than picking one.
//! - **Wilcoxon signed-rank** is computed **exactly** when the sample is small
//!   and has no ties among the absolute differences — the exact null
//!   distribution is a subset-sum count and ten functions is 1,024 subsets.
//!   With ties or a larger sample it falls back to the normal approximation
//!   with the tie correction. The result says which was used: an exact p-value
//!   and an approximate one are not the same claim and should not be printed
//!   the same way.
//!
//! Every function here is checked against SciPy's `friedmanchisquare` and
//! `wilcoxon` in `tests/`, because a statistic that is subtly wrong looks
//! exactly like a statistic that is right.

use std::f64::consts::PI;

// ───────────────────────────────────────────────────────────────── ranking

/// Ranks of `row`, smallest value getting rank 1, ties sharing the average of
/// the positions they span.
///
/// Returned in the order of `row`, not sorted.
pub fn average_ranks(row: &[f64]) -> Vec<f64> {
    let n = row.len();
    let mut order: Vec<usize> = (0..n).collect();
    order.sort_by(|&a, &b| row[a].partial_cmp(&row[b]).unwrap_or(std::cmp::Ordering::Equal));

    let mut ranks = vec![0.0; n];
    let mut i = 0;
    while i < n {
        let mut j = i;
        while j + 1 < n && row[order[j + 1]] == row[order[i]] {
            j += 1;
        }
        // Positions i..=j are 1-based ranks i+1..=j+1; they share the mean.
        let shared = ((i + 1 + j + 1) as f64) / 2.0;
        for &idx in &order[i..=j] {
            ranks[idx] = shared;
        }
        i = j + 1;
    }
    ranks
}

/// Sizes of each group of tied values in `row`, for the tie correction.
fn tie_group_sizes(row: &[f64]) -> Vec<usize> {
    let mut v: Vec<f64> = row.to_vec();
    v.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let mut out = Vec::new();
    let mut i = 0;
    while i < v.len() {
        let mut j = i;
        while j + 1 < v.len() && v[j + 1] == v[i] {
            j += 1;
        }
        out.push(j - i + 1);
        i = j + 1;
    }
    out
}

// ───────────────────────────────────────────────────────────────── Friedman

/// The Friedman test across `k` treatments measured on `n` blocks.
#[derive(Debug, Clone, PartialEq)]
pub struct FriedmanResult {
    /// Treatments compared.
    pub k: usize,
    /// Blocks (here: benchmark functions) each treatment was measured on.
    pub n: usize,
    /// Average rank per treatment, in the column order given.
    pub average_ranks: Vec<f64>,
    /// Tie-corrected chi-square statistic.
    pub chi_square: f64,
    pub chi_square_df: usize,
    pub chi_square_p: f64,
    /// Iman–Davenport F, derived from the same statistic.
    pub iman_davenport_f: f64,
    pub f_df1: usize,
    pub f_df2: usize,
    pub iman_davenport_p: f64,
}

/// Friedman over `blocks`, where `blocks[i][j]` is treatment `j` measured on
/// block `i`. Lower is better; every block must have the same width.
///
/// Returns `None` when there are fewer than two treatments or fewer than two
/// blocks, or when the rows are ragged — cases where the statistic is not
/// defined. A test that returns a number for an undefined input is worse than
/// one that refuses, because the number gets published.
pub fn friedman(blocks: &[Vec<f64>]) -> Option<FriedmanResult> {
    let n = blocks.len();
    if n < 2 {
        return None;
    }
    let k = blocks[0].len();
    if k < 2 || blocks.iter().any(|b| b.len() != k) {
        return None;
    }

    let mut rank_sums = vec![0.0; k];
    let mut tie_term = 0.0;
    for block in blocks {
        let r = average_ranks(block);
        for j in 0..k {
            rank_sums[j] += r[j];
        }
        for t in tie_group_sizes(block) {
            let t = t as f64;
            tie_term += t * t * t - t;
        }
    }

    let nf = n as f64;
    let kf = k as f64;
    let sum_sq: f64 = rank_sums.iter().map(|r| r * r).sum();
    let mut chi = 12.0 / (nf * kf * (kf + 1.0)) * sum_sq - 3.0 * nf * (kf + 1.0);

    // Tie correction. Without it a suite where several solvers hit the global
    // minimum on the same function reports a statistic that is too small, i.e.
    // it under-rejects: the direction that quietly says "no difference".
    let correction = 1.0 - tie_term / (nf * (kf * kf * kf - kf));
    if correction > 0.0 {
        chi /= correction;
    }

    let df = k - 1;
    let chi_p = chi_square_sf(chi, df as f64);

    // Iman-Davenport. Undefined when chi equals n(k-1) exactly (the divisor is
    // zero), which means the ranks are perfectly separated; report +inf and a
    // p-value of 0 rather than a NaN that serialises as `null`.
    let denom = nf * (kf - 1.0) - chi;
    let f = if denom <= 0.0 {
        f64::INFINITY
    } else {
        (nf - 1.0) * chi / denom
    };
    let df1 = k - 1;
    let df2 = (k - 1) * (n - 1);
    let f_p = if f.is_infinite() { 0.0 } else { f_sf(f, df1 as f64, df2 as f64) };

    Some(FriedmanResult {
        k,
        n,
        average_ranks: rank_sums.iter().map(|r| r / nf).collect(),
        chi_square: chi,
        chi_square_df: df,
        chi_square_p: chi_p,
        iman_davenport_f: f,
        f_df1: df1,
        f_df2: df2,
        iman_davenport_p: f_p,
    })
}

// ─────────────────────────────────────────────────────────────── Wilcoxon

/// How the p-value was obtained. Printed beside it, because an exact p-value
/// and an approximated one are different claims.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WilcoxonMethod {
    /// Enumerated null distribution. No ties, small sample.
    Exact,
    /// Normal approximation with the tie correction.
    Normal,
}

#[derive(Debug, Clone, PartialEq)]
pub struct WilcoxonResult {
    /// Pairs left after dropping zero differences.
    pub n: usize,
    /// Sum of ranks of positive differences, and of negative ones.
    pub w_plus: f64,
    pub w_minus: f64,
    /// `min(w_plus, w_minus)`, the two-sided statistic.
    pub statistic: f64,
    pub p_two_sided: f64,
    pub method: WilcoxonMethod,
}

/// Largest sample for which the exact distribution is enumerated. 2^25 is
/// about 34 million steps; the DP below is O(n · max_sum) and far cheaper, but
/// the cap keeps the memory bounded and nothing here needs more.
const EXACT_MAX_N: usize = 25;

/// Two-sided Wilcoxon signed-rank test on paired samples.
///
/// Zero differences are dropped, which is Wilcoxon's own handling and SciPy's
/// default. Returns `None` if the samples differ in length, are empty, or are
/// all zero differences — with nothing left to rank there is no test, and
/// returning `p = 1` would read as "measured, no difference" when the truth is
/// "the two solvers produced identical numbers".
pub fn wilcoxon_signed_rank(a: &[f64], b: &[f64]) -> Option<WilcoxonResult> {
    if a.len() != b.len() || a.is_empty() {
        return None;
    }
    let diffs: Vec<f64> = a
        .iter()
        .zip(b)
        .map(|(x, y)| x - y)
        .filter(|d| *d != 0.0)
        .collect();
    let n = diffs.len();
    if n == 0 {
        return None;
    }

    let abs: Vec<f64> = diffs.iter().map(|d| d.abs()).collect();
    let ranks = average_ranks(&abs);
    let mut w_plus = 0.0;
    let mut w_minus = 0.0;
    for (d, r) in diffs.iter().zip(&ranks) {
        if *d > 0.0 {
            w_plus += r;
        } else {
            w_minus += r;
        }
    }
    let statistic = w_plus.min(w_minus);

    let tie_sizes = tie_group_sizes(&abs);
    let has_ties = tie_sizes.iter().any(|&t| t > 1);
    let dropped_zeros = diffs.len() != a.len();

    // The exact null distribution assumes the ranks are 1..n with no ties. It
    // is also only the *conditional* distribution once zeros are dropped, so
    // both conditions have to hold.
    let (p, method) = if !has_ties && !dropped_zeros && n <= EXACT_MAX_N {
        (exact_two_sided_p(statistic, n), WilcoxonMethod::Exact)
    } else {
        (normal_two_sided_p(w_plus, n, &tie_sizes), WilcoxonMethod::Normal)
    };

    Some(WilcoxonResult {
        n,
        w_plus,
        w_minus,
        statistic,
        p_two_sided: p,
        method,
    })
}

/// `2 · P(W ≤ w)` under the null, by counting subsets of `{1..n}`.
///
/// `counts[s]` is the number of sign assignments whose positive-rank sum is
/// `s`; the null gives each of the `2^n` assignments equal probability.
fn exact_two_sided_p(w: f64, n: usize) -> f64 {
    let max_sum = n * (n + 1) / 2;
    let mut counts = vec![0f64; max_sum + 1];
    counts[0] = 1.0;
    for rank in 1..=n {
        for s in (rank..=max_sum).rev() {
            counts[s] += counts[s - rank];
        }
    }
    let total: f64 = 2f64.powi(n as i32);
    let w = w.floor() as usize;
    let tail: f64 = counts[..=w.min(max_sum)].iter().sum();
    (2.0 * tail / total).min(1.0)
}

/// Normal approximation with the tie correction, no continuity correction
/// (matching SciPy's default so the two can be compared directly).
fn normal_two_sided_p(w_plus: f64, n: usize, tie_sizes: &[usize]) -> f64 {
    let nf = n as f64;
    let mean = nf * (nf + 1.0) / 4.0;
    let tie_term: f64 = tie_sizes
        .iter()
        .map(|&t| {
            let t = t as f64;
            t * t * t - t
        })
        .sum();
    let var = nf * (nf + 1.0) * (2.0 * nf + 1.0) / 24.0 - tie_term / 48.0;
    if var <= 0.0 {
        return 1.0;
    }
    let z = (w_plus - mean) / var.sqrt();
    (2.0 * normal_sf(z.abs())).min(1.0)
}

// ──────────────────────────────────────────────── multiple comparisons

/// Holm–Bonferroni adjusted p-values, returned in the input order.
///
/// Comparing every pair of 31 solvers is 465 tests. At α = 0.05 that is about
/// 23 rejections expected from noise alone, so a table of raw pairwise
/// p-values does not support the sentence anybody reads it for — "solver A
/// beats solver B". Holm controls the family-wise error rate, is uniformly
/// more powerful than Bonferroni, and needs no independence assumption, which
/// matters here because the comparisons share solvers and are correlated by
/// construction.
///
/// Adjusted values are made monotone (each at least the previous), so a test
/// can never be rejected while a smaller raw p-value in the same family is
/// not. Values are capped at 1.
pub fn holm_adjust(p_values: &[f64]) -> Vec<f64> {
    let m = p_values.len();
    if m == 0 {
        return Vec::new();
    }
    let mut order: Vec<usize> = (0..m).collect();
    order.sort_by(|&a, &b| {
        p_values[a]
            .partial_cmp(&p_values[b])
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    let mut out = vec![0.0; m];
    let mut running: f64 = 0.0;
    for (i, &idx) in order.iter().enumerate() {
        let scaled = (m - i) as f64 * p_values[idx];
        running = running.max(scaled).min(1.0);
        out[idx] = running;
    }
    out
}

// ────────────────────────────────────────────────── distribution functions
//
// Standard series/continued-fraction evaluations. They are here rather than
// pulled in as a dependency because the crate has no statistics dependency and
// adding one for three functions is a larger change than writing them; the
// tests check them against SciPy.

/// Upper tail of the standard normal.
pub fn normal_sf(x: f64) -> f64 {
    0.5 * erfc(x / std::f64::consts::SQRT_2)
}

/// Complementary error function, by its relation to the incomplete gamma.
fn erfc(x: f64) -> f64 {
    if x < 0.0 {
        2.0 - erfc(-x)
    } else {
        gamma_q(0.5, x * x)
    }
}

/// Upper tail of the chi-square distribution with `df` degrees of freedom.
pub fn chi_square_sf(x: f64, df: f64) -> f64 {
    if x <= 0.0 {
        return 1.0;
    }
    gamma_q(df / 2.0, x / 2.0)
}

/// Upper tail of the F distribution.
pub fn f_sf(x: f64, df1: f64, df2: f64) -> f64 {
    if x <= 0.0 {
        return 1.0;
    }
    betainc_regularized(df2 / (df2 + df1 * x), df2 / 2.0, df1 / 2.0)
}

/// Regularized upper incomplete gamma `Q(a, x)`.
fn gamma_q(a: f64, x: f64) -> f64 {
    if x < 0.0 || a <= 0.0 {
        return f64::NAN;
    }
    if x == 0.0 {
        return 1.0;
    }
    if x < a + 1.0 {
        1.0 - gamma_p_series(a, x)
    } else {
        gamma_q_continued_fraction(a, x)
    }
}

fn gamma_p_series(a: f64, x: f64) -> f64 {
    let mut ap = a;
    let mut sum = 1.0 / a;
    let mut del = sum;
    for _ in 0..1000 {
        ap += 1.0;
        del *= x / ap;
        sum += del;
        if del.abs() < sum.abs() * 1e-16 {
            break;
        }
    }
    sum * (-x + a * x.ln() - ln_gamma(a)).exp()
}

fn gamma_q_continued_fraction(a: f64, x: f64) -> f64 {
    const TINY: f64 = 1e-300;
    let mut b = x + 1.0 - a;
    let mut c = 1.0 / TINY;
    let mut d = 1.0 / b;
    let mut h = d;
    for i in 1..1000 {
        let an = -(i as f64) * (i as f64 - a);
        b += 2.0;
        d = an * d + b;
        if d.abs() < TINY {
            d = TINY;
        }
        c = b + an / c;
        if c.abs() < TINY {
            c = TINY;
        }
        d = 1.0 / d;
        let del = d * c;
        h *= del;
        if (del - 1.0).abs() < 1e-16 {
            break;
        }
    }
    (-x + a * x.ln() - ln_gamma(a)).exp() * h
}

/// Regularized incomplete beta `I_x(a, b)`.
fn betainc_regularized(x: f64, a: f64, b: f64) -> f64 {
    if x <= 0.0 {
        return 0.0;
    }
    if x >= 1.0 {
        return 1.0;
    }
    let front =
        (ln_gamma(a + b) - ln_gamma(a) - ln_gamma(b) + a * x.ln() + b * (1.0 - x).ln()).exp();
    if x < (a + 1.0) / (a + b + 2.0) {
        front * beta_continued_fraction(x, a, b) / a
    } else {
        1.0 - front * beta_continued_fraction(1.0 - x, b, a) / b
    }
}

fn beta_continued_fraction(x: f64, a: f64, b: f64) -> f64 {
    const TINY: f64 = 1e-300;
    let qab = a + b;
    let qap = a + 1.0;
    let qam = a - 1.0;
    let mut c = 1.0;
    let mut d = 1.0 - qab * x / qap;
    if d.abs() < TINY {
        d = TINY;
    }
    d = 1.0 / d;
    let mut h = d;
    for m in 1..1000 {
        let m = m as f64;
        let m2 = 2.0 * m;

        let aa = m * (b - m) * x / ((qam + m2) * (a + m2));
        d = 1.0 + aa * d;
        if d.abs() < TINY {
            d = TINY;
        }
        c = 1.0 + aa / c;
        if c.abs() < TINY {
            c = TINY;
        }
        d = 1.0 / d;
        h *= d * c;

        let aa = -(a + m) * (qab + m) * x / ((a + m2) * (qap + m2));
        d = 1.0 + aa * d;
        if d.abs() < TINY {
            d = TINY;
        }
        c = 1.0 + aa / c;
        if c.abs() < TINY {
            c = TINY;
        }
        d = 1.0 / d;
        let del = d * c;
        h *= del;
        if (del - 1.0).abs() < 1e-16 {
            break;
        }
    }
    h
}

/// Lanczos approximation to `ln Γ(x)`.
fn ln_gamma(x: f64) -> f64 {
    const G: [f64; 9] = [
        0.999_999_999_999_809_93,
        676.520_368_121_885_1,
        -1259.139_216_722_402_8,
        771.323_428_777_653_1,
        -176.615_029_162_140_6,
        12.507_343_278_686_905,
        -0.138_571_095_265_720_12,
        9.984_369_578_019_572e-6,
        1.505_632_735_149_311_6e-7,
    ];
    if x < 0.5 {
        // Reflection, so the approximation is only ever evaluated where it is
        // accurate.
        (PI / (PI * x).sin()).ln() - ln_gamma(1.0 - x)
    } else {
        let x = x - 1.0;
        let mut a = G[0];
        let t = x + 7.5;
        for (i, g) in G.iter().enumerate().skip(1) {
            a += g / (x + i as f64);
        }
        0.5 * (2.0 * PI).ln() + (x + 0.5) * t.ln() - t + a.ln()
    }
}
