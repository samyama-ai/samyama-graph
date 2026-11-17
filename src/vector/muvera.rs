//! MUVERA fixed-dimensional encodings of multi-vectors (#1978).
//!
//! A late-interaction document (ColBERT, ColPali) is a set of token vectors,
//! scored against a query's set by MaxSim (`vector.similarity.maxSim`). An
//! HNSW index holds one vector per node, so it cannot search by MaxSim
//! directly. MUVERA (Dhulipala et al., NeurIPS 2024) maps each set to one
//! vector whose inner product approximates MaxSim. The encodings go into an
//! ordinary vector index for candidate generation, and the candidates are
//! reranked by exact MaxSim.
//!
//! One repetition does three things:
//!
//! 1. Partition: `bits` random Gaussian hyperplanes (SimHash) put each token
//!    into one of `2^bits` buckets, by the signs of its projections.
//! 2. Project: each token is mapped to `projection` dimensions by a random
//!    ±1 matrix scaled by `1/sqrt(projection)`, so inner products are kept in
//!    expectation.
//! 3. Aggregate per bucket. A **query** sums its projected tokens in each
//!    bucket. A **document** averages them, and fills a bucket it has no token
//!    in with its token whose bucket is nearest in Hamming distance (the
//!    paper's `fill_empty_partitions`). Every query token therefore meets a
//!    document vector from its own region of space.
//!
//! The `repetitions` blocks are concatenated and scaled by
//! `1/sqrt(repetitions)`, so the inner product of a query encoding and a
//! document encoding averages the repetitions instead of summing them. The
//! paper's final random projection is not applied.
//!
//! Each query token contributes, in each repetition, its inner product with
//! one document vector from its bucket: the mean of the document's tokens
//! there, standing in for its best match. The encoding's inner product is
//! therefore an estimate of MaxSim (DOT), not a bound on it, and it is only as
//! good as candidate generation needs. Rerank with exact MaxSim.
//!
//! The randomness is a function of `seed`, the token dimension, and the
//! shape parameters alone, so queries and documents encoded with the same
//! settings share their hyperplanes and projections on every machine and in
//! every release that keeps [`FORMAT`]. Encodings made with different
//! settings are not comparable.

/// Changes whenever an encoding produced from the same input and settings
/// would change. Stored encodings from another format must be recomputed.
pub const FORMAT: u32 = 1;

/// Upper bounds on the shape, so that one call cannot ask for an encoding
/// too large to hold. `2^bits * projection * repetitions` is the output
/// dimension; these bounds cap it at 2^20 floats (4 MiB).
pub const MAX_BITS: usize = 10;
pub const MAX_PROJECTION: usize = 1024;
pub const MAX_REPETITIONS: usize = 64;
pub const MAX_DIMENSION: usize = 1 << 20;

/// Which side of MaxSim a multi-vector is on. The two are encoded
/// differently, and only a query encoding against a document encoding
/// estimates MaxSim.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Role {
    Query,
    Document,
}

/// The shape of an encoding, and the seed of its randomness.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Params {
    /// SimHash hyperplanes per repetition: `2^bits` buckets.
    pub bits: usize,
    /// Dimensions each token is projected to.
    pub projection: usize,
    /// Independent repetitions, concatenated.
    pub repetitions: usize,
    pub seed: u64,
}

impl Default for Params {
    /// 16 buckets x 8 dimensions x 8 repetitions: a 1,024-dimension
    /// encoding. The paper's largest settings are larger; these are a
    /// starting point for an index of ordinary size, not a tuned choice.
    fn default() -> Self {
        Params {
            bits: 4,
            projection: 8,
            repetitions: 8,
            seed: 0,
        }
    }
}

impl Params {
    /// The length of every encoding made with these parameters.
    pub fn dimension(&self) -> usize {
        (1usize << self.bits) * self.projection * self.repetitions
    }

    fn check(&self) -> Result<(), String> {
        if self.bits > MAX_BITS {
            return Err(format!(
                "bits must be at most {MAX_BITS}; got {}",
                self.bits
            ));
        }
        if self.projection == 0 || self.projection > MAX_PROJECTION {
            return Err(format!(
                "projection must be between 1 and {MAX_PROJECTION}; got {}",
                self.projection
            ));
        }
        if self.repetitions == 0 || self.repetitions > MAX_REPETITIONS {
            return Err(format!(
                "repetitions must be between 1 and {MAX_REPETITIONS}; got {}",
                self.repetitions
            ));
        }
        if self.dimension() > MAX_DIMENSION {
            return Err(format!(
                "2^bits x projection x repetitions is {}; at most {MAX_DIMENSION}",
                self.dimension()
            ));
        }
        Ok(())
    }
}

/// SplitMix64: small, fast, and fully specified, so the stream is the same
/// everywhere. Not for anything that needs unpredictability.
struct SplitMix64(u64);

impl SplitMix64 {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Uniform in (0, 1]: never 0, so its logarithm is finite.
    fn unit(&mut self) -> f64 {
        ((self.next() >> 11) + 1) as f64 / (1u64 << 53) as f64
    }

    /// Standard normal, by Box-Muller (one of the pair, for simplicity).
    fn gaussian(&mut self) -> f64 {
        let (u, v) = (self.unit(), self.unit());
        (-2.0 * u.ln()).sqrt() * (std::f64::consts::TAU * v).cos()
    }
}

/// One repetition's hyperplanes (`bits x dim`) and projection
/// (`projection x dim`, entries ±1/sqrt(projection)).
struct Repetition {
    planes: Vec<f64>,
    signs: Vec<f64>,
}

impl Repetition {
    fn new(params: &Params, dim: usize, rep: usize) -> Self {
        // A stream per repetition, from the seed, the repetition, and the
        // shape, so adding repetitions leaves the earlier ones unchanged.
        let mut rng = SplitMix64(
            params.seed
                ^ (rep as u64).wrapping_mul(0xD1B5_4A32_D192_ED03)
                ^ (dim as u64).wrapping_mul(0xA24B_AED4_963E_E407)
                ^ ((params.bits as u64) << 48)
                ^ ((params.projection as u64) << 32),
        );
        let planes = (0..params.bits * dim).map(|_| rng.gaussian()).collect();
        let scale = 1.0 / (params.projection as f64).sqrt();
        let signs = (0..params.projection * dim)
            .map(|_| if rng.next() >> 63 == 0 { scale } else { -scale })
            .collect();
        Repetition { planes, signs }
    }

    fn bucket(&self, token: &[f32], bits: usize) -> usize {
        let dim = token.len();
        (0..bits).fold(0, |b, i| {
            let side: f64 = self.planes[i * dim..(i + 1) * dim]
                .iter()
                .zip(token)
                .map(|(p, x)| p * *x as f64)
                .sum();
            (b << 1) | usize::from(side > 0.0)
        })
    }

    fn project(&self, token: &[f32], out: &mut [f64]) {
        let dim = token.len();
        for (j, o) in out.iter_mut().enumerate() {
            *o = self.signs[j * dim..(j + 1) * dim]
                .iter()
                .zip(token)
                .map(|(s, x)| s * *x as f64)
                .sum();
        }
    }
}

/// The encoding of `tokens` (non-empty, one dimension, finite: the caller
/// checks) as `role`, `params.dimension()` long.
pub fn encode(tokens: &[Vec<f32>], role: Role, params: &Params) -> Result<Vec<f32>, String> {
    params.check()?;
    let dim = match tokens.first() {
        Some(t) if !t.is_empty() => t.len(),
        _ => return Err("an encoding needs at least one token".into()),
    };
    if tokens.iter().any(|t| t.len() != dim) {
        return Err("every token must have the same dimension".into());
    }
    let buckets = 1usize << params.bits;
    let p = params.projection;
    let block = buckets * p;
    let scale = 1.0 / (params.repetitions as f64).sqrt();
    let mut out = vec![0f32; params.dimension()];
    let mut projected = vec![vec![0f64; p]; tokens.len()];
    let mut sums = vec![0f64; block];
    let mut counts = vec![0usize; buckets];
    for rep in 0..params.repetitions {
        let r = Repetition::new(params, dim, rep);
        sums.iter_mut().for_each(|s| *s = 0.0);
        counts.iter_mut().for_each(|c| *c = 0);
        let mut homes = Vec::with_capacity(tokens.len());
        for (t, token) in tokens.iter().enumerate() {
            let b = r.bucket(token, params.bits);
            r.project(token, &mut projected[t]);
            for (s, x) in sums[b * p..(b + 1) * p].iter_mut().zip(&projected[t]) {
                *s += x;
            }
            counts[b] += 1;
            homes.push(b);
        }
        let dst = &mut out[rep * block..(rep + 1) * block];
        for b in 0..buckets {
            let cell = &mut dst[b * p..(b + 1) * p];
            match role {
                Role::Query => {
                    for (o, s) in cell.iter_mut().zip(&sums[b * p..(b + 1) * p]) {
                        *o = (s * scale) as f32;
                    }
                }
                Role::Document if counts[b] > 0 => {
                    let n = counts[b] as f64;
                    for (o, s) in cell.iter_mut().zip(&sums[b * p..(b + 1) * p]) {
                        *o = (s / n * scale) as f32;
                    }
                }
                Role::Document => {
                    // Nearest token by bucket Hamming distance; the first
                    // such token on a tie, so the result does not depend on
                    // anything but the input order.
                    let nearest = (0..tokens.len())
                        .min_by_key(|&t| ((homes[t] ^ b).count_ones(), t))
                        .expect("at least one token");
                    for (o, x) in cell.iter_mut().zip(&projected[nearest]) {
                        *o = (x * scale) as f32;
                    }
                }
            }
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tokens(n: usize, dim: usize, seed: u64) -> Vec<Vec<f32>> {
        let mut rng = SplitMix64(seed);
        (0..n)
            .map(|_| (0..dim).map(|_| rng.gaussian() as f32).collect())
            .collect()
    }

    #[test]
    fn the_shape_is_buckets_by_projection_by_repetitions() {
        let p = Params {
            bits: 3,
            projection: 5,
            repetitions: 2,
            seed: 1,
        };
        let e = encode(&tokens(4, 7, 1), Role::Query, &p).unwrap();
        assert_eq!(e.len(), 8 * 5 * 2);
        assert_eq!(p.dimension(), 80);
    }

    #[test]
    fn a_query_encoding_is_additive_over_tokens() {
        let p = Params {
            bits: 3,
            projection: 6,
            repetitions: 4,
            seed: 9,
        };
        let t = tokens(5, 12, 2);
        let whole = encode(&t, Role::Query, &p).unwrap();
        let mut parts = vec![0f64; whole.len()];
        for token in &t {
            for (s, x) in parts
                .iter_mut()
                .zip(encode(std::slice::from_ref(token), Role::Query, &p).unwrap())
            {
                *s += x as f64;
            }
        }
        for (a, b) in whole.iter().zip(&parts) {
            assert!((*a as f64 - b).abs() < 1e-4, "{a} vs {b}");
        }
    }

    #[test]
    fn a_one_token_document_fills_every_bucket_with_that_token() {
        let p = Params {
            bits: 3,
            projection: 4,
            repetitions: 2,
            seed: 3,
        };
        let e = encode(&tokens(1, 6, 4), Role::Document, &p).unwrap();
        for rep in e.chunks(8 * 4) {
            for cell in rep.chunks(4) {
                assert_eq!(cell, &rep[..4]);
            }
        }
    }

    #[test]
    fn a_document_averages_and_ignores_repeats() {
        let p = Params::default();
        let t = tokens(3, 16, 5);
        let doubled: Vec<_> = t.iter().chain(&t).cloned().collect();
        let a = encode(&t, Role::Document, &p).unwrap();
        let b = encode(&doubled, Role::Document, &p).unwrap();
        for (x, y) in a.iter().zip(&b) {
            assert!((x - y).abs() < 1e-5);
        }
    }

    #[test]
    fn the_same_settings_give_the_same_encoding_and_another_seed_does_not() {
        let t = tokens(4, 16, 6);
        let p = Params::default();
        assert_eq!(
            encode(&t, Role::Query, &p).unwrap(),
            encode(&t, Role::Query, &p).unwrap()
        );
        let other = Params { seed: 1, ..p };
        assert_ne!(
            encode(&t, Role::Query, &p).unwrap(),
            encode(&t, Role::Query, &other).unwrap()
        );
    }

    #[test]
    fn more_repetitions_keep_the_earlier_ones() {
        let t = tokens(4, 16, 7);
        let short = Params {
            repetitions: 2,
            ..Params::default()
        };
        let long = Params {
            repetitions: 5,
            ..Params::default()
        };
        let (a, b) = (
            encode(&t, Role::Query, &short).unwrap(),
            encode(&t, Role::Query, &long).unwrap(),
        );
        // Same blocks, scaled by 1/sqrt(repetitions) instead.
        let ratio = (5f64 / 2.0).sqrt();
        for (x, y) in a.iter().zip(&b[..a.len()]) {
            assert!((*x as f64 - *y as f64 * ratio).abs() < 1e-4);
        }
    }

    #[test]
    fn out_of_range_shapes_are_refused() {
        let t = tokens(1, 4, 8);
        for p in [
            Params {
                bits: MAX_BITS + 1,
                ..Params::default()
            },
            Params {
                projection: 0,
                ..Params::default()
            },
            Params {
                repetitions: 0,
                ..Params::default()
            },
            Params {
                bits: 10,
                projection: 1024,
                repetitions: 2,
                seed: 0,
            },
        ] {
            assert!(encode(&t, Role::Query, &p).is_err(), "{p:?}");
        }
    }

    #[test]
    fn the_gaussian_stream_has_the_moments_of_a_standard_normal() {
        let mut rng = SplitMix64(42);
        let xs: Vec<f64> = (0..200_000).map(|_| rng.gaussian()).collect();
        let mean = xs.iter().sum::<f64>() / xs.len() as f64;
        let var = xs.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / xs.len() as f64;
        assert!(mean.abs() < 0.01, "{mean}");
        assert!((var - 1.0).abs() < 0.02, "{var}");
    }
}
