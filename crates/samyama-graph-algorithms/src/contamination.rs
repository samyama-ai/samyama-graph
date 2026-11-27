//! Benchmark contamination: which corpus texts contain a benchmark item (ML-12).
//!
//! A model trained on a corpus that contains the questions of a public
//! benchmark scores well on that benchmark by remembering, not by answering.
//! The check is the one used for LLM pre-training data: compare word n-grams.
//! A benchmark item counts as **seen** by a corpus text when a large share of
//! the benchmark item's n-grams also occur in the corpus text.
//!
//! **Normalisation.** Text is lower-cased (Unicode case folding by
//! `char::to_lowercase`) and split into tokens on every character that is not
//! alphanumeric (`char::is_alphanumeric`, so accented and non-Latin letters and
//! digits are tokens, and punctuation, whitespace and symbols are separators).
//! "What is 2+2?" and "what is 2 + 2" are therefore the same four tokens.
//!
//! **n-grams.** An item's n-grams are its distinct windows of `n` consecutive
//! tokens, as a set: a phrase repeated in one text counts once. A benchmark
//! item with fewer than `n` tokens (but at least one) has a single gram, its
//! whole token sequence, and is seen by a corpus text that contains that
//! sequence contiguously. Without that rule a short benchmark question could
//! never be found, however literally it was copied. A text with no tokens has
//! no grams and is never matched.
//!
//! **Overlap** of a benchmark item `b` in a corpus text `c` is
//! `|grams(b) ∩ grams(c)| / |grams(b)|`: the share of the benchmark item that
//! appears in the corpus text, 1.0 for a verbatim copy wherever it sits in a
//! longer text. It is measured against the benchmark item, not the corpus
//! text, because contamination is about the benchmark item being seen; a long
//! corpus text quoting a short question is fully contaminated by it.
//!
//! Deterministic: no randomness, and rows come out in one fixed order.

use std::collections::{BTreeMap, HashMap, HashSet};

/// The default n: long enough that common phrases ("what is the") do not
/// count, short enough that a typical one-sentence benchmark question has
/// several grams. Callers set `n` for their benchmark.
pub const DEFAULT_NGRAM: usize = 8;

/// Lower-cased alphanumeric tokens of `text`, in order.
pub fn tokenize(text: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut cur = String::new();
    for ch in text.chars() {
        if ch.is_alphanumeric() {
            cur.extend(ch.to_lowercase());
        } else if !cur.is_empty() {
            out.push(std::mem::take(&mut cur));
        }
    }
    if !cur.is_empty() {
        out.push(cur);
    }
    out
}

/// The distinct windows of `k` tokens, each joined with a separator no token
/// can contain. Empty when there are fewer than `k` tokens or `k` is 0.
fn windows(tokens: &[String], k: usize) -> HashSet<String> {
    if k == 0 || tokens.len() < k {
        return HashSet::new();
    }
    tokens.windows(k).map(|w| w.join("\u{1}")).collect()
}

/// One benchmark item seen in one corpus text.
#[derive(Debug, Clone, PartialEq)]
pub struct Contamination {
    /// The corpus text's id.
    pub corpus: u64,
    /// The benchmark item's id.
    pub benchmark: u64,
    /// The benchmark item's grams that also occur in the corpus text.
    pub shared: usize,
    /// The benchmark item's grams, the denominator of `overlap`.
    pub ngrams: usize,
    /// `shared / ngrams`, in (0, 1].
    pub overlap: f64,
}

/// Every (corpus text, benchmark item) pair whose overlap is at least
/// `min_overlap`, ordered by overlap (highest first), then corpus id, then
/// benchmark id. A node in both lists is never paired with itself.
///
/// `n` must be at least 1 and `min_overlap` in (0, 1]; the caller checks both.
pub fn ngram_contamination(
    corpus: &[(u64, &str)],
    benchmark: &[(u64, &str)],
    n: usize,
    min_overlap: f64,
) -> Vec<Contamination> {
    assert!(n >= 1, "n must be at least 1");
    // Each benchmark item's grams, at its own window length k = min(n, len).
    // index[k][gram] lists the benchmark items holding that gram.
    let mut sizes: HashMap<u64, usize> = HashMap::new();
    let mut index: BTreeMap<usize, HashMap<String, Vec<u64>>> = BTreeMap::new();
    for &(id, text) in benchmark {
        let tokens = tokenize(text);
        if tokens.is_empty() {
            continue;
        }
        let k = n.min(tokens.len());
        let grams = windows(&tokens, k);
        sizes.insert(id, grams.len());
        let by_gram = index.entry(k).or_default();
        for g in grams {
            by_gram.entry(g).or_default().push(id);
        }
    }

    let mut out = Vec::new();
    for &(cid, text) in corpus {
        let tokens = tokenize(text);
        let mut shared: HashMap<u64, usize> = HashMap::new();
        for (&k, by_gram) in &index {
            for g in windows(&tokens, k) {
                if let Some(ids) = by_gram.get(&g) {
                    for &bid in ids {
                        if bid != cid {
                            *shared.entry(bid).or_default() += 1;
                        }
                    }
                }
            }
        }
        for (bid, s) in shared {
            let total = sizes[&bid];
            let overlap = s as f64 / total as f64;
            if overlap >= min_overlap {
                out.push(Contamination {
                    corpus: cid,
                    benchmark: bid,
                    shared: s,
                    ngrams: total,
                    overlap,
                });
            }
        }
    }
    out.sort_by(|a, b| {
        b.overlap
            .total_cmp(&a.overlap)
            .then(a.corpus.cmp(&b.corpus))
            .then(a.benchmark.cmp(&b.benchmark))
    });
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tokens_ignore_case_punctuation_and_spacing() {
        assert_eq!(tokenize("What is 2+2?"), tokenize("what   is 2 + 2"));
        assert_eq!(tokenize("Ünïcödé, ÉCOLE!"), vec!["ünïcödé", "école"]);
        assert!(tokenize("  ?!. ").is_empty());
    }

    #[test]
    fn a_verbatim_copy_inside_a_longer_text_is_fully_seen() {
        let q = "which river flows through the city of paris on its way to the sea";
        let c = format!("Some preamble here. {q} And then the article goes on.");
        let r = ngram_contamination(&[(1, c.as_str())], &[(10, q)], 8, 0.5);
        assert_eq!(r.len(), 1);
        assert_eq!(r[0].overlap, 1.0);
        assert_eq!(r[0].shared, r[0].ngrams);
    }

    #[test]
    fn unrelated_text_is_not_seen() {
        let r = ngram_contamination(
            &[(
                1,
                "the quick brown fox jumps over the lazy dog every single morning",
            )],
            &[(
                10,
                "which river flows through the city of paris on its way to the sea",
            )],
            8,
            0.01,
        );
        assert!(r.is_empty());
    }

    #[test]
    fn a_short_item_matches_as_a_whole_sequence() {
        let r = ngram_contamination(
            &[(1, "Q: What is 2 + 2? A: four")],
            &[(10, "what is 2+2")],
            8,
            1.0,
        );
        assert_eq!(r.len(), 1);
        assert_eq!((r[0].shared, r[0].ngrams), (1, 1));
        // The same tokens out of order are not the sequence.
        let r = ngram_contamination(&[(1, "2 2 is what")], &[(10, "what is 2+2")], 8, 0.01);
        assert!(r.is_empty());
    }

    #[test]
    fn empty_texts_never_match_and_a_node_is_not_paired_with_itself() {
        let r = ngram_contamination(&[(1, ""), (2, "a b c")], &[(2, "a b c"), (3, "")], 2, 0.01);
        assert!(r.is_empty(), "{r:?}");
    }

    #[test]
    fn rows_are_ordered_and_thresholded() {
        let q = "one two three four five six seven eight nine ten";
        let half = "one two three four five six xx yy zz ww";
        let r = ngram_contamination(&[(5, half), (4, q), (3, q)], &[(10, q)], 3, 0.25);
        let got: Vec<(u64, f64)> = r.iter().map(|c| (c.corpus, c.overlap)).collect();
        assert_eq!(got, vec![(3, 1.0), (4, 1.0), (5, 0.5)]);
        assert!(ngram_contamination(&[(5, half)], &[(10, q)], 3, 0.6).is_empty());
    }
}
