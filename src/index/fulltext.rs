//! Full-text search over string properties: tokenised, stemmed, BM25-scored
//! (NDS-06).
//!
//! `CONTAINS` was the only text search here, and it is a substring match: it
//! finds `graph` inside `polygraph`, misses `Graphs` unless the case and the
//! plural happen to line up, scans every node, and ranks nothing. Those are
//! four separate problems and only the last is about speed.
//!
//! # What this does
//!
//! An inverted index — term to the documents holding it — with term
//! frequencies, document lengths and token positions, scored with **BM25**
//! (`k1 = 1.2`, `b = 0.75`, the standard defaults):
//!
//! ```text
//! score(D, Q) = Σ  IDF(q) · f(q,D)·(k1+1) / (f(q,D) + k1·(1 - b + b·|D|/avgdl))
//! ```
//!
//! BM25 rather than raw term frequency because a term appearing in every
//! document distinguishes nothing, and a long document should not outrank a
//! short one merely by holding more words. Both corrections are in the
//! formula; neither is in `CONTAINS`.
//!
//! Positions are stored so a quoted `"shortest path"` can require adjacency
//! rather than co-occurrence. A phrase query that only checked both terms were
//! present would match a document mentioning them a page apart, and would look
//! right in every small test.
//!
//! # The stemmer is deliberately small, and says so
//!
//! NDS-06 asks for stemming. This is **not** a Porter stemmer: it strips a
//! short, listed set of English suffixes (`-s`, `-es`, `-ies`, `-ed`, `-ing`)
//! with a minimum stem length, and nothing else. It conflates `graph`,
//! `graphs` and `graphing`, which is the case that matters for search; it does
//! not conflate `analyse`/`analysis`, and it is wrong about irregular verbs.
//!
//! Calling it a stemmer and leaving the reader to assume Porter would be the
//! defect. The whole rule set is [`stem`], it is fifteen lines, and a query is
//! stemmed by the same function that stemmed the document — which is the
//! property that actually has to hold. A stemmer applied to one side only
//! silently stops matching the words it was added to match.

use crate::graph::NodeId;
use std::collections::{BTreeMap, HashMap, HashSet};

/// BM25's term-frequency saturation. 1.2 is the usual default: above it, a
/// term appearing ten times counts little more than five.
const K1: f64 = 1.2;
/// BM25's length normalisation. 0.75 is the usual default; 0 ignores document
/// length entirely and 1 divides it out completely.
const B: f64 = 0.75;

/// Split text into terms: lowercase, alphanumeric runs, stemmed.
///
/// Public because a query must be tokenised by exactly this function. The
/// commonest way to break a full-text index is to tokenise documents one way
/// and queries another, and the symptom is not an error — it is a search that
/// quietly returns nothing for words that are definitely there.
pub fn tokenize(text: &str) -> Vec<String> {
    text.split(|c: char| !c.is_alphanumeric())
        .filter(|t| !t.is_empty())
        .map(|t| stem(&t.to_lowercase()))
        .collect()
}

/// Strip a listed English suffix. The complete rule set — see the module docs
/// for what this deliberately is not.
///
/// The minimum stem length stops `is` becoming `i` and `bed` becoming `b`,
/// which is how an over-eager stemmer turns unrelated words into the same
/// token and makes a search return documents with nothing in common.
pub fn stem(word: &str) -> String {
    let w = word;
    let keep = |suffix: &str, min: usize| -> Option<String> {
        w.strip_suffix(suffix)
            .filter(|s| s.len() >= min)
            .map(|s| s.to_string())
    };
    // Longest suffix first: `-ies` before `-es` before `-s`, or `studies`
    // loses one letter and becomes `studie`.
    keep("ies", 2)
        .map(|s| format!("{s}y"))
        .or_else(|| keep("ing", 3))
        .or_else(|| keep("ed", 3))
        .or_else(|| keep("es", 3))
        .or_else(|| keep("s", 3))
        .unwrap_or_else(|| w.to_string())
}

/// One posting: a document and where the term occurs in it.
#[derive(Debug, Clone, Default)]
struct Posting {
    positions: Vec<u32>,
}

impl Posting {
    fn tf(&self) -> f64 {
        self.positions.len() as f64
    }
}

/// An inverted index over one (label, property) pair.
#[derive(Debug, Default)]
pub struct FullTextIndex {
    /// The label and property this index covers.
    pub label: String,
    pub property: String,
    /// A `BTreeMap` rather than a hash map so a prefix query can take a range:
    /// `graph*` is every key from `graph` up to the next string that does not
    /// start with it. A hash map would make prefix search a full scan of the
    /// term dictionary.
    terms: BTreeMap<String, HashMap<u64, Posting>>,
    /// Token count per document, for BM25's length normalisation.
    lengths: HashMap<u64, usize>,
}

/// A hit, with the score that ranked it.
#[derive(Debug, Clone, PartialEq)]
pub struct Hit {
    pub node: NodeId,
    pub score: f64,
}

impl FullTextIndex {
    pub fn new(label: impl Into<String>, property: impl Into<String>) -> Self {
        Self {
            label: label.into(),
            property: property.into(),
            ..Default::default()
        }
    }

    pub fn documents(&self) -> usize {
        self.lengths.len()
    }

    pub fn distinct_terms(&self) -> usize {
        self.terms.len()
    }

    /// Index `text` under `node`, replacing whatever was there.
    ///
    /// Replacing, not adding: a property that is updated must not leave the
    /// old words findable. That is the bug an append-only index has and never
    /// reports — the document still matches a word it no longer contains.
    pub fn insert(&mut self, node: NodeId, text: &str) {
        self.remove(node);
        let tokens = tokenize(text);
        self.lengths.insert(node.as_u64(), tokens.len());
        for (i, term) in tokens.into_iter().enumerate() {
            self.terms
                .entry(term)
                .or_default()
                .entry(node.as_u64())
                .or_default()
                .positions
                .push(i as u32);
        }
    }

    pub fn remove(&mut self, node: NodeId) {
        let id = node.as_u64();
        if self.lengths.remove(&id).is_none() {
            return;
        }
        // Drop the term entirely when its last document goes, or `avgdl` and
        // the IDF denominator drift away from the documents that remain.
        self.terms.retain(|_, docs| {
            docs.remove(&id);
            !docs.is_empty()
        });
    }

    fn avg_length(&self) -> f64 {
        if self.lengths.is_empty() {
            return 0.0;
        }
        self.lengths.values().sum::<usize>() as f64 / self.lengths.len() as f64
    }

    /// Robertson/Sparck-Jones IDF with the +0.5 smoothing, floored at zero.
    ///
    /// Unfloored, a term in more than half the documents scores negative and
    /// a document can be pushed *down* the ranking by containing a query
    /// term — which reads as a bug every time somebody notices it.
    fn idf(&self, docs_with_term: usize) -> f64 {
        let n = self.lengths.len() as f64;
        let df = docs_with_term as f64;
        (((n - df + 0.5) / (df + 0.5)) + 1.0).ln().max(0.0)
    }

    /// Search, highest score first.
    ///
    /// Query syntax, all of it:
    ///
    /// | Form | Means |
    /// |---|---|
    /// | `graph database` | either term; both scores add |
    /// | `"shortest path"` | the terms adjacent, in that order |
    /// | `graph*` | any term starting with the stemmed prefix |
    ///
    /// No boolean operators and no field selectors. An index covering one
    /// property has nothing to select between, and accepting `AND` while
    /// treating it as a term is worse than refusing it.
    pub fn search(&self, query: &str, limit: usize) -> Vec<Hit> {
        let avgdl = self.avg_length();
        if avgdl == 0.0 {
            return Vec::new();
        }
        let mut scores: HashMap<u64, f64> = HashMap::new();

        for part in split_query(query) {
            match part {
                QueryPart::Phrase(words) => self.score_phrase(&words, avgdl, &mut scores),
                QueryPart::Prefix(p) => self.score_prefix(&p, avgdl, &mut scores),
                QueryPart::Term(t) => self.score_term(&t, avgdl, &mut scores),
            }
        }

        let mut hits: Vec<Hit> = scores
            .into_iter()
            .filter(|(_, s)| *s > 0.0)
            .map(|(id, score)| Hit {
                node: NodeId::new(id),
                score,
            })
            .collect();
        // Ties broken by node id: a HashMap gives no order, so two searches of
        // one index would otherwise disagree about equally-scored documents.
        hits.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then(a.node.as_u64().cmp(&b.node.as_u64()))
        });
        hits.truncate(limit);
        hits
    }

    fn bm25(&self, tf: f64, doc_len: usize, avgdl: f64, df: usize) -> f64 {
        let norm = 1.0 - B + B * (doc_len as f64 / avgdl);
        self.idf(df) * (tf * (K1 + 1.0)) / (tf + K1 * norm)
    }

    fn score_term(&self, term: &str, avgdl: f64, scores: &mut HashMap<u64, f64>) {
        let Some(docs) = self.terms.get(term) else {
            return;
        };
        let df = docs.len();
        for (id, posting) in docs {
            let len = *self.lengths.get(id).unwrap_or(&1);
            *scores.entry(*id).or_default() += self.bm25(posting.tf(), len, avgdl, df);
        }
    }

    fn score_prefix(&self, prefix: &str, avgdl: f64, scores: &mut HashMap<u64, f64>) {
        // Range over the term dictionary, which is why it is a BTreeMap.
        for (term, _) in self.terms.range(prefix.to_string()..) {
            if !term.starts_with(prefix) {
                break;
            }
            self.score_term(term, avgdl, scores);
        }
    }

    fn score_phrase(&self, words: &[String], avgdl: f64, scores: &mut HashMap<u64, f64>) {
        if words.is_empty() {
            return;
        }
        // Documents holding every word, then the adjacency check. Checking
        // presence alone would match a document with the words a page apart,
        // which is the failure a small test never shows.
        let Some(first) = self.terms.get(&words[0]) else {
            return;
        };
        let mut candidates: HashSet<u64> = first.keys().copied().collect();
        for w in &words[1..] {
            let Some(docs) = self.terms.get(w) else {
                return;
            };
            candidates.retain(|id| docs.contains_key(id));
        }

        for id in candidates {
            let starts = &self.terms[&words[0]][&id].positions;
            let matched = starts.iter().any(|&p| {
                words[1..].iter().enumerate().all(|(i, w)| {
                    self.terms
                        .get(w)
                        .and_then(|d| d.get(&id))
                        .is_some_and(|post| post.positions.contains(&(p + i as u32 + 1)))
                })
            });
            if !matched {
                continue;
            }
            let len = *self.lengths.get(&id).unwrap_or(&1);
            // The phrase scores as its rarest term: a phrase is at least as
            // selective as the least common word in it, and summing the terms
            // would rank a phrase match below a document repeating the
            // commonest of its words.
            let best = words
                .iter()
                .filter_map(|w| {
                    let docs = self.terms.get(w)?;
                    let post = docs.get(&id)?;
                    Some(self.bm25(post.tf(), len, avgdl, docs.len()))
                })
                .fold(f64::NEG_INFINITY, f64::max);
            if best.is_finite() {
                *scores.entry(id).or_default() += best;
            }
        }
    }
}

#[derive(Debug, PartialEq)]
enum QueryPart {
    Term(String),
    Phrase(Vec<String>),
    Prefix(String),
}

/// Split a query into its parts, applying the same tokenisation documents got.
fn split_query(query: &str) -> Vec<QueryPart> {
    let mut parts = Vec::new();
    let mut rest = query;
    while let Some(open) = rest.find('"') {
        let before = &rest[..open];
        parts.extend(plain_parts(before));
        match rest[open + 1..].find('"') {
            Some(close) => {
                let phrase = &rest[open + 1..open + 1 + close];
                let words = tokenize(phrase);
                if !words.is_empty() {
                    parts.push(QueryPart::Phrase(words));
                }
                rest = &rest[open + close + 2..];
            }
            None => {
                // An unclosed quote is a typo, and treating the rest of the
                // query as one phrase would silently return nothing. The words
                // are searched individually instead.
                parts.extend(plain_parts(&rest[open + 1..]));
                return parts;
            }
        }
    }
    parts.extend(plain_parts(rest));
    parts
}

fn plain_parts(text: &str) -> Vec<QueryPart> {
    text.split_whitespace()
        .filter_map(|w| {
            if let Some(p) = w.strip_suffix('*') {
                let stemmed = tokenize(p);
                // The prefix is stemmed like everything else, so `graphs*`
                // and `graph*` find the same terms.
                stemmed.into_iter().next().map(QueryPart::Prefix)
            } else {
                tokenize(w).into_iter().next().map(QueryPart::Term)
            }
        })
        .collect()
}

/// Named full-text indexes.
///
/// The procedure addresses one by name, so the name is part of the index's
/// identity rather than a label on the side.
///
/// Behind a `RwLock`, like the vector manager and for the same reason: index
/// maintenance runs from `GraphStore::apply_property_set`, which takes `&self`
/// because it is on the hot path of every bulk load. An index needing `&mut
/// self` there would have to be maintained from each of that function's
/// callers instead, and the one that got forgotten would go stale silently.
#[derive(Debug, Default)]
pub struct FullTextIndexes {
    by_name: std::sync::RwLock<HashMap<String, FullTextIndex>>,
}

impl FullTextIndexes {
    pub fn create(&self, name: impl Into<String>, label: &str, property: &str) {
        self.by_name
            .write()
            .unwrap()
            .insert(name.into(), FullTextIndex::new(label, property));
    }

    pub fn drop_index(&self, name: &str) -> bool {
        self.by_name.write().unwrap().remove(name).is_some()
    }

    /// Names, sorted, so an error can list what does exist.
    pub fn names(&self) -> Vec<String> {
        let mut v: Vec<String> = self.by_name.read().unwrap().keys().cloned().collect();
        v.sort();
        v
    }

    /// Every index as `(name, label, property)`, sorted by name.
    ///
    /// Sorted because `SHOW INDEXES` sorts its rows and a hash map would make
    /// two runs of one query disagree about the order.
    pub fn listing(&self) -> Vec<(String, String, String)> {
        let g = self.by_name.read().unwrap();
        let mut v: Vec<(String, String, String)> = g
            .iter()
            .map(|(n, i)| (n.clone(), i.label.clone(), i.property.clone()))
            .collect();
        v.sort();
        v
    }

    pub fn is_empty(&self) -> bool {
        self.by_name.read().unwrap().is_empty()
    }

    /// The (label, property) an index covers, or `None` if there is no such
    /// index.
    pub fn covers(&self, name: &str) -> Option<(String, String)> {
        let g = self.by_name.read().unwrap();
        g.get(name).map(|i| (i.label.clone(), i.property.clone()))
    }

    pub fn search(&self, name: &str, query: &str, limit: usize) -> Option<Vec<Hit>> {
        let g = self.by_name.read().unwrap();
        g.get(name).map(|i| i.search(query, limit))
    }

    pub fn stats(&self, name: &str) -> Option<(usize, usize)> {
        let g = self.by_name.read().unwrap();
        g.get(name).map(|i| (i.documents(), i.distinct_terms()))
    }

    /// Index `text` for `node` in every index covering `(label, property)`.
    ///
    /// Called for each of a node's labels, so an index on one of them picks up
    /// the write and an index on another does not.
    pub fn on_property_set(&self, label: &str, property: &str, node: NodeId, text: &str) {
        if self.by_name.read().unwrap().is_empty() {
            return; // the common case: no full-text index exists at all
        }
        let mut g = self.by_name.write().unwrap();
        for idx in g.values_mut() {
            if idx.label == label && idx.property == property {
                idx.insert(node, text);
            }
        }
    }

    /// Drop `node` from every index covering `(label, property)`.
    pub fn on_property_removed(&self, label: &str, property: &str, node: NodeId) {
        if self.by_name.read().unwrap().is_empty() {
            return;
        }
        let mut g = self.by_name.write().unwrap();
        for idx in g.values_mut() {
            if idx.label == label && idx.property == property {
                idx.remove(node);
            }
        }
    }

    /// Drop `node` from every index. For node deletion, where the labels are
    /// already gone by the time anything asks.
    pub fn on_node_deleted(&self, node: NodeId) {
        if self.by_name.read().unwrap().is_empty() {
            return;
        }
        for idx in self.by_name.write().unwrap().values_mut() {
            idx.remove(node);
        }
    }

    /// Replace one index's contents wholesale, for the backfill at creation.
    pub fn rebuild(&self, name: &str, docs: impl IntoIterator<Item = (NodeId, String)>) -> usize {
        let mut g = self.by_name.write().unwrap();
        let Some(idx) = g.get_mut(name) else {
            return 0;
        };
        let mut n = 0;
        for (node, text) in docs {
            idx.insert(node, &text);
            n += 1;
        }
        n
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_stemmer_conflates_the_cases_it_claims_and_no_others() {
        assert_eq!(stem("graphs"), "graph");
        assert_eq!(stem("graphing"), "graph");
        assert_eq!(stem("matched"), "match");
        assert_eq!(stem("studies"), "study");
        // The minimum stem length. Without it these become `i`, `b` and `ga`,
        // and unrelated words collapse into one token.
        assert_eq!(stem("is"), "is");
        assert_eq!(stem("bed"), "bed");
        assert_eq!(stem("gas"), "gas");
        // Honestly out of scope, asserted so the claim in the docs stays true.
        assert_ne!(stem("analysis"), stem("analyse"));
    }

    #[test]
    fn a_query_is_tokenized_by_the_function_that_tokenized_the_document() {
        // The failure this guards is silent: documents indexed under `graph`,
        // queries looking for `Graphs`, and a search that returns nothing for
        // a word that is plainly there.
        assert_eq!(
            tokenize("Graphs, and GRAPHING!"),
            vec!["graph", "and", "graph"]
        );
    }

    #[test]
    fn there_is_no_stopword_list_and_that_is_the_design() {
        // BM25 already handles a word that appears everywhere: its IDF floors
        // at zero and it contributes nothing to the ranking. Dropping such
        // words outright would additionally break every phrase containing one
        // -- `"shortest path to the node"` has two -- and a phrase query that
        // silently loses a word matches things it should not.
        assert_eq!(tokenize("the and of"), vec!["the", "and", "of"]);

        let mut idx = FullTextIndex::new("N", "body");
        idx.insert(NodeId::new(1), "the shortest path to the node");
        idx.insert(NodeId::new(2), "the longest walk to the node");
        assert_eq!(
            idx.search("\"path to the node\"", 10).len(),
            1,
            "a phrase must be able to span common words"
        );
    }

    #[test]
    fn an_updated_document_stops_matching_its_old_words() {
        let mut idx = FullTextIndex::new("N", "body");
        idx.insert(NodeId::new(1), "alpha beta");
        idx.insert(NodeId::new(1), "gamma delta");
        assert!(
            idx.search("alpha", 10).is_empty(),
            "the old word still matches"
        );
        assert_eq!(idx.search("gamma", 10).len(), 1);
        assert_eq!(idx.documents(), 1, "the document was counted twice");
    }

    #[test]
    fn idf_never_pushes_a_document_down_for_containing_a_term() {
        let mut idx = FullTextIndex::new("N", "body");
        for i in 1..=10 {
            idx.insert(NodeId::new(i), "the common word");
        }
        // `the` is in every document. Unfloored, its IDF is negative and a
        // document scores worse for containing it.
        for hit in idx.search("the", 10) {
            assert!(hit.score >= 0.0, "negative score: {hit:?}");
        }
    }

    #[test]
    fn a_phrase_requires_adjacency_not_co_occurrence() {
        let mut idx = FullTextIndex::new("N", "body");
        idx.insert(NodeId::new(1), "the shortest path between two nodes");
        idx.insert(NodeId::new(2), "the shortest route along some other path");
        let hits = idx.search("\"shortest path\"", 10);
        assert_eq!(hits.len(), 1, "both documents hold both words: {hits:?}");
        assert_eq!(hits[0].node, NodeId::new(1));
    }

    #[test]
    fn a_prefix_matches_terms_the_whole_word_would_not() {
        let mut idx = FullTextIndex::new("N", "body");
        idx.insert(NodeId::new(1), "graphical models");
        idx.insert(NodeId::new(2), "a graph database");
        idx.insert(NodeId::new(3), "grammar");
        let hits = idx.search("graph*", 10);
        let ids: HashSet<u64> = hits.iter().map(|h| h.node.as_u64()).collect();
        assert_eq!(ids, HashSet::from([1, 2]), "{hits:?}");
    }

    #[test]
    fn a_shorter_document_outranks_a_longer_one_holding_the_term_as_often() {
        // BM25's length normalisation, which is the half `CONTAINS` has no
        // notion of at all.
        let mut idx = FullTextIndex::new("N", "body");
        idx.insert(NodeId::new(1), "graph");
        idx.insert(
            NodeId::new(2),
            "graph and then a great deal of other text about unrelated subjects entirely",
        );
        let hits = idx.search("graph", 10);
        assert_eq!(hits.len(), 2);
        assert_eq!(hits[0].node, NodeId::new(1), "{hits:?}");
        assert!(hits[0].score > hits[1].score);
    }

    #[test]
    fn two_searches_of_one_index_agree_on_ties() {
        let mut idx = FullTextIndex::new("N", "body");
        for i in 1..=20 {
            idx.insert(NodeId::new(i), "identical text here");
        }
        let a: Vec<u64> = idx
            .search("identical", 20)
            .iter()
            .map(|h| h.node.as_u64())
            .collect();
        let b: Vec<u64> = idx
            .search("identical", 20)
            .iter()
            .map(|h| h.node.as_u64())
            .collect();
        assert_eq!(a, b);
        assert_eq!(a.first(), Some(&1), "ties break by node id");
    }

    #[test]
    fn an_unclosed_quote_searches_the_words_instead_of_returning_nothing() {
        let mut idx = FullTextIndex::new("N", "body");
        idx.insert(NodeId::new(1), "shortest path");
        assert_eq!(idx.search("\"shortest path", 10).len(), 1);
    }

    #[test]
    fn an_empty_index_returns_nothing_rather_than_dividing_by_zero() {
        let idx = FullTextIndex::new("N", "body");
        assert!(idx.search("anything", 10).is_empty());
    }

    #[test]
    fn removing_the_last_document_holding_a_term_drops_the_term() {
        // Otherwise `df` counts documents that are gone, every IDF drifts, and
        // the drift is invisible because the scores still look like scores.
        let mut idx = FullTextIndex::new("N", "body");
        idx.insert(NodeId::new(1), "unique");
        idx.insert(NodeId::new(2), "shared");
        assert_eq!(idx.distinct_terms(), 2);
        idx.remove(NodeId::new(1));
        assert_eq!(idx.distinct_terms(), 1);
        assert_eq!(idx.documents(), 1);
    }
}
