//! Translations remembered by (question, schema, model) (AI-13, #1743).
//!
//! Every `/api/nlq` request and every `GRAPH.NLQ` used to be a fresh model
//! call, so asking the same question twice cost twice, and whether the two
//! answers matched was up to the model. With this cache, a repeat of a question
//! over the same schema with the same model returns the Cypher already
//! generated: the same statement by construction, at no token cost.
//!
//! # The key, and why each part is in it
//!
//! - **The question**, with runs of whitespace collapsed and nothing else
//!   changed. Case is kept: `'Alice'` and `'alice'` are different property
//!   values, so folding case could return a query for the wrong name.
//! - **The schema's shape**: the SHA-256 of `GraphStore::schema_summary`, the
//!   text the prompt is built from (deterministic since #1509), with its node
//!   and edge counts taken out. What is left is the labels, the relationship
//!   patterns and the property names: adding a label, an edge type or a
//!   property changes it, so a translation made for the old schema is not
//!   served for the new one. Adding a node of a label that already exists does
//!   not, which is the point: the counts change on every write, and a key that
//!   moved with them would empty the cache on any graph that is being written
//!   to. The cache holds the query, not its result.
//! - **The model**: provider, model name, base URL and system prompt. Switching
//!   any of them is asking a different translator.
//!
//! Only translations that passed the safety check are kept, so a hit never
//! returns a statement that a fresh call would have refused. Failures are not
//! kept: the next attempt asks the model again.
//!
//! # Size
//!
//! Least-recently-used, `NLQ_CACHE_SIZE` entries (default 1024; `0` turns it
//! off). An entry is a question and a statement, a few hundred bytes.

use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};

use lru::LruCache;
use sha2::{Digest, Sha256};

use crate::persistence::tenant::NLQConfig;

/// Entries in the shared cache unless `NLQ_CACHE_SIZE` says otherwise.
pub const DEFAULT_CAPACITY: usize = 1024;

/// What a translation is remembered by. See the module documentation.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct CacheKey {
    question: String,
    schema: [u8; 32],
    model: [u8; 32],
}

impl CacheKey {
    pub fn new(question: &str, schema_summary: &str, model: &ModelKey) -> Self {
        CacheKey {
            question: question.split_whitespace().collect::<Vec<_>>().join(" "),
            schema: Sha256::digest(schema_shape(schema_summary).as_bytes()).into(),
            model: model.0,
        }
    }
}

/// `schema_summary` without its counts: `  :Person (12 nodes)` becomes
/// `  :Person`, and `  (A)-[:R]->(B) (40 edges)` becomes `  (A)-[:R]->(B)`.
fn schema_shape(summary: &str) -> String {
    summary
        .lines()
        .map(|line| {
            let counted = line.ends_with(" nodes)") || line.ends_with(" edges)");
            match line.rfind(" (") {
                Some(i) if counted && is_count(&line[i + 2..line.len() - 7]) => &line[..i],
                _ => line,
            }
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn is_count(s: &str) -> bool {
    !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit())
}

/// The parts of an `NLQConfig` that decide what the model writes. The API key
/// is not one of them, and is never hashed into anything.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ModelKey([u8; 32]);

impl ModelKey {
    pub fn of(config: &NLQConfig) -> Self {
        let mut h = Sha256::new();
        // Length-prefixed, so no two different configs run together into the
        // same bytes ("ab" + "c" against "a" + "bc").
        for part in [
            format!("{:?}", config.provider),
            config.model.clone(),
            config.api_base_url.clone().unwrap_or_default(),
            config.system_prompt.clone().unwrap_or_default(),
        ] {
            h.update((part.len() as u64).to_le_bytes());
            h.update(part.as_bytes());
        }
        ModelKey(h.finalize().into())
    }
}

/// Hit and miss counts, and how full the cache is.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize)]
pub struct CacheStats {
    pub hits: u64,
    pub misses: u64,
    pub entries: usize,
    pub capacity: usize,
}

/// A bounded, least-recently-used map from [`CacheKey`] to Cypher.
pub struct TranslationCache {
    entries: Option<Mutex<LruCache<CacheKey, String>>>,
    capacity: usize,
    hits: AtomicU64,
    misses: AtomicU64,
}

impl TranslationCache {
    /// A cache of `capacity` entries; `0` keeps nothing.
    pub fn new(capacity: usize) -> Self {
        TranslationCache {
            entries: NonZeroUsize::new(capacity).map(|c| Mutex::new(LruCache::new(c))),
            capacity,
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
        }
    }

    pub fn get(&self, key: &CacheKey) -> Option<String> {
        let found = self.entries.as_ref().and_then(|m| {
            m.lock()
                .unwrap_or_else(|e| e.into_inner())
                .get(key)
                .cloned()
        });
        let counter = if found.is_some() {
            &self.hits
        } else {
            &self.misses
        };
        counter.fetch_add(1, Ordering::Relaxed);
        found
    }

    pub fn put(&self, key: CacheKey, cypher: String) {
        if let Some(m) = &self.entries {
            m.lock().unwrap_or_else(|e| e.into_inner()).put(key, cypher);
        }
    }

    pub fn stats(&self) -> CacheStats {
        CacheStats {
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            entries: self
                .entries
                .as_ref()
                .map_or(0, |m| m.lock().unwrap_or_else(|e| e.into_inner()).len()),
            capacity: self.capacity,
        }
    }

    pub fn clear(&self) {
        if let Some(m) = &self.entries {
            m.lock().unwrap_or_else(|e| e.into_inner()).clear();
        }
    }
}

/// The process-wide cache every `NLQPipeline` uses unless given another.
///
/// Sized from `NLQ_CACHE_SIZE` on first use. In this crate's own unit tests it
/// starts empty-capacity, so tests that run in parallel and translate the same
/// question with the mock provider cannot see each other's entries; the tests
/// of the cache itself use caches of their own.
pub fn shared() -> &'static TranslationCache {
    static SHARED: OnceLock<TranslationCache> = OnceLock::new();
    SHARED.get_or_init(|| {
        let default = if cfg!(test) { 0 } else { DEFAULT_CAPACITY };
        TranslationCache::new(capacity_from(std::env::var("NLQ_CACHE_SIZE").ok(), default))
    })
}

/// `NLQ_CACHE_SIZE`, read: a count, or `default` when unset or not a number.
fn capacity_from(value: Option<String>, default: usize) -> usize {
    value
        .and_then(|v| v.trim().parse::<usize>().ok())
        .unwrap_or(default)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::persistence::tenant::LLMProvider;

    fn config(model: &str) -> NLQConfig {
        NLQConfig {
            enabled: true,
            provider: LLMProvider::Mock,
            model: model.into(),
            api_key: Some("secret-1".into()),
            api_base_url: None,
            system_prompt: None,
        }
    }

    #[test]
    fn the_key_ignores_spacing_but_not_case_schema_or_model() {
        let m = ModelKey::of(&config("a"));
        let k = |q: &str, s: &str| CacheKey::new(q, s, &m);
        assert_eq!(k("who knows  Alice?", "S"), k(" who knows\nAlice? ", "S"));
        assert_ne!(k("who knows Alice?", "S"), k("who knows alice?", "S"));
        assert_ne!(k("who knows Alice?", "S"), k("who knows Alice?", "S2"));
        assert_ne!(
            CacheKey::new("q", "S", &ModelKey::of(&config("a"))),
            CacheKey::new("q", "S", &ModelKey::of(&config("b")))
        );
    }

    #[test]
    fn the_schema_key_follows_the_shape_and_not_the_counts() {
        use crate::graph::GraphStore;
        use crate::query::QueryEngine;
        let m = ModelKey::of(&config("m"));
        let e = QueryEngine::new();
        let mut g = GraphStore::new();
        e.execute_mut(
            "CREATE (a:Person {name: 'A'})-[:KNOWS]->(b:Person {name: 'B'})",
            &mut g,
            "default",
        )
        .unwrap();
        let key = |g: &GraphStore| CacheKey::new("q", &g.schema_summary(), &m);
        let before = key(&g);

        // More of the same: counts move, the shape does not.
        e.execute_mut(
            "CREATE (:Person {name: 'C'})-[:KNOWS]->(:Person {name: 'D'})",
            &mut g,
            "default",
        )
        .unwrap();
        assert!(
            g.schema_summary().contains("(4 nodes)"),
            "{}",
            g.schema_summary()
        );
        assert_eq!(
            key(&g),
            before,
            "a write that kept the schema changed the key"
        );

        // A new label, a new edge type: the shape changes.
        e.execute_mut("CREATE (:City {name: 'X'})", &mut g, "default")
            .unwrap();
        let with_city = key(&g);
        assert_ne!(with_city, before);
        e.execute_mut(
            "MATCH (p:Person {name: 'A'}), (c:City) CREATE (p)-[:LIVES_IN]->(c)",
            &mut g,
            "default",
        )
        .unwrap();
        assert_ne!(key(&g), with_city);
    }

    #[test]
    fn only_a_trailing_count_is_taken_out() {
        assert_eq!(schema_shape("  :Person (12 nodes)"), "  :Person");
        assert_eq!(
            schema_shape("  (A)-[:R]->(B) (40 edges)"),
            "  (A)-[:R]->(B)"
        );
        // Not a count: left alone.
        assert_eq!(schema_shape("  :Odd (many nodes)"), "  :Odd (many nodes)");
        assert_eq!(schema_shape("  :Odd ( nodes)"), "  :Odd ( nodes)");
        assert_eq!(
            schema_shape("  :Person has properties: age, name"),
            "  :Person has properties: age, name"
        );
    }

    #[test]
    fn the_model_key_covers_what_shapes_the_output_and_not_the_api_key() {
        let base = config("m");
        let mut other_key = base.clone();
        other_key.api_key = Some("secret-2".into());
        assert_eq!(ModelKey::of(&base), ModelKey::of(&other_key));

        let mut url = base.clone();
        url.api_base_url = Some("http://elsewhere".into());
        let mut prompt = base.clone();
        prompt.system_prompt = Some("be terse".into());
        let mut provider = base.clone();
        provider.provider = LLMProvider::Ollama;
        for changed in [url, prompt, provider] {
            assert_ne!(ModelKey::of(&base), ModelKey::of(&changed), "{changed:?}");
        }
        // Length-prefixed: moving a character between two parts is a change.
        let mut a = base.clone();
        a.model = "ab".into();
        a.system_prompt = Some("c".into());
        let mut b = base.clone();
        b.model = "a".into();
        b.system_prompt = Some("bc".into());
        assert_ne!(ModelKey::of(&a), ModelKey::of(&b));
    }

    #[test]
    fn it_keeps_the_most_recently_used_and_counts_hits_and_misses() {
        let m = ModelKey::of(&config("m"));
        let k = |q: &str| CacheKey::new(q, "S", &m);
        let c = TranslationCache::new(2);
        c.put(k("a"), "A".into());
        c.put(k("b"), "B".into());
        assert_eq!(c.get(&k("a")).as_deref(), Some("A")); // `a` is now the newest
        c.put(k("c"), "C".into()); // evicts `b`
        assert_eq!(c.get(&k("b")), None);
        assert_eq!(c.get(&k("c")).as_deref(), Some("C"));
        assert_eq!(
            c.stats(),
            CacheStats {
                hits: 2,
                misses: 1,
                entries: 2,
                capacity: 2
            }
        );
        c.clear();
        assert_eq!(c.stats().entries, 0);
    }

    #[test]
    fn capacity_zero_keeps_nothing() {
        let m = ModelKey::of(&config("m"));
        let c = TranslationCache::new(0);
        c.put(CacheKey::new("q", "S", &m), "Q".into());
        assert_eq!(c.get(&CacheKey::new("q", "S", &m)), None);
        assert_eq!(c.stats().entries, 0);
    }

    #[test]
    fn the_size_setting_falls_back_on_anything_that_is_not_a_count() {
        assert_eq!(capacity_from(None, 1024), 1024);
        assert_eq!(capacity_from(Some(" 16 ".into()), 1024), 16);
        assert_eq!(capacity_from(Some("0".into()), 1024), 0);
        assert_eq!(capacity_from(Some("lots".into()), 1024), 1024);
        assert_eq!(capacity_from(Some("-1".into()), 1024), 1024);
    }
}
