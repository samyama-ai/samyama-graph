//! # Query Processing Pipeline
//!
//! This module implements the full **query processing pipeline** for Samyama's OpenCypher
//! dialect, following the same staged architecture used by virtually every database engine
//! and compiler:
//!
//! ```text
//!   Source Text          Pest PEG Parser        Abstract Syntax Tree
//!  ┌──────────┐        ┌──────────────┐        ┌──────────────────┐
//!  │ MATCH    │──Lex──>│  cypher.pest │──AST──>│  Query struct    │
//!  │ (n:Foo)  │ +Parse │  (PEG rules) │        │  (ast.rs)        │
//!  │ RETURN n │        └──────────────┘        └────────┬─────────┘
//!  └──────────┘                                         │
//!                                                       │ plan()
//!                                                       v
//!                     Execution Plan             ┌──────────────┐
//!                    ┌──────────────┐            │ QueryPlanner │
//!                    │ Operator tree│<───────────│ (planner.rs) │
//!                    │ (Volcano)    │            └──────────────┘
//!                    └──────┬───────┘
//!                           │ next() / next_mut()
//!                           v
//!                    ┌──────────────┐
//!                    │  RecordBatch │  (final output)
//!                    └──────────────┘
//! ```
//!
//! This mirrors how compilers work: source code is lexed into tokens, parsed into an AST,
//! lowered to an intermediate representation (the execution plan), and finally "executed"
//! (in a compiler, that means code generation; here, it means pulling records through
//! operators). The analogy is not accidental -- query languages *are* domain-specific
//! programming languages.
//!
//! ## Parsing: PEG via Pest
//!
//! The parser uses [Pest](https://pest.rs), a Rust crate that implements **Parsing Expression
//! Grammars (PEGs)**. Unlike context-free grammars (CFGs) used by yacc/bison, PEGs use an
//! ordered-choice operator (`/`) that tries alternatives left-to-right and commits to the
//! first match. This makes PEGs **always unambiguous** -- there is exactly one parse tree for
//! any input, which eliminates an entire class of grammar debugging. The grammar lives in
//! [`cypher.pest`](cypher.pest) and is compiled into a Rust parser at build time via a proc
//! macro (`#[derive(Parser)]`).
//!
//! ## Execution: Volcano Iterator Model (ADR-007)
//!
//! Query execution follows the **Volcano iterator model** invented by Goetz Graefe. Each
//! physical operator (scan, filter, expand, project, etc.) implements a `next()` method that
//! **pulls** a single record from its child operator. Records flow upward through the
//! operator tree one at a time, like a lazy iterator chain in Rust (`iter().filter().map()`).
//! This is memory-efficient because intermediate results are never fully materialized -- each
//! operator processes one record and immediately passes it upstream.
//!
//! ## LRU Parse Cache
//!
//! Parsing is expensive (PEG matching, AST construction, string allocation). Since many
//! applications execute the same queries repeatedly with different parameters, this module
//! maintains an **LRU (Least Recently Used) cache** of parsed ASTs. On a cache hit, we skip
//! parsing entirely and jump straight to planning. The cache uses `Mutex<LruCache>` for
//! thread safety, with lock-free `AtomicU64` counters for hit/miss statistics.
//!
//! ## Read vs Write Execution Paths
//!
//! Queries are split into two execution paths based on mutability:
//! - **[`QueryExecutor`]**: read-only queries (MATCH, RETURN, EXPLAIN). Takes `&GraphStore`.
//! - **[`MutQueryExecutor`]**: write queries (CREATE, DELETE, SET, MERGE). Takes `&mut GraphStore`.
//!
//! This separation mirrors Rust's ownership model -- shared references (`&T`) allow
//! concurrent reads, while exclusive references (`&mut T`) guarantee single-writer access.
//! The type system enforces at compile time that no read query can accidentally modify the
//! graph.

pub mod ast;
pub mod error_code;
pub mod parser;
pub mod star;
pub mod validate;
pub mod executor;
pub mod csv_source;

use std::num::NonZeroUsize;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use lru::LruCache;

// Re-export main types
pub use ast::Query;
pub use parser::{parse_query, ParseError, ParseResult};
pub use executor::{
    QueryExecutor, ExecutionError, ExecutionResult,
    Record, RecordBatch, Value,
    MutQueryExecutor,  // Added for CREATE/DELETE/SET support
};

/// Default LRU cache capacity
const DEFAULT_CACHE_CAPACITY: usize = 1024;

/// Bytes the result cache may hold before it evicts, regardless of entry count.
///
/// An entry count cannot bound this cache. Measured on LDBC SF1
/// (`benches/result_cache_gain.rs`), a 256-row entry costs 8.3 KB and a
/// 100,000-row entry 32.1 MB -- a **3,867x spread** -- so the same 1024-entry
/// setting holds 8 MB of one workload or 32.8 GB of another. PERF-10 is a
/// bytes/edge budget; a cache capped in entries cannot be budgeted against it.
///
/// 256 MB by default, overridable with `SAMYAMA_RESULT_CACHE_BYTES`.
const DEFAULT_RESULT_CACHE_BYTES: usize = 256 * 1024 * 1024;

/// Lock-free cache hit/miss counters.
pub struct CacheStats {
    hits: AtomicU64,
    misses: AtomicU64,
}

impl CacheStats {
    fn new() -> Self {
        Self {
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
        }
    }

    /// Total cache hits since engine creation.
    pub fn hits(&self) -> u64 {
        self.hits.load(Ordering::Relaxed)
    }

    /// Total cache misses since engine creation.
    pub fn misses(&self) -> u64 {
        self.misses.load(Ordering::Relaxed)
    }

    fn record_hit(&self) {
        self.hits.fetch_add(1, Ordering::Relaxed);
    }

    fn record_miss(&self) {
        self.misses.fetch_add(1, Ordering::Relaxed);
    }
}

/// Query engine - high-level interface for executing queries
///
/// Includes an LRU AST cache that eliminates repeated parsing overhead
/// for identical queries. The cache is keyed by whitespace-normalized
/// query strings and evicts least-recently-used entries when full.
pub struct QueryEngine {
    /// Parsed AST cache: normalized query string -> Query AST
    ast_cache: Mutex<LruCache<String, Query>>,
    /// Lock-free hit/miss counters
    stats: CacheStats,
    /// Per-query timeout in seconds (0 = no timeout)
    query_timeout_secs: u64,
    /// Rows a single operator may produce before the query is refused
    /// (0 = unlimited). See `executor::budget`.
    row_budget: u64,
    /// Result cache: (normalized query, bound params, graph epoch) -> rows (#1153).
    ///
    /// Deliberately **not** consulted by `execute`. A caller opts in by calling
    /// `execute_cached`, so a benchmark cannot measure the cache by accident --
    /// which is the failure mode that turns a cache into a fake speedup in
    /// CH-REGRESS. Opt-in at the call site is a stronger guarantee than a
    /// config flag defaulting to off.
    result_cache: Mutex<LruCache<ResultKey, RecordBatch>>,
    /// Bytes currently held by `result_cache`, kept in step with it under the
    /// same lock. Tracked rather than recomputed: summing every entry on each
    /// insert would walk the whole cache per query.
    result_cache_bytes: Mutex<usize>,
    /// The byte ceiling this engine enforces.
    result_cache_budget: usize,
    /// Hit/miss counters for the result cache, separate from the AST cache's.
    result_stats: CacheStats,
}

/// What a cached result is keyed on.
///
/// The epoch is the correctness-critical component: any write to the store
/// bumps it (`GraphStore::bump_epoch`), so every entry from before that write
/// is unreachable rather than stale. Coarse on purpose -- a write kills the
/// whole cache -- because the alternative, tracking which labels and edge types
/// a query read, is where caches return a wrong answer that looks right.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ResultKey {
    /// Whitespace-normalized query text, as the AST cache uses.
    query: String,
    /// Bound parameters, canonicalized. Two calls with the same text and
    /// different parameters are different questions.
    params: String,
    /// The store's data epoch at the time the answer was computed.
    epoch: u64,
}

/// Canonical, order-independent rendering of bound parameters.
///
/// A `HashMap` iterates in an arbitrary order that changes per process, so
/// formatting it directly would give the same parameters two different keys
/// and silently halve the hit rate.
fn canonical_params(params: &std::collections::HashMap<String, crate::graph::PropertyValue>) -> String {
    if params.is_empty() {
        return String::new();
    }
    let mut pairs: Vec<(&String, &crate::graph::PropertyValue)> = params.iter().collect();
    pairs.sort_by(|a, b| a.0.cmp(b.0));
    pairs.iter().map(|(k, v)| format!("{k}={v:?}")).collect::<Vec<_>>().join("\u{1f}")
}

impl QueryEngine {
    /// Create a new query engine with the default cache capacity (1024 entries)
    pub fn new() -> Self {
        Self::with_capacity(DEFAULT_CACHE_CAPACITY)
    }

    /// Create a new query engine with a specific cache capacity
    pub fn with_capacity(capacity: usize) -> Self {
        let cap = NonZeroUsize::new(capacity).unwrap_or(NonZeroUsize::new(1).unwrap());
        Self {
            ast_cache: Mutex::new(LruCache::new(cap)),
            stats: CacheStats::new(),
            query_timeout_secs: std::env::var("SAMYAMA_QUERY_TIMEOUT")
                .ok().and_then(|s| s.parse().ok()).unwrap_or(120),
            row_budget: executor::budget::configured_budget(),
            result_cache: Mutex::new(LruCache::new(cap)),
            result_cache_bytes: Mutex::new(0),
            result_cache_budget: std::env::var("SAMYAMA_RESULT_CACHE_BYTES")
                .ok().and_then(|v| v.parse().ok())
                .unwrap_or(DEFAULT_RESULT_CACHE_BYTES),
            result_stats: CacheStats::new(),
        }
    }

    /// Set the per-operator row budget; `0` disables enforcement entirely.
    ///
    /// Present so a caller that knows its query is legitimately enormous can
    /// raise the bound for that engine, rather than having to unset a
    /// process-wide environment variable and lose the guard everywhere.
    pub fn with_row_budget(mut self, rows: u64) -> Self {
        self.row_budget = rows;
        self
    }

    /// The per-operator row budget in force.
    pub fn row_budget(&self) -> u64 {
        self.row_budget
    }

    /// Return a reference to the cache statistics (hits/misses).
    pub fn cache_stats(&self) -> &CacheStats {
        &self.stats
    }

    /// Return the current number of entries in the cache.
    pub fn cache_len(&self) -> usize {
        self.ast_cache.lock().unwrap().len()
    }

    /// Parse with caching — normalizes whitespace for cache hits
    fn cached_parse(&self, query_str: &str) -> Result<Query, Box<dyn std::error::Error>> {
        let normalized = query_str.split_whitespace().collect::<Vec<_>>().join(" ");

        // Check cache (LruCache::get promotes to most-recently-used)
        {
            let mut cache = self.ast_cache.lock().unwrap();
            if let Some(cached) = cache.get(&normalized) {
                self.stats.record_hit();
                return Ok(cached.clone());
            }
        }

        self.stats.record_miss();

        // Parse and cache (LRU evicts automatically when full)
        let query = parse_query(query_str)?;
        {
            let mut cache = self.ast_cache.lock().unwrap();
            cache.put(normalized, query.clone());
        }
        Ok(query)
    }

    /// Whether this statement can change the graph, answered by the parser.
    ///
    /// The servers each matched strings against the query text to decide which
    /// executor to use, and the two lists disagreed (#1111). The AST already knows,
    /// and `cached_parse` means asking it costs a cache lookup on the second and
    /// later sight of a statement — the same parse the execute call is about to do.
    ///
    /// A statement that does not parse is not classified here. The caller gets the
    /// parse error, which is the same error it would have got a beat later.
    pub fn statement_is_write(
        &self,
        query_str: &str,
    ) -> Result<bool, Box<dyn std::error::Error>> {
        Ok(self.cached_parse(query_str)?.is_write())
    }

    /// Parse and execute a read-only Cypher query (MATCH, RETURN, etc.)
    pub fn execute(
        &self,
        query_str: &str,
        store: &crate::graph::GraphStore,
    ) -> Result<RecordBatch, Box<dyn std::error::Error>> {
        let query = self.cached_parse(query_str)?;

        let mut executor = if std::env::var("SAMYAMA_GRAPH_NATIVE").unwrap_or_default() == "true" {
            QueryExecutor::with_planner(store, executor::planner::QueryPlanner::with_config(
                executor::planner::PlannerConfig { graph_native: true, max_candidate_plans: 64 }
            ))
        } else {
            QueryExecutor::new(store)
        };
        if self.query_timeout_secs > 0 {
            executor = executor.with_deadline(
                std::time::Instant::now() + std::time::Duration::from_secs(self.query_timeout_secs)
            );
        }
        let result = executor.with_row_budget(self.row_budget).execute(&query)?;

        Ok(result)
    }

    /// Result-cache statistics, separate from the AST cache's.
    pub fn result_cache_stats(&self) -> &CacheStats {
        &self.result_stats
    }

    /// Entries currently held in the result cache.
    pub fn result_cache_len(&self) -> usize {
        self.result_cache.lock().unwrap().len()
    }

    /// Execute a read-only query, serving it from the result cache when the
    /// store has not changed since the answer was computed (#1153).
    ///
    /// Returns the rows and whether they came from the cache, so the caller can
    /// say so in the response envelope. A benchmark that reports a cached
    /// latency as engine latency is the failure this second return value
    /// exists to make impossible to do silently.
    ///
    /// Correctness rests on one rule: `GraphStore::epoch()` changes on every
    /// write, and it is part of the key. So an entry is never stale -- it is
    /// unreachable. Nothing here checks whether a write was *relevant* to this
    /// query, deliberately.
    ///
    /// Only the read path has this. `execute_mut` takes `&mut GraphStore` and
    /// is never cached, so a write cannot be served from a cache by mistake.
    pub fn execute_cached(
        &self,
        query_str: &str,
        store: &crate::graph::GraphStore,
    ) -> Result<(RecordBatch, bool), Box<dyn std::error::Error>> {
        let query = self.cached_parse(query_str)?;
        let key = ResultKey {
            query: query_str.split_whitespace().collect::<Vec<_>>().join(" "),
            params: canonical_params(&query.params),
            epoch: store.epoch(),
        };

        {
            let mut cache = self.result_cache.lock().unwrap();
            if let Some(hit) = cache.get(&key) {
                self.result_stats.record_hit();
                return Ok((hit.clone(), true));
            }
        }
        self.result_stats.record_miss();

        let batch = self.execute(query_str, store)?;

        // Re-read the epoch rather than reusing the one read above. A write can
        // land while the query runs, and caching the result under the *old*
        // epoch would publish an answer computed partly before it. Storing
        // nothing when the epoch moved is the honest outcome: the answer is
        // still returned, it is just not remembered.
        if store.epoch() == key.epoch {
            self.insert_with_budget(key, batch.clone());
        }
        Ok((batch, false))
    }

    /// Bytes the result cache currently holds.
    pub fn result_cache_bytes(&self) -> usize {
        *self.result_cache_bytes.lock().unwrap()
    }

    /// The byte ceiling in force for this engine.
    pub fn result_cache_budget(&self) -> usize {
        self.result_cache_budget
    }

    /// Set the result cache's byte ceiling for this engine.
    ///
    /// A method rather than only the environment variable, because the budget
    /// has to be settable per engine: tests run in one process and share it,
    /// so an env var would make them fight over a global.
    pub fn with_result_cache_budget(mut self, bytes: usize) -> Self {
        self.result_cache_budget = bytes;
        self
    }

    /// Insert under the byte budget, evicting least-recently-used entries until
    /// the total fits.
    ///
    /// A single answer larger than the whole budget is **not cached at all**
    /// rather than evicting everything to make room for it. Admitting it would
    /// flush a warm cache to hold one result that the next query evicts again,
    /// which is worse than not caching it: the memory spike happens *and* the
    /// hit rate drops.
    fn insert_with_budget(&self, key: ResultKey, batch: RecordBatch) {
        let cost = batch.approx_heap_bytes();
        if cost > self.result_cache_budget {
            return;
        }

        let mut cache = self.result_cache.lock().unwrap();
        let mut held = self.result_cache_bytes.lock().unwrap();

        // Replacing an existing key returns what it displaced; charge the
        // difference rather than the new entry, or the total drifts up forever.
        if let Some(old) = cache.put(key, batch) {
            *held = held.saturating_sub(old.approx_heap_bytes());
        }
        *held += cost;

        while *held > self.result_cache_budget {
            match cache.pop_lru() {
                Some((_, evicted)) => {
                    *held = held.saturating_sub(evicted.approx_heap_bytes());
                }
                // Nothing left to evict: the accounting and the map disagree,
                // so trust the map and reset rather than spin.
                None => {
                    *held = 0;
                    break;
                }
            }
        }
    }

    /// Drop every cached result. For tests and for an operator who wants the
    /// memory back; correctness never depends on calling this.
    pub fn clear_result_cache(&self) {
        self.result_cache.lock().unwrap().clear();
        *self.result_cache_bytes.lock().unwrap() = 0;
    }

    /// Parse and execute a write Cypher query (CREATE, DELETE, SET, etc.)
    /// This method takes a mutable reference to the graph store
    pub fn execute_mut(
        &self,
        query_str: &str,
        store: &mut crate::graph::GraphStore,
        tenant_id: &str,
    ) -> Result<RecordBatch, Box<dyn std::error::Error>> {
        let query = self.cached_parse(query_str)?;

        let mut executor = MutQueryExecutor::new(store, tenant_id.to_string());
        let result = executor.with_row_budget(self.row_budget).execute(&query)?;

        Ok(result)
    }
}

impl Default for QueryEngine {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::{GraphStore, Label};

    #[test]
    fn test_query_engine_creation() {
        let engine = QueryEngine::new();
        drop(engine);
    }

    #[test]
    fn test_end_to_end_simple_query() {
        let mut store = GraphStore::new();

        // Create test data
        let alice = store.create_node("Person");
        if let Some(node) = store.get_node_mut(alice) {
            node.set_property("name", "Alice");
            node.set_property("age", 30i64);
        }

        let bob = store.create_node("Person");
        if let Some(node) = store.get_node_mut(bob) {
            node.set_property("name", "Bob");
            node.set_property("age", 25i64);
        }

        // Execute query
        let engine = QueryEngine::new();
        let result = engine.execute("MATCH (n:Person) RETURN n", &store);

        assert!(result.is_ok());
        let batch = result.unwrap();
        assert_eq!(batch.len(), 2);
        assert_eq!(batch.columns.len(), 1);
        assert_eq!(batch.columns[0], "n");
    }

    #[test]
    fn test_query_with_filter() {
        let mut store = GraphStore::new();

        let alice = store.create_node("Person");
        if let Some(node) = store.get_node_mut(alice) {
            node.set_property("name", "Alice");
            node.set_property("age", 30i64);
        }

        let bob = store.create_node("Person");
        if let Some(node) = store.get_node_mut(bob) {
            node.set_property("name", "Bob");
            node.set_property("age", 25i64);
        }

        let engine = QueryEngine::new();
        let result = engine.execute("MATCH (n:Person) WHERE n.age > 28 RETURN n", &store);

        assert!(result.is_ok());
        let batch = result.unwrap();
        assert_eq!(batch.len(), 1); // Only Alice
    }

    #[test]
    fn test_query_with_limit() {
        let mut store = GraphStore::new();

        for i in 0..10 {
            let node = store.create_node("Person");
            if let Some(n) = store.get_node_mut(node) {
                n.set_property("id", i as i64);
            }
        }

        let engine = QueryEngine::new();
        let result = engine.execute("MATCH (n:Person) RETURN n LIMIT 5", &store);

        assert!(result.is_ok());
        let batch = result.unwrap();
        assert_eq!(batch.len(), 5);
    }

    #[test]
    fn test_query_with_edge_traversal() {
        let mut store = GraphStore::new();

        let alice = store.create_node("Person");
        if let Some(node) = store.get_node_mut(alice) {
            node.set_property("name", "Alice");
        }

        let bob = store.create_node("Person");
        if let Some(node) = store.get_node_mut(bob) {
            node.set_property("name", "Bob");
        }

        store.create_edge(alice, bob, "KNOWS").unwrap();

        let engine = QueryEngine::new();
        let result = engine.execute(
            "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a, b",
            &store
        );

        assert!(result.is_ok());
        let batch = result.unwrap();
        assert_eq!(batch.len(), 1);
        assert_eq!(batch.columns.len(), 2);
    }

    #[test]
    fn test_property_projection() {
        let mut store = GraphStore::new();

        let alice = store.create_node("Person");
        if let Some(node) = store.get_node_mut(alice) {
            node.set_property("name", "Alice");
            node.set_property("age", 30i64);
        }

        let engine = QueryEngine::new();
        let result = engine.execute("MATCH (n:Person) RETURN n.name, n.age", &store);

        assert!(result.is_ok());
        let batch = result.unwrap();
        assert_eq!(batch.len(), 1);
        assert_eq!(batch.columns.len(), 2);
        assert_eq!(batch.columns[0], "n.name");
        assert_eq!(batch.columns[1], "n.age");
    }

    // ==================== CREATE TESTS ====================

    #[test]
    fn test_create_single_node() {
        // Test: CREATE (n:Person)
        let mut store = GraphStore::new();
        let engine = QueryEngine::new();

        // Execute CREATE query
        let result = engine.execute_mut(r#"CREATE (n:Person)"#, &mut store, "default");

        assert!(result.is_ok(), "CREATE query should succeed");

        // Verify node was created by querying it
        let query_result = engine.execute("MATCH (n:Person) RETURN n", &store);
        assert!(query_result.is_ok());
        let batch = query_result.unwrap();
        assert_eq!(batch.len(), 1, "Should have created 1 Person node");
    }

    #[test]
    fn test_create_node_with_properties() {
        // Test: CREATE (n:Person {name: "Alice", age: 30})
        let mut store = GraphStore::new();
        let engine = QueryEngine::new();

        // Execute CREATE query with properties
        let result = engine.execute_mut(
            r#"CREATE (n:Person {name: "Alice", age: 30})"#,
            &mut store,
            "default"
        );

        assert!(result.is_ok(), "CREATE query with properties should succeed");

        // Verify node was created with correct properties
        let query_result = engine.execute("MATCH (n:Person) RETURN n.name, n.age", &store);
        assert!(query_result.is_ok());
        let batch = query_result.unwrap();
        assert_eq!(batch.len(), 1, "Should have created 1 Person node");
    }

    #[test]
    fn test_create_multiple_nodes() {
        // Test multiple CREATE operations
        let mut store = GraphStore::new();
        let engine = QueryEngine::new();

        // Create first node
        let result1 = engine.execute_mut(r#"CREATE (a:Person {name: "Alice"})"#, &mut store, "default");
        assert!(result1.is_ok());

        // Create second node
        let result2 = engine.execute_mut(r#"CREATE (b:Person {name: "Bob"})"#, &mut store, "default");
        assert!(result2.is_ok());

        // Verify both nodes exist
        let query_result = engine.execute("MATCH (n:Person) RETURN n", &store);
        assert!(query_result.is_ok());
        let batch = query_result.unwrap();
        assert_eq!(batch.len(), 2, "Should have created 2 Person nodes");
    }

    #[test]
    fn test_create_returns_error_on_readonly_executor() {
        // Test that using read-only executor for CREATE fails
        let store = GraphStore::new();
        let engine = QueryEngine::new();

        // Try to execute CREATE with read-only execute() - should fail
        let result = engine.execute(r#"CREATE (n:Person)"#, &store);

        assert!(result.is_err(), "CREATE should fail with read-only executor");
    }

    // ==================== CREATE EDGE TESTS ====================

    #[test]
    fn test_create_edge_simple() {
        // Test: CREATE (a:Person)-[:KNOWS]->(b:Person)
        let mut store = GraphStore::new();
        let engine = QueryEngine::new();

        // Execute CREATE query with edge
        let result = engine.execute_mut(
            r#"CREATE (a:Person {name: "Alice"})-[:KNOWS]->(b:Person {name: "Bob"})"#,
            &mut store,
            "default"
        );

        assert!(result.is_ok(), "CREATE with edge should succeed: {:?}", result.err());

        // Verify nodes were created
        let query_result = engine.execute("MATCH (n:Person) RETURN n", &store);
        assert!(query_result.is_ok());
        let batch = query_result.unwrap();
        assert_eq!(batch.len(), 2, "Should have created 2 Person nodes");

        // Verify edge was created by querying the relationship
        let edge_result = engine.execute(
            "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a, b",
            &store
        );
        assert!(edge_result.is_ok(), "Edge query should succeed");
        let edge_batch = edge_result.unwrap();
        assert_eq!(edge_batch.len(), 1, "Should have 1 KNOWS relationship");
    }

    #[test]
    fn test_create_edge_with_properties() {
        // Test: CREATE (a:Person)-[:KNOWS {since: 2020}]->(b:Person)
        let mut store = GraphStore::new();
        let engine = QueryEngine::new();

        // Execute CREATE query with edge properties
        let result = engine.execute_mut(
            r#"CREATE (a:Person {name: "Alice"})-[:FRIENDS {since: 2020}]->(b:Person {name: "Bob"})"#,
            &mut store,
            "default"
        );

        assert!(result.is_ok(), "CREATE with edge properties should succeed: {:?}", result.err());

        // Verify edge was created
        let edge_result = engine.execute(
            "MATCH (a:Person)-[r:FRIENDS]->(b:Person) RETURN a, r, b",
            &store
        );
        assert!(edge_result.is_ok(), "Edge query should succeed");
        let edge_batch = edge_result.unwrap();
        assert_eq!(edge_batch.len(), 1, "Should have 1 FRIENDS relationship");
    }

    #[test]
    fn test_create_chain_pattern() {
        // Test: CREATE (a:Person)-[:KNOWS]->(b:Person)-[:LIKES]->(c:Movie)
        let mut store = GraphStore::new();
        let engine = QueryEngine::new();

        // Execute CREATE query with chain of edges
        let result = engine.execute_mut(
            r#"CREATE (a:Person {name: "Alice"})-[:KNOWS]->(b:Person {name: "Bob"})-[:LIKES]->(c:Movie {title: "Matrix"})"#,
            &mut store,
            "default"
        );

        assert!(result.is_ok(), "CREATE chain should succeed: {:?}", result.err());

        // Verify 2 Person nodes and 1 Movie node created
        let person_result = engine.execute("MATCH (n:Person) RETURN n", &store);
        assert!(person_result.is_ok());
        assert_eq!(person_result.unwrap().len(), 2, "Should have 2 Person nodes");

        let movie_result = engine.execute("MATCH (n:Movie) RETURN n", &store);
        assert!(movie_result.is_ok());
        assert_eq!(movie_result.unwrap().len(), 1, "Should have 1 Movie node");

        // Verify both edges were created
        let knows_result = engine.execute(
            "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a, b",
            &store
        );
        assert!(knows_result.is_ok());
        assert_eq!(knows_result.unwrap().len(), 1, "Should have 1 KNOWS relationship");

        let likes_result = engine.execute(
            "MATCH (a:Person)-[:LIKES]->(b:Movie) RETURN a, b",
            &store
        );
        assert!(likes_result.is_ok());
        assert_eq!(likes_result.unwrap().len(), 1, "Should have 1 LIKES relationship");
    }

    #[test]
    fn test_cache_hit_miss_tracking() {
        let store = GraphStore::new();
        let engine = QueryEngine::new();

        // First execution — cache miss
        let _ = engine.execute("MATCH (n:Person) RETURN n", &store);
        assert_eq!(engine.cache_stats().hits(), 0);
        assert_eq!(engine.cache_stats().misses(), 1);
        assert_eq!(engine.cache_len(), 1);

        // Second identical query — cache hit
        let _ = engine.execute("MATCH (n:Person) RETURN n", &store);
        assert_eq!(engine.cache_stats().hits(), 1);
        assert_eq!(engine.cache_stats().misses(), 1);

        // Different query — cache miss
        let _ = engine.execute("MATCH (n:Movie) RETURN n", &store);
        assert_eq!(engine.cache_stats().hits(), 1);
        assert_eq!(engine.cache_stats().misses(), 2);
        assert_eq!(engine.cache_len(), 2);

        // Whitespace-normalized hit
        let _ = engine.execute("MATCH  (n:Person)  RETURN  n", &store);
        assert_eq!(engine.cache_stats().hits(), 2);
        assert_eq!(engine.cache_stats().misses(), 2);
    }

    #[test]
    fn test_lru_eviction() {
        let store = GraphStore::new();
        let engine = QueryEngine::with_capacity(2);

        // Fill cache to capacity
        let _ = engine.execute("MATCH (a:Person) RETURN a", &store);
        let _ = engine.execute("MATCH (b:Movie) RETURN b", &store);
        assert_eq!(engine.cache_len(), 2);

        // Third distinct query should evict the LRU entry
        let _ = engine.execute("MATCH (c:Company) RETURN c", &store);
        assert_eq!(engine.cache_len(), 2); // Still 2, not 3

        // The first query should have been evicted (was LRU)
        let _ = engine.execute("MATCH (a:Person) RETURN a", &store);
        // If evicted: miss count goes up; if still cached: hit count goes up
        // We had 3 misses so far, this should be a 4th miss
        assert_eq!(engine.cache_stats().misses(), 4);
    }
}
