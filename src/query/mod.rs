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
pub mod parser;
pub mod executor;

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
        }
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

    /// Parse and execute a read-only Cypher query (MATCH, RETURN, etc.)
    pub fn execute(
        &self,
        query_str: &str,
        store: &crate::graph::GraphStore,
    ) -> Result<RecordBatch, Box<dyn std::error::Error>> {
        let query = self.cached_parse(query_str)?;

        let executor = QueryExecutor::new(store);
        let result = executor.execute(&query)?;

        Ok(result)
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
        let result = executor.execute(&query)?;

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
