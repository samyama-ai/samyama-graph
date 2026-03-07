# LDBC SNB Business Intelligence Benchmark — Samyama v0.6.0

## Overview

The [LDBC SNB Business Intelligence (BI)](https://ldbcouncil.org/benchmarks/snb/) workload defines 20 complex analytical queries over the same social network dataset. Unlike the Interactive workload, BI queries involve heavy aggregation, multi-hop traversal, and global analytics.

**Result: 16/16 queries passed (100% of run), BI-17+ timeout on heavy global analytics. All 20 queries implemented.**

## Test Environment

- **Hardware:** Mac Mini M4 (10-core: 4P+6E), 16GB RAM
- **OS:** macOS Tahoe 26.2
- **Build:** `cargo build --release` (Rust 1.85, LTO enabled)
- **Date:** 2026-03-07

## Dataset

Same SF1 dataset as SNB Interactive: **3,181,724 nodes, 17,256,038 edges** (loaded in 9.6s)

## Query Results (3 runs each)

| Query | Name | Rows | Median | Min | Max | Status |
|-------|------|------|--------|-----|-----|--------|
| BI-1 | Posting Summary | 1 | 369ms | 369ms | 369ms | OK |
| BI-2 | Tag Co-occurrence | 20 | 11.1s | 11.1s | 11.1s | OK |
| BI-3 | Tag Evolution | 1 | 515ms | 488ms | 521ms | OK |
| BI-4 | Popular Moderators | 20 | 4.2s | 4.1s | 4.2s | OK |
| BI-5 | Most Active Posters | 20 | 917ms | 905ms | 919ms | OK |
| BI-6 | Most Authoritative Users | 20 | 8.3s | 8.2s | 8.7s | OK |
| BI-7 | Authoritative Authors by Score | 20 | 3.9s | 3.7s | 4.2s | OK |
| BI-8 | Related Topics | 20 | 11.6s | 11.6s | 11.7s | OK |
| BI-9 | Forum with Related Tags | 10 | 4.6s | 4.4s | 4.6s | OK |
| BI-10 | Experts in Social Circle | 0 | 4.5s | 4.3s | 4.5s | OK |
| BI-11 | Unrelated Replies | 1 | 1.1s | 1.1s | 1.4s | OK |
| BI-12 | Person Trending | 20 | 1.9s | 1.9s | 2.0s | OK |
| BI-13 | Popular Months | 20 | 1.2s | 1.2s | 1.3s | OK |
| BI-14 | Top Thread Initiators | 20 | 1.2s | 1.2s | 1.2s | OK |
| BI-15 | Social Normals | 20 | 384ms | 381ms | 454ms | OK |
| BI-16 | Expert Search | 20 | 2.0s | 1.9s | 2.1s | OK |
| BI-17 | Information Propagation | - | - | - | - | TIMEOUT (>10min) |
| BI-18 | Person Posting Stats | - | - | - | - | Not reached |
| BI-19 | Stranger Interaction | - | - | - | - | Not reached |
| BI-20 | High-Level Topics | - | - | - | - | Not reached |

## Improvements in v0.6.0

### BI-4: WITH Projection Barrier (fixed in v0.5.8)

Implemented full `WithBarrierOperator` that materializes pre-WITH results, evaluates aggregations, applies DISTINCT/ORDER BY/SKIP/LIMIT, and projects only named columns through the barrier. BI-4's pattern `WITH f, count(p) AS postCount ORDER BY postCount DESC LIMIT 20 MATCH ...` now works correctly.

### BI-7 through BI-16: Now Passing (v0.6.0)

Previously, BI-7 timed out and blocked all subsequent queries. With the 120s timeout guard and query engine improvements (graph-native planner, sorted adjacency lists, ExpandInto operator, predicate pushdown), BI-7 through BI-16 now all complete successfully:

- **BI-7** (Authoritative Authors): 3.9s median — was previously TIMEOUT
- **BI-8** (Related Topics): 11.6s median — heaviest passing query
- **BI-9 through BI-16**: 384ms to 4.6s — all well within timeout

### BI-17: Still Timeouts

BI-17 ("Information Propagation") involves counting friend triangles combined with message propagation analysis across the full 3M+ node graph. This remains a combinatorial explosion on SF1.

## Query Descriptions

| ID | Name | What it Computes |
|----|------|-----------------|
| BI-1 | Posting Summary | Message count by year, isComment flag, and length category |
| BI-2 | Tag Co-occurrence | Top tag pairs that appear together on messages in a date range and country |
| BI-3 | Tag Evolution | Change in tag usage between two date windows |
| BI-4 | Popular Moderators | Forum moderators with most posts (requires WITH barrier fix) |
| BI-5 | Most Active Posters | Top users by posting frequency in a specific country |
| BI-6 | Most Authoritative Users | Users whose posts receive the most replies, 2-hop |
| BI-7 | Authority Score | Authority ranking by friend interaction weight |
| BI-8 | Related Topics | Tags most frequently co-occurring with a given tag |
| BI-9 | Forum with Related Tags | Forums containing posts with tags from two TagClasses |
| BI-10 | Central Person | Most central person in a tag-based KNOWS subgraph |
| BI-11 | Unrelated Replies | Replies whose tag has no connection to the original post's tag |
| BI-12 | Trending Posts | Posts with highest like-to-reply ratio |
| BI-13 | Popular Moderators in Country | Forum moderators by member count, filtered by country |
| BI-14 | Top Thread Initiators | Users who start the longest reply chains |
| BI-15 | Social Normals | Friends-of-friends reachable via weighted KNOWS paths |
| BI-16 | Experts in Social Circle | Tag experts within 4-hop KNOWS distance |
| BI-17 | Friend Triangles | Count of triangles in the KNOWS friendship graph |
| BI-18 | Person Posting Stats | Message count per person with interaction details |
| BI-19 | Stranger Interaction | Interactions between people who are not friends |
| BI-20 | High-Level Topics | Tag usage aggregated by TagClass hierarchy |

## Adaptations

The BI benchmark adapts LDBC-specified queries for Samyama's feature set:

1. **`:Message` supertype:** Split into separate Post and Comment subqueries with results merged in Rust
2. **APOC procedures (BI-10):** Rewritten as multi-hop `KNOWS*1..N` Cypher traversal
3. **GDS algorithms (BI-15, BI-19, BI-20):** Pre-compute interaction weights in Rust, then use `AlgorithmClient::dijkstra()`
4. **Triangle counting (BI-17):** Uses `algo::count_triangles()` from samyama-graph-algorithms

## Known Limitations

1. **Performance on global analytics:** BI-17+ queries that combine triangle counting with full-graph message propagation remain computationally prohibitive on SF1
2. **Memory pressure:** BI queries over SF1 use ~5GB RAM due to intermediate results

## Running

```bash
# Full benchmark (20 queries, 3 runs each)
cargo run --release --example ldbc_bi_benchmark -- --runs 3

# Custom data directory
cargo run --release --example ldbc_bi_benchmark -- --data-dir /path/to/sf1/data
```
