# LDBC SNB Business Intelligence Benchmark — Samyama v0.5.8

## Overview

The [LDBC SNB Business Intelligence (BI)](https://ldbcouncil.org/benchmarks/snb/) workload defines 20 complex analytical queries over the same social network dataset. Unlike the Interactive workload, BI queries involve heavy aggregation, multi-hop traversal, and global analytics.

**Result: 5/6 queries passed before BI-7 timed out (83% partial, 20 queries implemented)**

## Test Environment

- **Hardware:** Mac Mini M2 Pro, 16GB RAM
- **OS:** macOS Sonoma
- **Build:** `cargo build --release` (Rust 1.83, LTO enabled)
- **Date:** 2026-02-25

## Dataset

Same SF1 dataset as SNB Interactive: **3,181,724 nodes, 17,256,038 edges** (loaded in 9.6s)

## Query Results (3 runs each)

| Query | Name | Rows | Median | Min | Max | Status |
|-------|------|------|--------|-----|-----|--------|
| BI-1 | Posting Summary | 1 | 397ms | 396ms | 401ms | OK |
| BI-2 | Tag Co-occurrence | 20 | 11.3s | 11.0s | 12.0s | OK |
| BI-3 | Tag Evolution | 1 | 504ms | 501ms | 511ms | OK |
| BI-4 | Popular Moderators | - | - | - | - | ERROR |
| BI-5 | Most Active Posters | 20 | 773ms | 770ms | 775ms | OK |
| BI-6 | Most Authoritative Users | 20 | 8.3s | 7.7s | 9.1s | OK |
| BI-7 | Authority Score | - | - | - | - | TIMEOUT (>10min) |
| BI-8 | Related Topics | - | - | - | - | Not reached |
| BI-9 | Forum with Related Tags | - | - | - | - | Not reached |
| BI-10 | Central Person | - | - | - | - | Not reached |
| BI-11 | Unrelated Replies | - | - | - | - | Not reached |
| BI-12 | Trending Posts | - | - | - | - | Not reached |
| BI-13 | Popular Moderators in Country | - | - | - | - | Not reached |
| BI-14 | Top Thread Initiators | - | - | - | - | Not reached |
| BI-15 | Social Normals | - | - | - | - | Not reached |
| BI-16 | Experts in Social Circle | - | - | - | - | Not reached |
| BI-17 | Friend Triangles | - | - | - | - | Not reached |
| BI-18 | Person Posting Stats | - | - | - | - | Not reached |
| BI-19 | Stranger Interaction | - | - | - | - | Not reached |
| BI-20 | High-Level Topics | - | - | - | - | Not reached |

## Error Details

### BI-4: Variable Not Found
```
Query error: Variable not found: postCount
```
The query uses a WITH clause that introduces an alias `postCount` which is not being carried through the projection barrier correctly. This is a known limitation of the WITH projection barrier implementation.

### BI-7: Timeout (>10 minutes)
BI-7 ("Authority Score") performs a multi-hop traversal computing authority scores across the KNOWS network combined with message interactions. On SF1 with 180K KNOWS edges and 3M+ message nodes, this produces a combinatorial explosion. The query was killed after 10 minutes.

**Root cause:** BI-7 requires iterating over all Person-KNOWS-Person pairs, then for each pair counting shared message interactions across 1M+ Post and 2M+ Comment nodes. Without indexing on message creator, this becomes O(persons x friends x messages).

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

1. **WITH projection barrier:** BI-4 fails because aliased aggregation variables don't survive WITH correctly
2. **Performance on global analytics:** BI-7+ queries that scan all messages against all person pairs need query optimization (hash joins, predicate pushdown)
3. **Memory pressure:** BI queries over SF1 use ~5GB RAM due to intermediate results

## Running

```bash
# Full benchmark (20 queries, 3 runs each)
cargo run --release --example ldbc_bi_benchmark -- --runs 3

# Custom data directory
cargo run --release --example ldbc_bi_benchmark -- --data-dir /path/to/sf1/data
```
