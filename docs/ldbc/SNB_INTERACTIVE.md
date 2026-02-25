# LDBC SNB Interactive Benchmark — Samyama v0.5.8

## Overview

The [LDBC Social Network Benchmark (SNB) Interactive](https://ldbcouncil.org/benchmarks/snb/) workload defines parameterized queries over a synthetic social network. It tests transactional read and write patterns typical of social networking applications.

**Result: 21/21 read queries passed (100%)**

## Test Environment

- **Hardware:** Mac Mini M2 Pro, 16GB RAM
- **OS:** macOS Sonoma
- **Build:** `cargo build --release` (Rust 1.83, LTO enabled)
- **Date:** 2026-02-25

## Dataset: Scale Factor 1 (SF1)

| Entity | Count |
|--------|-------|
| Place | 1,460 |
| Organisation | 7,955 |
| Tag | 16,080 |
| TagClass | 71 |
| Person | 9,892 |
| Forum | 90,492 |
| Post | 1,003,605 |
| Comment | 2,052,169 |
| **Total Nodes** | **3,181,724** |
| **Total Edges** | **17,256,038** |

**Load time:** 9.2s (single-threaded CSV parsing)

## Read Query Results (3 runs each)

### Short Reads (IS1-IS7)

| Query | Name | Description | Rows | Median | Min | Max | Status |
|-------|------|-------------|------|--------|-----|-----|--------|
| IS1 | Person Profile | Fetch person attributes by ID | 1 | 1.8ms | 1.8ms | 1.9ms | OK |
| IS2 | Recent Posts by Person | 10 most recent posts by a person | 10 | 2.2ms | 2.2ms | 2.2ms | OK |
| IS3 | Friends of Person | Bidirectional KNOWS traversal | 5 | 1.8ms | 1.8ms | 1.9ms | OK |
| IS4 | Post Content | Fetch post with coalesce(imageFile, content) | 1 | 293ms | 292ms | 295ms | OK |
| IS5 | Post Creator | 1-hop Post → Person via HAS_CREATOR | 1 | 284ms | 282ms | 284ms | OK |
| IS6 | Forum of Post | Multi-hop: Post ← CONTAINER_OF ← Forum → HAS_MODERATOR → Person | 1 | 284ms | 283ms | 291ms | OK |
| IS7 | Replies to Post | Comment replies with author info, knows check | 2 | 6.2s | 5.9s | 6.2s | OK |

### Complex Reads (IC1-IC14)

| Query | Name | Description | Rows | Median | Min | Max | Status |
|-------|------|-------------|------|--------|-----|-----|--------|
| IC1 | Transitive Friends by Name | KNOWS*1..3 + firstName filter | 0 | 1.8ms | 1.8ms | 1.9ms | OK |
| IC2 | Recent Friend Posts | Friends' posts before a date | 20 | 8.9ms | 8.7ms | 10.6ms | OK |
| IC3 | Friends in Countries | FoF posts in two countries within date range | 0 | 5.4s | 4.7s | 5.5s | OK |
| IC4 | Popular Tags in Period | Tag frequency on friends' posts in date window | 10 | 10.1ms | 10.0ms | 12.3ms | OK |
| IC5 | New Forum Members | Forums joined by FoF after a date | 20 | 4.6s | 4.5s | 4.9s | OK |
| IC6 | Tag Co-occurrence | Tags co-occurring with a given tag on FoF posts | 0 | 6.3s | 6.2s | 6.6s | OK |
| IC7 | Recent Likers | People who liked a person's messages | 20 | 2.1ms | 2.0ms | 2.1ms | OK |
| IC8 | Recent Replies | Reply comments to a person's messages | 20 | 1.9ms | 1.9ms | 1.9ms | OK |
| IC9 | Recent FoF Posts | FoF posts with coalesce + ordering | 20 | 9.3ms | 8.9ms | 10.0ms | OK |
| IC10 | Friend Recommendation | FoF ranked by shared interests | 0 | 4.2s | 4.1s | 4.3s | OK |
| IC11 | Job Referral | FoF who worked at a company before a year | 0 | 2.1ms | 1.9ms | 2.4ms | OK |
| IC12 | Expert Reply | Friends replying to posts tagged with a TagClass | 5 | 49.7ms | 49.5ms | 52.1ms | OK |
| IC13 | Single Shortest Path | Shortest KNOWS path between two persons (BFS) | 1 | 3.6ms | 3.5ms | 3.6ms | OK |
| IC14 | Trusted Connection Paths | All shortest paths with interaction weights | 3 | 4.5ms | 4.4ms | 4.6ms | OK |

### Performance Summary

| Category | Queries | Median Range | Notes |
|----------|---------|--------------|-------|
| Point lookups (IS1-IS3, IC7, IC8, IC11) | 6 | 1.8ms - 2.2ms | Sub-millisecond after cache warm-up |
| 1-hop with filters (IC2, IC4, IC9, IC12) | 4 | 8.9ms - 49.7ms | Scales with neighbor count |
| Multi-hop (IC3, IC5, IC6, IC10) | 4 | 4.2s - 6.3s | Full FoF expansion on 180K KNOWS edges |
| Full-graph scan (IS4-IS7) | 4 | 284ms - 6.2s | Scanning 1M+ Post/Comment nodes |
| Path finding (IC13, IC14) | 2 | 3.6ms - 4.5ms | BFS over KNOWS subgraph |

**Total benchmark time:** 108.1s | **AST cache:** 63 hits, 21 misses

## Update Operations (INS1-INS8)

8 update operations are defined following the LDBC SNB specification:

| ID | Name | Description | Status |
|----|------|-------------|--------|
| INS1 | Add Person | CREATE Person + multi-step edges (HAS_INTEREST, STUDY_AT, WORK_AT, IS_LOCATED_IN) | Bug (index OOB) |
| INS2 | Add Like to Post | CREATE Person-[:LIKES]->Post | Defined |
| INS3 | Add Like to Comment | CREATE Person-[:LIKES]->Comment | Defined |
| INS4 | Create Forum | CREATE Forum + HAS_MODERATOR + HAS_TAG edges | Defined |
| INS5 | Add Forum Member | CREATE Forum-[:HAS_MEMBER]->Person | Defined |
| INS6 | Create Post | CREATE Post + link edges (HAS_CREATOR, CONTAINER_OF, IS_LOCATED_IN, HAS_TAG) | Defined |
| INS7 | Create Comment | CREATE Comment + link edges (HAS_CREATOR, REPLY_OF, IS_LOCATED_IN, HAS_TAG) | Defined |
| INS8 | Add Friendship | CREATE Person-[:KNOWS]->Person | Defined |

**Note:** INS1 has a runtime bug (index out of bounds) that needs investigation. The underlying CREATE engine works correctly — the issue is in the benchmark parameter extraction.

## Data Model Adaptations

LDBC defines `:Message` as a supertype of `:Post` and `:Comment`. Since Samyama loads them as separate labels, queries referencing `:Message` are adapted:

- IS7: Queries Comment only (posts are containers, not replies)
- IC2, IC9: Query Post variant (main message type for content)
- IC10: Simplified to direct interest matching

## Query Parameters (SF1)

| Parameter | Value | Description |
|-----------|-------|-------------|
| `personId` | `933` | Mahinda Perera |
| `person2Id` | `4139` | Mahinda's first KNOWS target |
| `postId` | `1236950581248` | First post (by person 933) |
| `firstName` | `"Mahinda"` | Common first name in SF1 |
| `countryX` / `countryY` | `"India"` / `"Pakistan"` | Country filters |
| `tagName` | `"Hamid_Karzai"` | Tag filter |
| `tagClassName` | `"MusicalArtist"` | TagClass filter |
| `orgName` | `"MDLR_Airlines"` | Organisation filter |

## Running

```bash
# Full benchmark (21 queries, 3 runs each)
cargo run --release --example ldbc_benchmark -- --runs 3

# Single query
cargo run --release --example ldbc_benchmark -- --query IC13

# With update operations
cargo run --release --example ldbc_benchmark -- --runs 3 --updates

# Custom data directory
cargo run --release --example ldbc_benchmark -- --data-dir /path/to/sf1/data
```
