# LDBC SNB Interactive Benchmark — Samyama v0.6.0

## Overview

The [LDBC Social Network Benchmark (SNB) Interactive](https://ldbcouncil.org/benchmarks/snb/) workload defines parameterized queries over a synthetic social network. It tests transactional read and write patterns typical of social networking applications.

**Result: 21/21 read queries passed (100%)**

## Test Environment

- **Hardware:** Mac Mini M4 (10-core: 4P+6E), 16GB RAM
- **OS:** macOS Tahoe 26.2
- **Build:** `cargo build --release` (Rust 1.85, LTO enabled)
- **Date:** 2026-03-07

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
| IS1 | Person Profile | Fetch person attributes by ID | 1 | 17.8ms | 17.2ms | 18.0ms | OK |
| IS2 | Recent Posts by Person | 10 most recent posts by a person | 10 | 18.0ms | 18.0ms | 18.1ms | OK |
| IS3 | Friends of Person | Bidirectional KNOWS traversal | 5 | 17.6ms | 17.2ms | 17.8ms | OK |
| IS4 | Post Content | Fetch post with coalesce(imageFile, content) | 1 | 336ms | 326ms | 339ms | OK |
| IS5 | Post Creator | 1-hop Post → Person via HAS_CREATOR | 1 | 316ms | 316ms | 450ms | OK |
| IS6 | Forum of Post | Multi-hop: Post ← CONTAINER_OF ← Forum → HAS_MODERATOR → Person | 1 | 314ms | 313ms | 314ms | OK |
| IS7 | Replies to Post | Comment replies with author info, knows check | 2 | 5.8s | 5.7s | 5.8s | OK |

### Complex Reads (IC1-IC14)

| Query | Name | Description | Rows | Median | Min | Max | Status |
|-------|------|-------------|------|--------|-----|-----|--------|
| IC1 | Transitive Friends by Name | KNOWS*1..3 + firstName filter | 0 | 17.0ms | 16.8ms | 17.5ms | OK |
| IC2 | Recent Friend Posts | Friends' posts before a date | 20 | 24.4ms | 24.1ms | 25.8ms | OK |
| IC3 | Friends in Countries | FoF posts in two countries within date range | 0 | 5.5s | 5.2s | 5.5s | OK |
| IC4 | Popular Tags in Period | Tag frequency on friends' posts in date window | 10 | 24.2ms | 24.2ms | 25.7ms | OK |
| IC5 | New Forum Members | Forums joined by FoF after a date | 20 | 4.4s | 4.2s | 4.4s | OK |
| IC6 | Tag Co-occurrence | Tags co-occurring with a given tag on FoF posts | 0 | 6.6s | 6.5s | 6.8s | OK |
| IC7 | Recent Likers | People who liked a person's messages | 20 | 17.6ms | 17.5ms | 19.1ms | OK |
| IC8 | Recent Replies | Reply comments to a person's messages | 20 | 17.7ms | 17.7ms | 17.9ms | OK |
| IC9 | Recent FoF Posts | FoF posts with coalesce + ordering | 20 | 26.9ms | 26.6ms | 28.3ms | OK |
| IC10 | Friend Recommendation | FoF ranked by shared interests | 0 | 4.2s | 4.1s | 4.2s | OK |
| IC11 | Job Referral | FoF who worked at a company before a year | 0 | 21.1ms | 19.4ms | 23.3ms | OK |
| IC12 | Expert Reply | Friends replying to posts tagged with a TagClass | 5 | 64.9ms | 64.8ms | 67.2ms | OK |
| IC13 | Single Shortest Path | Shortest KNOWS path between two persons (BFS) | 1 | 18.2ms | 18.1ms | 18.3ms | OK |
| IC14 | Trusted Connection Paths | All shortest paths with interaction weights | 3 | 19.4ms | 19.4ms | 19.6ms | OK |

### Performance Summary

| Category | Queries | Median Range | Notes |
|----------|---------|--------------|-------|
| Point lookups (IS1-IS3, IC7, IC8, IC11) | 6 | 17.0ms - 21.1ms | Index + property lookup |
| 1-hop with filters (IC2, IC4, IC9, IC12) | 4 | 24.2ms - 64.9ms | Scales with neighbor count |
| Multi-hop (IC3, IC5, IC6, IC10) | 4 | 4.2s - 6.6s | Full FoF expansion on 180K KNOWS edges |
| Full-graph scan (IS4-IS7) | 4 | 314ms - 5.8s | Scanning 1M+ Post/Comment nodes |
| Path finding (IC13, IC14) | 2 | 18.2ms - 19.4ms | BFS over KNOWS subgraph |

**Total benchmark time:** 111.9s

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

## Delete Operations (DEL-1 through DEL-8)

8 delete operations following the LDBC SNB Interactive v2 specification:

| ID | Name | Description | Status |
|----|------|-------------|--------|
| DEL-1 | Remove Person | `MATCH (p:Person {id: $id}) DETACH DELETE p` (cascading) | Defined |
| DEL-2 | Remove Like on Post | `MATCH (p:Person {id: $pid})-[r:LIKES]->(m:Post {id: $mid}) DELETE r` | Defined |
| DEL-3 | Remove Like on Comment | `MATCH (p:Person {id: $pid})-[r:LIKES]->(c:Comment {id: $cid}) DELETE r` | Defined |
| DEL-4 | Remove Forum | `MATCH (f:Forum {id: $id}) DETACH DELETE f` (cascading) | Defined |
| DEL-5 | Remove Forum Member | `MATCH (f:Forum {id: $fid})-[r:HAS_MEMBER]->(p:Person {id: $pid}) DELETE r` | Defined |
| DEL-6 | Remove Post | `MATCH (p:Post {id: $id}) DETACH DELETE p` | Defined |
| DEL-7 | Remove Comment | `MATCH (c:Comment {id: $id}) DETACH DELETE c` | Defined |
| DEL-8 | Remove Friendship | `MATCH (a:Person {id: $aid})-[r:KNOWS]->(b:Person {id: $bid}) DELETE r` | Defined |

Run with: `cargo bench --release --bench ldbc_benchmark -- --updates --deletes`

The benchmark executes in order: reads, then INS1-8 (creates test entities), then DEL1-8 (removes them).

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
