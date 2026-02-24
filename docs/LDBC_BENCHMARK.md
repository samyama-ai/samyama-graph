# LDBC SNB Interactive Benchmark

Samyama's query engine benchmarked against the [LDBC Social Network Benchmark](https://ldbcouncil.org/benchmarks/snb/) Interactive workload.

## What is LDBC SNB?

The **Linked Data Benchmark Council (LDBC) Social Network Benchmark (SNB)** is the industry-standard benchmark for graph databases. The **Interactive** workload defines 21 parameterized read queries (7 short reads IS1-IS7, 14 complex reads IC1-IC14) over a synthetic social network dataset.

**Scale Factor 1 (SF1)** produces:
- ~3.18M nodes (Person, Post, Comment, Forum, Tag, Place, Organisation, TagClass)
- ~17M edges (KNOWS, HAS_CREATOR, REPLY_OF, LIKES, HAS_TAG, IS_LOCATED_IN, etc.)

## Prerequisites

### 1. Download the SF1 Dataset

```bash
# Download from LDBC's official repository
wget https://repository.surfsara.nl/datasets/cwi/ldbc-snb-sf1/files/social_network-sf1-CsvBasic-LongDateFormatter.tar.zst

# Or use the LDBC SNB Datagen Docker image:
# https://github.com/ldbc/ldbc_snb_datagen_spark

# Extract to the project's data directory
mkdir -p data/ldbc-sf1
tar --use-compress-program=unzstd -xf social_network-sf1-CsvBasic-LongDateFormatter.tar.zst -C data/ldbc-sf1/
```

After extraction you should have:
```
data/ldbc-sf1/social_network-sf1-CsvBasic-LongDateFormatter/
├── static/
│   ├── place_0_0.csv
│   ├── organisation_0_0.csv
│   ├── tag_0_0.csv
│   ├── tagclass_0_0.csv
│   └── ...
└── dynamic/
    ├── person_0_0.csv
    ├── forum_0_0.csv
    ├── post_0_0.csv
    ├── comment_0_0.csv
    ├── person_knows_person_0_0.csv
    └── ...
```

### 2. Load the Dataset

```bash
cargo run --release --example ldbc_loader
```

Or specify a custom path:
```bash
cargo run --release --example ldbc_loader -- --data-dir /path/to/social_network-sf1-CsvBasic-LongDateFormatter
```

## Running the Benchmark

```bash
# Run all 19 queries (5 iterations each)
cargo run --release --example ldbc_benchmark

# Custom number of iterations
cargo run --release --example ldbc_benchmark -- --runs 10

# Run a single query
cargo run --release --example ldbc_benchmark -- --query IS1
cargo run --release --example ldbc_benchmark -- --query IC3

# Custom data directory
cargo run --release --example ldbc_benchmark -- --data-dir /path/to/data
```

### CLI Options

| Flag | Default | Description |
|------|---------|-------------|
| `--data-dir PATH` | `data/ldbc-sf1/social_network-sf1-CsvBasic-LongDateFormatter` | LDBC SF1 data directory |
| `--runs N` | `5` | Iterations per query (excluding warm-up) |
| `--query ID` | *(all)* | Run a specific query only (e.g. `IS1`, `IC3`) |

## Query Reference

### Data Model Adaptation

LDBC uses `:Message` as a supertype for `:Post` and `:Comment`. Since Samyama loads them as separate labels, queries referencing `:Message` are adapted — either split into separate Post/Comment variants or simplified to query one type.

### Short Reads (IS1-IS7)

| ID | Name | Description | Status |
|----|------|-------------|--------|
| IS1 | Person Profile | Fetch person attributes by ID | Supported |
| IS2 | Recent Posts | Recent posts by a person (Post variant) | Adapted |
| IS3 | Friends of Person | Bidirectional KNOWS traversal | Supported |
| IS4 | Post Content | Fetch post content with coalesce | Supported |
| IS5 | Post Creator | 1-hop from Post to Person via HAS_CREATOR | Supported |
| IS6 | Forum of Post | Multi-hop: Post←CONTAINER_OF←Forum→HAS_MODERATOR→Person | Supported |
| IS7 | Replies to Post | Reply comments with author info | Adapted |

### Complex Reads (IC1-IC14)

| ID | Name | Description | Status |
|----|------|-------------|--------|
| IC1 | Transitive Friends by Name | `KNOWS*1..3` + firstName filter | Supported |
| IC2 | Recent Friend Posts | Friends' posts before a date | Supported |
| IC3 | Friends in Countries | FoF posts in two countries within date range | Supported |
| IC4 | Popular Tags in Period | Tag frequency on friends' posts in date window | Supported |
| IC5 | New Forum Members | Forums joined by FoF | Supported |
| IC6 | Tag Co-occurrence | Tags co-occurring with a given tag on FoF posts | Supported |
| IC7 | Recent Likers | People who liked a person's posts | Supported |
| IC8 | Recent Replies | Reply comments to a person's posts | Supported |
| IC9 | Recent FoF Posts | FoF posts with coalesce + ordering | Supported |
| IC10 | Friend Recommendation | FoF ranked by interests (simplified) | Adapted |
| IC11 | Job Referral | FoF who worked at a company before a year | Supported |
| IC12 | Expert Reply | Friends replying to posts tagged with a tag class | Adapted |
| IC13 | Shortest Path | `shortestPath()` between two persons | **Unsupported** |
| IC14 | Weighted Shortest Path | `allShortestPaths()` with weights | **Unsupported** |

**Coverage: 19/21 queries (90.5%)**

IC13 and IC14 require `shortestPath()`/`allShortestPaths()` which are not yet implemented in the Cypher parser.

### Query Parameters (SF1)

| Parameter | Value | Description |
|-----------|-------|-------------|
| `personId` | `933` | Mahinda Perera |
| `person2Id` | `4139` | Mahinda's first KNOWS target |
| `postId` | `1236950581248` | First post (by person 933) |
| `messageId` | `1236950581249` | First comment |
| `firstName` | `"Mahinda"` | Common first name in SF1 |
| `countryX` | `"India"` | Country filter |
| `countryY` | `"Pakistan"` | Country filter |
| `tagName` | `"Hamid_Karzai"` | Tag filter |
| `tagClassName` | `"MusicalArtist"` | TagClass filter |
| `orgName` | `"MDLR_Airlines"` | Organisation filter (most common employer) |
| `maxDate` | `1354320000000` | 2012-12-01 epoch ms |
| `startDate` | `1338508800000` | 2012-06-01 epoch ms |
| `endDate` | `1341100800000` | 2012-07-01 epoch ms |

## Known Gaps

1. **IC13/IC14**: Require `shortestPath()` / `allShortestPaths()` graph functions
2. **`:Message` supertype**: LDBC uses `:Message` as a union of `:Post` and `:Comment`. We query `:Post` only in adapted queries.
3. **Date arithmetic**: Some LDBC queries use `duration()` and date math; we use pre-computed epoch millisecond thresholds instead.

## Architecture

The benchmark reuses the shared LDBC loading module (`examples/ldbc_common/`) which is also used by `ldbc_loader.rs`. The benchmark:

1. **Loads** the full SF1 dataset into an in-memory `GraphStore` via `EmbeddedClient`
2. **Warms up** each query once (populates AST cache)
3. **Benchmarks** each query N times, recording min/median/max latency
4. **Reports** results in a formatted table

All queries use `query_readonly()` which takes a read lock on the store, matching the read-only nature of the LDBC Interactive workload.
