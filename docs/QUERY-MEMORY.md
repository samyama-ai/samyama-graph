# Memory while a query runs

What a Cypher query costs in heap while it executes, what it leaves behind,
and how that behaves under concurrency and a time limit (#2084). The stored
graph's footprint is a different question, answered by
`benches/memory_footprint.rs` (#477).

## How it is measured

`benches/query_memory.rs`, with a counting allocator
(`benches/common/peak_alloc.rs`) that wraps the shipped allocator and records
live heap bytes and their **peak**. Each workload is run once to warm the parse
cache, then measured. The figures are:

* **peak** -- the highest live heap above where the query started;
* **retained** -- what is still allocated once the query's own result is
  dropped.

These are exact heap bytes, not RSS, so they reproduce from run to run.
Allocator slack and pages the allocator keeps after a free are not counted.

```
cargo bench --bench query_memory
cargo bench --bench query_memory -- --scale 200000 --json query_memory.json
```

## Baseline (2026-11-17, `dev` 8ab8e804, release, macOS arm64)

The graph is synthetic: 100,000 `:P` nodes with two properties and 300,000
`:R` edges (fanout 3). It holds **72.7 MB** of live heap.

| workload | rows | ms | peak MB | peak / graph | retained B |
|---|---:|---:|---:|---:|---:|
| 1-hop `count` | 1 | 77 | 13.9 | 0.19x | 0 |
| 2-hop `count` | 1 | 386 | 20.4 | 0.28x | 0 |
| 3-hop `count` | 1 | 1,383 | 25.0 | 0.34x | 60 |
| 1-hop rows (`a.i, b.i`) | 300,000 | 153 | 112.5 | **1.55x** | 0 |
| group + `collect` | 100,000 | 186 | 57.6 | 0.79x | 0 |
| sort all (`ORDER BY`) | 100,000 | 99 | 59.2 | 0.81x | 0 |
| top 10 (`ORDER BY … LIMIT 10`) | 10 | 50 | **35.4** | 0.49x | 0 |
| 2-hop `DISTINCT` | 32,000 | 642 | 15.6 | 0.21x | 0 |
| var-length `*1..3`, anchored | 1 | 16 | 4.0 | 0.05x | 0 |

**Repeated 100 times** (2-hop count, group + collect, 2-hop distinct): the
heap grew **0 bytes** in each case.

**k identical queries started together** (combined peak):

| workload | k=1 | k=2 | k=4 | k=8 |
|---|---:|---:|---:|---:|
| 2-hop count | 20.5 MB | 40.9 MB (2.01x) | 78.1 MB (3.83x) | 153.6 MB (7.54x) |
| sort all | 59.3 MB | 118.6 MB (2.00x) | 233.6 MB (3.94x) | **466.0 MB (7.87x)** |

**With a 1 ms time limit** (both queries are stopped by it):

| workload | peak, no limit | peak, 1 ms limit | retained |
|---|---:|---:|---:|
| 3-hop count | 25.0 MB | **25.0 MB** | 0 |
| sort all | 59.2 MB | **59.2 MB** | 0 |

The regression test (`tests/query_memory_released.rs`, below) adds three
results on a 2,000-node graph. A query that fails part way, by division by
zero per row, retains nothing. A query stopped by its deadline retains
nothing. And the first run of a new statement keeps about 170 KB, its
parse-cache entry, which is kept on purpose; every later run keeps 0.

## Findings

1. **No leaks found.** Every workload gives back what it allocated, whether
   it succeeds, fails part way or runs out of time, and the heap does not
   creep over 100 repetitions. The one allocation a query keeps is its
   parse-cache entry, the first time a statement is seen.

2. **A time limit does not limit memory.** A query given 1 ms reaches exactly
   the peak it reaches with no limit, and is only then stopped. The deadline
   is checked after an operator has built its intermediate result, not while
   building it. A runaway query is therefore bounded in time but not in
   memory: it can allocate its whole intermediate result before it is
   refused.

3. **An aggregate over an expansion materialises the expansion.** `count(b)`
   over a 1-hop pattern returns one row and peaks at 13.9 MB, about 46 bytes
   for each of the 300,000 edges it counts. A streaming count needs a few
   bytes. The same pattern holds at 2 and 3 hops.

   *Cause and change (#2274).* The peak was one input batch, not the whole
   expansion: the aggregate pulled 65,536 rows at a time, and 13.9 MB is
   65,536 records at about 212 bytes (the 3-hop count folds 2.7 million rows
   and peaks at 25 MB, so it cannot have been holding them all). The
   aggregate now pulls 4,096 rows at a time (`AGGREGATE_BATCH_ROWS`), which
   bounds that batch at roughly 0.9 MB for the 1-hop count whatever the row
   count. `tests/an_aggregate_over_an_expansion_streams.rs` asserts that ten
   times the rows peak at under three times the memory. The baseline table
   above is the figure before the change; rerun the benchmark for the after.

4. **`ORDER BY … LIMIT 10` costs 60% of a full sort.** Top-10 peaks at
   35.4 MB against 59.2 MB for sorting all 100,000 rows. A bounded top-k needs
   memory for k rows; most of the 35 MB is the projected input, held before
   the top-k sees it.

5. **A result row is large.** 300,000 rows of two integers peak at 112.5 MB,
   374 bytes per row and 1.55 times the graph itself. Each `Value` in a
   `Record` carries enum and string-key overhead well beyond the 16 bytes of
   data.

6. **Concurrent queries add up linearly, with no ceiling.** Eight concurrent
   full sorts peak at 466 MB, 6.4 times the graph. Nothing bounds the total
   memory of the queries in flight. The row budget bounds rows per operator,
   and the tenant memory quota (#2225) bounds stored data, not execution.

## Recommendations

In order of what they would bound:

* **Check the deadline, or a byte budget, inside the operators that
  materialise** -- expand-to-buffer, sort input, aggregation state -- so a
  limit stops a query before its peak rather than after (finding 2). The row
  budget (`executor::budget`) already counts rows per operator, so it is the
  natural place to add bytes.
* **Stream `count`, `sum` and similar aggregates through an expansion** rather
  than over a materialised one (finding 3).
* **Feed a top-k directly from the scan or projection**, keeping k rows
  (finding 4).
* **A per-query memory budget, and optionally a server-wide cap on the
  memory of in-flight queries** (finding 6). This is distinct from the
  tenant quota of #2225.
* **A leaner row representation for wide results** (finding 5). This is the
  largest change and the least urgent of the five.

## The regression gate

`tests/query_memory_released.rs` runs in CI's debug build in about 9 s. It
asserts, in bytes rather than time:

* six query shapes give back what they allocated (at most 16 KB retained);
* a query failing part way, and one stopped by its deadline, give back what
  they allocated;
* 100 runs of three shapes do not grow the heap.

A 64 KB leak injected into the read path fails all four tests. Peaks are
reported by the benchmark and not asserted, because a peak moves with every
legitimate change to an operator. Compare runs of the benchmark instead
(`--json`).
