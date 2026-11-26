# ADR-042: Incremental PageRank, WCC and Degree on a Live Graph

## Status
**Accepted** -- implemented for ALGO-14 (#1697).

## Date
2026-11-18

## Context

Every algorithm call built a graph view from the whole store and computed from
scratch. `CH-ALGO-INCR` measured what that costs after a small change: on
120,000 nodes and 479,028 relationships, PageRank with one relationship created
just before it took 0.979 of a run with none -- a one-edge delta cost a full
recomputation. None of the three algorithms the spec names (PageRank, WCC,
degree-based metrics) had an entry point that could do better: `pageRank`
refused `incremental` as an unknown key and `algo.pageRankIncremental` did not
exist.

Two things were missing: a way for an algorithm to learn **what changed** since
it last ran, and algorithms that can **take a change** without starting over.

What the store already had did not answer the first. `data_changes` says that
the graph changed, not what. The durability write log (`take_write_log`, #1094)
records every mutation, but its one consumer drains it each statement, so a
second reader cannot see what it missed.

## Decision

### 1. A structural log in the store

`GraphStore` keeps a `StructureLog` (`src/graph/structure_log.rs`):

- **Additions**, as ids: every node and relationship created, in order. A
  bounded deque (`MAX_RECORDED_ADDITIONS`, 2^20); when full, the older half is
  dropped and a reader behind it rebuilds.
- **Rewrites**, as a count: every delete and every label change.

The calls sit next to the write log's own calls in the store's mutators, so a
path that is durable is also logged. Additions are recorded only after some
reader asked (`start_recording`), so a store nobody runs an incremental
algorithm on pays one branch per write. The rewrite count is always kept.

### 2. Additions are deltas; anything else is a rebuild

Deletions are not replayed. A removed relationship can split a component (a
union-find merge cannot be undone without rebuilding it) or move PageRank mass
anywhere. Rebuilding gives the plain call's answer at the plain call's cost,
which is correct and no worse than before. Graphs that change by growing --
event logs, citations, interaction graphs -- get the incremental path; graphs
with heavy deletion do not, and lose nothing.

### 3. Per-algorithm state (`samyama-graph-algorithms::incremental`)

| Algorithm | State | A new relationship `u -> v` | Promise against a plain call |
|---|---|---|---|
| Degree | in + out degree per node | two increments | identical scores |
| WCC | union-find, built in the batch version's union order | one union | identical partition; identical ids on the first call |
| PageRank | estimate `x`, residual `r`, successor lists | adjust the residuals of `u`'s successors, then push | within `2 * 1e-4 / (1 - d)` L1 |

PageRank solves the unnormalised system `x = (1 - d) + d P^T x`, in which a
dangling node's mass is lost, and reports `x / sum(x)`. That is the same vector
as PageRank with dangling mass spread uniformly -- both are
`(I - d P^T)^-1 1` up to scale -- but every change stays local: a new node or
relationship changes no other node's teleport term. The residual
`r = (1 - d) + d P^T x - x` is kept exactly, so the error is always
`(I - d P^T)^-1 r` and settling to small residuals is enough. The first build
settles to a hundredth of the tolerance (`FIRST_BUILD_HEADROOM`): settled at
the tolerance itself, most nodes sit just under it, and the first change
cascades over thousands of them.

### 4. Where the state lives and how it is reached

`IncrementalStates` (`src/algo/incremental.rs`) is a field of `GraphStore`, so
it is per tenant and goes with the graph. One state per algorithm and filter
(`label`, `edgeType`, and `damping` for PageRank), at most `MAX_KEPT_STATES`
(8), the least recently used evicted. Each call catches up from the log, then
checks the state's node count -- and, unfiltered, its relationship count --
against the store's; a mismatch rebuilds. That check is what turns a write
path that forgot to log into a slower answer rather than a wrong one.

The entry point is a config key, `incremental: true`, on `algo.pageRank`,
`algo.wcc` and `algo.degree`, not three new procedure names: the arguments,
filters and output columns are the plain call's, so a caller changes one key.

## Consequences

- One new relationship costs the algorithm about what it changes. Measured on
  the `CH-ALGO-INCR` graph (120,000 nodes, 479,028 relationships, release build,
  laptop): the incremental PageRank update took **0.93 ms** against **609 ms**
  for a full call.
- **The call is not yet 10x cheaper.** A call still returns one row per node,
  and building 120,000 rows costs 170-380 ms in the executor regardless of how
  the scores were found. The measured call-level ratio after one new edge was
  0.62 (PageRank), 0.29 (WCC) and 0.53 (degree), against the 0.1 target. A
  node reference in place of a cloned node in each row brought degree's ratio
  to 0.40 -- not enough on its own. Closing the gap is a row-materialisation
  change across the executor, not an algorithm change, and is left to its own
  issue.
- Memory: a PageRank state copies the adjacency it reads; eight of them bound
  what a caller can make the engine hold by varying filters.
- A restart forgets every state, as it forgets graph projections.

## Alternatives Considered

- **Reuse the durability write log.** It is drained by its consumer every
  statement and records property writes as the same `Upserted` entries as
  creations, so a second reader could neither see what it missed nor tell a
  new relationship from a changed property.
- **Detect additions by id range.** Edge and node ids are recycled after a
  delete (`free_edge_ids`), so a new relationship can take an id below any
  watermark.
- **Decremental connectivity and PageRank for deletions.** Possible, and far
  more code for graphs that mostly grow. A rebuild is correct; this can be
  added later behind the same key.
- **Warm-started power iteration for PageRank.** Converges in fewer
  iterations, but each iteration still reads every relationship, so a one-edge
  change still costs a large fraction of a full run.
- **New procedure names (`algo.pageRankIncremental`).** Three more names whose
  arguments and output duplicate the plain ones; a key on the existing call is
  one place to document and one to test.

## Related Decisions

- ALGO-05 graph projections: a projection is a snapshot; `incremental` follows
  the live graph, and the two are refused together.
- ALGO-12 (#1693) progress and cancel: the first build reports its view build
  and sweeps through the same `Progress` hook, so the deadline stops it.
