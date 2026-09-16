# ADR-038: Memory Allocator for the Server Binary

## Status
**Accepted** (2026-09-17). Chosen by the decision rule below, which was written before
the deciding SF10 measurement was taken. Issue: samyama-graph#1267.

## Date
2026-09-17

## Context

### The regression that raised the question

CH-REGRESS failed on main after the MVCC stack (#1200). A same-host Vultr A/B of
`531771f` against `0eb6a55` on 2026-09-16 found these queries slower, each confirmed
over two runs:

| query | slowdown |
|---|---|
| BI-9 | 1.8× |
| BI-14 | 1.7× |
| BI-8 | 1.4× |
| IC14 | 1.3× |
| IC5 | 1.2× |

H1 condition 3 (spec 19 §2) cannot close while it stands.

Bisection on GitHub runners, each an A/B on one runner, put all of it in **one commit**,
MVCC step 5b (`97e97ea`), which dropped the relationship row copy (#545). On that step:

| query | before | after | ratio |
|---|---:|---:|---:|
| BI-14 | 558 ms | 935 ms | 1.68× |
| BI-12 | 780 ms | 1,244 ms | 1.59× |
| BI-9 | 498 ms | 723 ms | 1.45× |
| BI-8 | 451 ms | 572 ms | 1.27× |
| BI-4 | 208 ms | 262 ms | 1.26× |

Steps 3, 4 and 5a, and the 8 executor commits in the same range, were flat
(runs 35099052379, 35104224715).

The same change is what removed 15.5 GB of relationship row properties at SF10.
Reverting it is not an option.

### It is the allocator, not the query code

**Profile.** `perf` on BI-14, flat self time, run 35109413988 and, with libc symbols,
run 35113288768. Step 5b adds about 20 points inside glibc:

- `_int_malloc` 29.6%;
- `cfree` 13.2%;
- `malloc_consolidate` 7.7%;
- `_int_free` 4.0%;
- `unlink_chunk` 2.4%.

The engine's own operators take the same share as before.

**Allocator swap** (run 35113288768, same runner, 3 interleaved rounds). The regression
disappears under jemalloc:

| query | glibc, pre-5b | glibc, 5b | ratio | jemalloc, pre-5b | jemalloc, 5b | ratio |
|---|---:|---:|---:|---:|---:|---:|
| BI-14 | 667 ms | 1,025 ms | 1.54× | 495 ms | 507 ms | 1.02× |
| BI-12 | 936 | 1,414 | 1.51× | 661 | 671 | 1.02× |
| BI-9 | 550 | 819 | 1.49× | 352 | 346 | 0.98× |

**glibc's heap after an SF1 load** (`mallinfo2`, run 35126988660):

| build | free chunks | free bytes | RSS |
|---|---:|---:|---:|
| pre-5b | 219,109 | 42 MB | 5,683 MB |
| main | 797,355 | 64 MB | 3,777 MB |

Inferred from the profile and these counts, not measured directly: the regression is
glibc searching a free list 3.6× as long on the allocations a query makes.

**What is not known.** Which load-time allocation pattern leaves those chunks. The
first hypothesis was the per-property key `String` the loaders allocate and, since
5b, free; it is **wrong**. Removing that churn (option A below) leaves the free-chunk
count at 797,700 and no query faster. The chunks follow the removal of the
per-relationship row maps themselves. A plausible reading, not yet shown, is that the
long-lived maps used to fill the holes the column store leaves as it grows.

### Constraints

- **Library users cannot be given an allocator.** The engine ships as a library too:
  the Python wheel (`sdk/python`, `cdylib`) and the Rust SDK (`crates/samyama-sdk`).
  A library must not set `#[global_allocator]` for its host process, so whatever this
  ADR decides reaches the **server binary** (`samyama`, and the Docker image built
  from it) and the CLI, not embedded users.
- **Shipped targets.**
  - Docker: `linux/amd64` and `linux/arm64`, on `debian:bookworm-slim` (glibc).
  - PyPI: manylinux_2_28 x86_64 wheels and a macOS wheel.
  - npm.
- **PERF-10 is an RSS target:** B/edge, measured at SF10. It is 207.1 on `531771f`
  against 128. An allocator's retained memory counts against it.
- **Benchmarks must measure what ships.** CH-REGRESS and the PERF-* numbers come from
  bench binaries (`ldbc_benchmark`, `ldbc_bi_benchmark`, `finbench_benchmark`,
  `memory_footprint`), not from the server. If the server's allocator changes and the
  benches' does not, every published latency and footprint describes a build nobody
  runs.

## Options and measurements

All runs are on GitHub `ubuntu-latest` runners (4 vCPU), main `e2d5604`, LDBC SF1.
Latency is the median of per-query medians over 3 interleaved rounds. Each ratio is
against that job's own glibc control. Two jobs ran on separate runners, so compare
ratios within a job, not absolute times across jobs.

| option | BI-14 | BI-12 | BI-9 | BI-2 | IC5 | IC9 | IS / IC7 / IC8 | RSS after load | glibc free chunks |
|---|---|---|---|---|---|---|---|---|---|
| glibc (job 1 control) | 991 ms | 1,351 | 754 | 5,633 | 413 | 284 | | 3,777 MB | 797,355 |
| **A** borrowed keys at load (no key `String` churn) | 1.01 | 0.99 | 1.06 | 1.01 | 0.99 | 0.98 | ≈1.00 | 3,775 | 797,700 |
| **B** `malloc_trim(0)` after load | 0.99 | 0.94 | 1.02 | 0.98 | 0.91 | 0.96 | ≈1.00 | 3,779 | 797,315 |
| **C** `GLIBC_TUNABLES=glibc.malloc.mxfast=0` | 0.71 | 0.80 | 0.99 | 0.92 | 0.95 | 0.88 | **1.4–2.0 slower** | 3,787 | 967,527 |
| glibc (job 2 control) | 981 ms | 1,311 | 823 | 5,660 | 355 | 304 | | 3,774 MB | 797,690 |
| **D** jemalloc | 0.50 | 0.47 | 0.38 | 0.69 | 0.70 | 0.76 | 0.5–0.9 | 3,616 (−4%) | |
| **E** mimalloc | 0.46 | 0.47 | 0.36 | 0.61 | 0.61 | 0.69 | 0.5–1.0 | 3,772–3,810 | |

Across the remaining BI and IC queries, D and E are faster on all but BI-19, IC1 and
IC13 (≈1.0) and BI-20 (1.04 under D).

**Write churn** (run 35127344928). SF1 loaded, then 10 minutes of `UNWIND 2000 …
CREATE` relationships with properties, `SET`, `DETACH DELETE`:

| allocator | RSS, load → 10 min | BI-14, load → during churn | batches in 10 min |
|---|---|---|---:|
| glibc | 3,882 → 3,885 MB | 736 → 758–896 ms, drifting up | 3,683 |
| C (`mxfast=0`) | 3,888 → 3,894 | 691 → up to 1,040 | 2,954 |
| D (jemalloc) | 3,700 → 3,724 | 604 → 498–634 | 4,606 |
| E (mimalloc) | 3,886 → 3,816 | 412 → 407–426, flat | 5,425 |

**Platforms** (run 35127548751). The server binary was built with a Cargo feature per
allocator, `#[global_allocator]` in `src/main.rs` only. The smoke test creates 1,000
relationships over HTTP, then checks `count` = 1000 and `sum` = 500500. All nine builds
pass.

| | binary MB (system / jemalloc / mimalloc) | idle RSS MB (system / jemalloc / mimalloc) |
|---|---|---|
| Linux amd64 | 35 / 36 / 35 | 28 / 46 / 55 |
| Linux arm64 | 31 / 32 / 32 | 24 / 27 / 51 |
| macOS arm64 | 28 / 29 / 28 | 21 / 24 / 23 |

Not covered: arm64 kernels with 16 K or 64 K pages. A jemalloc configured for 4 K pages
is known to fail on them.

**SF10 RSS, PERF-10** (Vultr voc-m-24c-192gb, one box, glibc repeated for noise):

All four runs used the same box, the same `memory_footprint` binary built from `e2d5604`,
and SF10: 29,987,835 nodes and 176,623,433 edges. jemalloc and mimalloc were loaded with
`LD_PRELOAD` from Ubuntu 24.04 packages (mimalloc 2.1).

| allocator | RSS B/edge (PERF-10) | RSS | live heap | load | `RETURN n`, 1,000 Person (p50 / p95) | `RETURN n`, 10,000 Post (p50 / p95) |
|---|---:|---:|---:|---:|---|---|
| glibc | 207.1 | 36,570,738,688 B | 31.47 GB | 592 s | 14.67 / 19.54 ms | 131.4 / 168.8 ms |
| glibc, repeated | 207.1 | 36,572,200,960 B | 31.47 GB | 555 s | 12.49 / 16.28 ms | 124.5 / 140.9 ms |
| jemalloc | **198.5** | 35,065,171,968 B | 31.47 GB | 497 s | 3.08 / 10.07 ms | **71.3 / 96.0 ms** |
| mimalloc | 199.3 | 35,197,067,264 B | 31.47 GB | **476 s** | **2.72 / 2.79 ms** | 87.7 / 120.3 ms |

- **Noise.** The two glibc runs agree on RSS to 0.004%, so the differences below are
  real.
- **Live heap.** It is identical under every allocator, because it counts what the
  engine holds, not what the allocator keeps.
- **Both alternatives lower PERF-10**, jemalloc by 4.2% and mimalloc by 3.8%, and cut
  load time by 16–20%.
- **mimalloc against jemalloc.** mimalloc's RSS is **0.4% above** jemalloc's.

### Eliminated

- **A** removes a churn that turned out not to be the cause. It is not worth its API.
- **B** does not change the free list and does not help.
- **C** helps some BI queries, makes point reads 1.4–2.0× slower, and degrades under
  write churn.

### Decision rule, fixed before the SF10 numbers were read

- **mimalloc (E)** leads on latency. Of 41 queries in the same job, it is more than 2%
  faster than jemalloc on 21, jemalloc is more than 2% faster on 6, and 14 are within 2%. It also has 18% more write throughput under churn, flat latency, and no
  page-size hazard.
- **jemalloc (D)** had 4% less RSS at SF1.
- PERF-10 is an RSS target. **Choose mimalloc unless its SF10 RSS exceeds jemalloc's
  by more than 5%** (about 10 B/edge at today's 207), **in which case choose jemalloc.**

## Decision

**The server binary and the benchmark binaries use mimalloc as the global allocator.**

Under the rule above, mimalloc's SF10 RSS is 0.4% above jemalloc's, well inside the 5%
threshold, so mimalloc is chosen.

**How:**
1. **Dependency.** `mimalloc` (MIT) with `default-features = false`.
2. **One definition.** A module in the library crate,
   `samyama::alloc::ShippedAllocator`, names the allocator but does **not** install it.
   A library never sets `#[global_allocator]`.
3. **Where it is installed.** `#[global_allocator] static GLOBAL: ShippedAllocator` in:
   - `src/main.rs`, the server and the Docker image;
   - the CLI;
   - every bench binary that produces a published or gated number: `ldbc_benchmark`,
     `ldbc_bi_benchmark`, `finbench_benchmark`, and `memory_footprint`, whose counting
     allocator wraps `ShippedAllocator` instead of `System`.
4. **Opt-out.** A Cargo feature, `system-allocator`, installs `std::alloc::System`
   instead, for anyone who has to build without mimalloc.

**Not adopted:** A, B and C, as above.

## Consequences

**Gains, measured.**
- **H1 condition 3.** Under mimalloc the step-5b regression is gone, as well as the
  pre-5b glibc baseline being beaten on SF1: BI-14 0.46×, BI-12 0.47×, BI-9 0.36× of
  glibc on main.
- **Faster across queries.** Of 41 SF1 queries, mimalloc is more than 2% faster than
  glibc on 38. BI-19 (1.00), IC1 (1.02) and IC13 (1.01) are unchanged.
- **Stable under writes.** Under continuous write churn, BI-14 stays flat (407–426 ms),
  and write throughput is 47% higher than on glibc.
- **PERF-10.** 207.1 → 199.3 B/edge at SF10.
- **Load time.** SF10 loads in 476 s against 592 s.

**Costs, measured.**
- **Idle memory.** About +27 MB idle RSS on Linux amd64 and arm64 (55 against 28 MB, and
  51 against 24 MB), and about +2 MB on macOS. This is a fixed per-process cost; per-edge
  RSS goes down.
- **Binary size.** Unchanged: within 1 MB on every target.

**Every published latency and footprint moves.** The bench binaries change allocator, so:
- **Comparisons across the change are not like for like.** A number measured before and
  one measured after differ by allocator as well as by code. Envelopes after this record
  the allocator in their notes.
- **CH-REGRESS.** Its baseline (`0eb6a55`, glibc) becomes a comparison across
  allocators. The gate would pass by the allocator's margin, not the code's. **The
  baseline must be re-seeded** deliberately on the fixed host once the nightly runs
  again. Until then, a CH-REGRESS pass after this change does not show the absence of a
  code regression.
- **Competitor comparisons.** SCORECARD figures against competitors are re-measured
  rather than carried over.

**Embedded users keep their host's allocator and the regression.** The Python wheel and
the Rust SDK load the engine as a library, so they still run on glibc, and on glibc
step 5b's BI slowdown stands: 1.5–1.7× on BI-9, BI-12 and BI-14. The fragmentation
pattern is not yet explained. The load-path follow-up is tracked separately:
- **Find the pattern.** Which allocations leave the ~800 K free chunks after load.
- **Fix it in the load path.** Candidates are pre-sizing the column store and compacting
  columns after bulk load.
- **Workaround until then.** Embedded Linux users can `LD_PRELOAD` mimalloc or jemalloc.

**Not covered by the evidence:**
- **Platforms.** 16 K and 64 K page arm64 kernels; the page-size hazard applies to
  jemalloc, not mimalloc.
- **Windows,** which is not shipped.
- **Loads over time.** Allocator behaviour under hours or days of mixed load. The churn
  test ran 10 minutes.

## Evidence

| what | run |
|---|---|
| CH-REGRESS failure | Vultr 2026-09-16, `531771f` vs `0eb6a55` |
| bisection | GitHub runs 35099052379, 35104224715 |
| profile | 35109413988, 35113288768 |
| options | 35126988660 |
| churn | 35127344928 |
| platforms | 35127548751 |
| SF10 | Vultr voc-m-24c-192gb, 2026-09-17, 4 runs on one box |

Experiment code is on the throwaway branches `adr038-alloc-experiment`, `adr038-churn`
and `adr038-platforms`, never merged. Issue #1267 carries the analysis thread,
including a retracted mechanism and a corrected code audit.
