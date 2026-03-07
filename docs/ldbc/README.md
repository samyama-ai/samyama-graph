# LDBC Benchmark Results — Samyama v0.6.0

Samyama's query engine benchmarked against all four [LDBC Council](https://ldbcouncil.org/) benchmark suites.

**Test Environment:** Mac Mini M4 (10-core, 16GB RAM), macOS Tahoe 26.2, Rust 1.85 release build

**Date:** 2026-03-07

## Summary

| Benchmark | Queries | Passed | Pass Rate | Dataset | Total Time |
|-----------|---------|--------|-----------|---------|------------|
| [SNB Interactive](./SNB_INTERACTIVE.md) | 21 reads + 8 inserts + 8 deletes | 21/21 reads | **100%** | SF1 (3.18M nodes, 17.26M edges) | 111.9s |
| [SNB Business Intelligence](./SNB_BI.md) | 20 | 16/16 run (BI-17+ timeout) | **100% of run** | SF1 (same dataset) | ~52s (16 queries) |
| [Graphalytics](./GRAPHALYTICS.md) | 28 (6 algos x 5 datasets) | 28/28 | **100%** | XS (2) + S-size (3) datasets | <1ms (XS), 0.1–167s (S) |
| [FinBench](./FINBENCH.md) | 40 (12 CR + 6 SR + 3 RW + 19 W) | 40/40 | **100%** | Synthetic (7.7K nodes, 42.2K edges) | 665ms |

### Overall Coverage

- **4 LDBC benchmark suites** implemented
- **82 unique query/algorithm implementations** across all suites (including 8 deletes)
- **Graphalytics 28/28 passing** — XS 12/12 + S-size 16/16 (PageRank exact iterations, dangling redistribution, directed LCC)
- **S-size datasets validated**: wiki-Talk (2.4M V), cit-Patents (3.8M V), datagen-7_5-fb (633K V, 68M E)
- **WITH projection barrier** implemented for BI query support
- **GPU acceleration** (Enterprise) for PageRank, LCC, CDLP, WCC, BFS

## Quick Start

```bash
# SNB Interactive (21 read queries, SF1 dataset required)
cargo bench --release --bench ldbc_benchmark -- --runs 3

# SNB Interactive with update + delete operations
cargo bench --release --bench ldbc_benchmark -- --runs 3 --updates --deletes

# SNB Business Intelligence (20 analytical queries, 120s timeout per query)
cargo bench --release --bench ldbc_bi_benchmark -- --runs 3

# Graphalytics (6 algorithms, XS datasets)
bash scripts/download_graphalytics.sh
cargo bench --release --bench graphalytics_benchmark -- --all

# Graphalytics (S-size datasets)
bash scripts/download_graphalytics.sh --size S
cargo bench --release --bench graphalytics_benchmark -- --size S --all

# FinBench (21 queries, synthetic data auto-generated)
cargo bench --release --bench finbench_benchmark -- --runs 3
```

## Detailed Reports

- [SNB Interactive — Full Results](./SNB_INTERACTIVE.md)
- [SNB Business Intelligence — Full Results](./SNB_BI.md)
- [Graphalytics — Full Results](./GRAPHALYTICS.md)
- [FinBench — Full Results](./FINBENCH.md)

## Benchmark Architecture

All benchmarks share a common pattern:

1. **Load** dataset into in-memory `GraphStore` via `EmbeddedClient`
2. **Warm up** each query once (populates AST cache)
3. **Benchmark** each query N times, recording min/median/max latency
4. **Report** results in a formatted table

Data loading uses the shared `ldbc_common` module for CSV parsing and graph population. Queries use `query_readonly()` (read lock) for reads and `query()` (write lock) for updates.
