# ADR-026: Rao-Family Optimization Crate (`samyama-optimization`)

## Status
**Shipped** (v1.0.0, 2026-04-11) — retroactively documenting the production-Rust-first decision (2026-04-21).

## Date
2026-05-05

## Context

Several customer use-cases — supply-chain dosing (UC2), hospital capacity planning (UC3), KG-completion scoring (UC4), agentic routing (UC5) — frame their problem as parameter optimisation against a Cypher-defined fitness function. The "right" optimisation algorithm for these workloads is parameter-free, metaphor-free, and converges quickly without expert tuning.

The Rao family (TLBO → Jaya → Rao-1/2/3 → BMR/BWR/BMWR + multi-objective extensions) is a deliberate, well-published family with these properties. The Python `rao-algorithms` package (v0.9.3, in `optimization_algorithms/`) was the original delivery vehicle; the production decision (2026-04-21, project_optimization_workstream.md) was Rust-first.

The integration point is **Cypher-driven fitness**: a candidate parameter vector is bound into a Cypher query, the engine returns a scalar, and the optimiser uses that as the fitness. The optimisation is therefore in-engine, not a separate service.

## Decision

We will ship a Rust crate `samyama-optimization` (in `samyama-graph/crates/samyama-optimization/`) with:

- **Rao-family algorithms**: TLBO, ITLBO, MOTLBO, GOTLBO, Jaya, Rao-1/2/3, BMR, BWR, BMWR, MO-BMWR, MO-Rao-DE, SAMP-Jaya, SAPHR, EHRJAYA, QO-Rao, QO-Jaya.
- **Comparison baselines** (15+): GA, DE, PSO, GSA, GWO, ABC, HS, SA, Firefly, Cuckoo, FPA, Bat, NSGA-II.
- **Cypher-driven fitness integration**: the optimiser drives the engine via the executor; each evaluation is a Cypher round-trip.
- **Python wrapper** maintained in lockstep (the `rao-algorithms` PyPI package).

Algorithm-set decision (2026-04-21): all algorithms ship in production. We do not ship a curated subset; the breadth is the differentiator.

Cypher-driven fitness contract: the optimiser sees a `Fn(&[f64]) -> f64` (or `&[f64] -> Vec<f64>` for multi-objective). Behind that closure is the engine, but the optimiser does not know that — the integration is at the closure level, which keeps the crate engine-agnostic.

## Consequences

### Positive
- Rust-first for production paths; Python wrapper for non-Rust callers, no implementation drift.
- Comprehensive algorithm catalogue defends "Rao-family is competitive" claim with head-to-head benchmarks.
- Customer use-cases UC2, UC3, UC4, UC5 share one optimisation infrastructure.
- Engine-agnostic API: the crate could be lifted to a non-graph context.

### Negative
- Cypher-driven fitness evaluation cost dominates total optimisation time (1 K – 10 K evaluations × 100 ms/query = 17 minutes per optimisation).
- No fitness caching, no batched fitness, no surrogate models. Each is filed.
- No constraint handling beyond bounds (we use penalty functions; Augmented Lagrangian not built).
- Algorithm selection is manual; no auto-selector.
- Population evaluation is serial; trivially parallelisable but not done.
- Multi-objective surface (MO-*) is shallower than single-objective.
- Benchmarks are internal; no third-party comparison.

### Neutral
- The crate compiles standalone (no engine dependency). Engine integration is a thin shim.

## Alternatives Considered

| Option | Rejected because |
|--------|------------------|
| Bayesian optimisation (BoTorch / GP) | Strong sample efficiency, much higher implementation cost; filed. |
| Gradient-based (Adam, L-BFGS over numerical gradient) | Cypher fitness is non-differentiable; numerical gradients noisy + expensive. |
| Reinforcement learning | Different problem framing. |
| CMA-ES | Reasonable baseline; not implemented yet. Filed. |
| Just expose `scipy.optimize` to Python users | Loses Rust-native production story; loses metaphor-free pitch. |

## Follow-ups

1. **Parallel population evaluation** — trivial 4–32× speedup on multi-core.
2. **Memoised fitness** on integer-valued parameter vectors.
3. **Batched Cypher fitness** — one query yields K candidates' scores when expressible.
4. **Surrogate models** (Gaussian Process) for expensive fitness.
5. **Augmented Lagrangian constraint handling**.
6. **Algorithm auto-selector** based on problem characteristics.
7. **Independent third-party benchmark suite** to defuse "self-graded" critique.

## References

- Code: `samyama-graph/crates/samyama-optimization/src/algorithms/` (28+ files, one per algorithm).
- Python: `optimization_algorithms/` (PyPI `rao-algorithms` v0.9.3).
- Demos: `optimization/` directory.
- Wiki: [[compute-rao-optimization.md]], [[concepts/rao-algorithm-family.md]], [[concepts/cypher-driven-fitness.md]], [[topics/optimization-algorithms.md]], [[project_optimization_workstream.md]], [[topics/sge-optimization-uc-results.md]].
