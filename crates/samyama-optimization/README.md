# Samyama Optimization Engine (Rust)

A high-performance library implementing **metaphor-less** and **nature-inspired** metaheuristic optimization algorithms.

This engine allows you to solve complex resource allocation, scheduling, and engineering problems by defining an objective function and constraints.

## Reproducibility

Every solver accepts an optional seed. Without one, behaviour is unchanged (drawn from entropy);
with one, the run can be re-derived exactly — same best fitness, same solution vector, same
convergence history.

```rust
let result = DESolver::new(config).with_seed(4242).solve(&problem);
```

This matters for published results: a reported optimum that cannot be regenerated can only be
re-sampled, which is not the same claim.

The parallel solvers are the part worth understanding. Seeding only the top-level RNG and letting
rayon workers draw from `thread_rng()` would give runs that *look* reproducible in a
single-threaded test and are not — the seed would be recorded next to results it cannot
regenerate. Each element's stream is instead derived from `(seed, iteration, index)`, so the
answer does not depend on how work is scheduled. `tests/test_reproducibility.rs` asserts this
directly: the same seed under a 1-thread pool and an 8-thread pool must give bit-identical results.

## Algorithms Supported

We support 15+ algorithms across various families:

### Metaphor-less
- **Jaya**: Parameter-less optimization (Toward best, away from worst).
- **Rao (1, 2, 3)**: Algorithms using best, worst, and mean solutions with varying interaction levels.
- **TLBO**: Teaching-Learning-Based Optimization.
- **ITLBO**: Improved TLBO with Elitism.
- **BMR / BWR**: Best-Mean-Random and Best-Worst-Random strategies.
- **QOJaya**: Quasi-Oppositional Jaya (using Opposition-Based Learning).

### Nature-Inspired
- **GWO**: Grey Wolf Optimizer (Alpha, Beta, Delta hierarchy).
- **PSO**: Particle Swarm Optimization.
- **DE**: Differential Evolution.
- **L-SHADE**: Differential Evolution that adapts its own F and CR from a success history and shrinks its population linearly (Tanabe & Fukunaga, CEC 2014). Not parallel: one generation runs sequentially.
- **CMA-ES**: Covariance Matrix Adaptation Evolution Strategy (Hansen & Ostermeier, 2001): samples from a multivariate normal and learns its covariance and step size from successful steps, so badly scaled and rotated problems become round to the search. `O(n^3)` per generation: meant for tens to a few hundred variables.
- **Firefly**: Firefly Algorithm (Light intensity based attraction).
- **Cuckoo**: Cuckoo Search (Levy flights and nest abandonment).
- **Bat**: Bat Algorithm (Echolocation).
- **ABC**: Artificial Bee Colony.
- **FPA**: Flower Pollination Algorithm.
- **GA**: Genetic Algorithm (Tournament selection, Uniform crossover).

### Stochastic / Physics
- **SA**: Simulated Annealing.
- **HS**: Harmony Search.
- **GSA**: Gravitational Search Algorithm.

### Multi-Objective (Pareto)
- **NSGA-II**: Non-dominated Sorting Genetic Algorithm II.
- **MOTLBO**: Multi-Objective TLBO.
- **SPEA2**: Strength Pareto Evolutionary Algorithm 2 (Zitzler, Laumanns & Thiele, 2001): strength-based fitness, a k-nearest-neighbour density, and an archive truncated by crowding.
- **MOEA/D**: Multi-objective EA based on Decomposition (Zhang & Li, 2007): one Tchebycheff sub-problem per weight vector, each solved with its neighbours.

## Features
- **Parallel Evaluation**: Automatic multi-threaded fitness calculation via `rayon`.
- **Zero-Copy**: Minimal overhead when operating on large vectors.
- **Constraints**: three selectable strategies (`penalty`, `repair`, `feasibility`) for `min_total` and `budget`; see "Constraint handling" below.
- **History Tracking**: Solvers yield convergence history for visualization.

## What a problem is here, and why there is no MPS/LP export

The problem model is a **black box**. `Problem` (`src/common.rs`) is an objective
function from a variable vector to a scalar, an optional scalar penalty, a dimension, and
box bounds. That is all a solver sees. Every solver in this crate is a population
metaheuristic that samples that function. Every implementor has the same shape, including
the engine's `GraphOptimizationProblem` (`src/query/executor/operator.rs`) and
`CypherProblem` (`src/optimization/cypher_problem.rs`), whose objective runs a Cypher query.

MPS and LP are file formats for a linear or mixed-integer program. They hold an objective
coefficient row, a constraint matrix, constraint senses, right-hand sides and integrality
markers. **None of that exists in this crate, and none of it can be recovered from a
function.** So the missing part of MPS/LP export (requirement OPT-14) is not a serializer.
It is a **structured linear/MILP problem representation**, and this crate does not have
one. An MPS or LP writer depends on that representation. Writing one over the black-box
trait would mean inventing a linear model that is not the problem being solved, so the
exported file would lead to a different optimum.

What that work would involve, none of which exists today:

- a linear model type (objective coefficients, constraint matrix, senses, right-hand
  sides, bounds, integrality) alongside the black-box `Problem` trait;
- a way for callers, including the Cypher `algo.or.solve` path, to build one (today
  constraints become a penalty term);
- MPS/LP writers over that type, and importing results back;
- a decision about problems with no linear form. That is most of the problems here: the
  objectives call the database, which is why this crate uses metaheuristics at all.

Until that representation exists, OPT-14 is blocked on the model, not on a file writer.

## Usage

### Single-Objective (Rust)
```rust
use samyama_optimization::algorithms::*;
use samyama_optimization::common::*;
use ndarray::array;

let problem = SimpleProblem {
    objective_func: |x| x.iter().map(|&v| v * v).sum(), // Sphere function
    dim: 2,
    lower: array![-10.0, -10.0],
    upper: array![10.0, 10.0],
};

// Use Grey Wolf Optimizer
let config = SolverConfig { population_size: 50, max_iterations: 100 };
let solver = GWOSolver::new(config);
let result = solver.solve(&problem);

println!("Best: {:?}", result.best_variables);
println!("Fitness: {}", result.best_fitness);
```

### Multi-Objective (Rust)
```rust
use samyama_optimization::algorithms::NSGA2Solver;

// Define a struct impl MultiObjectiveProblem...
// Then:
let solver = NSGA2Solver::new(config);
let result = solver.solve(&mo_problem);

for ind in result.pareto_front {
    println!("Pareto Solution: {:?} -> Fitness: {:?}", ind.variables, ind.fitness);
}
```

## Integration with Samyama DB
You can access these solvers via Cypher!

```cypher graph=factories
CALL algo.or.solve({
  algorithm: 'GWO',
  label: 'Factory',
  property: 'production',
  min: 0.0, max: 100.0,
  cost_property: 'cost',
  budget: 50000.0
})
```

### Modelling a problem in Cypher (OPT-07)

A problem is stated in one `CALL`: its config map is the whole modelling
language `algo.or.solve` reads (OPT-07, #2264). Nothing is declared outside
the call, and every part of the model is read from the graph when it runs:

| Part of the model | Written as | What it means on the graph |
|---|---|---|
| Decision variables | `label: 'Item'` | One continuous variable per node with that label. |
| Domain | `lowerBound`, `upperBound` | The same interval for every variable. |
| Objective | `costProperty: 'cost'` | Minimise Σ value × cost, each node's cost from its own property. |
| Several objectives | `costProperties: ['cost', 'risk']` | One Σ value × cost per property, solved as a Pareto front. |
| Constraints | `budget`, `minTotal` | Σ value × cost ≤ `budget`; Σ value ≥ `minTotal`. |
| How constraints act | `constraintHandling` | `penalty`, `repair` or `feasibility` (below). |
| The solution | `property: 'qty'` | Written to that property on every variable's node, all or nothing (below). |
| How good it is | `YIELD objective, feasible, gap, ...` | The result envelope (below). |

```cypher graph=items
CALL algo.or.solve({label: 'Item', property: 'qty', costProperty: 'cost',
                    upperBound: 5, minTotal: 6, budget: 30, seed: 1})
YIELD objective, feasible
RETURN objective, feasible
```

That call reads as: choose a quantity between 0 and 5 for each `:Item`,
minimising what they cost in total, buying at least 6 in all and spending at
most 30. The chosen quantities land on `qty`; `MATCH (i:Item) RETURN i.name,
i.qty` reads the plan back.

**Costs are numbers, whole or not.** `{cost: 3}` and `{cost: 3.0}` are the
same cost. A node without the cost property costs `1.0`. Any other value (a
string, a list) is refused, naming the property and the node, and nothing is
written. Before #2264 a whole-number cost was read as `1.0`: a graph written
as `CREATE (:Item {cost: 3})` was solved as a different problem, without an
error (`tests/or_solve_reads_whole_number_costs.rs`).

**What the language cannot express yet.** Each of these is a different
problem from the one above, and none is approximated by it:

- variables chosen by a pattern or a `WHERE`: it is every node of one label;
- an objective that is not linear in the node values, or that reads
  relationships or paths (the graph-native templates below are the exception,
  one procedure per problem);
- any constraint beyond `budget` and `minTotal`, and constraints written as
  Cypher predicates over each candidate (OPT-07's H2 target);
- writing the solution to relationships: it is a node property;
- integer or binary variables: every variable is continuous.

From Rust, `samyama::optimization::CypherProblem` takes an objective that is
any Cypher query over `$x0 … $xN` (`examples/cypher_problem_demo.rs`); that is
not reachable from a Cypher `CALL`.

The keys in full:

| Key | Meaning | Default |
|---|---|---|
| `label` | Every node with this label is one decision variable. | required |
| `property` | The node property the solution is written to. | required |
| `lowerBound` / `min`, `upperBound` / `max` | Every variable lies in `[lowerBound, upperBound]`. `lowerBound` must be less than `upperBound`; equal or inverted bounds are refused. | `0.0`, `100.0` |
| `costProperty` / `cost_property` | The objective is **minimise Σ value × cost**, reading each node's cost from this property, integer or float (`1.0` where a node has none). | every cost `1.0` |
| `costProperties` / `cost_properties` | Two or more cost properties make it multi-objective: one Σ value × cost per property, solved as a Pareto front. | — |
| `budget` | Constraint Σ value × cost ≤ `budget`. | none |
| `minTotal` / `min_total` | Constraint Σ value ≥ `minTotal`. | none |
| `algorithm` | Any solver in `algo.or.solve`'s list; an unknown name is refused, not replaced. | `Jaya` |
| `populationSize`, `maxIterations` | Solver settings. | `50`, `100` |
| `seed` | A non-negative integer; the same call on the same graph then gives the same answer. | none (a fresh draw each call) |
| `bestKnown` | A reference objective to report `gap` against (single-objective only). | none |
| `timeBudgetMs` / `time_budget_ms` | Stop once this many milliseconds have passed since the first evaluation, with the best answer so far. A whole number of at least 1. | no limit |
| `maxEvaluations` / `max_evaluations` | Stop once the objective has been evaluated this many times. A whole number of at least 1. | no limit |

`budget` limits the answer's cost; `timeBudgetMs` and `maxEvaluations` limit the
run (OPT-11). Both run limits are checked between iterations, so a run can
overshoot by up to one iteration's evaluations. The problem has one variable per
node, so it is as large as the label: 10,000 nodes is a 10,000-variable solve.

How the constraints act on the search is chosen with `constraintHandling`
(next section). Under the default for a single-objective solve, `penalty`, they
are not hard limits: a solver can return a point that breaks them, and
`feasible` says whether it did. `budget` is measured with the first cost
property. A label with no nodes returns no rows and writes nothing.

### Constraint handling

`constraintHandling` (OPT-10, #2266) chooses what a solver does with a
candidate that breaks `budget` or `minTotal`. Every solver takes all three; the
strategy wraps the problem (`samyama_optimization::constraints::Constrained`),
not the solver.

| Value | What the solver sees | Default for |
|---|---|---|
| `penalty` | objective plus penalty (budget overrun squared, minimum shortfall squared × 100) | single-objective solvers |
| `repair` | the candidate repaired first, then scored; the repaired point is the one written back | -- |
| `feasibility` | Deb's rules: any feasible point beats any infeasible one, a smaller violation beats a larger | multi-objective solvers |

```cypher graph=items
CALL algo.or.solve({label: 'Item', property: 'qty', costProperty: 'cost',
                    minTotal: 6, budget: 10, constraintHandling: 'repair'})
YIELD objective, feasible
RETURN objective, feasible
```

- **The penalty is inexact.** A quadratic penalty is smaller than the saving
  near the boundary, so the penalised optimum can sit just outside a
  constraint and come back `feasible: false`. `repair` and `feasibility` do not
  trade a violation for objective that way
  (`tests/constraint_handling_strategies.rs` shows the three disagreeing).
- **The repair** is greedy and deterministic: clamp to the bounds, raise the
  cheapest variables to reach `minTotal`, lower the most expensive to meet
  `budget`, then shift quantity from expensive to cheap. It aims a hair (1e-9
  relative) inside each limit, because `feasible` is strict. When the
  constraints cannot all be met it gets as close as those steps do, and the
  answer says `feasible: false`.
- **Feasibility rules are not feasibility-preserving operators.** OPT-10 names
  operators that cannot leave the feasible region; that would mean changing
  every solver's crossover and mutation. Deb's rules give the guarantee that
  matters for the answer -- a feasible point, once found, is never displaced
  by an infeasible one -- without touching a solver. For a single-objective
  solve they are encoded in the fitness (an infeasible point scores `1e12`
  plus its violation), which assumes feasible objectives stay below `1e12`.
- **Multi-objective solves now see the constraints.** Before, `budget` and
  `minTotal` were checked only after a multi-objective run, to report
  `feasible`; the solvers searched without them.
- `fitness` is what the solver minimised under the chosen strategy, so under
  `feasibility` an infeasible answer's fitness is `1e12` plus its violation;
  `objective` is always the plan's own objective.

`/optimize/solve` takes the same choice as `constraint_handling`. Of its
benchmarks only `uc2_dosing` is constrained; its repair drops the smaller dose
of any contraindicated pair given together. `repair` on a benchmark with no
repair (`sphere`, say) fails the solve with that reason rather than running a
penalty.

### Writing the solution back

The solution is written to `property` on every node of `label`. For a
multi-objective solve that is the first point of the Pareto front.

**It is written whole or not at all.** Each value is an ordinary property
write, so constraints, indexes and the write log apply. If the store refuses
any one of them (a `UNIQUE` constraint, a quota), the call fails with the
store's message and the whole statement is undone: no node keeps a value from
the failed solve (#1593, #1637). `tests/or_solve_write_back_is_atomic.rs`
checks this by making a write fail part-way, for both a single- and a
multi-objective solve (OPT-07, #1773).

### What a solve returns

Every row reports how good the answer is, not only the answer (OPT-09):

| Column | Meaning |
|---|---|
| `fitness` | What the solver minimised under the constraint strategy: objective plus penalty under `penalty`. A list for a multi-objective solve. |
| `objective` | The objective alone, without the penalty. Equal to `fitness` when the answer is feasible. |
| `feasible` | `true` when the answer meets every constraint (`budget`, `minTotal`). Strict: any violation is `false`. |
| `gap` | `objective - bestKnown`, in the objective's units; negative means the answer beat it. `null` unless you pass `bestKnown`, because no solver here can prove a bound. |
| `iterations` / `max_iterations` | Iterations run, and the cap you set. |
| `total_time_ms` | Wall-clock time of the solve alone, in milliseconds. |
| `evaluations` / `stopped_by` | Objective evaluations made, and why the run ended: `iterations` (it ran them all), `time_budget` or `evaluation_budget`. |
| `algorithm`, `history` | The solver, and its best fitness per iteration. Multi-objective solves return `front_size` in place of `history`. |

`bestKnown` (or `best_known`) must be a finite number, and applies to
single-objective solves only: a Pareto front has no single best known value,
so passing it there is an error rather than a `gap` that is silently `null`.

```cypher graph=factories
CALL algo.or.solve({label: 'Factory', property: 'production', cost_property: 'cost',
                    budget: 50000.0, bestKnown: 41000.0})
YIELD objective, feasible, gap, total_time_ms
RETURN objective, feasible, gap, total_time_ms
```

`/optimize/solve` reports `feasible` and `gap` in its `done` event as well:
`gap` against the benchmark's known optimum, and both `null` for a
multi-objective benchmark (see #1795 for why feasibility cannot be read there yet).

### Graph-native templates (OPT-06)

`algo.or.solve` optimises numbers stored on nodes. Some problems recur often
enough to have their own procedure, which states the problem in graph terms
and solves it exactly. The first one (#1770):

**`algo.or.constrainedShortestPath(source, target, {config})`**, also callable
as `algo.or.shortestPathWithConstraints`: the cheapest path from `source` to
`target` whose `resourceProperty` adds up to at most `maxResource`. For
example, the cheapest route whose travel time stays within two hours.

```cypher
CALL algo.or.constrainedShortestPath($source, $target,
     {costProperty: 'cost', resourceProperty: 'minutes', maxResource: 120, edgeType: 'ROAD'})
YIELD path, cost, resource
RETURN path, cost, resource
```

`source` and `target` are node ids, as for `algo.weightedPath`: numbers or
parameters, not expressions such as `id(n)`. The config is a map of values,
or the whole map as one parameter (`$config`); a parameter *inside* the map is
not read.

| Key | Meaning | Default |
|---|---|---|
| `resourceProperty` | The relationship property the limit is on. | required |
| `maxResource` | The most the resource may add up to (`<=`). | required |
| `costProperty` | The relationship property to minimise. | 1 per relationship (fewest hops) |
| `label`, `edgeType` | Only these nodes and relationships. | all |

It returns one row: `path` (node ids), `cost` and `resource`. When no path
is within the limit, it returns no row. A negative cost or resource is
refused, and so is a graph projection, which keeps only one weight.

It is exact. The search keeps, at each node, only the partial paths that no
other partial path there beats on both cost and resource, and expands them
cheapest first. Its answers are checked against every simple path on 2,000
random graphs (`constrained_path.rs`). The problem is NP-hard in general, so a
large graph can take long. The query deadline stops it.

### Measuring a multi-objective front

A multi-objective solve returns a front of trade-offs, not one answer, so its
quality is measured against the problem's known Pareto front (OPT-03).
`moo::quality_metrics(front, reference_front)` computes all five:

| Measure | Function | Better | What it says |
|---|---|---|---|
| Hypervolume | `moo::hypervolume` | larger | Volume the front dominates, up to a reference point past the known front's worst values. Exact, any number of objectives. |
| IGD | `moo::igd` | smaller | Mean distance from each known-front point to the nearest front point. |
| IGD+ | `moo::igd_plus` | smaller | Like IGD, but a point is never charged for beating the known front. |
| Spread | `moo::spread` | smaller | Generalised Δ: how evenly the front is spaced, and whether it reaches each extreme. 0 is perfectly even. |
| Coverage | `moo::coverage(known, front)` | smaller | Share of the front's points the known front weakly dominates, so that are provably not optimal. |

The hypervolume reference point is the known front's worst value in each
objective, pushed out by `moo::HV_REFERENCE_MARGIN` (10%) of its range.

**Known fronts** come from the problems themselves: `ZDT::reference_front` and
`DTLZ::reference_front` evaluate the problem's own objectives on a grid of its
position variables, with its distance variables at the value where it is
optimal, and keep the non-dominated points. There is no copied table to drift
from the formula. The tests check every point against each front's equation
(ZDT1/4: f2 = 1 − √f1; ZDT2/6: f2 = 1 − f1²; DTLZ1: Σf = ½; DTLZ2–6: Σf² = 1).

`/optimize/solve` reports them in its `done` event as `quality`, for every
benchmark with a known front: ZDT1–4, ZDT6 and DTLZ1–7. It is `null` for
`uc2_dosing`, which has no known front, and for single-objective runs. ZDT5 is
binary-coded and is not offered. The measures are computed after the solve's
clock stops, so `total_time_ms` is still the solve alone.

## License
Apache-2.0