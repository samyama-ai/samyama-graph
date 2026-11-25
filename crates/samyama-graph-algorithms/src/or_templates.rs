//! Graph-native optimisation templates (OPT-06, #1770).
//!
//! Five recurring problems, each stated over plain data the engine reads from
//! the graph, and each solved here:
//!
//! | Template | Problem | Method | Optimal? |
//! |---|---|---|---|
//! | [`p_median`] | open `p` facilities, minimising demand-weighted distance to customers | every choice of `p` when there are few, else greedy then swaps | proven when enumerated |
//! | [`set_cover`] | the cheapest sets that together cover every element | branch and bound, greedy to start | proven when the search finishes |
//! | [`max_coverage`] | the sets within a budget that cover the most element weight | branch and bound, greedy to start | proven when the search finishes |
//! | [`assignment`] | match workers to tasks at least total cost | Hungarian algorithm | always |
//! | [`min_cost_flow`] | ship every supply to a demand at least cost within capacities | successive shortest paths | always |
//!
//! The first three are NP-hard. Each runs an exact method while it can -- up
//! to a stated amount of work -- and says whether it finished. A result that
//! did not finish is the best found, not a guess at the optimum, and its
//! `exact` flag is false: the caller reports a gap of "unknown" rather than 0.
//!
//! Every solver takes `interrupted`, asked as the work goes, and stops with
//! [`TemplateError::Interrupted`] when it answers true.

use std::cmp::Ordering;
use std::collections::BinaryHeap;

/// Why a template could not answer.
#[derive(Debug, Clone, PartialEq)]
pub enum TemplateError {
    /// `interrupted` answered true.
    Interrupted,
    /// A value that must not be negative (a distance, a cost, a demand, a
    /// capacity, a weight) was, or was NaN. Named, with the value.
    Negative { what: &'static str, value: f64 },
    /// The instance is larger than this method holds in memory.
    TooLarge {
        what: &'static str,
        size: usize,
        max: usize,
    },
    /// Anything else wrong with the instance.
    Invalid(String),
}

impl std::fmt::Display for TemplateError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TemplateError::Interrupted => write!(f, "interrupted"),
            TemplateError::Negative { what, value } => {
                write!(f, "a {what} of {value}: it must be a number >= 0")
            }
            TemplateError::TooLarge { what, size, max } => {
                write!(
                    f,
                    "{size} {what} is more than the {max} this template holds"
                )
            }
            TemplateError::Invalid(why) => write!(f, "{why}"),
        }
    }
}

impl std::error::Error for TemplateError {}

fn non_negative(what: &'static str, values: &[f64]) -> Result<(), TemplateError> {
    match values.iter().find(|v| v.is_nan() || **v < 0.0) {
        Some(v) => Err(TemplateError::Negative { what, value: *v }),
        None => Ok(()),
    }
}

/// How often, in units of work, `interrupted` is asked.
const CHECK_EVERY: u64 = 1024;

// ─────────────────────────────────────────────────────── facility location

/// Most choices of `p` facilities [`p_median`] enumerates. Above this it
/// searches heuristically and says so.
pub const P_MEDIAN_EXACT_LIMIT: u64 = 200_000;

/// Most rounds of swaps the heuristic makes.
pub const P_MEDIAN_SWAP_ROUNDS: usize = 50;

/// A p-median instance. `distance[f][c]` is the distance from candidate
/// facility `f` to customer `c`, `f64::INFINITY` when it cannot reach it.
#[derive(Debug, Clone, PartialEq)]
pub struct FacilityProblem {
    pub distance: Vec<Vec<f64>>,
    pub demand: Vec<f64>,
    pub p: usize,
}

/// A p-median answer.
#[derive(Debug, Clone, PartialEq)]
pub struct FacilitySolution {
    /// Indices of the open facilities, ascending.
    pub open: Vec<usize>,
    /// For each customer, the open facility serving it: the nearest, the
    /// lower index on a tie. `None` when no open facility reaches it.
    pub serving: Vec<Option<usize>>,
    /// Demand-weighted distance over the customers that are served.
    pub cost: f64,
    /// Customers with demand that no open facility reaches.
    pub unserved: usize,
    /// Proven optimal: every choice of `p` was tried.
    pub exact: bool,
    /// Choices evaluated.
    pub iterations: u64,
}

impl FacilitySolution {
    /// Every customer with demand is served.
    pub fn feasible(&self) -> bool {
        self.unserved == 0
    }
}

/// Open `p` of the candidate facilities so that the demand-weighted distance
/// from each customer to its nearest open facility is least.
///
/// Fewer customers left unreached always beats a lower cost: a choice that
/// serves everyone is preferred to a cheaper one that strands a customer. A
/// customer with zero demand never counts as stranded.
pub fn p_median(
    problem: &FacilityProblem,
    interrupted: &dyn Fn() -> bool,
) -> Result<FacilitySolution, TemplateError> {
    let m = problem.distance.len();
    let n = problem.demand.len();
    if problem.p == 0 {
        return Err(TemplateError::Invalid("p must be at least 1".to_string()));
    }
    if m == 0 {
        return Err(TemplateError::Invalid(
            "there are no candidate facilities".to_string(),
        ));
    }
    if let Some(row) = problem.distance.iter().find(|r| r.len() != n) {
        return Err(TemplateError::Invalid(format!(
            "a facility has {} distances for {n} customers",
            row.len()
        )));
    }
    non_negative("demand", &problem.demand)?;
    for row in &problem.distance {
        // Infinity is "unreachable", not an error.
        if let Some(v) = row.iter().find(|v| v.is_nan() || **v < 0.0) {
            return Err(TemplateError::Negative {
                what: "distance",
                value: *v,
            });
        }
    }
    let p = problem.p.min(m);
    let mut work = 0u64;
    let mut tick = || -> Result<(), TemplateError> {
        work += 1;
        if work.is_multiple_of(CHECK_EVERY) && interrupted() {
            return Err(TemplateError::Interrupted);
        }
        Ok(())
    };

    let choices = binomial(m as u64, p as u64);
    let (open, exact) = if choices <= P_MEDIAN_EXACT_LIMIT {
        // Every choice, in lexicographic order: the first best one wins.
        let mut best: Option<((usize, f64), Vec<usize>)> = None;
        let mut pick: Vec<usize> = (0..p).collect();
        loop {
            tick()?;
            let score = evaluate(problem, &pick);
            if best.as_ref().is_none_or(|(b, _)| better(score, *b)) {
                best = Some((score, pick.clone()));
            }
            if !next_combination(&mut pick, m) {
                break;
            }
        }
        (best.map(|(_, o)| o).unwrap_or_default(), true)
    } else {
        // Greedy: add the facility that lowers the score most, p times.
        let mut open: Vec<usize> = Vec::with_capacity(p);
        while open.len() < p {
            let mut best: Option<((usize, f64), usize)> = None;
            for f in (0..m).filter(|f| !open.contains(f)) {
                tick()?;
                let mut trial = open.clone();
                trial.push(f);
                let s = evaluate(problem, &trial);
                if best.as_ref().is_none_or(|(b, _)| better(s, *b)) {
                    best = Some((s, f));
                }
            }
            open.push(best.expect("a facility is left to add").1);
        }
        // Swaps: replace an open facility with a closed one while that helps.
        let mut score = evaluate(problem, &open);
        for _ in 0..P_MEDIAN_SWAP_ROUNDS {
            let mut improved = false;
            for i in 0..open.len() {
                for f in 0..m {
                    if open.contains(&f) {
                        continue;
                    }
                    tick()?;
                    let mut trial = open.clone();
                    trial[i] = f;
                    let s = evaluate(problem, &trial);
                    if better(s, score) {
                        score = s;
                        open = trial;
                        improved = true;
                    }
                }
            }
            if !improved {
                break;
            }
        }
        (open, false)
    };
    let mut open = open;
    open.sort_unstable();
    let (unserved, cost) = evaluate(problem, &open);
    let serving = (0..n)
        .map(|c| {
            open.iter()
                .copied()
                .filter(|&f| problem.distance[f][c].is_finite())
                .min_by(|&a, &b| {
                    problem.distance[a][c]
                        .total_cmp(&problem.distance[b][c])
                        .then(a.cmp(&b))
                })
        })
        .collect();
    Ok(FacilitySolution {
        open,
        serving,
        cost,
        unserved,
        exact,
        iterations: work,
    })
}

/// (customers with demand left unreached, demand-weighted distance).
fn evaluate(problem: &FacilityProblem, open: &[usize]) -> (usize, f64) {
    let mut unserved = 0;
    let mut cost = 0.0;
    for (c, &d) in problem.demand.iter().enumerate() {
        let nearest = open
            .iter()
            .map(|&f| problem.distance[f][c])
            .fold(f64::INFINITY, f64::min);
        if nearest.is_finite() {
            cost += d * nearest;
        } else if d > 0.0 {
            unserved += 1;
        }
    }
    (unserved, cost)
}

fn better(a: (usize, f64), b: (usize, f64)) -> bool {
    a.0 < b.0 || (a.0 == b.0 && a.1 < b.1)
}

/// n choose k, saturating.
fn binomial(n: u64, k: u64) -> u64 {
    let k = k.min(n - k.min(n));
    let mut r: u64 = 1;
    for i in 0..k {
        r = match r.checked_mul(n - i) {
            Some(v) => v / (i + 1),
            None => return u64::MAX,
        };
    }
    r
}

/// Advance `pick` (strictly increasing, values < n) to the next combination.
fn next_combination(pick: &mut [usize], n: usize) -> bool {
    let k = pick.len();
    for i in (0..k).rev() {
        if pick[i] < n - k + i {
            pick[i] += 1;
            for j in i + 1..k {
                pick[j] = pick[j - 1] + 1;
            }
            return true;
        }
    }
    false
}

// ─────────────────────────────────────────────────────────── set cover

/// Partial solutions [`set_cover`] and [`max_coverage`] expand before they
/// settle for the best found.
pub const COVER_MAX_EXPANSIONS: u64 = 2_000_000;

/// Sets over elements `0..elements`, each with a cost.
#[derive(Debug, Clone, PartialEq)]
pub struct CoverProblem {
    /// For each set, the elements it covers.
    pub sets: Vec<Vec<usize>>,
    pub cost: Vec<f64>,
    pub elements: usize,
}

impl CoverProblem {
    fn check(&self) -> Result<(), TemplateError> {
        if self.cost.len() != self.sets.len() {
            return Err(TemplateError::Invalid(format!(
                "{} costs for {} sets",
                self.cost.len(),
                self.sets.len()
            )));
        }
        non_negative("set cost", &self.cost)?;
        if let Some(e) = self.sets.iter().flatten().find(|&&e| e >= self.elements) {
            return Err(TemplateError::Invalid(format!(
                "element {e} is not one of the {}",
                self.elements
            )));
        }
        Ok(())
    }
}

/// A set-cover answer.
#[derive(Debug, Clone, PartialEq)]
pub struct CoverSolution {
    /// Indices of the chosen sets, ascending.
    pub chosen: Vec<usize>,
    pub cost: f64,
    /// Elements no set covers. They cannot be covered, so the cover is of the
    /// rest, and the answer is not feasible.
    pub uncoverable: Vec<usize>,
    /// Proven the cheapest: the search finished.
    pub exact: bool,
    /// Partial solutions expanded.
    pub iterations: u64,
}

impl CoverSolution {
    pub fn feasible(&self) -> bool {
        self.uncoverable.is_empty()
    }
}

/// The cheapest sets that together cover every element some set covers.
pub fn set_cover(
    problem: &CoverProblem,
    max_expansions: u64,
    interrupted: &dyn Fn() -> bool,
) -> Result<CoverSolution, TemplateError> {
    problem.check()?;
    let n = problem.elements;
    let mut by_element: Vec<Vec<usize>> = vec![Vec::new(); n];
    for (s, members) in problem.sets.iter().enumerate() {
        for &e in members {
            if !by_element[e].contains(&s) {
                by_element[e].push(s);
            }
        }
    }
    // Cheapest first, then lower index: the order sets are tried in.
    for list in &mut by_element {
        list.sort_by(|&a, &b| problem.cost[a].total_cmp(&problem.cost[b]).then(a.cmp(&b)));
    }
    let uncoverable: Vec<usize> = (0..n).filter(|&e| by_element[e].is_empty()).collect();
    let required: Vec<usize> = (0..n).filter(|&e| !by_element[e].is_empty()).collect();

    // Greedy: the set with the least cost per newly covered element.
    let mut covered = vec![false; n];
    for &e in &uncoverable {
        covered[e] = true;
    }
    let mut greedy: Vec<usize> = Vec::new();
    while required.iter().any(|&e| !covered[e]) {
        let pick = (0..problem.sets.len())
            .filter(|s| !greedy.contains(s))
            .filter_map(|s| {
                let new = problem.sets[s].iter().filter(|&&e| !covered[e]).count();
                (new > 0).then(|| (problem.cost[s] / new as f64, s))
            })
            .min_by(|a, b| a.0.total_cmp(&b.0).then(a.1.cmp(&b.1)))
            .map(|(_, s)| s)
            .expect("an uncovered element some set covers");
        greedy.push(pick);
        for &e in &problem.sets[pick] {
            covered[e] = true;
        }
    }
    let greedy_cost: f64 = greedy.iter().map(|&s| problem.cost[s]).sum();

    // Branch and bound: take the uncovered element with fewest sets, and try
    // each set that covers it. A partial cover whose cost, plus the dearest
    // "cheapest set" among its uncovered elements, reaches the best is cut.
    struct Search<'a> {
        problem: &'a CoverProblem,
        by_element: &'a [Vec<usize>],
        best_cost: f64,
        best: Vec<usize>,
        expanded: u64,
        limit: u64,
        stopped: bool,
        interrupted: &'a dyn Fn() -> bool,
    }
    impl Search<'_> {
        fn go(
            &mut self,
            chosen: &mut Vec<usize>,
            count: &mut [u32],
            cost: f64,
            required: &[usize],
        ) -> Result<(), TemplateError> {
            if self.expanded >= self.limit {
                self.stopped = true;
                return Ok(());
            }
            self.expanded += 1;
            if self.expanded.is_multiple_of(CHECK_EVERY) && (self.interrupted)() {
                return Err(TemplateError::Interrupted);
            }
            let uncovered = required.iter().filter(|&&e| count[e] == 0);
            let mut bound = 0.0f64;
            let mut branch: Option<usize> = None;
            for &e in uncovered {
                let cheapest = self.problem.cost[self.by_element[e][0]];
                bound = bound.max(cheapest);
                if branch.is_none_or(|b| self.by_element[e].len() < self.by_element[b].len()) {
                    branch = Some(e);
                }
            }
            let Some(e) = branch else {
                if cost < self.best_cost {
                    self.best_cost = cost;
                    self.best = chosen.clone();
                }
                return Ok(());
            };
            if cost + bound >= self.best_cost {
                return Ok(());
            }
            for &s in &self.by_element[e] {
                let c = cost + self.problem.cost[s];
                if c >= self.best_cost {
                    continue;
                }
                chosen.push(s);
                for &x in &self.problem.sets[s] {
                    count[x] += 1;
                }
                self.go(chosen, count, c, required)?;
                for &x in &self.problem.sets[s] {
                    count[x] -= 1;
                }
                chosen.pop();
                if self.stopped {
                    return Ok(());
                }
            }
            Ok(())
        }
    }
    let mut search = Search {
        problem,
        by_element: &by_element,
        best_cost: greedy_cost,
        best: greedy.clone(),
        expanded: 0,
        limit: max_expansions,
        stopped: false,
        interrupted,
    };
    // The greedy cover is already a candidate; the search looks for a
    // strictly cheaper one, so it must beat `greedy_cost`.
    let mut count = vec![0u32; n];
    search.go(&mut Vec::new(), &mut count, 0.0, &required)?;
    let mut chosen = search.best;
    chosen.sort_unstable();
    chosen.dedup();
    let cost = chosen.iter().map(|&s| problem.cost[s]).sum();
    Ok(CoverSolution {
        chosen,
        cost,
        uncoverable,
        exact: !search.stopped,
        iterations: search.expanded,
    })
}

// ─────────────────────────────────────────────────────── max coverage

/// A budgeted max-coverage answer.
#[derive(Debug, Clone, PartialEq)]
pub struct CoverageSolution {
    /// Indices of the chosen sets, ascending.
    pub chosen: Vec<usize>,
    /// Elements the chosen sets cover, ascending.
    pub covered: Vec<usize>,
    /// Total weight of `covered`.
    pub weight: f64,
    /// Total cost of `chosen`, at most the budget.
    pub cost: f64,
    pub exact: bool,
    pub iterations: u64,
}

/// The sets, of total cost at most `budget`, that cover the most element
/// weight. Ties go to the lower cost, then to the choice found first.
pub fn max_coverage(
    problem: &CoverProblem,
    weight: &[f64],
    budget: f64,
    max_expansions: u64,
    interrupted: &dyn Fn() -> bool,
) -> Result<CoverageSolution, TemplateError> {
    problem.check()?;
    if weight.len() != problem.elements {
        return Err(TemplateError::Invalid(format!(
            "{} weights for {} elements",
            weight.len(),
            problem.elements
        )));
    }
    non_negative("element weight", weight)?;
    non_negative("budget", &[budget])?;
    let sets = problem.sets.len();
    let gain = |chosen: &[usize]| -> f64 {
        let mut seen = vec![false; problem.elements];
        let mut w = 0.0;
        for &s in chosen {
            for &e in &problem.sets[s] {
                if !seen[e] {
                    seen[e] = true;
                    w += weight[e];
                }
            }
        }
        w
    };

    // Greedy by weight gained per unit cost (a free set first), compared with
    // the best single affordable set: the pair is the classic guarantee.
    let mut greedy: Vec<usize> = Vec::new();
    let mut spent = 0.0;
    loop {
        let base = gain(&greedy);
        let pick = (0..sets)
            .filter(|s| !greedy.contains(s) && spent + problem.cost[*s] <= budget)
            .filter_map(|s| {
                let mut trial = greedy.clone();
                trial.push(s);
                let add = gain(&trial) - base;
                (add > 0.0).then(|| {
                    let ratio = if problem.cost[s] == 0.0 {
                        f64::INFINITY
                    } else {
                        add / problem.cost[s]
                    };
                    (ratio, s)
                })
            })
            .max_by(|a, b| a.0.total_cmp(&b.0).then(b.1.cmp(&a.1)))
            .map(|(_, s)| s);
        let Some(s) = pick else { break };
        spent += problem.cost[s];
        greedy.push(s);
    }
    if let Some(single) = (0..sets)
        .filter(|&s| problem.cost[s] <= budget)
        .max_by(|&a, &b| gain(&[a]).total_cmp(&gain(&[b])).then(b.cmp(&a)))
    {
        if gain(&[single]) > gain(&greedy) {
            greedy = vec![single];
        }
    }

    // Branch and bound over the sets in order, each taken or left. The bound
    // is the weight covered so far plus every uncovered element some
    // remaining affordable set covers.
    struct Search<'a> {
        problem: &'a CoverProblem,
        weight: &'a [f64],
        budget: f64,
        best: (f64, f64),
        best_sets: Vec<usize>,
        expanded: u64,
        limit: u64,
        stopped: bool,
        interrupted: &'a dyn Fn() -> bool,
    }
    impl Search<'_> {
        fn go(
            &mut self,
            next: usize,
            chosen: &mut Vec<usize>,
            count: &mut [u32],
            got: f64,
            spent: f64,
        ) -> Result<(), TemplateError> {
            if self.expanded >= self.limit {
                self.stopped = true;
                return Ok(());
            }
            self.expanded += 1;
            if self.expanded.is_multiple_of(CHECK_EVERY) && (self.interrupted)() {
                return Err(TemplateError::Interrupted);
            }
            if got > self.best.0 || (got == self.best.0 && spent < self.best.1) {
                self.best = (got, spent);
                self.best_sets = chosen.clone();
            }
            let sets = self.problem.sets.len();
            if next == sets {
                return Ok(());
            }
            let left = self.budget - spent;
            let mut reachable = vec![false; count.len()];
            let mut bound = got;
            for s in next..sets {
                if self.problem.cost[s] > left {
                    continue;
                }
                for &e in &self.problem.sets[s] {
                    if count[e] == 0 && !reachable[e] {
                        reachable[e] = true;
                        bound += self.weight[e];
                    }
                }
            }
            if bound <= self.best.0 {
                return Ok(());
            }
            let s = next;
            if self.problem.cost[s] <= left {
                chosen.push(s);
                let mut add = 0.0;
                for &e in &self.problem.sets[s] {
                    if count[e] == 0 {
                        add += self.weight[e];
                    }
                    count[e] += 1;
                }
                self.go(
                    next + 1,
                    chosen,
                    count,
                    got + add,
                    spent + self.problem.cost[s],
                )?;
                for &e in &self.problem.sets[s] {
                    count[e] -= 1;
                }
                chosen.pop();
                if self.stopped {
                    return Ok(());
                }
            }
            self.go(next + 1, chosen, count, got, spent)
        }
    }
    let greedy_cost: f64 = greedy.iter().map(|&s| problem.cost[s]).sum();
    let mut search = Search {
        problem,
        weight,
        budget,
        best: (gain(&greedy), greedy_cost),
        best_sets: greedy,
        expanded: 0,
        limit: max_expansions,
        stopped: false,
        interrupted,
    };
    let mut count = vec![0u32; problem.elements];
    search.go(0, &mut Vec::new(), &mut count, 0.0, 0.0)?;
    let mut chosen = search.best_sets;
    chosen.sort_unstable();
    let mut covered: Vec<usize> = chosen
        .iter()
        .flat_map(|&s| problem.sets[s].iter().copied())
        .collect();
    covered.sort_unstable();
    covered.dedup();
    let weight_total = covered.iter().map(|&e| weight[e]).sum();
    let cost = chosen.iter().map(|&s| problem.cost[s]).sum();
    Ok(CoverageSolution {
        chosen,
        covered,
        weight: weight_total,
        cost,
        exact: !search.stopped,
        iterations: search.expanded,
    })
}

// ─────────────────────────────────────────────────────────── assignment

/// Most workers or tasks [`assignment`] takes: it holds a square matrix of
/// that side, `8 * side^2` bytes.
pub const ASSIGNMENT_MAX_SIDE: usize = 2_000;

/// An assignment answer.
#[derive(Debug, Clone, PartialEq)]
pub struct AssignmentSolution {
    /// (worker, task, cost), by worker.
    pub pairs: Vec<(usize, usize, f64)>,
    pub cost: f64,
    /// Every worker has a task, or every task a worker, whichever side is
    /// smaller: as many pairs as there could be.
    pub complete: bool,
    /// Augmenting steps taken.
    pub iterations: u64,
}

/// Pair workers with tasks, one task per worker and one worker per task, at
/// least total cost. `cost[w][t]` is `None` where worker `w` cannot take task
/// `t`. As many pairs as possible come first; least cost among those.
pub fn assignment(
    workers: usize,
    tasks: usize,
    cost: &[Vec<Option<f64>>],
    interrupted: &dyn Fn() -> bool,
) -> Result<AssignmentSolution, TemplateError> {
    let side = workers.max(tasks);
    if side > ASSIGNMENT_MAX_SIDE {
        return Err(TemplateError::TooLarge {
            what: "workers or tasks",
            size: side,
            max: ASSIGNMENT_MAX_SIDE,
        });
    }
    if cost.len() != workers || cost.iter().any(|r| r.len() != tasks) {
        return Err(TemplateError::Invalid(format!(
            "the cost matrix is not {workers} x {tasks}"
        )));
    }
    let allowed: Vec<f64> = cost.iter().flatten().flatten().copied().collect();
    non_negative("cost", &allowed)?;
    if side == 0 {
        return Ok(AssignmentSolution {
            pairs: vec![],
            cost: 0.0,
            complete: true,
            iterations: 0,
        });
    }
    // A forbidden pair costs more than any whole assignment of allowed ones,
    // so the fewest forbidden pairs are used -- the most real pairs -- and
    // then the least cost. Padding to a square is free.
    let max_allowed = allowed.iter().copied().fold(0.0, f64::max);
    let forbidden = (max_allowed + 1.0) * (side as f64 + 1.0);
    let a = |w: usize, t: usize| -> f64 {
        if w < workers && t < tasks {
            cost[w][t].unwrap_or(forbidden)
        } else {
            0.0
        }
    };

    // Hungarian algorithm with potentials, 1-indexed (e-maxx).
    let n = side;
    let mut u = vec![0.0f64; n + 1];
    let mut v = vec![0.0f64; n + 1];
    let mut p = vec![0usize; n + 1];
    let mut way = vec![0usize; n + 1];
    let mut steps = 0u64;
    for i in 1..=n {
        p[0] = i;
        let mut j0 = 0usize;
        let mut minv = vec![f64::INFINITY; n + 1];
        let mut used = vec![false; n + 1];
        loop {
            steps += 1;
            if steps.is_multiple_of(CHECK_EVERY) && interrupted() {
                return Err(TemplateError::Interrupted);
            }
            used[j0] = true;
            let i0 = p[j0];
            let mut delta = f64::INFINITY;
            let mut j1 = 0usize;
            for j in 1..=n {
                if !used[j] {
                    let cur = a(i0 - 1, j - 1) - u[i0] - v[j];
                    if cur < minv[j] {
                        minv[j] = cur;
                        way[j] = j0;
                    }
                    if minv[j] < delta {
                        delta = minv[j];
                        j1 = j;
                    }
                }
            }
            for j in 0..=n {
                if used[j] {
                    u[p[j]] += delta;
                    v[j] -= delta;
                } else {
                    minv[j] -= delta;
                }
            }
            j0 = j1;
            if p[j0] == 0 {
                break;
            }
        }
        loop {
            let j1 = way[j0];
            p[j0] = p[j1];
            j0 = j1;
            if j0 == 0 {
                break;
            }
        }
    }
    let mut pairs: Vec<(usize, usize, f64)> = (1..=n)
        .filter_map(|j| {
            let (w, t) = (p[j] - 1, j - 1);
            (w < workers && t < tasks).then_some(())?;
            cost[w][t].map(|c| (w, t, c))
        })
        .collect();
    pairs.sort_by_key(|&(w, _, _)| w);
    let total = pairs.iter().map(|&(_, _, c)| c).sum();
    let complete = pairs.len() == workers.min(tasks);
    Ok(AssignmentSolution {
        pairs,
        cost: total,
        complete,
        iterations: steps,
    })
}

// ─────────────────────────────────────────────────────── min-cost flow

/// One arc of a flow network.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Arc {
    pub from: usize,
    pub to: usize,
    /// Most it can carry; `f64::INFINITY` for no limit.
    pub capacity: f64,
    /// Cost per unit carried.
    pub cost: f64,
}

/// A min-cost-flow answer.
#[derive(Debug, Clone, PartialEq)]
pub struct FlowSolution {
    /// Flow on each arc, in the order given.
    pub flow: Vec<f64>,
    pub cost: f64,
    /// Units moved from supply to demand.
    pub shipped: f64,
    /// Total supply (the sum of the positive balances).
    pub supply: f64,
    /// Total demand (the sum of the negative balances, as a positive number).
    pub demand: f64,
    /// Augmenting paths used.
    pub iterations: u64,
}

impl FlowSolution {
    /// Every supply reached a demand and every demand was met.
    pub fn feasible(&self) -> bool {
        let tol = 1e-9 * self.supply.max(self.demand).max(1.0);
        (self.shipped - self.supply).abs() <= tol && (self.shipped - self.demand).abs() <= tol
    }
}

/// Move as much as possible from the nodes with positive `balance` (supply)
/// to those with negative `balance` (demand), at least cost.
///
/// Exact, by successive shortest paths: each step sends flow along the
/// cheapest remaining route, with node potentials keeping every reduced cost
/// non-negative so each route is found by Dijkstra. Negative costs are
/// refused.
pub fn min_cost_flow(
    nodes: usize,
    arcs: &[Arc],
    balance: &[f64],
    interrupted: &dyn Fn() -> bool,
) -> Result<FlowSolution, TemplateError> {
    if balance.len() != nodes {
        return Err(TemplateError::Invalid(format!(
            "{} balances for {nodes} nodes",
            balance.len()
        )));
    }
    if let Some(a) = arcs.iter().find(|a| a.from >= nodes || a.to >= nodes) {
        return Err(TemplateError::Invalid(format!(
            "an arc {} -> {} leaves the {nodes} nodes",
            a.from, a.to
        )));
    }
    non_negative("cost", &arcs.iter().map(|a| a.cost).collect::<Vec<_>>())?;
    non_negative(
        "capacity",
        &arcs.iter().map(|a| a.capacity).collect::<Vec<_>>(),
    )?;
    if let Some(b) = balance.iter().find(|b| !b.is_finite()) {
        return Err(TemplateError::Invalid(format!(
            "a supply of {b}: it must be a finite number"
        )));
    }
    let supply: f64 = balance.iter().filter(|b| **b > 0.0).sum();
    let demand: f64 = -balance.iter().filter(|b| **b < 0.0).sum::<f64>();

    // Residual graph with a super source s and super sink t.
    let (s, t) = (nodes, nodes + 1);
    let size = nodes + 2;
    struct E {
        to: usize,
        cap: f64,
        cost: f64,
        rev: usize,
    }
    let mut g: Vec<Vec<E>> = (0..size).map(|_| Vec::new()).collect();
    let add = |g: &mut Vec<Vec<E>>, a: usize, b: usize, cap: f64, cost: f64| -> (usize, usize) {
        let (ia, ib) = (g[a].len(), g[b].len() + usize::from(a == b));
        g[a].push(E {
            to: b,
            cap,
            cost,
            rev: ib,
        });
        g[b].push(E {
            to: a,
            cap: 0.0,
            cost: -cost,
            rev: ia,
        });
        (a, ia)
    };
    let handles: Vec<(usize, usize)> = arcs
        .iter()
        .map(|a| add(&mut g, a.from, a.to, a.capacity, a.cost))
        .collect();
    for (v, &b) in balance.iter().enumerate() {
        if b > 0.0 {
            add(&mut g, s, v, b, 0.0);
        } else if b < 0.0 {
            add(&mut g, v, t, -b, 0.0);
        }
    }

    let mut potential = vec![0.0f64; size];
    let mut shipped = 0.0;
    let mut total = 0.0;
    let mut steps = 0u64;
    let eps = 1e-12;
    loop {
        steps += 1;
        if interrupted() {
            return Err(TemplateError::Interrupted);
        }
        // Dijkstra on reduced costs.
        let mut dist = vec![f64::INFINITY; size];
        let mut prev: Vec<Option<(usize, usize)>> = vec![None; size];
        dist[s] = 0.0;
        let mut heap = BinaryHeap::new();
        heap.push(Near(0.0, s));
        while let Some(Near(d, v)) = heap.pop() {
            if d > dist[v] {
                continue;
            }
            for (i, e) in g[v].iter().enumerate() {
                if e.cap <= eps {
                    continue;
                }
                let nd = d + e.cost + potential[v] - potential[e.to];
                if nd < dist[e.to] - 1e-12 {
                    dist[e.to] = nd;
                    prev[e.to] = Some((v, i));
                    heap.push(Near(nd, e.to));
                }
            }
        }
        if !dist[t].is_finite() {
            break;
        }
        for v in 0..size {
            if dist[v].is_finite() {
                potential[v] += dist[v];
            }
        }
        // Bottleneck along the path.
        let mut push = f64::INFINITY;
        let mut v = t;
        while let Some((u, i)) = prev[v] {
            push = push.min(g[u][i].cap);
            v = u;
        }
        if !push.is_finite() || push <= eps {
            break;
        }
        let mut v = t;
        while let Some((u, i)) = prev[v] {
            g[u][i].cap -= push;
            let (to, rev) = (g[u][i].to, g[u][i].rev);
            g[to][rev].cap += push;
            total += push * g[u][i].cost;
            v = u;
        }
        shipped += push;
    }
    let flow = handles
        .iter()
        .zip(arcs)
        .map(|(&(a, i), arc)| {
            let left = g[a][i].cap;
            if arc.capacity.is_infinite() {
                // Flow on an unbounded arc is what its reverse edge holds.
                let e = &g[a][i];
                g[e.to][e.rev].cap
            } else {
                (arc.capacity - left).max(0.0)
            }
        })
        .collect();
    Ok(FlowSolution {
        flow,
        cost: total,
        shipped,
        supply,
        demand,
        iterations: steps,
    })
}

/// A heap entry ordered by least distance first.
#[derive(Debug, Clone, Copy, PartialEq)]
struct Near(f64, usize);

impl Eq for Near {}

impl Ord for Near {
    fn cmp(&self, other: &Self) -> Ordering {
        other.0.total_cmp(&self.0).then(other.1.cmp(&self.1))
    }
}

impl PartialOrd for Near {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Distances from `source` over out-edges with non-negative `weights`
/// aligned to `out_targets`, `f64::INFINITY` where unreachable. For the
/// facility template's distance matrix.
pub fn distances_from(
    out_offsets: &[usize],
    out_targets: &[usize],
    weights: Option<&[f64]>,
    source: usize,
    interrupted: &dyn Fn() -> bool,
) -> Result<Vec<f64>, TemplateError> {
    let n = out_offsets.len().saturating_sub(1);
    if let Some(w) = weights {
        non_negative("distance", w)?;
    }
    let mut dist = vec![f64::INFINITY; n];
    if source >= n {
        return Ok(dist);
    }
    dist[source] = 0.0;
    let mut heap = BinaryHeap::new();
    heap.push(Near(0.0, source));
    let mut popped = 0u64;
    while let Some(Near(d, v)) = heap.pop() {
        popped += 1;
        if popped.is_multiple_of(CHECK_EVERY) && interrupted() {
            return Err(TemplateError::Interrupted);
        }
        if d > dist[v] {
            continue;
        }
        for k in out_offsets[v]..out_offsets[v + 1] {
            let w = weights.map_or(1.0, |w| w[k]);
            let to = out_targets[k];
            if d + w < dist[to] {
                dist[to] = d + w;
                heap.push(Near(d + w, to));
            }
        }
    }
    Ok(dist)
}

#[cfg(test)]
mod tests {
    use super::*;

    const NEVER: &dyn Fn() -> bool = &|| false;

    /// Every choice, by brute force, for cross-checking.
    fn brute_cover(p: &CoverProblem) -> f64 {
        let k = p.sets.len();
        let need: Vec<usize> = (0..p.elements)
            .filter(|&e| p.sets.iter().any(|s| s.contains(&e)))
            .collect();
        (0u32..1 << k)
            .filter(|mask| {
                need.iter()
                    .all(|&e| (0..k).any(|s| mask & (1 << s) != 0 && p.sets[s].contains(&e)))
            })
            .map(|mask| {
                (0..k)
                    .filter(|s| mask & (1 << s) != 0)
                    .map(|s| p.cost[s])
                    .sum::<f64>()
            })
            .fold(f64::INFINITY, f64::min)
    }

    fn brute_coverage(p: &CoverProblem, w: &[f64], budget: f64) -> f64 {
        let k = p.sets.len();
        (0u32..1 << k)
            .filter(|mask| {
                (0..k)
                    .filter(|s| mask & (1 << s) != 0)
                    .map(|s| p.cost[s])
                    .sum::<f64>()
                    <= budget
            })
            .map(|mask| {
                (0..p.elements)
                    .filter(|&e| (0..k).any(|s| mask & (1 << s) != 0 && p.sets[s].contains(&e)))
                    .map(|e| w[e])
                    .sum::<f64>()
            })
            .fold(0.0, f64::max)
    }

    /// A small deterministic generator for random instances.
    fn lcg(seed: &mut u64) -> u64 {
        *seed = seed
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        *seed >> 33
    }

    #[test]
    fn set_cover_matches_brute_force_on_random_instances() {
        let mut seed = 7;
        for _ in 0..60 {
            let sets = 3 + (lcg(&mut seed) % 9) as usize;
            let elements = 2 + (lcg(&mut seed) % 10) as usize;
            let p = CoverProblem {
                sets: (0..sets)
                    .map(|_| {
                        (0..elements)
                            .filter(|_| lcg(&mut seed).is_multiple_of(3))
                            .collect()
                    })
                    .collect(),
                cost: (0..sets)
                    .map(|_| 1.0 + (lcg(&mut seed) % 9) as f64)
                    .collect(),
                elements,
            };
            let got = set_cover(&p, COVER_MAX_EXPANSIONS, NEVER).unwrap();
            assert!(got.exact);
            assert_eq!(got.cost, brute_cover(&p), "{p:?}");
            for e in 0..elements {
                let coverable = p.sets.iter().any(|s| s.contains(&e));
                let covered = got.chosen.iter().any(|&s| p.sets[s].contains(&e));
                if coverable {
                    assert!(covered, "element {e} can be covered and was not: {p:?}");
                } else {
                    assert!(got.uncoverable.contains(&e) && !covered, "element {e}");
                }
            }
        }
    }

    #[test]
    fn a_set_cover_whose_search_runs_out_says_it_is_not_proven() {
        let p = CoverProblem {
            sets: vec![vec![0, 1], vec![1, 2], vec![2, 3], vec![0, 3]],
            cost: vec![1.0; 4],
            elements: 4,
        };
        let got = set_cover(&p, 1, NEVER).unwrap();
        assert!(!got.exact);
        assert_eq!(got.cost, 2.0, "the greedy cover is still returned");
        let elements_left_out = CoverProblem {
            sets: vec![vec![0]],
            cost: vec![1.0],
            elements: 2,
        };
        let got = set_cover(&elements_left_out, COVER_MAX_EXPANSIONS, NEVER).unwrap();
        assert_eq!(got.uncoverable, vec![1]);
        assert!(!got.feasible());
    }

    #[test]
    fn max_coverage_matches_brute_force_on_random_instances() {
        let mut seed = 11;
        for _ in 0..60 {
            let sets = 2 + (lcg(&mut seed) % 9) as usize;
            let elements = 2 + (lcg(&mut seed) % 10) as usize;
            let p = CoverProblem {
                sets: (0..sets)
                    .map(|_| {
                        (0..elements)
                            .filter(|_| lcg(&mut seed).is_multiple_of(3))
                            .collect()
                    })
                    .collect(),
                cost: (0..sets).map(|_| (lcg(&mut seed) % 6) as f64).collect(),
                elements,
            };
            let w: Vec<f64> = (0..elements)
                .map(|_| 1.0 + (lcg(&mut seed) % 5) as f64)
                .collect();
            let budget = (lcg(&mut seed) % 10) as f64;
            let got = max_coverage(&p, &w, budget, COVER_MAX_EXPANSIONS, NEVER).unwrap();
            assert!(got.exact);
            assert!(got.cost <= budget);
            assert_eq!(
                got.weight,
                brute_coverage(&p, &w, budget),
                "{p:?} {w:?} {budget}"
            );
        }
    }

    /// Every assignment, by brute force over permutations of the larger side.
    fn brute_assignment(cost: &[Vec<Option<f64>>], workers: usize, tasks: usize) -> (usize, f64) {
        fn go(
            w: usize,
            cost: &[Vec<Option<f64>>],
            used: &mut Vec<bool>,
            pairs: usize,
            c: f64,
            best: &mut (usize, f64),
        ) {
            if w == cost.len() {
                if pairs > best.0 || (pairs == best.0 && c < best.1) {
                    *best = (pairs, c);
                }
                return;
            }
            go(w + 1, cost, used, pairs, c, best);
            for t in 0..used.len() {
                if let (false, Some(x)) = (used[t], cost[w][t]) {
                    used[t] = true;
                    go(w + 1, cost, used, pairs + 1, c + x, best);
                    used[t] = false;
                }
            }
        }
        let mut best = (0, f64::INFINITY);
        go(0, cost, &mut vec![false; tasks], 0, 0.0, &mut best);
        let _ = workers;
        best
    }

    #[test]
    fn assignment_matches_brute_force_including_missing_pairs_and_rectangles() {
        let mut seed = 3;
        for _ in 0..80 {
            let workers = 1 + (lcg(&mut seed) % 5) as usize;
            let tasks = 1 + (lcg(&mut seed) % 5) as usize;
            let cost: Vec<Vec<Option<f64>>> = (0..workers)
                .map(|_| {
                    (0..tasks)
                        .map(|_| {
                            (!lcg(&mut seed).is_multiple_of(4))
                                .then(|| (lcg(&mut seed) % 20) as f64)
                        })
                        .collect()
                })
                .collect();
            let got = assignment(workers, tasks, &cost, NEVER).unwrap();
            let (pairs, best) = brute_assignment(&cost, workers, tasks);
            assert_eq!(got.pairs.len(), pairs, "{cost:?}");
            assert!(
                (got.cost - if pairs == 0 { 0.0 } else { best }).abs() < 1e-9,
                "{cost:?}: {} vs {best}",
                got.cost
            );
            assert_eq!(got.complete, pairs == workers.min(tasks));
            let ws: std::collections::HashSet<_> = got.pairs.iter().map(|p| p.0).collect();
            let ts: std::collections::HashSet<_> = got.pairs.iter().map(|p| p.1).collect();
            assert_eq!(ws.len(), got.pairs.len());
            assert_eq!(ts.len(), got.pairs.len());
        }
    }

    /// An independent min-cost flow for cross-checking: successive shortest
    /// paths by Bellman-Ford on the residual graph, which needs no potentials
    /// because Bellman-Ford handles the negative residual costs directly.
    fn reference_flow(nodes: usize, arcs: &[Arc], balance: &[f64]) -> (f64, f64) {
        let (s, t, n) = (nodes, nodes + 1, nodes + 2);
        // (from, to, cap, cost); arc i and i ^ 1 are a pair.
        let mut e: Vec<(usize, usize, f64, f64)> = Vec::new();
        let push = |e: &mut Vec<(usize, usize, f64, f64)>, a, b, cap, cost| {
            e.push((a, b, cap, cost));
            e.push((b, a, 0.0, -cost));
        };
        for a in arcs {
            push(&mut e, a.from, a.to, a.capacity, a.cost);
        }
        for (v, &b) in balance.iter().enumerate() {
            if b > 0.0 {
                push(&mut e, s, v, b, 0.0);
            } else if b < 0.0 {
                push(&mut e, v, t, -b, 0.0);
            }
        }
        let (mut shipped, mut cost) = (0.0, 0.0);
        loop {
            let mut dist = vec![f64::INFINITY; n];
            let mut via = vec![usize::MAX; n];
            dist[s] = 0.0;
            for _ in 0..n {
                for (i, &(a, b, cap, c)) in e.iter().enumerate() {
                    if cap > 1e-12 && dist[a] + c < dist[b] - 1e-12 {
                        dist[b] = dist[a] + c;
                        via[b] = i;
                    }
                }
            }
            if !dist[t].is_finite() {
                return (shipped, cost);
            }
            let mut f = f64::INFINITY;
            let mut v = t;
            while v != s {
                f = f.min(e[via[v]].2);
                v = e[via[v]].0;
            }
            let mut v = t;
            while v != s {
                let i = via[v];
                e[i].2 -= f;
                e[i ^ 1].2 += f;
                cost += f * e[i].3;
                v = e[i].0;
            }
            shipped += f;
        }
    }

    #[test]
    fn min_cost_flow_matches_an_independent_solver_on_random_networks() {
        let mut seed = 19;
        for _ in 0..200 {
            let nodes = 3 + (lcg(&mut seed) % 6) as usize;
            let arcs: Vec<Arc> = (0..(nodes * 2 + (lcg(&mut seed) % 6) as usize))
                .map(|_| Arc {
                    from: (lcg(&mut seed) % nodes as u64) as usize,
                    to: (lcg(&mut seed) % nodes as u64) as usize,
                    capacity: (1 + lcg(&mut seed) % 5) as f64,
                    cost: (lcg(&mut seed) % 7) as f64,
                })
                .collect();
            let mut balance = vec![0.0; nodes];
            let amount = (1 + lcg(&mut seed) % 6) as f64;
            balance[0] = amount;
            balance[nodes - 1] = -amount;
            let got = min_cost_flow(nodes, &arcs, &balance, NEVER).unwrap();
            let (shipped, cost) = reference_flow(nodes, &arcs, &balance);
            assert!(
                (got.shipped - shipped).abs() < 1e-9,
                "{arcs:?}: shipped {} vs {shipped}",
                got.shipped
            );
            assert!(
                (got.cost - cost).abs() < 1e-9,
                "{arcs:?}: cost {} vs {cost}",
                got.cost
            );
            // The reported flow respects every capacity and conserves at
            // every node but the two ends.
            let mut net = vec![0.0; nodes];
            for (a, &f) in arcs.iter().zip(&got.flow) {
                assert!(f >= -1e-9 && f <= a.capacity + 1e-9, "{a:?} carries {f}");
                net[a.from] -= f;
                net[a.to] += f;
            }
            for v in 1..nodes - 1 {
                assert!(net[v].abs() < 1e-9, "node {v} is not conserved: {net:?}");
            }
        }
    }

    #[test]
    fn min_cost_flow_known_answer() {
        // s(0) supplies 4. Two routes to t(3): via 1 (cost 1+1, cap 2) and
        // via 2 (cost 3+3, cap 5). Cheapest 4 units: 2 via 1 (cost 4), 2 via
        // 2 (cost 12) = 16.
        let arcs = [
            Arc {
                from: 0,
                to: 1,
                capacity: 2.0,
                cost: 1.0,
            },
            Arc {
                from: 1,
                to: 3,
                capacity: 2.0,
                cost: 1.0,
            },
            Arc {
                from: 0,
                to: 2,
                capacity: 5.0,
                cost: 3.0,
            },
            Arc {
                from: 2,
                to: 3,
                capacity: 5.0,
                cost: 3.0,
            },
        ];
        let got = min_cost_flow(4, &arcs, &[4.0, 0.0, 0.0, -4.0], NEVER).unwrap();
        assert_eq!(got.cost, 16.0);
        assert_eq!(got.flow, vec![2.0, 2.0, 2.0, 2.0]);
        assert!(got.feasible());
        // Not enough capacity: ships what fits, says it is not feasible.
        let got = min_cost_flow(4, &arcs, &[9.0, 0.0, 0.0, -9.0], NEVER).unwrap();
        assert_eq!(got.shipped, 7.0);
        assert!(!got.feasible());
        // An unbounded arc.
        let free = [Arc {
            from: 0,
            to: 1,
            capacity: f64::INFINITY,
            cost: 2.0,
        }];
        let got = min_cost_flow(2, &free, &[3.0, -3.0], NEVER).unwrap();
        assert_eq!((got.flow[0], got.cost), (3.0, 6.0));
    }

    #[test]
    fn p_median_known_answer_and_heuristic_flag() {
        // Customers on a line at 0, 1, 10, 11; candidates at the same points.
        let pos = [0.0, 1.0, 10.0, 11.0];
        let distance: Vec<Vec<f64>> = pos
            .iter()
            .map(|&f| pos.iter().map(|&c| f64::abs(f - c)).collect())
            .collect();
        let problem = FacilityProblem {
            distance,
            demand: vec![1.0; 4],
            p: 2,
        };
        let got = p_median(&problem, NEVER).unwrap();
        assert!(got.exact);
        assert_eq!(got.cost, 2.0);
        assert_eq!(got.open, vec![0, 2], "the first best choice in order");
        assert_eq!(got.serving, vec![Some(0), Some(0), Some(2), Some(2)]);
        assert!(got.feasible());
    }

    #[test]
    fn p_median_prefers_serving_everyone_to_a_lower_cost() {
        // Facility 0 is next to customer 0 only; facility 1 reaches both, far.
        let inf = f64::INFINITY;
        let problem = FacilityProblem {
            distance: vec![vec![0.0, inf], vec![50.0, 50.0]],
            demand: vec![1.0, 1.0],
            p: 1,
        };
        let got = p_median(&problem, NEVER).unwrap();
        assert_eq!(got.open, vec![1]);
        assert_eq!(got.unserved, 0);
        let none = FacilityProblem {
            distance: vec![vec![inf]],
            demand: vec![1.0],
            p: 1,
        };
        assert!(!p_median(&none, NEVER).unwrap().feasible());
    }

    #[test]
    fn p_median_heuristic_matches_enumeration_on_a_clustered_line() {
        // 40 candidates: enumeration of 40 choose 4 = 91,390 is under the
        // limit, so compare against the heuristic run on a copy above it.
        let pos: Vec<f64> = (0..40)
            .map(|i| (i / 10) as f64 * 100.0 + (i % 10) as f64)
            .collect();
        let distance: Vec<Vec<f64>> = pos
            .iter()
            .map(|&f| pos.iter().map(|&c| f64::abs(f - c)).collect())
            .collect();
        let exact = p_median(
            &FacilityProblem {
                distance: distance.clone(),
                demand: vec![1.0; 40],
                p: 4,
            },
            NEVER,
        )
        .unwrap();
        assert!(exact.exact);
        assert_eq!(exact.cost, 4.0 * 25.0, "one per cluster, at its median");
        // 70 candidates choose 4 is over the limit: the heuristic runs.
        let pos: Vec<f64> = (0..70)
            .map(|i| (i / 10) as f64 * 100.0 + (i % 10) as f64)
            .collect();
        let distance: Vec<Vec<f64>> = pos
            .iter()
            .map(|&f| pos.iter().map(|&c| f64::abs(f - c)).collect())
            .collect();
        let heur = p_median(
            &FacilityProblem {
                distance,
                demand: vec![1.0; 70],
                p: 7,
            },
            NEVER,
        )
        .unwrap();
        assert!(!heur.exact);
        assert_eq!(
            heur.cost,
            7.0 * 25.0,
            "greedy plus swaps finds one per cluster"
        );
    }

    #[test]
    fn negative_values_and_bad_shapes_are_refused() {
        let neg = FacilityProblem {
            distance: vec![vec![-1.0]],
            demand: vec![1.0],
            p: 1,
        };
        assert!(matches!(
            p_median(&neg, NEVER),
            Err(TemplateError::Negative {
                what: "distance",
                ..
            })
        ));
        let p0 = FacilityProblem {
            distance: vec![vec![1.0]],
            demand: vec![1.0],
            p: 0,
        };
        assert!(matches!(
            p_median(&p0, NEVER),
            Err(TemplateError::Invalid(_))
        ));
        let c = CoverProblem {
            sets: vec![vec![0]],
            cost: vec![-2.0],
            elements: 1,
        };
        assert!(matches!(
            set_cover(&c, 10, NEVER),
            Err(TemplateError::Negative {
                what: "set cost",
                ..
            })
        ));
        assert!(matches!(
            assignment(1, 1, &[vec![Some(f64::NAN)]], NEVER),
            Err(TemplateError::Negative { what: "cost", .. })
        ));
        assert!(matches!(
            assignment(ASSIGNMENT_MAX_SIDE + 1, 1, &[], NEVER),
            Err(TemplateError::TooLarge { .. })
        ));
        let arcs = [Arc {
            from: 0,
            to: 1,
            capacity: 1.0,
            cost: -1.0,
        }];
        assert!(matches!(
            min_cost_flow(2, &arcs, &[1.0, -1.0], NEVER),
            Err(TemplateError::Negative { what: "cost", .. })
        ));
    }

    #[test]
    fn interruption_stops_each_template() {
        let always: &dyn Fn() -> bool = &|| true;
        let pos: Vec<f64> = (0..30).map(|i| i as f64).collect();
        let distance: Vec<Vec<f64>> = pos
            .iter()
            .map(|&f| pos.iter().map(|&c| f64::abs(f - c)).collect())
            .collect();
        let fp = FacilityProblem {
            distance,
            demand: vec![1.0; 30],
            p: 5,
        };
        assert_eq!(p_median(&fp, always), Err(TemplateError::Interrupted));
        let arcs = [Arc {
            from: 0,
            to: 1,
            capacity: 1.0,
            cost: 1.0,
        }];
        assert_eq!(
            min_cost_flow(2, &arcs, &[1.0, -1.0], always),
            Err(TemplateError::Interrupted)
        );
    }

    #[test]
    fn distances_follow_out_edges_only() {
        // 0 -> 1 (2.0), 1 -> 2 (3.0); nothing reaches 0.
        let d = distances_from(&[0, 1, 2, 2], &[1, 2], Some(&[2.0, 3.0]), 0, NEVER).unwrap();
        assert_eq!(d, vec![0.0, 2.0, 5.0]);
        let back = distances_from(&[0, 1, 2, 2], &[1, 2], None, 2, NEVER).unwrap();
        assert_eq!(back, vec![f64::INFINITY, f64::INFINITY, 0.0]);
    }

    #[test]
    fn small_helpers() {
        assert_eq!(binomial(5, 2), 10);
        assert_eq!(binomial(40, 4), 91_390);
        assert_eq!(
            binomial(3, 5),
            1,
            "p above m is clamped by the caller; never panics"
        );
        let mut c = vec![0, 1];
        let mut all = vec![c.clone()];
        while next_combination(&mut c, 4) {
            all.push(c.clone());
        }
        assert_eq!(all.len(), 6);
    }
}
