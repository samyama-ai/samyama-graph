//! PageRank, WCC and degree centrality kept up to date as a graph grows
//! (ALGO-14, #1697).
//!
//! The batch algorithms answer from scratch every time: one new relationship
//! cost 0.979 of a full PageRank run. These keep their state between calls and
//! take additions -- a node, a relationship -- as deltas, so the work after a
//! small change is proportional to the change, not to the graph.
//!
//! **Additions only.** A deleted relationship can split a component or shift
//! PageRank mass anywhere, and undoing a union-find merge is not possible
//! without rebuilding it. The caller rebuilds from a fresh [`GraphView`] when
//! anything was removed; these types never see a removal.
//!
//! What each one promises against its batch counterpart on the same graph:
//!
//! * [`IncrementalDegree`] -- the same scores, bit for bit.
//! * [`IncrementalWcc`] -- the same partition. On the first build the component
//!   ids are the same too; after additions an unchanged component keeps its
//!   id, and a merged one takes the id of one of its parts, which a fresh run
//!   may number differently. An id names a component within one answer.
//! * [`IncrementalPageRank`] -- the same fixed point, to within a bound set by
//!   [`RESIDUAL_TOLERANCE`] (see that constant). The batch run stops after a
//!   fixed number of iterations, so it is the less exact of the two.

use std::collections::{HashMap, VecDeque};

use crate::common::{GraphView, NodeId};
use crate::community::UnionFind;
use crate::progress::{Progress, Stopped};

/// The largest residual any node may hold once [`IncrementalPageRank`] has
/// settled, in the units of its unnormalised system (where each node's
/// teleport term is `1 - damping`, so a typical score is about 1).
///
/// The unnormalised error is at most the total residual over `1 - damping`,
/// so the reported scores' L1 error is at most
/// `2 * n * RESIDUAL_TOLERANCE / ((1 - damping) * sum(x))`. With no dangling
/// nodes `sum(x) = n` and that is `2 * RESIDUAL_TOLERANCE / (1 - damping)`:
/// 1.3e-3 at the usual damping of 0.85. For comparison, the batch run's 20
/// iterations leave `0.85^20`, about 4%, of its starting error in place.
/// Smaller makes each update touch more of the graph: a residual spreads to a
/// node's successors until it falls below this.
pub const RESIDUAL_TOLERANCE: f64 = 1e-4;

/// The first build settles this much below [`RESIDUAL_TOLERANCE`].
///
/// Settled exactly to the tolerance, nearly every node sits just under it,
/// and the first small addition tips thousands of them over: one relationship
/// on 5,000 nodes and 25,000 relationships cost 3,792 pushes, three quarters
/// of a sweep. The headroom makes later updates pay only for what they add;
/// it costs the first build a few more sweeps, once.
pub const FIRST_BUILD_HEADROOM: f64 = 0.01;

/// Dense indexes for the node ids a state has seen, in the order it saw them.
///
/// A batch [`GraphView`] numbers nodes by ascending id. Here a node added
/// later is appended, even when its id is a recycled smaller one: renumbering
/// would move every index after it. The order affects only which index a node
/// has, never a score.
#[derive(Debug, Clone, Default)]
struct NodeIndex {
    ids: Vec<NodeId>,
    index: HashMap<NodeId, usize>,
}

impl NodeIndex {
    fn from_view(view: &GraphView) -> Self {
        Self { ids: view.index_to_node.clone(), index: view.node_to_index.clone() }
    }

    fn len(&self) -> usize {
        self.ids.len()
    }

    fn get(&self, id: NodeId) -> Option<usize> {
        self.index.get(&id).copied()
    }

    /// The new node's index, or `None` when it is already present.
    fn insert(&mut self, id: NodeId) -> Option<usize> {
        if self.index.contains_key(&id) {
            return None;
        }
        let i = self.ids.len();
        self.ids.push(id);
        self.index.insert(id, i);
        Some(i)
    }

    /// Both endpoints' indexes, when both are present.
    fn pair(&self, source: NodeId, target: NodeId) -> Option<(usize, usize)> {
        Some((self.get(source)?, self.get(target)?))
    }
}

/// Degree centrality, `(in + out) / (n - 1)`, as
/// [`degree_centrality`](crate::centrality::degree_centrality) computes it.
#[derive(Debug, Clone)]
pub struct IncrementalDegree {
    nodes: NodeIndex,
    /// In-degree plus out-degree, so a self-loop counts twice as it does in
    /// the batch version.
    degree: Vec<usize>,
}

impl IncrementalDegree {
    pub fn from_view(view: &GraphView) -> Self {
        let degree = (0..view.node_count).map(|i| view.out_degree(i) + view.in_degree(i)).collect();
        Self { nodes: NodeIndex::from_view(view), degree }
    }

    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    /// Add a node with no relationships. `false` when it was already present.
    pub fn add_node(&mut self, id: NodeId) -> bool {
        let added = self.nodes.insert(id).is_some();
        if added {
            self.degree.push(0);
        }
        added
    }

    /// Add one relationship. `false`, and nothing changes, when either
    /// endpoint is not a node of this state -- the caller's filter left it out.
    pub fn add_edge(&mut self, source: NodeId, target: NodeId) -> bool {
        let Some((s, t)) = self.nodes.pair(source, target) else {
            return false;
        };
        self.degree[s] += 1;
        self.degree[t] += 1;
        true
    }

    /// Every node's score, in index order.
    pub fn scores(&self) -> Vec<(NodeId, f64)> {
        let n = self.nodes.len();
        if n <= 1 {
            return self.nodes.ids.iter().map(|&id| (id, 0.0)).collect();
        }
        let denom = (n - 1) as f64;
        self.nodes.ids.iter().zip(&self.degree).map(|(&id, &d)| (id, d as f64 / denom)).collect()
    }
}

/// Weakly connected components, as
/// [`weakly_connected_components`](crate::community::weakly_connected_components)
/// finds them.
pub struct IncrementalWcc {
    nodes: NodeIndex,
    sets: UnionFind,
}

impl std::fmt::Debug for IncrementalWcc {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IncrementalWcc").field("nodes", &self.nodes.len()).finish_non_exhaustive()
    }
}

impl IncrementalWcc {
    /// Unions in the batch version's order, so the first answer's component
    /// ids are the batch version's ids.
    pub fn from_view(view: &GraphView) -> Self {
        let mut sets = UnionFind::new(view.node_count);
        for u in 0..view.node_count {
            for &v in view.successors(u) {
                sets.union(u, v);
            }
        }
        Self { nodes: NodeIndex::from_view(view), sets }
    }

    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    /// Add a node, in a component of its own. `false` when already present.
    pub fn add_node(&mut self, id: NodeId) -> bool {
        let added = self.nodes.insert(id).is_some();
        if added {
            self.sets.push();
        }
        added
    }

    /// Add one relationship, joining its endpoints' components. `false`, and
    /// nothing changes, when either endpoint is not a node of this state.
    pub fn add_edge(&mut self, source: NodeId, target: NodeId) -> bool {
        let Some((s, t)) = self.nodes.pair(source, target) else {
            return false;
        };
        self.sets.union(s, t);
        true
    }

    /// Every node and its component id, in index order.
    pub fn components(&mut self) -> Vec<(NodeId, usize)> {
        (0..self.nodes.len()).map(|i| (self.nodes.ids[i], self.sets.find(i))).collect()
    }
}

/// PageRank with dangling-mass redistribution, as [`page_rank`](crate::page_rank)
/// computes it, kept by residual pushing.
///
/// The state solves the unnormalised system `x = (1 - d) + d * P^T x`, in
/// which a dangling node's mass is simply lost, and reports `x / sum(x)`.
/// That is the same vector as PageRank with the dangling mass spread
/// uniformly: both are `(I - d P^T)^-1 * 1` up to scale. Solving the
/// unnormalised form keeps every change local -- a new node or relationship
/// changes no other node's teleport term, where the normalised form's `1/n`
/// and dangling share touch every node.
///
/// Alongside the estimate `x` it keeps the residual
/// `r = (1 - d) + d * P^T x - x`, exactly. Pushing a node moves its residual
/// into its estimate and passes `d / out_degree` of it to each successor,
/// which keeps that identity, so after any sequence of additions and pushes
/// the error is `(I - d P^T)^-1 r` and settling to small residuals is enough.
#[derive(Debug, Clone)]
pub struct IncrementalPageRank {
    damping: f64,
    nodes: NodeIndex,
    /// Successor indexes, one entry per relationship, parallel ones repeated
    /// as the batch view repeats them.
    successors: Vec<Vec<usize>>,
    estimate: Vec<f64>,
    residual: Vec<f64>,
    /// Nodes whose residual may be above the tolerance, each at most once.
    pending: VecDeque<usize>,
    queued: Vec<bool>,
}

/// Why an [`IncrementalPageRank`] cannot be built.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum IncrementalPageRankError {
    /// Damping outside `(0, 1)`: at 1 nothing converges, and at 0 or below
    /// there is no ranking to keep.
    Damping(f64),
    /// The run's [`Progress`] stopped it during the first build.
    Stopped(Stopped),
}

impl std::fmt::Display for IncrementalPageRankError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Damping(d) => write!(f, "damping must be above 0 and below 1, got {d}"),
            Self::Stopped(s) => s.fmt(f),
        }
    }
}

impl std::error::Error for IncrementalPageRankError {}

impl IncrementalPageRank {
    /// Solve from scratch on `view`.
    ///
    /// Whole-graph sweeps first (every node pushed at once, which is a Jacobi
    /// iteration), because on a full graph that is cheaper per unit of
    /// residual than a queue; then the queue clears what the sweeps leave.
    /// Each sweep shrinks the total residual by at least `damping`, so the
    /// number of sweeps is bounded and reported to `progress` as the total.
    pub fn from_view(view: &GraphView, damping: f64, progress: &dyn Progress) -> Result<Self, IncrementalPageRankError> {
        if !(damping > 0.0 && damping < 1.0) {
            return Err(IncrementalPageRankError::Damping(damping));
        }
        let n = view.node_count;
        let successors: Vec<Vec<usize>> = (0..n).map(|u| view.successors(u).to_vec()).collect();
        let mut state = Self {
            damping,
            nodes: NodeIndex::from_view(view),
            successors,
            estimate: vec![0.0; n],
            residual: vec![1.0 - damping; n],
            pending: VecDeque::new(),
            queued: vec![false; n],
        };

        let sweeps = Self::sweeps_needed(n, damping);
        let stop = |done| IncrementalPageRankError::Stopped(Stopped { done, total: sweeps });
        if !progress.step(0, sweeps) {
            return Err(stop(0));
        }
        let target = RESIDUAL_TOLERANCE * FIRST_BUILD_HEADROOM;
        for done in 1..=sweeps {
            if state.residual.iter().map(|r| r.abs()).sum::<f64>() <= n as f64 * target {
                break;
            }
            state.sweep(view);
            if !progress.step(done, sweeps) {
                return Err(stop(done));
            }
        }
        for i in 0..n {
            state.enqueue_above(i, target);
        }
        state.settle_to(target);
        Ok(state)
    }

    /// Sweeps that bring the total residual from its start, `n * (1 - d)`, to
    /// `n * t` for the first build's tolerance `t`: `d^k * (1 - d) <= t`.
    fn sweeps_needed(n: usize, damping: f64) -> usize {
        if n == 0 {
            return 0;
        }
        let k = (RESIDUAL_TOLERANCE * FIRST_BUILD_HEADROOM / (1.0 - damping)).ln() / damping.ln();
        k.ceil().max(0.0) as usize
    }

    /// Push every node at once: `x += r`, then `r = d * P^T r`.
    fn sweep(&mut self, view: &GraphView) {
        let d = self.damping;
        let n = view.node_count;
        let share: Vec<f64> = (0..n)
            .map(|u| {
                let k = self.successors[u].len();
                if k == 0 { 0.0 } else { d * self.residual[u] / k as f64 }
            })
            .collect();
        for (x, r) in self.estimate.iter_mut().zip(&self.residual) {
            *x += r;
        }
        for v in 0..n {
            let start = view.in_offsets[v];
            let end = view.in_offsets[v + 1];
            self.residual[v] = view.in_sources[start..end].iter().map(|&u| share[u]).sum();
        }
    }

    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    pub fn damping(&self) -> f64 {
        self.damping
    }

    /// Add a node with no relationships. `false` when it was already present.
    /// Call [`settle`](Self::settle) before reading scores.
    pub fn add_node(&mut self, id: NodeId) -> bool {
        let Some(i) = self.nodes.insert(id) else {
            return false;
        };
        self.successors.push(Vec::new());
        self.estimate.push(0.0);
        self.residual.push(1.0 - self.damping);
        self.queued.push(false);
        self.enqueue(i);
        true
    }

    /// Add one relationship. `false`, and nothing changes, when either
    /// endpoint is not a node of this state. Call [`settle`](Self::settle)
    /// before reading scores.
    ///
    /// The source's estimate now splits `k + 1` ways instead of `k`: each old
    /// successor's residual falls by the difference and the new one's rises by
    /// a full share. A source with no successors was dangling, its mass lost;
    /// all of it now goes to the target.
    pub fn add_edge(&mut self, source: NodeId, target: NodeId) -> bool {
        let Some((s, t)) = self.nodes.pair(source, target) else {
            return false;
        };
        let d = self.damping;
        let x = self.estimate[s];
        let k = self.successors[s].len();
        if k > 0 {
            let fall = d * x / (k * (k + 1)) as f64;
            for j in 0..k {
                let w = self.successors[s][j];
                self.residual[w] -= fall;
                self.enqueue(w);
            }
        }
        self.residual[t] += d * x / (k + 1) as f64;
        self.enqueue(t);
        self.successors[s].push(t);
        true
    }

    fn enqueue(&mut self, i: usize) {
        self.enqueue_above(i, RESIDUAL_TOLERANCE);
    }

    fn enqueue_above(&mut self, i: usize, threshold: f64) {
        if !self.queued[i] && self.residual[i].abs() > threshold {
            self.queued[i] = true;
            self.pending.push_back(i);
        }
    }

    /// Push until no residual is above [`RESIDUAL_TOLERANCE`]; returns how
    /// many pushes it took, the measure of how much of the graph a change
    /// reached.
    ///
    /// Terminates: each push removes a residual above the threshold and passes
    /// on at most `d` of it, so the total residual falls by at least
    /// `(1 - d) * threshold` per push.
    pub fn settle(&mut self) -> usize {
        self.settle_to(RESIDUAL_TOLERANCE)
    }

    /// [`settle`](Self::settle) to `threshold` instead of the tolerance.
    fn settle_to(&mut self, threshold: f64) -> usize {
        let d = self.damping;
        let mut pushes = 0;
        while let Some(u) = self.pending.pop_front() {
            self.queued[u] = false;
            let r = self.residual[u];
            if r.abs() <= threshold {
                continue;
            }
            pushes += 1;
            self.estimate[u] += r;
            self.residual[u] = 0.0;
            let k = self.successors[u].len();
            if k == 0 {
                continue;
            }
            let share = d * r / k as f64;
            for j in 0..k {
                let w = self.successors[u][j];
                self.residual[w] += share;
                self.enqueue_above(w, threshold);
            }
        }
        pushes
    }

    /// The largest residual left, for tests and diagnostics; at most
    /// [`RESIDUAL_TOLERANCE`] after [`settle`](Self::settle).
    pub fn max_residual(&self) -> f64 {
        self.residual.iter().fold(0.0, |m, r| m.max(r.abs()))
    }

    /// Every node's PageRank, summing to 1, in index order. Settles first.
    pub fn scores(&mut self) -> Vec<(NodeId, f64)> {
        self.settle();
        let total: f64 = self.estimate.iter().sum();
        if total <= 0.0 {
            return Vec::new();
        }
        self.nodes.ids.iter().zip(&self.estimate).map(|(&id, &x)| (id, x / total)).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::progress::NoProgress;
    use crate::{degree_centrality, page_rank, weakly_connected_components, PageRankConfig};

    /// A view over `nodes` ids with `edges` as `(source, target)` ids, built
    /// the way the engine's view builder lays one out.
    fn view(nodes: &[NodeId], edges: &[(NodeId, NodeId)]) -> GraphView {
        let mut ids = nodes.to_vec();
        ids.sort_unstable();
        let index: HashMap<NodeId, usize> = ids.iter().enumerate().map(|(i, &id)| (id, i)).collect();
        let n = ids.len();
        let mut out = vec![Vec::new(); n];
        let mut inc = vec![Vec::new(); n];
        for &(s, t) in edges {
            let (s, t) = (index[&s], index[&t]);
            out[s].push(t);
            inc[t].push(s);
        }
        let flatten = |lists: Vec<Vec<usize>>| {
            let mut offsets = vec![0];
            let mut flat = Vec::new();
            for l in lists {
                flat.extend(l);
                offsets.push(flat.len());
            }
            (offsets, flat)
        };
        let (out_offsets, out_targets) = flatten(out);
        let (in_offsets, in_sources) = flatten(inc);
        GraphView {
            node_count: n,
            index_to_node: ids,
            node_to_index: index,
            out_offsets,
            out_targets,
            in_offsets,
            in_sources,
            weights: None,
        }
    }

    /// Deterministic pseudo-random edges, no RNG dependency.
    fn random_edges(n: u64, m: usize, seed: u64) -> Vec<(NodeId, NodeId)> {
        let mut x = seed | 1;
        let mut next = move || {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x
        };
        (0..m).map(|_| (next() % n, next() % n)).collect()
    }

    /// PageRank run long enough to be the fixed point, to compare against.
    fn exact_pagerank(v: &GraphView) -> HashMap<NodeId, f64> {
        page_rank(v, PageRankConfig { iterations: 1000, tolerance: 0.0, ..PageRankConfig::default() })
    }

    fn l1(a: &[(NodeId, f64)], b: &HashMap<NodeId, f64>) -> f64 {
        assert_eq!(a.len(), b.len(), "different node sets");
        a.iter().map(|(id, s)| (s - b[id]).abs()).sum()
    }

    /// The bound the module documentation states, for a graph of `n` nodes:
    /// total residual over `1 - d`, relative to the unnormalised total.
    fn stated_bound(state: &IncrementalPageRank) -> f64 {
        let n = state.node_count() as f64;
        let total: f64 = state.estimate.iter().sum();
        2.0 * n * RESIDUAL_TOLERANCE / ((1.0 - state.damping) * total)
    }

    fn ids(n: u64) -> Vec<NodeId> {
        (0..n).collect()
    }

    #[test]
    fn the_first_pagerank_matches_the_fixed_point() {
        let edges = random_edges(300, 1500, 7);
        let v = view(&ids(300), &edges);
        let mut state = IncrementalPageRank::from_view(&v, 0.85, &NoProgress).unwrap();
        assert!(state.max_residual() <= RESIDUAL_TOLERANCE);
        let bound = stated_bound(&state);
        let got = state.scores();
        let err = l1(&got, &exact_pagerank(&v));
        assert!(err <= bound, "L1 error {err} above the stated bound {bound}");
        assert!((got.iter().map(|(_, s)| s).sum::<f64>() - 1.0).abs() < 1e-9, "scores do not sum to 1");
    }

    #[test]
    fn pagerank_after_added_relationships_matches_a_fresh_run() {
        let n = 400;
        let all = random_edges(n, 2400, 11);
        let (first, later) = all.split_at(2000);
        let mut state = IncrementalPageRank::from_view(&view(&ids(n), first), 0.85, &NoProgress).unwrap();
        for &(s, t) in later {
            assert!(state.add_edge(s, t));
        }
        let bound = stated_bound(&state);
        let err = l1(&state.scores(), &exact_pagerank(&view(&ids(n), &all)));
        assert!(err <= bound, "L1 error {err} above the stated bound {bound}");
    }

    #[test]
    fn pagerank_handles_dangling_sources_self_loops_parallel_edges_and_new_nodes() {
        // 3 is dangling until it gains an edge; 1 -> 1 is a self-loop; 0 -> 2
        // twice is a parallel pair; 9 arrives later, joins, and 10 stays alone.
        let base = [(0, 1), (0, 2), (0, 2), (1, 1), (2, 0)];
        let mut state = IncrementalPageRank::from_view(&view(&[0, 1, 2, 3], &base), 0.85, &NoProgress).unwrap();
        assert!(state.add_node(9));
        assert!(!state.add_node(9), "a node was added twice");
        assert!(state.add_node(10));
        let added = [(3, 0), (9, 3), (2, 9), (0, 2)];
        for &(s, t) in &added {
            assert!(state.add_edge(s, t));
        }
        let all: Vec<_> = base.iter().chain(&added).copied().collect();
        let bound = stated_bound(&state);
        let err = l1(&state.scores(), &exact_pagerank(&view(&[0, 1, 2, 3, 9, 10], &all)));
        assert!(err <= bound, "L1 error {err} above the stated bound {bound}");
    }

    #[test]
    fn one_new_relationship_costs_under_a_tenth_of_a_batch_run() {
        let n = 20_000;
        let mut state =
            IncrementalPageRank::from_view(&view(&ids(n), &random_edges(n, 100_000, 3)), 0.85, &NoProgress).unwrap();
        state.add_edge(17, 4242);
        let pushes = state.settle();
        // The batch run updates every node once per iteration; ALGO-14's
        // target is an update at a tenth of that or less. Each push is one
        // node update over the same successor list a batch iteration reads.
        let batch_updates = PageRankConfig::default().iterations * n as usize;
        assert!(pushes * 10 < batch_updates, "{pushes} pushes against {batch_updates} batch node updates");
        assert!(state.max_residual() <= RESIDUAL_TOLERANCE);
    }

    #[test]
    fn an_edge_to_an_unknown_node_changes_nothing() {
        let mut state = IncrementalPageRank::from_view(&view(&[0, 1], &[(0, 1)]), 0.85, &NoProgress).unwrap();
        let before = state.scores();
        assert!(!state.add_edge(0, 99));
        assert!(!state.add_edge(99, 0));
        assert_eq!(state.scores(), before);
    }

    #[test]
    fn damping_outside_zero_to_one_is_refused() {
        let v = view(&[0, 1], &[(0, 1)]);
        for d in [0.0, 1.0, -0.5, 1.5, f64::NAN] {
            let err = IncrementalPageRank::from_view(&v, d, &NoProgress).unwrap_err();
            assert!(matches!(err, IncrementalPageRankError::Damping(_)), "{d}: {err:?}");
            assert!(err.to_string().contains("damping"), "{err}");
        }
    }

    #[test]
    fn the_first_build_reports_its_sweeps_and_can_be_stopped() {
        struct StopAt(usize, std::cell::RefCell<Vec<(usize, usize)>>);
        impl Progress for StopAt {
            fn step(&self, done: usize, total: usize) -> bool {
                self.1.borrow_mut().push((done, total));
                done < self.0
            }
        }
        let v = view(&ids(200), &random_edges(200, 1000, 5));
        let stop = StopAt(2, Default::default());
        let err = IncrementalPageRank::from_view(&v, 0.85, &stop).unwrap_err();
        let total = IncrementalPageRank::sweeps_needed(200, 0.85);
        assert_eq!(err, IncrementalPageRankError::Stopped(Stopped { done: 2, total }));
        assert_eq!(*stop.1.borrow(), vec![(0, total), (1, total), (2, total)]);
    }

    #[test]
    fn an_empty_graph_has_no_scores() {
        let mut state = IncrementalPageRank::from_view(&view(&[], &[]), 0.85, &NoProgress).unwrap();
        assert!(state.scores().is_empty());
        assert!(state.add_node(4));
        assert_eq!(state.scores(), vec![(4, 1.0)]);
    }

    #[test]
    fn wcc_first_answer_is_the_batch_answer_ids_included() {
        let n = 500;
        let v = view(&ids(n), &random_edges(n, 300, 13));
        let batch = weakly_connected_components(&v).node_component;
        let mut state = IncrementalWcc::from_view(&v);
        let got: HashMap<NodeId, usize> = state.components().into_iter().collect();
        assert_eq!(got, batch);
    }

    /// Same partition: two nodes share an id in one answer exactly when they
    /// share one in the other.
    fn same_partition(a: &HashMap<NodeId, usize>, b: &HashMap<NodeId, usize>) -> bool {
        let mut pairs: HashMap<usize, usize> = HashMap::new();
        let mut back: HashMap<usize, usize> = HashMap::new();
        a.len() == b.len()
            && a.iter().all(|(id, &ca)| {
                let Some(&cb) = b.get(id) else { return false };
                *pairs.entry(ca).or_insert(cb) == cb && *back.entry(cb).or_insert(ca) == ca
            })
    }

    #[test]
    fn wcc_after_additions_has_the_partition_of_a_fresh_run() {
        let n = 600;
        let all = random_edges(n, 500, 17);
        let (first, later) = all.split_at(350);
        let mut state = IncrementalWcc::from_view(&view(&ids(n - 50), &first.iter().copied().filter(|&(s, t)| s < n - 50 && t < n - 50).collect::<Vec<_>>()));
        for id in n - 50..n {
            assert!(state.add_node(id));
        }
        for &(s, t) in first.iter().filter(|&&(s, t)| s >= n - 50 || t >= n - 50) {
            assert!(state.add_edge(s, t));
        }
        for &(s, t) in later {
            assert!(state.add_edge(s, t));
        }
        let got: HashMap<NodeId, usize> = state.components().into_iter().collect();
        let fresh = weakly_connected_components(&view(&ids(n), &all)).node_component;
        assert!(same_partition(&got, &fresh), "the partitions differ");
    }

    #[test]
    fn wcc_keeps_an_untouched_components_id() {
        // {0,1} and {2,3} and {4}; joining 4 to {2,3} leaves {0,1} alone.
        let mut state = IncrementalWcc::from_view(&view(&ids(5), &[(0, 1), (2, 3)]));
        let before: HashMap<NodeId, usize> = state.components().into_iter().collect();
        assert!(state.add_edge(4, 2));
        assert!(!state.add_edge(4, 77));
        let after: HashMap<NodeId, usize> = state.components().into_iter().collect();
        assert_eq!(after[&0], before[&0]);
        assert_eq!(after[&1], before[&1]);
        assert_eq!(after[&4], after[&2]);
        assert_eq!(after[&3], after[&2]);
        assert_ne!(after[&0], after[&2]);
        assert!(!state.add_node(4), "a node was added twice");
        assert_eq!(state.node_count(), 5);
    }

    #[test]
    fn degree_matches_the_batch_scores_exactly_after_additions() {
        let n = 300;
        let all = random_edges(n, 900, 23);
        let (first, later) = all.split_at(600);
        let mut state = IncrementalDegree::from_view(&view(&ids(n - 1), &first.iter().copied().filter(|&(s, t)| s < n - 1 && t < n - 1).collect::<Vec<_>>()));
        assert!(state.add_node(n - 1));
        assert!(!state.add_node(n - 1), "a node was added twice");
        for &(s, t) in first.iter().filter(|&&(s, t)| s == n - 1 || t == n - 1).chain(later) {
            assert!(state.add_edge(s, t));
        }
        assert!(!state.add_edge(0, 10_000));
        let fresh = view(&ids(n), &all);
        let batch = degree_centrality(&fresh, true);
        let got: HashMap<NodeId, f64> = state.scores().into_iter().collect();
        for (i, &id) in fresh.index_to_node.iter().enumerate() {
            assert_eq!(got[&id].to_bits(), batch[i].to_bits(), "node {id}");
        }
    }

    #[test]
    fn degree_of_a_graph_of_one_node_is_zero() {
        let mut state = IncrementalDegree::from_view(&view(&[], &[]));
        assert!(state.scores().is_empty());
        state.add_node(3);
        assert_eq!(state.scores(), vec![(3, 0.0)]);
        assert_eq!(state.node_count(), 1);
    }
}
