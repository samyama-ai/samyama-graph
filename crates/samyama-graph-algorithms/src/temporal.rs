//! Time-respecting traversal, and the four causal/temporal primitives built on
//! it (ALGO-15).
//!
//! # Why these are not ordinary graph algorithms
//!
//! In a static graph, reachability is transitive: if `a` reaches `b` and `b`
//! reaches `c`, then `a` reaches `c`. **In a temporal graph it is not.** If the
//! edge `a→b` fires at 10:00 and `b→c` fired at 09:00, then `a` cannot reach
//! `c` through `b` — the second edge is already in the past when you arrive.
//!
//! Every result here follows from that one fact, and it is why an
//! infrastructure fault cannot be traced with a plain BFS: a service that
//! failed *before* its dependency did is not explained by that dependency.
//!
//! # The core
//!
//! One routine, [`earliest_arrival`], answers "at the earliest, when can each
//! node be reached from here?" and everything else is a reading of it:
//!
//! | primitive | question | direction |
//! |---|---|---|
//! | [`temporal_reachability`] | what can this reach in time? | forward |
//! | [`temporal_shortest_path`] | by what route, arriving soonest? | forward |
//! | [`propagation_ranking`] | what does this break, and in what order? | forward |
//! | [`symptom_explanation`] | what could have caused these? | backward |
//!
//! The first three are forward from a cause; the last is backward from
//! observed symptoms, which is the direction an operator actually starts from.
//!
//! # Timestamps
//!
//! Edge times are supplied alongside the [`GraphView`] rather than inside it,
//! aligned with `out_targets`. [`TemporalEdges::new`] **checks that
//! alignment** instead of trusting it: a times array off by one silently
//! answers a different question on every edge, and the answer still looks like
//! a plausible set of nodes and times.

use std::cmp::Reverse;
use std::collections::BinaryHeap;

use crate::common::{GraphView, NodeId};

/// Edge timestamps, aligned with a [`GraphView`]'s `out_targets`.
#[derive(Debug, Clone)]
pub struct TemporalEdges {
    times: Vec<i64>,
}

/// Why a temporal input was refused.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TemporalError {
    /// The times array does not match the view's edge count.
    Misaligned { edges: usize, times: usize },
    /// A node index that the view does not contain.
    NoSuchNode(usize),
}

impl std::fmt::Display for TemporalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TemporalError::Misaligned { edges, times } => write!(
                f,
                "temporal edge times must align with the graph's out-edges: \
                 the view has {edges} and {times} times were given"
            ),
            TemporalError::NoSuchNode(i) => write!(f, "node index {i} is not in this graph"),
        }
    }
}

impl std::error::Error for TemporalError {}

impl TemporalEdges {
    /// Wrap a times array, checking it against the view.
    ///
    /// The length check is the whole point of the type. An array off by one
    /// pairs every edge with its neighbour's timestamp, which changes every
    /// answer and produces no symptom -- the result is still a well-formed set
    /// of nodes with plausible times.
    pub fn new(view: &GraphView, times: Vec<i64>) -> Result<Self, TemporalError> {
        if times.len() != view.out_targets.len() {
            return Err(TemporalError::Misaligned {
                edges: view.out_targets.len(),
                times: times.len(),
            });
        }
        Ok(Self { times })
    }

    /// The timestamp of the `k`-th out-edge, in the view's own ordering.
    #[inline]
    pub fn at(&self, edge_slot: usize) -> i64 {
        self.times[edge_slot]
    }

    /// How many edges this covers.
    pub fn len(&self) -> usize {
        self.times.len()
    }

    /// Is it empty?
    pub fn is_empty(&self) -> bool {
        self.times.is_empty()
    }
}

/// Earliest time each node can be reached, or `None` if it cannot.
///
/// Indexed by the view's dense node index. `arrival[source] == start`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArrivalTimes {
    pub arrival: Vec<Option<i64>>,
    /// The out-edge slot each node was first reached through, for path
    /// reconstruction. `None` for a source or an unreached node.
    parent_edge: Vec<Option<(usize, usize)>>,
}

impl ArrivalTimes {
    /// Can `node` be reached at all?
    pub fn reaches(&self, node: usize) -> bool {
        self.arrival.get(node).copied().flatten().is_some()
    }

    /// How many nodes were reached, excluding the sources themselves.
    pub fn reached_count(&self, sources: &[usize]) -> usize {
        self.arrival
            .iter()
            .enumerate()
            .filter(|(i, a)| a.is_some() && !sources.contains(i))
            .count()
    }
}

/// The earliest time each node is reachable from `sources`, starting no
/// earlier than `start`, along a walk whose edge times never decrease.
///
/// This is Dijkstra with an unusual relaxation. An ordinary shortest path adds
/// a cost to reach the next node; here traversing an edge that fires at `t`
/// puts you at the far end *at* `t`, whatever time you arrived at the near
/// end -- provided `t` is not already past. So the relaxation is
/// `arrival[v] = min(arrival[v], t)` guarded by `t >= arrival[u]`, and it is
/// monotone for the same reason a non-negative edge weight is: arrival times
/// never decrease along a walk, so the first time a node leaves the heap is
/// its earliest.
///
/// Multi-source because the questions above want it: a fault with several
/// simultaneous origins is one traversal, not a union of several.
pub fn earliest_arrival(
    view: &GraphView,
    times: &TemporalEdges,
    sources: &[usize],
    start: i64,
) -> Result<ArrivalTimes, TemporalError> {
    let n = view.node_count;
    for &s in sources {
        if s >= n {
            return Err(TemporalError::NoSuchNode(s));
        }
    }

    let mut arrival: Vec<Option<i64>> = vec![None; n];
    let mut parent_edge: Vec<Option<(usize, usize)>> = vec![None; n];
    let mut heap: BinaryHeap<Reverse<(i64, usize)>> = BinaryHeap::new();

    for &s in sources {
        // A repeated source is not an error and must not enqueue twice.
        if arrival[s].is_none() {
            arrival[s] = Some(start);
            heap.push(Reverse((start, s)));
        }
    }

    while let Some(Reverse((at, u))) = heap.pop() {
        // A stale heap entry: this node already left with an earlier time.
        if arrival[u].is_some_and(|a| at > a) {
            continue;
        }
        let lo = view.out_offsets[u];
        let hi = view.out_offsets[u + 1];
        for slot in lo..hi {
            let t = times.at(slot);
            // The edge has already fired by the time we get here. This single
            // comparison is the whole difference from a static BFS.
            if t < at {
                continue;
            }
            let v = view.out_targets[slot];
            if arrival[v].is_none_or(|a| t < a) {
                arrival[v] = Some(t);
                parent_edge[v] = Some((u, slot));
                heap.push(Reverse((t, v)));
            }
        }
    }

    Ok(ArrivalTimes { arrival, parent_edge })
}

/// Which nodes a source can reach in time, and when (ALGO-15).
///
/// Returns `(node_id, arrival)` pairs in increasing arrival order, excluding
/// the sources. Sorted because the order *is* the answer: it is the sequence a
/// fault would propagate in.
pub fn temporal_reachability(
    view: &GraphView,
    times: &TemporalEdges,
    sources: &[usize],
    start: i64,
) -> Result<Vec<(NodeId, i64)>, TemporalError> {
    let arrivals = earliest_arrival(view, times, sources, start)?;
    let mut out: Vec<(NodeId, i64)> = arrivals
        .arrival
        .iter()
        .enumerate()
        .filter_map(|(i, a)| {
            if sources.contains(&i) {
                return None;
            }
            a.map(|t| (view.index_to_node[i], t))
        })
        .collect();
    // By time, then by node id, so two runs over the same data agree. An
    // unstable order here would surface as a flapping test and, worse, as a
    // different "first affected service" on each run.
    out.sort_by_key(|&(id, t)| (t, id));
    Ok(out)
}

/// A time-respecting walk.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TemporalPath {
    /// Nodes in order, source first.
    pub nodes: Vec<NodeId>,
    /// The time each edge fired; `edge_times[i]` is between `nodes[i]` and
    /// `nodes[i + 1]`, so it is one shorter than `nodes`.
    pub edge_times: Vec<i64>,
    /// When the walk reaches its target.
    pub arrival: i64,
}

/// The earliest-arriving time-respecting path from `source` to `target`.
///
/// "Shortest" in a temporal graph is ambiguous -- it can mean fewest hops,
/// earliest arrival, latest departure, or shortest duration, and they select
/// different paths. This is **earliest arrival**, which is the one that pairs
/// with reachability: a target is reachable exactly when this returns `Some`.
pub fn temporal_shortest_path(
    view: &GraphView,
    times: &TemporalEdges,
    source: usize,
    target: usize,
    start: i64,
) -> Result<Option<TemporalPath>, TemporalError> {
    if target >= view.node_count {
        return Err(TemporalError::NoSuchNode(target));
    }
    let a = earliest_arrival(view, times, &[source], start)?;
    let Some(arrival) = a.arrival[target] else {
        return Ok(None);
    };
    if target == source {
        return Ok(Some(TemporalPath {
            nodes: vec![view.index_to_node[source]],
            edge_times: Vec::new(),
            arrival,
        }));
    }

    let mut nodes = vec![target];
    let mut edge_times = Vec::new();
    let mut cur = target;
    while let Some((prev, slot)) = a.parent_edge[cur] {
        edge_times.push(times.at(slot));
        nodes.push(prev);
        cur = prev;
        if cur == source {
            break;
        }
    }
    nodes.reverse();
    edge_times.reverse();
    Ok(Some(TemporalPath {
        nodes: nodes.into_iter().map(|i| view.index_to_node[i]).collect(),
        edge_times,
        arrival,
    }))
}

/// What a fault at `source` reaches, ranked by how soon (ALGO-15).
///
/// The same traversal as [`temporal_reachability`], named separately because
/// it answers the operator's forward question -- *"I know this broke; what
/// does it take down, and in what order?"* -- and because the ranking, not the
/// set, is the product.
pub fn propagation_ranking(
    view: &GraphView,
    times: &TemporalEdges,
    sources: &[usize],
    start: i64,
) -> Result<Vec<(NodeId, i64)>, TemporalError> {
    temporal_reachability(view, times, sources, start)
}

/// One candidate cause, and how much of the evidence it accounts for.
#[derive(Debug, Clone, PartialEq)]
pub struct Explanation {
    pub node: NodeId,
    /// How many observed symptoms this node could have caused in time.
    pub symptoms_explained: usize,
    /// The latest moment it could have started and still explain all of the
    /// symptoms it explains. Later is a tighter fit: a cause that must have
    /// fired long before the first symptom explains less well than one that
    /// fired just before it.
    pub latest_onset: i64,
}

/// Rank candidate causes for a set of observed symptoms (ALGO-15).
///
/// The direction that matters operationally. An operator does not start from
/// the fault; they start from a page listing five broken services and a time
/// for each, and the question is what could have caused all five *given when
/// each was seen*.
///
/// A node explains a symptom if it has a time-respecting walk to that symptom
/// arriving no later than the symptom was observed. Ranking is by the number
/// of symptoms explained, then by the latest onset that still explains them --
/// a cause that must have fired hours earlier is a worse fit than one that
/// fired just before the first symptom, even when both are consistent.
///
/// Computed by walking **backwards** from each symptom rather than forwards
/// from every candidate: symptoms are few and candidates are the whole graph,
/// so this is `|symptoms|` traversals rather than `|nodes|`.
pub fn symptom_explanation(
    view: &GraphView,
    times: &TemporalEdges,
    symptoms: &[(usize, i64)],
) -> Result<Vec<Explanation>, TemporalError> {
    let n = view.node_count;
    for &(s, _) in symptoms {
        if s >= n {
            return Err(TemporalError::NoSuchNode(s));
        }
    }

    // `latest_departure[v]` after one backward walk from symptom `s`: the
    // latest time a fault at `v` could have started and still reached `s` by
    // its observed time. The mirror of earliest arrival, so the heap is a
    // max-heap and the guard is `t <= current`.
    let mut explained: Vec<usize> = vec![0; n];
    let mut onset: Vec<i64> = vec![i64::MAX; n];

    for &(sym, seen_at) in symptoms {
        let mut latest: Vec<Option<i64>> = vec![None; n];
        let mut heap: BinaryHeap<(i64, usize)> = BinaryHeap::new();
        latest[sym] = Some(seen_at);
        heap.push((seen_at, sym));

        while let Some((by, v)) = heap.pop() {
            if latest[v].is_some_and(|l| by < l) {
                continue; // stale
            }
            // Every edge *into* v. `in_sources` gives the neighbours but not
            // the out-edge slot the time lives in, so the slot is found on the
            // source's own out-edge list.
            let lo = view.in_offsets[v];
            let hi = view.in_offsets[v + 1];
            for k in lo..hi {
                let u = view.in_sources[k];
                let ulo = view.out_offsets[u];
                let uhi = view.out_offsets[u + 1];
                for slot in ulo..uhi {
                    if view.out_targets[slot] != v {
                        continue;
                    }
                    let t = times.at(slot);
                    // The edge must fire no later than we need to be at `v`.
                    if t > by {
                        continue;
                    }
                    if latest[u].is_none_or(|l| t > l) {
                        latest[u] = Some(t);
                        heap.push((t, u));
                    }
                }
            }
        }

        for (i, l) in latest.iter().enumerate() {
            if let Some(t) = l {
                if i == sym {
                    continue; // a symptom does not explain itself
                }
                explained[i] += 1;
                // The binding constraint across symptoms: a cause must have
                // started early enough for *every* symptom it explains, so the
                // tightest (smallest) latest-departure wins.
                onset[i] = onset[i].min(*t);
            }
        }
    }

    let mut out: Vec<Explanation> = (0..n)
        .filter(|&i| explained[i] > 0)
        .map(|i| Explanation {
            node: view.index_to_node[i],
            symptoms_explained: explained[i],
            latest_onset: onset[i],
        })
        .collect();
    // Most symptoms first; then the tightest fit; then node id, so the order
    // is total and two runs agree.
    out.sort_by(|a, b| {
        b.symptoms_explained
            .cmp(&a.symptoms_explained)
            .then(b.latest_onset.cmp(&a.latest_onset))
            .then(a.node.cmp(&b.node))
    });
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    /// A view over `edges`, plus the aligned times. Edges are given as
    /// `(from, to, time)` and inserted in that order, so the times array is
    /// built the same way the view is and the alignment is real rather than
    /// assumed.
    fn view(n: usize, edges: &[(usize, usize, i64)]) -> (GraphView, TemporalEdges) {
        let mut out: Vec<Vec<(usize, i64)>> = vec![Vec::new(); n];
        let mut inc: Vec<Vec<usize>> = vec![Vec::new(); n];
        for &(a, b, t) in edges {
            out[a].push((b, t));
            inc[b].push(a);
        }
        let (mut out_offsets, mut out_targets, mut times) = (vec![0], Vec::new(), Vec::new());
        for adj in &out {
            for &(b, t) in adj {
                out_targets.push(b);
                times.push(t);
            }
            out_offsets.push(out_targets.len());
        }
        let (mut in_offsets, mut in_sources) = (vec![0], Vec::new());
        for adj in &inc {
            in_sources.extend(adj);
            in_offsets.push(in_sources.len());
        }
        let v = GraphView {
            node_count: n,
            index_to_node: (0..n as NodeId).collect(),
            node_to_index: (0..n).map(|i| (i as NodeId, i)).collect::<HashMap<_, _>>(),
            out_offsets,
            out_targets,
            in_offsets,
            in_sources,
            weights: None,
        };
        let t = TemporalEdges::new(&v, times).expect("aligned by construction");
        (v, t)
    }

    #[test]
    fn reachability_is_not_transitive_in_time() {
        // a -> b at 10, b -> c at 5. In a static graph a reaches c; here it
        // cannot, because the second edge already fired when we arrive. This
        // is the whole difference between these algorithms and a BFS, so it
        // is the first thing asserted.
        let (v, t) = view(3, &[(0, 1, 10), (1, 2, 5)]);
        let r = temporal_reachability(&v, &t, &[0], 0).unwrap();
        assert_eq!(r, vec![(1, 10)], "c must not be reachable: {r:?}");
    }

    #[test]
    fn the_same_edges_in_a_workable_order_do_reach() {
        // The control for the test above: identical topology, times swapped.
        let (v, t) = view(3, &[(0, 1, 5), (1, 2, 10)]);
        let r = temporal_reachability(&v, &t, &[0], 0).unwrap();
        assert_eq!(r, vec![(1, 5), (2, 10)]);
    }

    #[test]
    fn an_equal_timestamp_is_traversable() {
        // Zero-duration propagation: two events at the same instant are
        // causally chainable. `>` rather than `>=` here would silently drop
        // every simultaneous hop, which is the common case in a trace.
        let (v, t) = view(3, &[(0, 1, 7), (1, 2, 7)]);
        assert_eq!(temporal_reachability(&v, &t, &[0], 7).unwrap(), vec![(1, 7), (2, 7)]);
    }

    #[test]
    fn a_start_time_after_the_edge_blocks_it() {
        let (v, t) = view(2, &[(0, 1, 5)]);
        assert!(temporal_reachability(&v, &t, &[0], 6).unwrap().is_empty());
        assert_eq!(temporal_reachability(&v, &t, &[0], 5).unwrap(), vec![(1, 5)]);
    }

    #[test]
    fn the_earliest_arrival_wins_not_the_shortest_hop_count() {
        // 0->3 directly at 20, or 0->1->2->3 arriving at 12. The three-hop
        // route is earlier, and earliest arrival is what is asked for.
        let (v, t) = view(4, &[(0, 3, 20), (0, 1, 1), (1, 2, 2), (2, 3, 12)]);
        let p = temporal_shortest_path(&v, &t, 0, 3, 0).unwrap().unwrap();
        assert_eq!(p.arrival, 12);
        assert_eq!(p.nodes, vec![0, 1, 2, 3]);
        assert_eq!(p.edge_times, vec![1, 2, 12]);
    }

    #[test]
    fn an_unreachable_target_is_none_not_an_error() {
        let (v, t) = view(3, &[(0, 1, 10), (1, 2, 5)]);
        assert!(temporal_shortest_path(&v, &t, 0, 2, 0).unwrap().is_none());
    }

    #[test]
    fn a_source_reaches_itself_at_the_start_time() {
        let (v, t) = view(2, &[(0, 1, 5)]);
        let p = temporal_shortest_path(&v, &t, 0, 0, 3).unwrap().unwrap();
        assert_eq!((p.nodes, p.edge_times, p.arrival), (vec![0], vec![], 3));
    }

    #[test]
    fn multiple_sources_are_one_traversal_not_a_union() {
        // 2 is reachable from 1 at t=4 and from 0 only via 1 at t=4 as well;
        // a union of single-source runs would still be right here, so the
        // assertion that matters is that a repeated source does not double up
        // or corrupt the arrival.
        let (v, t) = view(3, &[(0, 1, 1), (1, 2, 4)]);
        let r = temporal_reachability(&v, &t, &[0, 1, 0], 0).unwrap();
        assert_eq!(r, vec![(2, 4)]);
    }

    #[test]
    fn propagation_is_ranked_by_when_not_by_distance() {
        // 3 is two hops away but arrives before 1's other neighbour.
        let (v, t) = view(4, &[(0, 1, 1), (1, 3, 2), (0, 2, 9)]);
        let r = propagation_ranking(&v, &t, &[0], 0).unwrap();
        assert_eq!(r, vec![(1, 1), (3, 2), (2, 9)]);
    }

    #[test]
    fn a_cause_explaining_more_symptoms_ranks_higher() {
        //   0 -> 1 (t=1) -> 3 (t=2)
        //   0 -> 2 (t=1) -> 4 (t=2)
        // Symptoms at 3 and 4, both seen at t=5. Node 0 explains both; 1 and
        // 2 explain one each.
        let (v, t) = view(5, &[(0, 1, 1), (1, 3, 2), (0, 2, 1), (2, 4, 2)]);
        let e = symptom_explanation(&v, &t, &[(3, 5), (4, 5)]).unwrap();
        assert_eq!(e[0].node, 0);
        assert_eq!(e[0].symptoms_explained, 2);
        assert!(e[1..].iter().all(|x| x.symptoms_explained == 1), "{e:?}");
    }

    #[test]
    fn a_cause_too_late_to_have_done_it_is_not_offered() {
        // The edge into the symptom fires at t=9 but the symptom was seen at
        // t=4, so nothing upstream of it explains the symptom.
        let (v, t) = view(2, &[(0, 1, 9)]);
        assert!(symptom_explanation(&v, &t, &[(1, 4)]).unwrap().is_empty());
    }

    #[test]
    fn a_tighter_onset_breaks_the_tie() {
        // 0 and 1 both explain the single symptom at 2, but 1's latest
        // possible onset is later -- it fired just before, where 0 must have
        // fired long before. Later onset is the tighter fit and ranks first.
        let (v, t) = view(3, &[(0, 1, 1), (1, 2, 8)]);
        let e = symptom_explanation(&v, &t, &[(2, 10)]).unwrap();
        assert_eq!(e.len(), 2);
        assert_eq!(e[0].node, 1, "{e:?}");
        assert_eq!(e[0].latest_onset, 8);
        assert_eq!(e[1].node, 0);
    }

    #[test]
    fn a_symptom_does_not_explain_itself() {
        let (v, t) = view(2, &[(0, 1, 1)]);
        let e = symptom_explanation(&v, &t, &[(1, 5)]).unwrap();
        assert!(e.iter().all(|x| x.node != 1), "{e:?}");
    }

    #[test]
    fn misaligned_times_are_refused_rather_than_used() {
        // The failure this type exists to stop: an off-by-one array pairs
        // every edge with its neighbour's timestamp and still answers.
        let (v, _) = view(2, &[(0, 1, 5)]);
        assert_eq!(
            TemporalEdges::new(&v, vec![5, 6]).unwrap_err(),
            TemporalError::Misaligned { edges: 1, times: 2 },
        );
        assert!(TemporalEdges::new(&v, vec![]).is_err());
    }

    #[test]
    fn a_node_index_outside_the_graph_is_refused() {
        let (v, t) = view(2, &[(0, 1, 5)]);
        assert_eq!(earliest_arrival(&v, &t, &[7], 0).unwrap_err(), TemporalError::NoSuchNode(7));
        assert!(temporal_shortest_path(&v, &t, 0, 9, 0).is_err());
        assert!(symptom_explanation(&v, &t, &[(9, 1)]).is_err());
    }

    #[test]
    fn a_cycle_terminates_and_does_not_revisit_at_a_later_time() {
        // Time makes cycles finite: once you leave a node at its earliest
        // arrival, coming back later can never improve it.
        let (v, t) = view(3, &[(0, 1, 1), (1, 2, 2), (2, 0, 3), (0, 1, 4)]);
        let r = temporal_reachability(&v, &t, &[0], 0).unwrap();
        assert_eq!(r, vec![(1, 1), (2, 2)]);
    }

    #[test]
    fn parallel_edges_take_the_usable_one() {
        // Two edges 0->1, at 2 and at 8. From start=5 only the later one is
        // available, so the arrival is 8 rather than unreachable.
        let (v, t) = view(2, &[(0, 1, 2), (0, 1, 8)]);
        assert_eq!(temporal_reachability(&v, &t, &[0], 5).unwrap(), vec![(1, 8)]);
        assert_eq!(temporal_reachability(&v, &t, &[0], 0).unwrap(), vec![(1, 2)]);
    }
}
