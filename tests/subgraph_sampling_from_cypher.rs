//! `CALL algo.sample.*`: subgraph sampling from Cypher (ML-03, #1720).
//!
//! The issue found `algo.forestFire` and `algo.forestFireSample` refused as
//! `UnknownAlgorithm`. These tests go through the parser, the planner and the
//! operator for each of the five methods, on graphs whose answers can be
//! worked out by hand: a 10 x 10 grid, a binary tree and a labelled population.

use std::collections::{HashMap, HashSet};

use samyama::graph::{GraphStore, Label, NodeId, PropertyValue};
use samyama::query::executor::{QueryExecutor, Value};
use samyama::query::parser::parse_query;

/// Run `call`, yielding `node` and `column`: `(id(node), column)` per row.
fn try_rows(store: &GraphStore, call: &str, column: &str) -> Result<Vec<(u64, PropertyValue)>, String> {
    let cypher = format!("{call} YIELD node, {column} RETURN id(node) AS n, {column} AS v");
    let q = parse_query(&cypher).map_err(|e| format!("{e:?}"))?;
    let result = QueryExecutor::new(store).with_params(HashMap::new()).execute(&q).map_err(|e| e.to_string())?;
    Ok(result
        .records
        .iter()
        .map(|r| {
            let n = match r.get("n") {
                Some(Value::Property(PropertyValue::Integer(n))) => *n as u64,
                other => panic!("{cypher}: id was {other:?}"),
            };
            let v = match r.get("v") {
                Some(Value::Property(p)) => p.clone(),
                other => panic!("{cypher}: {column} was {other:?}"),
            };
            (n, v)
        })
        .collect())
}

fn rows(store: &GraphStore, call: &str, column: &str) -> Vec<(u64, i64)> {
    try_rows(store, call, column)
        .unwrap_or_else(|e| panic!("{call}: {e}"))
        .into_iter()
        .map(|(n, v)| match v {
            PropertyValue::Integer(i) => (n, i),
            other => panic!("{call}: {column} was {other:?}"),
        })
        .collect()
}

fn ids(rows: &[(u64, i64)]) -> Vec<u64> {
    rows.iter().map(|r| r.0).collect()
}

fn refused(store: &GraphStore, call: &str, column: &str) -> String {
    match try_rows(store, call, column) {
        Ok(r) => panic!("{call} should be refused, returned {} rows", r.len()),
        Err(e) => e,
    }
}

/// A 10 x 10 grid of `Cell`s joined both ways by `NEXT`, plus one `Tag` per
/// cell joined by `TAGGED`, which the samplers must ignore when told to.
struct Grid {
    store: GraphStore,
    cell: Vec<NodeId>,
    next: HashSet<(u64, u64)>,
}

fn grid() -> Grid {
    let mut s = GraphStore::new();
    let cell: Vec<NodeId> = (0..100).map(|_| s.create_node_with_labels([Label::new("Cell")])).collect();
    let mut next = HashSet::new();
    let mut join = |s: &mut GraphStore, a: usize, b: usize| {
        for (x, y) in [(a, b), (b, a)] {
            s.create_edge(cell[x], cell[y], "NEXT").unwrap();
            next.insert((cell[x].as_u64(), cell[y].as_u64()));
        }
    };
    for r in 0..10 {
        for c in 0..10 {
            let i = r * 10 + c;
            if c < 9 {
                join(&mut s, i, i + 1);
            }
            if r < 9 {
                join(&mut s, i, i + 10);
            }
        }
    }
    for &c in &cell {
        let t = s.create_node_with_labels([Label::new("Tag")]);
        s.create_edge(c, t, "TAGGED").unwrap();
    }
    Grid { store: s, cell, next }
}

impl Grid {
    fn id(&self, i: usize) -> u64 {
        self.cell[i].as_u64()
    }
    /// Grid position of a cell id.
    fn pos(&self, id: u64) -> (i64, i64) {
        let i = self.cell.iter().position(|c| c.as_u64() == id).expect("a cell");
        ((i / 10) as i64, (i % 10) as i64)
    }
    fn adjacent(&self, a: u64, b: u64) -> bool {
        self.next.contains(&(a, b))
    }
}

fn distinct(v: &[u64]) -> bool {
    v.iter().collect::<HashSet<_>>().len() == v.len()
}

// --- forest fire ---------------------------------------------------------------

#[test]
fn forest_fire_returns_size_distinct_nodes_in_burn_order_and_is_seeded() {
    let g = grid();
    let call = |seed: i64| {
        format!(
            "CALL algo.sample.forestFire({}, {{size: 30, edgeType: 'NEXT', seed: {seed}}})",
            g.id(0)
        )
    };
    let got = rows(&g.store, &call(7), "position");
    assert_eq!(got.len(), 30);
    assert!(distinct(&ids(&got)));
    assert_eq!(got.iter().map(|r| r.1).collect::<Vec<_>>(), (0..30).collect::<Vec<_>>());
    assert_eq!(got[0].0, g.id(0), "the fire starts at the seed");
    assert!(got.iter().all(|r| g.pos(r.0).0 >= 0), "only cells: the NEXT filter holds");
    // On a connected grid at p = 0.7 almost every node catches fire from
    // one burnt before it.
    let along = (1..got.len()).filter(|&i| got[..i].iter().any(|b| g.adjacent(b.0, got[i].0))).count();
    assert!(along >= 25, "{along} of 29 caught fire from a neighbour");
    assert_eq!(got, rows(&g.store, &call(7), "position"));
    assert_ne!(got, rows(&g.store, &call(8), "position"));
}

#[test]
fn forest_fire_can_start_anywhere_and_answers_to_the_names_tried() {
    let g = grid();
    for name in ["algo.sample.forestFire", "algo.forestFire", "algo.forestFireSample", "algo.forestFireSampling"] {
        let got = rows(&g.store, &format!("CALL {name}({{size: 12, label: 'Cell', edgeType: 'NEXT'}})"), "position");
        assert_eq!(got.len(), 12, "{name}");
        assert!(distinct(&ids(&got)), "{name}");
    }
}

// --- snowball --------------------------------------------------------------------

/// A binary tree of 31 nodes: `CHILD` from each node to its two children.
fn tree() -> (GraphStore, Vec<NodeId>) {
    let mut s = GraphStore::new();
    let n: Vec<NodeId> = (0..31).map(|_| s.create_node_with_labels([Label::new("T")])).collect();
    for v in 1..31 {
        s.create_edge(n[(v - 1) / 2], n[v], "CHILD").unwrap();
    }
    (s, n)
}

#[test]
fn snowball_takes_waves_and_per_node_caps_each_one() {
    let (s, n) = tree();
    let id = |i: usize| n[i].as_u64();
    let all = rows(&s, &format!("CALL algo.sample.snowball({}, {{hops: 2}})", id(0)), "hop");
    assert_eq!(all, vec![(id(0), 0), (id(1), 1), (id(2), 1), (id(3), 2), (id(4), 2), (id(5), 2), (id(6), 2)]);
    let chain = rows(&s, &format!("CALL algo.snowball({}, {{hops: 4, perNode: 1, seed: 3}})", id(0)), "hop");
    assert_eq!(chain.len(), 5, "one child per node for four waves");
    let parent = |c: u64| n.iter().position(|x| x.as_u64() == c).map(|i| n[(i - 1) / 2].as_u64());
    for w in chain.windows(2) {
        assert_eq!(parent(w[1].0), Some(w[0].0), "{chain:?} follows CHILD");
        assert_eq!(w[1].1, w[0].1 + 1);
    }
    let capped = rows(&s, &format!("CALL algo.sample.snowball({}, {{hops: 4, maxNodes: 6}})", id(0)), "hop");
    assert_eq!(capped.len(), 6);
}

// --- ego network -----------------------------------------------------------------

#[test]
fn an_ego_network_is_exactly_the_cells_within_k_steps() {
    let g = grid();
    let got = rows(&g.store, &format!("CALL algo.sample.egoNetwork({}, {{hops: 3, edgeType: 'NEXT'}})", g.id(0)), "distance");
    assert_eq!(got.len(), 1 + 2 + 3 + 4);
    for &(id, d) in &got {
        let (r, c) = g.pos(id);
        assert_eq!(r + c, d, "cell {id} at Manhattan distance {d}");
    }
    // Without the filter the tags join in: 10 cells and the 6 tags of the
    // cells within 2 steps (a tag is one step beyond its cell).
    let wide = rows(&g.store, &format!("CALL algo.egoNetwork({}, {{hops: 3}})", g.id(0)), "distance");
    assert_eq!(wide.len(), 10 + 6);
    assert_eq!(rows(&g.store, &format!("CALL algo.ego({}, {{hops: 0}})", g.id(0)), "distance"), vec![(g.id(0), 0)]);
}

#[test]
fn direction_decides_which_side_an_ego_network_grows() {
    let (s, n) = tree();
    let id = |i: usize| n[i].as_u64();
    let call = |d: &str| format!("CALL algo.sample.egoNetwork({}, {{hops: 1, direction: '{d}'}})", id(1));
    assert_eq!(ids(&rows(&s, &call("out"), "distance")), vec![id(1), id(3), id(4)]);
    assert_eq!(ids(&rows(&s, &call("in"), "distance")), vec![id(1), id(0)]);
    assert_eq!(rows(&s, &call("both"), "distance").len(), 4);
}

// --- random walk -------------------------------------------------------------------

#[test]
fn a_random_walk_sample_follows_next_and_is_seeded() {
    let g = grid();
    let call = |seed: i64| {
        format!(
            "CALL algo.sample.randomWalk({}, {{size: 25, restartProbability: 0.0, edgeType: 'NEXT', seed: {seed}}})",
            g.id(44)
        )
    };
    let got = rows(&g.store, &call(9), "position");
    assert_eq!(got.len(), 25);
    assert!(distinct(&ids(&got)));
    assert_eq!(got[0].0, g.id(44));
    for i in 1..got.len() {
        assert!(got[..i].iter().any(|b| g.adjacent(b.0, got[i].0)), "{} reached without a NEXT step", got[i].0);
    }
    assert_eq!(got, rows(&g.store, &call(9), "position"));
    assert_ne!(got, rows(&g.store, &call(10), "position"));
    assert_eq!(rows(&g.store, &format!("CALL algo.randomWalkSample({}, {{size: 5}})", g.id(0)), "position").len(), 5);
}

// --- stratified --------------------------------------------------------------------

fn population() -> GraphStore {
    let mut s = GraphStore::new();
    for (label, n) in [("Paper", 60), ("Author", 30), ("Venue", 10)] {
        for _ in 0..n {
            s.create_node_with_labels([Label::new(label)]);
        }
    }
    // Two nodes with both labels: their own group, "Author:Paper".
    for _ in 0..2 {
        s.create_node_with_labels([Label::new("Paper"), Label::new("Author")]);
    }
    s
}

fn strata(store: &GraphStore, call: &str) -> Vec<(u64, String)> {
    try_rows(store, call, "stratum")
        .unwrap_or_else(|e| panic!("{call}: {e}"))
        .into_iter()
        .map(|(n, v)| match v {
            PropertyValue::String(s) => (n, s),
            other => panic!("stratum was {other:?}"),
        })
        .collect()
}

#[test]
fn a_stratified_sample_keeps_the_label_mix() {
    let s = population();
    let got = strata(&s, "CALL algo.sample.stratified({fraction: 0.1, seed: 4})");
    let count = |g: &str| got.iter().filter(|(_, st)| st == g).count();
    assert_eq!((count("Paper"), count("Author"), count("Venue")), (6, 3, 1));
    assert_eq!(count("Author:Paper"), 0, "0.1 of a group of 2 rounds to 0");
    // Each node is reported in its own group.
    for (id, stratum) in &got {
        let labels: HashSet<String> = s.get_node(NodeId::new(*id)).unwrap().labels.iter().map(|l| l.as_str().to_string()).collect();
        assert_eq!(labels, stratum.split(':').map(str::to_string).collect(), "node {id}");
    }
    assert_eq!(got, strata(&s, "CALL algo.stratifiedSample({fraction: 0.1, seed: 4})"));
    assert_ne!(got, strata(&s, "CALL algo.sample.stratified({fraction: 0.1, seed: 5})"));
    let per = strata(&s, "CALL algo.sample.stratified({perStratum: 20})");
    let count = |g: &str| per.iter().filter(|(_, st)| st == g).count();
    assert_eq!((count("Paper"), count("Author"), count("Venue"), count("Author:Paper")), (20, 20, 10, 2));
}

// --- refusals ----------------------------------------------------------------------

#[test]
fn bad_options_are_refused_with_what_to_pass_instead() {
    let g = grid();
    let c = g.id(0);
    let cases: Vec<(String, &str, &str)> = vec![
        (format!("CALL algo.sample.forestFire({c})"), "position", "`size` is required"),
        (format!("CALL algo.sample.forestFire({c}, {{size: 0}})"), "position", "`size` must be a whole number of at least 1"),
        (format!("CALL algo.sample.forestFire({c}, {{size: 201}})"), "position", "has only 200 nodes"),
        (format!("CALL algo.sample.forestFire({c}, {{size: 5, forwardProbability: 1.5}})"), "position", "between 0 and 1"),
        (format!("CALL algo.sample.randomWalk({c}, {{size: 5, restartProbability: -0.1}})"), "position", "between 0 and 1"),
        (format!("CALL algo.sample.randomWalk({c}, {{size: 5, maxSteps: 0}})"), "position", "`maxSteps`"),
        (format!("CALL algo.sample.snowball({c}, {{perNode: 0}})"), "hop", "`perNode` must be at least 1"),
        (format!("CALL algo.sample.snowball({c}, {{maxNodes: 0}})"), "hop", "`maxNodes`"),
        (format!("CALL algo.sample.snowball({c}, {{hops: 0}})"), "hop", "`hops`"),
        (format!("CALL algo.sample.egoNetwork({c}, {{hops: -1}})"), "distance", "`hops`"),
        (format!("CALL algo.sample.egoNetwork({c}, {{direction: 'sideways'}})"), "distance", "use 'out', 'in' or 'both'"),
        (format!("CALL algo.sample.egoNetwork({c}, {{radius: 2}})"), "distance", "radius"),
        ("CALL algo.sample.snowball({hops: 2})".to_string(), "hop", "first argument must be a seed node id"),
        ("CALL algo.sample.egoNetwork([], {hops: 1})".to_string(), "distance", "list of seed nodes is empty"),
        ("CALL algo.sample.egoNetwork(['x'], {hops: 1})".to_string(), "distance", "every seed must be a node id"),
        ("CALL algo.sample.egoNetwork(999999, {hops: 1})".to_string(), "distance", "node 999999 is not in the projected graph"),
        (format!("CALL algo.sample.egoNetwork({}, {{label: 'Cell'}})", g.store.all_nodes().iter().map(|n| n.id.as_u64()).max().unwrap()), "distance", "not in the projected graph"),
        ("CALL algo.sample.stratified({})".to_string(), "stratum", "`fraction` (e.g. 0.1) or `perStratum`"),
        ("CALL algo.sample.stratified({fraction: 0.1, perStratum: 3})".to_string(), "stratum", "not both"),
        ("CALL algo.sample.stratified({fraction: 1.5})".to_string(), "stratum", "above 0 and at most 1"),
        ("CALL algo.sample.stratified({fraction: 0.0})".to_string(), "stratum", "above 0 and at most 1"),
        (format!("CALL algo.sample.stratified({c}, {{fraction: 0.1}})"), "stratum", "takes no seed nodes"),
        ("CALL algo.sample.stratified({fraction: 0.1, graph: 'g'})".to_string(), "stratum", "cannot run on a graph projection"),
    ];
    for (call, column, wanted) in cases {
        let e = refused(&g.store, &call, column);
        assert!(e.contains(wanted), "{call}\n  wanted {wanted:?}\n  got {e}");
    }
}
