//! `CALL algo.sample.neighbors(...)`: GNN-style neighbour sampling from Cypher
//! (ML-02, #1717).
//!
//! The issue found every spelling refused as `UnknownAlgorithm`. These tests
//! go through the parser, the planner and the operator, on graphs whose
//! answers can be counted by hand, and check the rules the module documents:
//! the fanout caps each hop, every row is a relationship, the same seed gives
//! the same rows, weights move the odds, and bad options are refused.

use std::collections::{HashMap, HashSet};

use samyama::graph::{GraphStore, Label, NodeId, PropertyValue};
use samyama::query::executor::{QueryExecutor, Value};
use samyama::query::parser::parse_query;

/// `(batch, hop, node, neighbour)`, ids as numbers.
type Row = (i64, i64, u64, u64);

const YIELD: &str = "YIELD batch, hop, node, neighbour RETURN batch, hop, id(node) AS n, id(neighbour) AS m";

fn try_rows(store: &GraphStore, call: &str, params: &[(&str, PropertyValue)]) -> Result<Vec<Row>, String> {
    let cypher = format!("{call} {YIELD}");
    let q = parse_query(&cypher).map_err(|e| format!("{e:?}"))?;
    let params: HashMap<String, PropertyValue> =
        params.iter().map(|(k, v)| (k.to_string(), v.clone())).collect();
    let result = QueryExecutor::new(store).with_params(params).execute(&q).map_err(|e| e.to_string())?;
    let int = |v: Option<&Value>| match v {
        Some(Value::Property(PropertyValue::Integer(n))) => *n,
        other => panic!("{cypher}: expected an integer, got {other:?}"),
    };
    Ok(result
        .records
        .iter()
        .map(|r| (int(r.get("batch")), int(r.get("hop")), int(r.get("n")) as u64, int(r.get("m")) as u64))
        .collect())
}

fn rows(store: &GraphStore, call: &str) -> Vec<Row> {
    try_rows(store, call, &[]).unwrap_or_else(|e| panic!("{call}: {e}"))
}

fn refused(store: &GraphStore, call: &str) -> String {
    match try_rows(store, call, &[]) {
        Ok(r) => panic!("{call} should be refused, returned {} rows", r.len()),
        Err(e) => e,
    }
}

fn ids(list: &[NodeId]) -> String {
    format!("[{}]", list.iter().map(|n| n.as_u64().to_string()).collect::<Vec<_>>().join(", "))
}

/// A citation tree: `root` cites 20 papers; each of those cites 3 more.
/// Every relationship is `CITES` with weight 1, and each paper has an author
/// joined by `WROTE`, which the sampling must ignore when asked to.
struct Tree {
    store: GraphStore,
    root: NodeId,
    papers: Vec<NodeId>,
    cites: HashSet<(u64, u64)>,
}

fn tree() -> Tree {
    let mut s = GraphStore::new();
    let paper = |s: &mut GraphStore| s.create_node_with_labels([Label::new("Paper")]);
    let root = paper(&mut s);
    let mut papers = vec![root];
    let mut cites = HashSet::new();
    let mut cite = |s: &mut GraphStore, a: NodeId, b: NodeId| {
        let e = s.create_edge(a, b, "CITES").unwrap();
        s.set_edge_property(e, "w", PropertyValue::Integer(1)).unwrap();
        cites.insert((a.as_u64(), b.as_u64()));
    };
    for _ in 0..20 {
        let p = paper(&mut s);
        papers.push(p);
        cite(&mut s, root, p);
        for _ in 0..3 {
            let q = paper(&mut s);
            papers.push(q);
            cite(&mut s, p, q);
        }
    }
    for &p in &papers.clone() {
        let a = s.create_node_with_labels([Label::new("Author")]);
        s.create_edge(a, p, "WROTE").unwrap();
    }
    Tree { store: s, root, papers, cites }
}

// --- the shape of a sample ---------------------------------------------------

#[test]
fn the_fanout_caps_each_hop_and_every_row_is_a_relationship() {
    let t = tree();
    let call = format!(
        "CALL algo.sample.neighbors({}, {{fanout: [5, 2], edgeType: 'CITES'}})",
        t.root.as_u64()
    );
    let got = rows(&t.store, &call);
    let hop1: Vec<&Row> = got.iter().filter(|r| r.1 == 1).collect();
    let hop2: Vec<&Row> = got.iter().filter(|r| r.1 == 2).collect();
    assert_eq!(hop1.len(), 5, "5 of the root's 20 citations");
    assert_eq!(hop2.len(), 10, "2 of 3 for each of the 5");
    for r in &got {
        assert_eq!(r.0, 0, "one batch");
        assert!(t.cites.contains(&(r.2, r.3)), "{r:?} is not a CITES relationship");
    }
    assert!(hop1.iter().all(|r| r.2 == t.root.as_u64()));
    let drawn: HashSet<u64> = hop1.iter().map(|r| r.3).collect();
    assert_eq!(drawn.len(), 5, "no neighbour twice");
    assert_eq!(hop2.iter().map(|r| r.2).collect::<HashSet<_>>(), drawn, "hop 2 expands what hop 1 drew");
}

#[test]
fn minus_one_takes_every_neighbour() {
    let t = tree();
    let call = format!("CALL algo.sample.neighbors({}, {{fanout: [-1, -1], edgeType: 'CITES'}})", t.root.as_u64());
    let got = rows(&t.store, &call);
    assert_eq!(got.len(), 20 + 60);
    assert_eq!(got.iter().map(|r| (r.2, r.3)).collect::<HashSet<_>>(), t.cites);
}

#[test]
fn the_same_seed_gives_the_same_rows_and_another_seed_does_not() {
    let t = tree();
    let call = |seed: i64| {
        format!("CALL algo.sample.neighbors({}, {{fanout: [4, 2], seed: {seed}, edgeType: 'CITES'}})", t.root.as_u64())
    };
    assert_eq!(rows(&t.store, &call(7)), rows(&t.store, &call(7)));
    assert_ne!(rows(&t.store, &call(7)), rows(&t.store, &call(8)));
    // No seed is seed 42, and still reproducible.
    let default = format!("CALL algo.sample.neighbors({}, {{fanout: [4, 2], edgeType: 'CITES'}})", t.root.as_u64());
    assert_eq!(rows(&t.store, &default), rows(&t.store, &call(42)));
}

#[test]
fn seeds_can_be_a_list_or_a_parameter_and_batch_size_splits_them() {
    let t = tree();
    let seeds = [t.papers[1], t.papers[5], t.papers[9]];
    let listed = format!(
        "CALL algo.sample.neighbors({}, {{fanout: [-1], batchSize: 2, edgeType: 'CITES'}})",
        ids(&seeds)
    );
    let got = rows(&t.store, &listed);
    let param = PropertyValue::Array(seeds.iter().map(|n| PropertyValue::Integer(n.as_u64() as i64)).collect());
    let via_param = try_rows(
        &t.store,
        "CALL algo.sample.neighbors($seeds, {fanout: [-1], batchSize: 2, edgeType: 'CITES'})",
        &[("seeds", param)],
    )
    .unwrap();
    assert_eq!(got, via_param);
    // Batch 0 holds the first two seeds, batch 1 the third; 3 citations each.
    let in_batch = |b: i64| got.iter().filter(|r| r.0 == b).map(|r| r.2).collect::<HashSet<_>>();
    assert_eq!(in_batch(0), HashSet::from([seeds[0].as_u64(), seeds[1].as_u64()]));
    assert_eq!(in_batch(1), HashSet::from([seeds[2].as_u64()]));
    assert_eq!(got.len(), 9);
}

#[test]
fn edge_type_and_direction_decide_who_the_neighbours_are() {
    let t = tree();
    let p = t.papers[1]; // cited by the root, cites 3, written by one author
    let call = |cfg: &str| format!("CALL algo.sample.neighbors({}, {{fanout: [-1], {cfg}}})", p.as_u64());
    assert_eq!(rows(&t.store, &call("edgeType: 'CITES'")).len(), 3, "out: the 3 it cites");
    let incoming = rows(&t.store, &call("edgeType: 'CITES', direction: 'in'"));
    assert_eq!(incoming.iter().map(|r| r.3).collect::<Vec<_>>(), vec![t.root.as_u64()], "in: the root");
    assert_eq!(rows(&t.store, &call("edgeType: 'CITES', direction: 'both'")).len(), 4);
    // Every relationship type: the author joins the incoming side.
    assert_eq!(rows(&t.store, &call("direction: 'both'")).len(), 5);
}

#[test]
fn every_spelling_reaches_the_procedure() {
    let t = tree();
    for name in [
        "algo.sample.neighbors", "algo.sample.neighbours", "algo.sampleNeighbors",
        "algo.sampleNeighbours", "algo.neighbourSampling", "algo.neighborSampling",
        "sample.neighbors",
    ] {
        let call = format!("CALL {name}({}, {{fanout: [-1], edgeType: 'CITES'}})", t.root.as_u64());
        assert_eq!(rows(&t.store, &call).len(), 20, "{name}");
    }
}

// --- weights -------------------------------------------------------------------

/// `hub` -> 10 leaves; leaf 0 has weight `heavy`, leaf 1 weight 0, the rest 1.
fn weighted_star(heavy: f64) -> (GraphStore, NodeId, Vec<NodeId>) {
    let mut s = GraphStore::new();
    let hub = s.create_node_with_labels([Label::new("N")]);
    let mut leaves = Vec::new();
    for i in 0..10 {
        let leaf = s.create_node_with_labels([Label::new("N")]);
        let e = s.create_edge(hub, leaf, "R").unwrap();
        let w = match i { 0 => heavy, 1 => 0.0, _ => 1.0 };
        s.set_edge_property(e, "w", PropertyValue::Float(w)).unwrap();
        leaves.push(leaf);
    }
    (s, hub, leaves)
}

#[test]
fn weighted_sampling_follows_the_weight_and_never_draws_a_zero() {
    // Weight 72 against 8 leaves of weight 1: p(heavy) = 72/80 = 0.9.
    let (s, hub, leaves) = weighted_star(72.0);
    let trials = 1000;
    let (mut heavy, mut zero, mut uniform_heavy) = (0, 0, 0);
    for seed in 0..trials {
        let call = format!("CALL algo.sample.neighbors({}, {{fanout: [1], weightProperty: 'w', seed: {seed}}})", hub.as_u64());
        let got = rows(&s, &call);
        assert_eq!(got.len(), 1);
        heavy += usize::from(got[0].3 == leaves[0].as_u64());
        zero += usize::from(got[0].3 == leaves[1].as_u64());
        let flat = format!("CALL algo.sample.neighbors({}, {{fanout: [1], seed: {seed}}})", hub.as_u64());
        uniform_heavy += usize::from(rows(&s, &flat)[0].3 == leaves[0].as_u64());
    }
    // Mean 900, sd ~9.5: 860..940 is over 4 sd either side.
    assert!((860..=940).contains(&heavy), "heavy drawn {heavy} of {trials}");
    assert_eq!(zero, 0, "weight 0 is never drawn");
    // Without `weightProperty` it is one of ten: mean 100, sd ~9.5.
    assert!((60..=140).contains(&uniform_heavy), "uniform drew heavy {uniform_heavy} of {trials}");
}

#[test]
fn a_weight_that_cannot_be_a_probability_is_refused_naming_the_relationship() {
    for bad in [-1.0, f64::INFINITY] {
        let (s, hub, leaves) = weighted_star(bad);
        let e = refused(&s, &format!("CALL algo.sample.neighbors({}, {{fanout: [1], weightProperty: 'w'}})", hub.as_u64()));
        assert!(e.contains(&format!("from node {} to node {}", hub.as_u64(), leaves[0].as_u64())), "{e}");
        assert!(e.contains("`w`") && e.contains("at least 0"), "{e}");
    }
}

// --- refusals --------------------------------------------------------------------

#[test]
fn bad_options_are_refused_with_what_to_pass_instead() {
    let t = tree();
    let r = t.root.as_u64();
    let cases: Vec<(String, &str)> = vec![
        (format!("CALL algo.sample.neighbors({r})"), "`fanout` is required"),
        (format!("CALL algo.sample.neighbors({r}, {{fanout: []}})"), "`fanout` is empty"),
        (format!("CALL algo.sample.neighbors({r}, {{fanout: [5, 0]}})"), "`fanout` entry 1"),
        (format!("CALL algo.sample.neighbors({r}, {{fanout: [-2]}})"), "or -1 for every neighbour"),
        (format!("CALL algo.sample.neighbors({r}, {{fanout: 5}})"), "must be a list"),
        (format!("CALL algo.sample.neighbors({r}, {{fanout: [1], batchSize: 0}})"), "`batchSize` must be at least 1"),
        (format!("CALL algo.sample.neighbors({r}, {{fanout: [1], direction: 'up'}})"), "use 'out', 'in' or 'both'"),
        (format!("CALL algo.sample.neighbors({r}, {{fanout: [1], fanouts: [2]}})"), "fanouts"),
        ("CALL algo.sample.neighbors([], {fanout: [1]})".to_string(), "list of seed nodes is empty"),
        ("CALL algo.sample.neighbors(['a'], {fanout: [1]})".to_string(), "every seed must be a node id"),
        ("CALL algo.sample.neighbors({fanout: [1]})".to_string(), "first argument must be a node id"),
        ("CALL algo.sample.neighbors(999999, {fanout: [1]})".to_string(), "node 999999 is not in the projected graph"),
    ];
    for (call, wanted) in cases {
        let e = refused(&t.store, &call);
        assert!(e.contains(wanted), "{call}\n  wanted {wanted:?}\n  got {e}");
    }
    // A seed the label filter leaves out is not in the projection either.
    let author_filtered = format!("CALL algo.sample.neighbors({r}, {{fanout: [1], label: 'Author'}})");
    assert!(refused(&t.store, &author_filtered).contains("not in the projected graph"));
}

/// Throughput on a larger graph, for the PR's numbers; not a pass/fail bar.
/// `cargo test --release --test neighbour_sampling_from_cypher -- --ignored --nocapture`
#[test]
#[ignore]
fn throughput() {
    use std::time::Instant;
    let (n, degree) = (100_000u64, 10u64);
    let mut s = GraphStore::new();
    let nodes: Vec<NodeId> = (0..n).map(|_| s.create_node_with_labels([Label::new("N")])).collect();
    // Each node cites `degree` pseudo-random others.
    let mut x = 88172645463325252u64;
    for &a in &nodes {
        for _ in 0..degree {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            let b = nodes[(x % n) as usize];
            if a != b {
                s.create_edge(a, b, "R").unwrap();
            }
        }
    }
    let seeds: Vec<NodeId> = nodes.iter().step_by(10).copied().collect();
    let call = format!("CALL algo.sample.neighbors({}, {{fanout: [15, 10], batchSize: 512}})", ids(&seeds));
    let t0 = Instant::now();
    let got = rows(&s, &call);
    let whole = t0.elapsed();
    println!(
        "{} nodes, ~{} relationships: {} seeds, {} rows in {:.3}s (whole call, including the graph view): {:.0} seeds/s",
        n, n * degree, seeds.len(), got.len(), whole.as_secs_f64(), seeds.len() as f64 / whole.as_secs_f64()
    );

    // The same sample with the view built once, as a trainer drawing many
    // minibatches from one graph would: the sampling alone.
    use samyama::algo::{build_view, sample_neighbours, Fanout, SampleDirection, SampleSpec};
    let t1 = Instant::now();
    let view = build_view(&s, None, None, None);
    let view_time = t1.elapsed();
    let idx: Vec<usize> = seeds.iter().map(|id| view.node_to_index[&id.as_u64()]).collect();
    let spec = SampleSpec {
        fanouts: vec![Fanout::AtMost(15), Fanout::AtMost(10)],
        direction: SampleDirection::Out,
        weighted: false,
        batch_size: 512,
        seed: 42,
    };
    let t2 = Instant::now();
    let sampled = sample_neighbours(&view, &idx, &spec).unwrap();
    let sample_time = t2.elapsed();
    assert_eq!(sampled.len(), got.len(), "the same sample as the Cypher call");
    println!(
        "graph view built in {:.3}s; sampling alone {:.3}s: {:.0} seeds/s",
        view_time.as_secs_f64(), sample_time.as_secs_f64(), idx.len() as f64 / sample_time.as_secs_f64()
    );
}
