//! `algo.structural.astar` and `algo.structural.stress` from Cypher (#2037).
//!
//! The guided path is checked against the engine's own unguided answers
//! (`algo.weightedPath` for length, `algo.shortestPath` for hops) rather than
//! against numbers written here, so "same path, less work" is shown on each
//! graph instead of asserted once.

use std::collections::HashMap;

use samyama::graph::{GraphStore, NodeId, PropertyValue};
use samyama::query::QueryEngine;

fn run(s: &mut GraphStore, q: &str) {
    QueryEngine::new()
        .execute_mut(q, s, "default")
        .unwrap_or_else(|e| panic!("{q}: {e}"));
}

fn rows(s: &GraphStore, q: &str) -> Result<Vec<HashMap<String, PropertyValue>>, String> {
    let batch = QueryEngine::new()
        .execute(q, s)
        .map_err(|e| e.to_string())?;
    Ok(batch
        .records
        .iter()
        .map(|r| {
            r.bound_variables()
                .into_iter()
                .map(|k| {
                    let v = r
                        .get(&k)
                        .and_then(|v| v.as_property())
                        .cloned()
                        .unwrap_or(PropertyValue::Null);
                    (k, v)
                })
                .collect()
        })
        .collect())
}

fn float(v: &PropertyValue) -> f64 {
    v.as_float()
        .or_else(|| v.as_integer().map(|i| i as f64))
        .unwrap_or_else(|| panic!("not a number: {v:?}"))
}

fn ids(v: &PropertyValue) -> Vec<i64> {
    match v {
        PropertyValue::Array(a) => a.iter().map(|x| x.as_integer().unwrap()).collect(),
        other => panic!("not a list: {other:?}"),
    }
}

fn dist(a: [f64; 3], b: [f64; 3]) -> f64 {
    (0..3).map(|i| (a[i] - b[i]).powi(2)).sum::<f64>().sqrt()
}

/// A jittered `side x side x 2` grid of `:Residue`, each joined to its
/// neighbours along x and y and through z, plus a few long jumps; every
/// relationship carries its straight-line length as `w`.
fn grid(side: usize) -> (GraphStore, Vec<NodeId>, Vec<[f64; 3]>) {
    let mut s = GraphStore::new();
    let mut x = 0x2545_F491_4F6C_DD1Du64;
    let mut jitter = move || {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        (x % 1000) as f64 / 1000.0 * 0.3
    };
    let (mut ids, mut pos) = (Vec::new(), Vec::new());
    for z in 0..2 {
        for j in 0..side {
            for i in 0..side {
                let p = [
                    i as f64 + jitter(),
                    j as f64 + jitter(),
                    z as f64 * 1.2 + jitter(),
                ];
                let id = s.create_node("Residue");
                for (k, v) in ["x", "y", "z"].iter().zip(p) {
                    s.set_node_property("default", id, *k, v).unwrap();
                }
                ids.push(id);
                pos.push(p);
            }
        }
    }
    let at = |i: usize, j: usize, z: usize| z * side * side + j * side + i;
    let mut edges = Vec::new();
    for z in 0..2 {
        for j in 0..side {
            for i in 0..side {
                if i + 1 < side {
                    edges.push((at(i, j, z), at(i + 1, j, z)));
                }
                if j + 1 < side {
                    edges.push((at(i, j, z), at(i, j + 1, z)));
                }
                if z == 0 {
                    edges.push((at(i, j, 0), at(i, j, 1)));
                }
            }
        }
    }
    edges.extend([
        (at(0, side - 1, 0), at(side - 1, 0, 1)),
        (at(1, 1, 1), at(side - 2, side - 2, 0)),
    ]);
    for (a, b) in edges {
        let e = s.create_edge(ids[a], ids[b], "BOND").unwrap();
        s.set_edge_property(e, "w", dist(pos[a], pos[b])).unwrap();
    }
    run(
        &mut s,
        "CREATE STRUCTURE INDEX pdb ON :Residue(x, y, z) METRIC euclidean",
    );
    (s, ids, pos)
}

fn astar(
    s: &GraphStore,
    a: NodeId,
    b: NodeId,
    config: &str,
) -> Result<Vec<HashMap<String, PropertyValue>>, String> {
    rows(
        s,
        &format!(
            "CALL algo.structural.astar({}, {}, {config}) YIELD path, hops, length, direct, stretch, expanded, expandedUnguided \
             RETURN path, hops, length, direct, stretch, expanded, expandedUnguided",
            a.as_u64(),
            b.as_u64()
        ),
    )
}

#[test]
fn guided_length_paths_match_the_unguided_weighted_path_and_save_work() {
    let (s, ids, pos) = grid(12);
    let n = ids.len();
    let (mut saved, mut compared) = (0, 0);
    for (a, b) in [(0, n - 1), (5, n / 2 + 7), (n - 13, 30)] {
        let got = &astar(&s, ids[a], ids[b], "{structure: 'pdb', cost: 'length'}").unwrap()[0];
        let length = float(&got["length"]);
        // Following direction, the guided length is exactly what the unguided
        // weighted search finds -- including when neither finds a path.
        let reference = rows(
            &s,
            &format!(
                "CALL algo.weightedPath({}, {}, 'w') YIELD cost RETURN cost",
                ids[a].as_u64(),
                ids[b].as_u64()
            ),
        )
        .unwrap();
        let directed = astar(
            &s,
            ids[a],
            ids[b],
            "{structure: 'pdb', cost: 'length', undirected: false}",
        )
        .unwrap();
        match (directed.first(), reference.first()) {
            (Some(d), Some(r)) => {
                assert!(
                    (float(&d["length"]) - float(&r["cost"])).abs() < 1e-9,
                    "pair {a} -> {b}"
                );
                // Undirected can only be as short or shorter.
                assert!(length <= float(&r["cost"]) + 1e-9);
                compared += 1;
            }
            (None, None) => {}
            other => panic!("reachability disagrees for {a} -> {b}: {other:?}"),
        }
        // The path is real, starts and ends where asked, and is as long as reported.
        let path = ids_of(&got["path"]);
        assert_eq!((path[0], *path.last().unwrap()), (ids[a], ids[b]));
        let along: f64 = path
            .windows(2)
            .map(|p| dist(pos[index(&ids, p[0])], pos[index(&ids, p[1])]))
            .sum();
        assert!((along - length).abs() < 1e-9);
        assert!((float(&got["direct"]) - dist(pos[a], pos[b])).abs() < 1e-9);
        assert!(float(&got["stretch"]) >= 1.0 - 1e-12);
        let (e, u) = (float(&got["expanded"]), float(&got["expandedUnguided"]));
        assert!(e <= u, "the guide expanded more: {e} vs {u}");
        saved += (e * 2.0 < u) as usize;
    }
    assert!(
        saved >= 2,
        "the guide halved the work on fewer than 2 of 3 pairs"
    );
    assert!(
        compared >= 1,
        "no pair was reachable following direction, so nothing was compared"
    );
}

fn ids_of(v: &PropertyValue) -> Vec<NodeId> {
    ids(v).into_iter().map(|i| NodeId::new(i as u64)).collect()
}

fn index(ids: &[NodeId], id: NodeId) -> usize {
    ids.iter().position(|&x| x == id).unwrap()
}

#[test]
fn guided_hop_paths_take_as_few_hops_as_plain_shortest_path() {
    let (s, ids, _) = grid(10);
    let n = ids.len();
    for (a, b) in [(0, n - 1), (3, n - 20), (n / 2, 9)] {
        let got = &astar(
            &s,
            ids[a],
            ids[b],
            "{structure: 'pdb', cost: 'hops', undirected: false}",
        )
        .unwrap();
        let plain = rows(
            &s,
            &format!(
                "CALL algo.shortestPath({}, {}) YIELD cost RETURN cost",
                ids[a].as_u64(),
                ids[b].as_u64()
            ),
        )
        .unwrap();
        match (got.first(), plain.first()) {
            (Some(g), Some(p)) => {
                assert_eq!(float(&g["hops"]), float(&p["cost"]), "pair {a} -> {b}")
            }
            (None, None) => {}
            other => panic!("reachability disagrees for {a} -> {b}: {other:?}"),
        }
    }
}

#[test]
fn a_node_without_a_position_is_not_walked_and_an_unreachable_target_has_no_row() {
    let (mut s, ids, _) = grid(4);
    let n = ids.len();
    // A shortcut through a node with no position: not walked, so the path
    // is the long way round.
    let hub = s.create_node("Residue");
    s.create_edge(ids[0], hub, "BOND").unwrap();
    s.create_edge(hub, ids[n - 1], "BOND").unwrap();
    let got = &astar(&s, ids[0], ids[n - 1], "{structure: 'pdb', cost: 'hops'}").unwrap()[0];
    assert!(
        float(&got["hops"]) > 2.0,
        "the path went through a node with no position"
    );
    assert!(!ids_of(&got["path"]).contains(&hub));
    // Positioned but unconnected.
    let lone = s.create_node("Residue");
    for (k, v) in [("x", 50.0), ("y", 50.0), ("z", 0.0)] {
        s.set_node_property("default", lone, k, v).unwrap();
    }
    run(&mut s, "REBUILD STRUCTURE INDEX pdb");
    assert!(astar(&s, ids[0], lone, "{structure: 'pdb'}")
        .unwrap()
        .is_empty());
    let err = astar(&s, ids[0], hub, "{structure: 'pdb'}").unwrap_err();
    assert!(
        err.contains("has no position under structure `pdb`"),
        "{err}"
    );
}

/// A chain of 20 `:Residue` folded into a hairpin: 0..9 out along x, 10..19
/// back 1.0 above them, so i and 19 - i sit 1.0 apart but 19 - 2i bonds away.
/// A second, separate chain of two sits beside the hairpin's start.
fn hairpin() -> (GraphStore, Vec<NodeId>, Vec<NodeId>) {
    let mut s = GraphStore::new();
    let mut chain = Vec::new();
    for i in 0..20usize {
        let (x, y) = if i < 10 {
            (i as f64 * 3.0, 0.0)
        } else {
            ((19 - i) as f64 * 3.0, 1.0)
        };
        let id = s.create_node("Residue");
        for (k, v) in [("x", x), ("y", y), ("z", 0.0)] {
            s.set_node_property("default", id, k, v).unwrap();
        }
        if let Some(&prev) = chain.last() {
            s.create_edge(prev, id, "BOND").unwrap();
        }
        chain.push(id);
    }
    let mut other = Vec::new();
    for (x, y) in [(0.0, -1.0), (3.0, -1.0)] {
        let id = s.create_node("Residue");
        for (k, v) in [("x", x), ("y", y), ("z", 0.0)] {
            s.set_node_property("default", id, k, v).unwrap();
        }
        other.push(id);
    }
    s.create_edge(other[0], other[1], "BOND").unwrap();
    run(
        &mut s,
        "CREATE STRUCTURE INDEX fold ON :Residue(x, y, z) METRIC euclidean CONTACT_RADIUS 1.5",
    );
    (s, chain, other)
}

fn stress(s: &GraphStore, config: &str) -> Result<Vec<HashMap<String, PropertyValue>>, String> {
    rows(
        s,
        &format!(
            "CALL algo.structural.stress({config}) YIELD source, target, distance, pathLength, hops, stretch \
             RETURN id(source) AS source, id(target) AS target, distance, pathLength, hops, stretch"
        ),
    )
}

#[test]
fn stress_finds_the_hairpins_contacts_and_ranks_the_farthest_round_first() {
    let (s, chain, other) = hairpin();
    // `structure` left out: the graph has one, and the contact radius is the
    // declared 1.5.
    let got = stress(&s, "{}").unwrap();
    let pair = |r: &HashMap<String, PropertyValue>| {
        let (a, b) = (float(&r["source"]) as u64, float(&r["target"]) as u64);
        (NodeId::new(a), NodeId::new(b))
    };
    // Unreachable first: the separate chain sits 1.0 below residues 0 and 1.
    let unreachable: Vec<_> = got
        .iter()
        .take_while(|r| r["stretch"] == PropertyValue::Null)
        .collect();
    assert_eq!(unreachable.len(), 2, "{got:?}");
    for r in &unreachable {
        let (a, b) = pair(r);
        assert!(other.contains(&a) || other.contains(&b));
        assert_eq!(
            (r["pathLength"].clone(), r["hops"].clone()),
            (PropertyValue::Null, PropertyValue::Null)
        );
    }
    // Then the hairpin contacts, i with 19 - i for i in 0..10, by stretch: the
    // pair at the open end (0, 19) is 19 bonds round, the one at the fold
    // (9, 10) is bonded and absent.
    let reachable: Vec<_> = got.iter().skip(2).collect();
    assert_eq!(reachable.len(), 9, "{reachable:?}");
    let first = pair(reachable[0]);
    assert_eq!(
        (first.0.min(first.1), first.0.max(first.1)),
        (chain[0].min(chain[19]), chain[0].max(chain[19]))
    );
    assert_eq!(float(&reachable[0]["hops"]), 19.0);
    // pathLength is 9 * 3 out, 1 across... along the chain: 9 steps of 3.0,
    // the fold's 1.0, and 9 more of 3.0.
    assert!((float(&reachable[0]["pathLength"]) - 55.0).abs() < 1e-9);
    assert!(
        (float(&reachable[0]["stretch"]) - 55.0).abs() < 1e-9,
        "distance is 1.0"
    );
    assert!(reachable
        .windows(2)
        .all(|w| float(&w[0]["stretch"]) >= float(&w[1]["stretch"])));
    // No pair is directly bonded.
    assert!(got.iter().all(|r| r["hops"] != PropertyValue::Integer(1)));
    let limited = stress(&s, "{structure: 'fold', limit: 3}").unwrap();
    assert_eq!(limited, got[..3].to_vec());
}

#[test]
fn stress_on_map_positions_uses_metres() {
    let mut s = GraphStore::new();
    // Two towers 500 m apart (0.0045 deg of latitude), joined only through a
    // third 5 km away.
    let towers = [(12.0, 77.0), (12.0045, 77.0), (12.045, 77.0)];
    let ids: Vec<NodeId> = towers
        .iter()
        .map(|&(lat, lon)| {
            let id = s.create_node("Tower");
            s.set_node_property("default", id, "lat", lat).unwrap();
            s.set_node_property("default", id, "lon", lon).unwrap();
            id
        })
        .collect();
    s.create_edge(ids[0], ids[2], "LINK").unwrap();
    s.create_edge(ids[2], ids[1], "LINK").unwrap();
    run(&mut s, "CREATE STRUCTURE INDEX towers ON :Tower(lat, lon)");
    let got = stress(&s, "{radius: 1000}").unwrap();
    assert_eq!(got.len(), 1, "{got:?}");
    let d = float(&got[0]["distance"]);
    assert!((d - 500.4).abs() < 1.0, "not metres: {d}");
    assert!(float(&got[0]["stretch"]) > 18.0, "{got:?}");
    assert!(
        stress(&s, "{radius: 100}").unwrap().is_empty(),
        "a 100 m radius found a 500 m pair"
    );
}

#[test]
fn bad_calls_are_refused_with_the_reason() {
    let (mut s, ids, _) = grid(3);
    let (a, b) = (ids[0].as_u64(), ids[1].as_u64());
    let refused = |s: &GraphStore, q: &str| {
        QueryEngine::new()
            .execute(q, s)
            .err()
            .map(|e| e.to_string())
            .unwrap_or_default()
    };
    let cases = [
        (format!("CALL algo.structural.astar({a}, {b}, {{structure: 'nope'}}) YIELD path RETURN path"), "no structure named `nope`; this graph has: pdb"),
        (format!("CALL algo.structural.astar({a}, {b}, {{cost: 'time'}}) YIELD path RETURN path"), "unknown cost `time`"),
        (format!("CALL algo.structural.astar({a}, {b}, {{undirected: 'yes'}}) YIELD path RETURN path"), "`undirected` must be true or false"),
        (format!("CALL algo.structural.astar({a}) YIELD path RETURN path"), "takes a source and a target"),
        (format!("CALL algo.structural.astar({a}, {b}, {{bogus: 1}}) YIELD path RETURN path"), "unknown config key `bogus`"),
        ("CALL algo.structural.stress({radius: -1}) YIELD stretch RETURN stretch".to_string(), "`radius` must be a positive distance"),
        ("CALL algo.structural.stress({radius: 'far'}) YIELD stretch RETURN stretch".to_string(), "`radius` must be a number"),
        ("CALL algo.structural.stress({}) YIELD stretch RETURN stretch".to_string(), "was declared without a contact radius"),
        ("CALL algo.structural.stress({radius: 2, limit: 0}) YIELD stretch RETURN stretch".to_string(), "`limit` must be a positive integer"),
    ];
    let mut wrong = Vec::new();
    for (q, expected) in &cases {
        let e = refused(&s, q);
        if !e.contains(expected) {
            wrong.push(format!("{q}\n  expected `{expected}`, got: {e}"));
        }
    }
    // Two structures: a name is needed. A hierarchy has no distance.
    run(
        &mut s,
        "CREATE STRUCTURE INDEX again ON :Residue(x, y, z) METRIC euclidean",
    );
    let e = refused(
        &s,
        &format!("CALL algo.structural.astar({a}, {b}) YIELD path RETURN path"),
    );
    if !e.contains("several structures; name one") {
        wrong.push(format!("two structures: {e}"));
    }
    let mut h = GraphStore::new();
    run(&mut h, "CREATE (:T {code: 'a'})-[:IS_A]->(:T {code: 'b'})");
    run(&mut h, "CREATE HIERARCHY INDEX tax ON ()-[:IS_A]->()");
    let e = refused(&h, "CALL algo.structural.stress({structure: 'hier_tax', radius: 1}) YIELD stretch RETURN stretch");
    if !e.contains("has no distance") {
        wrong.push(format!("hierarchy: {e}"));
    }
    let e = refused(
        &GraphStore::new(),
        "CALL algo.structural.stress({radius: 1}) YIELD stretch RETURN stretch",
    );
    if !e.contains("no 3D or geographic structure") {
        wrong.push(format!("no structure: {e}"));
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}

/// The fewest-hops route can use one long relationship. Guided by distance
/// over the *longest* relationship it is still found; guided by raw distance
/// the long hop looks expensive and a four-hop route of short steps wins.
#[test]
fn the_hop_guide_allows_for_long_relationships() {
    let mut s = GraphStore::new();
    let place = |s: &mut GraphStore, x: f64, y: f64| {
        let id = s.create_node("Site");
        for (k, v) in [("x", x), ("y", y), ("z", 0.0)] {
            s.set_node_property("default", id, k, v).unwrap();
        }
        id
    };
    // source -> b1 -> b2 -> b3 -> target along x, one unit apart; and
    // source -> far -> target, two hops of about 20 units each.
    let source = place(&mut s, 0.0, 0.0);
    let steps: Vec<NodeId> = (1..=3).map(|i| place(&mut s, i as f64, 0.0)).collect();
    let target = place(&mut s, 4.0, 0.0);
    let far = place(&mut s, 2.0, 20.0);
    let mut chain = vec![source];
    chain.extend(&steps);
    chain.push(target);
    for w in chain.windows(2) {
        s.create_edge(w[0], w[1], "LINK").unwrap();
    }
    s.create_edge(source, far, "LINK").unwrap();
    s.create_edge(far, target, "LINK").unwrap();
    run(
        &mut s,
        "CREATE STRUCTURE INDEX sites ON :Site(x, y, z) METRIC euclidean",
    );
    let got = &astar(&s, source, target, "{cost: 'hops'}").unwrap()[0];
    assert_eq!(float(&got["hops"]), 2.0, "{got:?}");
    assert_eq!(ids_of(&got["path"]), vec![source, far, target]);
    // By length the short steps win.
    let by_length = &astar(&s, source, target, "{cost: 'length'}").unwrap()[0];
    assert_eq!(float(&by_length["hops"]), 4.0);
}

/// Normalised mutual information between two labellings of the same nodes.
fn nmi(a: &[i64], b: &[i64]) -> f64 {
    let n = a.len() as f64;
    let mut joint: HashMap<(i64, i64), f64> = HashMap::new();
    let (mut pa, mut pb): (HashMap<i64, f64>, HashMap<i64, f64>) = (HashMap::new(), HashMap::new());
    for (&x, &y) in a.iter().zip(b) {
        *joint.entry((x, y)).or_default() += 1.0;
        *pa.entry(x).or_default() += 1.0;
        *pb.entry(y).or_default() += 1.0;
    }
    let h = |p: &HashMap<i64, f64>| -p.values().map(|c| c / n * (c / n).ln()).sum::<f64>();
    let i: f64 = joint.iter().map(|(&(x, y), &c)| c / n * ((c / n) / (pa[&x] / n * pb[&y] / n)).ln()).sum();
    let (ha, hb) = (h(&pa), h(&pb));
    if ha + hb == 0.0 {
        1.0
    } else {
        2.0 * i / (ha + hb)
    }
}

/// Two groups mixed at random in a unit square; links likelier near and
/// within a group (Expert et al.'s benchmark, about 23 links per node).
fn mixed_groups(n: usize, seed: u64) -> (GraphStore, Vec<NodeId>, Vec<i64>) {
    let mut s = GraphStore::new();
    let mut x = seed | 1;
    let mut unit = move || {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        (x % 1_000_000) as f64 / 1_000_000.0
    };
    let pos: Vec<[f64; 2]> = (0..n).map(|_| [unit(), unit()]).collect();
    let group: Vec<i64> = (0..n).map(|_| (unit() < 0.5) as i64).collect();
    let ids: Vec<NodeId> = pos
        .iter()
        .map(|p| {
            let id = s.create_node("Site");
            for (k, v) in [("x", p[0]), ("y", p[1]), ("z", 0.0)] {
                s.set_node_property("default", id, k, v).unwrap();
            }
            id
        })
        .collect();
    for i in 0..n {
        for j in i + 1..n {
            let d = ((pos[i][0] - pos[j][0]).powi(2) + (pos[i][1] - pos[j][1]).powi(2)).sqrt();
            let affinity = if group[i] == group[j] { 1.0 } else { 0.1 };
            if unit() < affinity / (1.0 + (d / 0.15).powi(2)) {
                s.create_edge(ids[i], ids[j], "LINK").unwrap();
            }
        }
    }
    run(&mut s, "CREATE STRUCTURE INDEX sites ON :Site(x, y, z) METRIC euclidean");
    (s, ids, group)
}

fn communities(s: &GraphStore, q: &str, ids: &[NodeId]) -> Vec<i64> {
    let got: HashMap<i64, i64> = rows(s, q)
        .unwrap()
        .into_iter()
        .map(|r| (float(&r["id"]) as i64, float(&r["c"]) as i64))
        .collect();
    ids.iter().map(|id| got[&(id.as_u64() as i64)]).collect()
}

#[test]
fn spatial_louvain_recovers_groups_mixed_in_space_that_louvain_splits_by_place() {
    let (mut spatial_sum, mut plain_sum) = (0.0, 0.0);
    for seed in [3, 7, 11] {
        let (s, ids, group) = mixed_groups(300, seed);
        let spatial = communities(
            &s,
            "CALL algo.structural.spatialLouvain() YIELD node, communityId RETURN id(node) AS id, communityId AS c",
            &ids,
        );
        let plain = communities(&s, "CALL algo.louvain() YIELD node, communityId RETURN id(node) AS id, communityId AS c", &ids);
        spatial_sum += nmi(&spatial, &group) / 3.0;
        plain_sum += nmi(&plain, &group) / 3.0;
    }
    assert!(spatial_sum > 0.95, "spatial NMI {spatial_sum:.3}");
    assert!(spatial_sum > plain_sum + 0.2, "spatial {spatial_sum:.3} vs louvain {plain_sum:.3}");
}

#[test]
fn spatial_louvain_reports_one_modularity_and_leaves_unpositioned_nodes_out() {
    let (mut s, ids, _) = mixed_groups(80, 5);
    let stray = s.create_node("Site");
    s.create_edge(ids[0], stray, "LINK").unwrap();
    let got = rows(
        &s,
        "CALL algo.structural.spatialLouvain({structure: 'sites', bins: 8}) YIELD node, communityId, modularity \
         RETURN id(node) AS id, communityId, modularity",
    )
    .unwrap();
    assert_eq!(got.len(), ids.len(), "an unpositioned node was grouped");
    assert!(got.iter().all(|r| float(&r["id"]) as u64 != stray.as_u64()));
    let q = float(&got[0]["modularity"]);
    assert!(q > 0.0 && got.iter().all(|r| float(&r["modularity"]) == q));
    // Same call, same answer.
    let again = rows(
        &s,
        "CALL algo.structural.spatialLouvain({structure: 'sites', bins: 8}) YIELD node, communityId, modularity \
         RETURN id(node) AS id, communityId, modularity",
    )
    .unwrap();
    assert_eq!(got, again);
    // No relationships: everyone alone, modularity null.
    let mut lonely = GraphStore::new();
    for i in 0..3 {
        let id = lonely.create_node("Site");
        for (k, v) in [("x", i as f64), ("y", 0.0), ("z", 0.0)] {
            lonely.set_node_property("default", id, k, v).unwrap();
        }
    }
    run(&mut lonely, "CREATE STRUCTURE INDEX sites ON :Site(x, y, z) METRIC euclidean");
    let alone = rows(&lonely, "CALL algo.structural.spatialLouvain() YIELD communityId, modularity RETURN communityId, modularity").unwrap();
    assert_eq!(alone.len(), 3);
    assert!(alone.iter().all(|r| r["modularity"] == PropertyValue::Null));
}

#[test]
fn spatial_louvain_refuses_bad_bins_and_too_many_nodes() {
    let (s, _, _) = mixed_groups(20, 9);
    for (q, expected) in [
        ("CALL algo.structural.spatialLouvain({bins: 0}) YIELD node RETURN node", "`bins` must be an integer from 1 to 1000"),
        ("CALL algo.structural.spatialLouvain({bins: 'many'}) YIELD node RETURN node", "`bins` must be an integer"),
        ("CALL algo.structural.spatialLouvain({colour: 1}) YIELD node RETURN node", "unknown config key `colour`"),
    ] {
        let e = QueryEngine::new().execute(q, &s).err().map(|e| e.to_string()).unwrap_or_default();
        assert!(e.contains(expected), "{q}: {e}");
    }
    let mut big = GraphStore::new();
    for i in 0..2001 {
        let id = big.create_node("Site");
        for (k, v) in [("x", i as f64), ("y", 0.0), ("z", 0.0)] {
            big.set_node_property("default", id, k, v).unwrap();
        }
    }
    run(&mut big, "CREATE STRUCTURE INDEX sites ON :Site(x, y, z) METRIC euclidean");
    let e = QueryEngine::new()
        .execute("CALL algo.structural.spatialLouvain() YIELD node RETURN node", &big)
        .unwrap_err()
        .to_string();
    assert!(e.contains("2001 nodes is more than the 2000"), "{e}");
}
