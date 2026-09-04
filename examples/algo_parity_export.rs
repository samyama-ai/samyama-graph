//! Run every shipped algorithm on reference graphs and emit the graphs *and*
//! the answers as JSON, for comparison against NetworkX (ALGO-02).
//!
//! The graph is emitted alongside the results on purpose. A parity check whose
//! two sides build their own copy of "the same" graph is checking two graph
//! builders as much as two algorithms, and when it disagrees you cannot tell
//! which. Here the edge list in the output *is* the graph both sides ran on.
//!
//! Determinism matters for the same reason: the graphs are generated from a
//! fixed linear congruential sequence rather than a random source, so a
//! disagreement is reproducible from the file alone.
//!
//!     cargo run --release --example algo_parity_export -- out.json

use std::collections::HashMap;

use samyama_graph_algorithms::{
    bfs, cdlp, count_triangles, dijkstra, edmonds_karp, local_clustering_coefficient,
    local_clustering_coefficient_directed, page_rank, prim_mst, strongly_connected_components,
    weakly_connected_components, CdlpConfig, GraphView, NodeId, PageRankConfig,
    all_shortest_paths, a_star, yens_k_shortest, modularity,
    hits, katz_centrality, personalised_page_rank,
    bellman_ford, dag_longest_path, transitive_closure, wiener_index,
    biconnected_components, bipartite_sets, dominating_set, global_efficiency, k_truss,
    greedy_colouring, maximal_matching,
    rich_club_coefficient, square_clustering, transitivity,
    constraint, cosine_similarity, effective_size, overlap_coefficient, reciprocity,
    betweenness_centrality, closeness_centrality, core_number, degree_centrality,
    eigenvector_centrality, harmonic_centrality,
    link_prediction::{score_one, LinkScore},
    average_neighbour_degree, degree_assortativity, diameter, eccentricity, radius,
    pathfinding_extra::random_walk,
};

/// How many shortest paths to enumerate per pair before stopping. The count is
/// exponential in the worst case; the reference recorder uses the same cap, so
/// a graph that hits it is not the thing being compared.
const PATH_ENUM_CAP: usize = 64;

/// A fixed sequence, so every run builds the same graphs.
struct Lcg(u64);
impl Lcg {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        self.0 >> 33
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

struct Reference {
    name: String,
    directed: bool,
    n: usize,
    /// `(source, target, weight)`, already de-duplicated and self-loop free.
    edges: Vec<(usize, usize, f64)>,
}

fn build(name: &str, directed: bool, n: usize, m: usize, seed: u64) -> Reference {
    let mut rng = Lcg(seed);
    let mut seen: std::collections::HashSet<(usize, usize)> = std::collections::HashSet::new();
    let mut edges = Vec::new();
    // A spanning chain first, so the graph is connected and max-flow and MST
    // have something to find. Random graphs at this size are often not.
    for i in 1..n {
        let w = 1.0 + (rng.below(9) as f64);
        seen.insert((i - 1, i));
        edges.push((i - 1, i, w));
    }
    while edges.len() < m {
        let a = rng.below(n as u64) as usize;
        let b = rng.below(n as u64) as usize;
        if a == b {
            continue;
        }
        let key = if directed { (a, b) } else { (a.min(b), a.max(b)) };
        if !seen.insert(key) {
            continue;
        }
        let w = 1.0 + (rng.below(9) as f64);
        edges.push((key.0, key.1, w));
    }
    Reference { name: name.to_string(), directed, n, edges }
}

/// The same graph with each edge stored once, weights kept.
///
/// `view_of` doubles an undirected edge so both endpoints see it. The
/// statistics defined on the undirected *collapse* — the shape metrics,
/// modularity — need to be told which convention they are reading, because
/// the totals they divide by are twice as large in a doubled view. Getting
/// that backwards halves or doubles the answer without failing anything.
fn view_single(r: &Reference) -> GraphView {
    let index_to_node: Vec<NodeId> = (0..r.n).map(|i| i as NodeId).collect();
    let node_to_index: HashMap<NodeId, usize> = (0..r.n).map(|i| (i as NodeId, i)).collect();
    let mut outgoing = vec![Vec::new(); r.n];
    let mut incoming = vec![Vec::new(); r.n];
    let mut weights = vec![Vec::new(); r.n];
    for &(a, b, w) in &r.edges {
        outgoing[a].push(b);
        weights[a].push(w);
        incoming[b].push(a);
    }
    GraphView::from_adjacency_list(
        r.n, index_to_node, node_to_index, outgoing, incoming, Some(weights),
    )
}

fn view_of(r: &Reference) -> GraphView {
    let index_to_node: Vec<NodeId> = (0..r.n).map(|i| i as NodeId).collect();
    let node_to_index: HashMap<NodeId, usize> = (0..r.n).map(|i| (i as NodeId, i)).collect();
    let mut outgoing = vec![Vec::new(); r.n];
    let mut incoming = vec![Vec::new(); r.n];
    let mut weights = vec![Vec::new(); r.n];
    for &(a, b, w) in &r.edges {
        outgoing[a].push(b);
        weights[a].push(w);
        incoming[b].push(a);
        if !r.directed {
            outgoing[b].push(a);
            weights[b].push(w);
            incoming[a].push(b);
        }
    }
    GraphView::from_adjacency_list(r.n, index_to_node, node_to_index, outgoing, incoming, Some(weights))
}

fn main() {
    let out_path = std::env::args().nth(1).unwrap_or_else(|| "algo-parity.json".to_string());

    let refs = vec![
        build("undirected-40", false, 40, 90, 12345),
        build("directed-40", true, 40, 110, 777),
        build("undirected-120", false, 120, 300, 99),
    ];

    let mut graphs = Vec::new();
    for r in &refs {
        let view = view_of(r);

        // Two configurations. The default is what a caller gets; the tightened
        // one exists to separate "the algorithm is wrong" from "the default
        // stops early", which a single number cannot distinguish.
        let pr = page_rank(&view, PageRankConfig::default());
        let mut pr_vec: Vec<(String, f64)> =
            pr.iter().map(|(k, v)| (k.to_string(), *v)).collect();
        pr_vec.sort_by(|a, b| a.0.cmp(&b.0));

        let tight = PageRankConfig { iterations: 200, tolerance: 1e-12, ..Default::default() };
        let pr_tight = page_rank(&view, tight);
        let mut pr_tight_vec: Vec<(String, f64)> =
            pr_tight.iter().map(|(k, v)| (k.to_string(), *v)).collect();
        pr_tight_vec.sort_by(|a, b| a.0.cmp(&b.0));

        // The directed coefficient for a directed graph. Calling the
        // undirected one on a directed view answers a different question, and
        // silently: it agrees exactly with NetworkX's *undirected* clustering,
        // so the disagreement looks like a defect in ours.
        let lcc = if r.directed {
            local_clustering_coefficient_directed(&view, true)
        } else {
            local_clustering_coefficient(&view)
        };
        let mut lcc_vec: Vec<(String, f64)> =
            lcc.coefficients.iter().map(|(k, v)| (k.to_string(), *v)).collect();
        lcc_vec.sort_by(|a, b| a.0.cmp(&b.0));

        let wcc = weakly_connected_components(&view);
        let scc = strongly_connected_components(&view);
        let mst = prim_mst(&view);
        let tri = count_triangles(&view);

        // Shortest paths from node 0 to a handful of targets, unweighted and
        // weighted, rather than all pairs — enough to catch a wrong answer
        // without making the file unreadable.
        let targets: Vec<usize> = (1..r.n).step_by(r.n / 8 + 1).collect();
        let bfs_costs: Vec<(String, Option<f64>)> = targets
            .iter()
            .map(|t| ((*t).to_string(), bfs(&view, 0, *t as NodeId).map(|p| p.cost)))
            .collect();
        let dij_costs: Vec<(String, Option<f64>)> = targets
            .iter()
            .map(|t| ((*t).to_string(), dijkstra(&view, 0, *t as NodeId).map(|p| p.cost)))
            .collect();

        let flow = edmonds_karp(&view, 0, (r.n - 1) as NodeId).map(|f| f.max_flow);

        // Label propagation is not deterministic in either implementation, so
        // only the *number* of communities is emitted, and even that is
        // reported as advisory rather than compared.
        let cd = cdlp(&view, &CdlpConfig::default());
        let communities: std::collections::HashSet<_> = cd.labels.values().collect();

        // Everything below is defined on the undirected collapse or takes the
        // convention as an argument, so it reads the singly-stored view and
        // says which it is. The families here mirror what
        // `record_reference.py` computes; the live check was capped at the
        // eleven the export happened to emit, which is a limit of this file
        // rather than of what NetworkX can answer.
        let single = view_single(r);
        let bidir = !r.directed;
        let idx = |v: &Vec<f64>| -> HashMap<String, f64> {
            v.iter().enumerate().map(|(i, x)| (i.to_string(), *x)).collect()
        };

        let ecc: HashMap<String, Option<i64>> = eccentricity(&single, true)
            .into_iter().enumerate().map(|(i, e)| (i.to_string(), e)).collect();

        // Every unconnected pair, which is what link prediction is for: a
        // ranking whose top entries are pairs already joined answers a
        // question nobody asked.
        let mut joined: std::collections::HashSet<(usize, usize)> =
            std::collections::HashSet::new();
        for &(a, b, _) in &r.edges {
            joined.insert((a.min(b), a.max(b)));
        }
        let mut jaccard = HashMap::new();
        let mut adamic = HashMap::new();
        let mut common = HashMap::new();
        for a in 0..r.n {
            for b in (a + 1)..r.n {
                if joined.contains(&(a, b)) {
                    continue;
                }
                let key = format!("{a}-{b}");
                jaccard.insert(key.clone(), score_one(&single, LinkScore::Jaccard, a, b));
                adamic.insert(key.clone(), score_one(&single, LinkScore::AdamicAdar, a, b));
                common.insert(key, score_one(&single, LinkScore::CommonNeighbours, a, b));
            }
        }

        // Overlap and cosine over **every** unordered pair, joined or not.
        //
        // Link prediction above skips joined pairs, because a ranking whose top
        // entries are already-connected pairs answers a question nobody asked.
        // These two are not link prediction: they ask how alike two
        // neighbourhoods are, and the answer for a connected pair is as
        // meaningful as for any other. Restricting them to unconnected pairs
        // would have exported a different function from the one the engine
        // ships.
        //
        // They were listed as having "no NetworkX equivalent" and therefore no
        // deterministic reference (benchmarks#125). NetworkX indeed has
        // neither, and that is not the same statement: both are closed-form set
        // expressions over the neighbourhood, |A ∩ B| divided by min(|A|,|B|)
        // and by sqrt(|A|·|B|), and a reference for them needs the graph, not a
        // library. `None` where the engine returns one -- an isolated node
        // resembles nothing -- so the reference has to agree about that too.
        let mut overlap = HashMap::new();
        let mut cosine = HashMap::new();
        for a in 0..r.n {
            for b in (a + 1)..r.n {
                let key = format!("{a}-{b}");
                overlap.insert(key.clone(), overlap_coefficient(&single, a, b));
                cosine.insert(key, cosine_similarity(&single, a, b));
            }
        }

        // Paths over every ordered reachable pair. Three separate facts,
        // because three separate algorithms read them: the hop distance, the
        // *number* of shortest paths, and the weighted distance. The count is
        // the one a single-path API cannot show — a pair joined by one route
        // and a pair joined by forty look identical without it.
        let mut hop = HashMap::new();
        let mut count = HashMap::new();
        let mut wdist = HashMap::new();
        let zero = vec![0.0; r.n];
        for sv in 0..r.n {
            for tv in 0..r.n {
                if sv == tv {
                    continue;
                }
                let paths = all_shortest_paths(&view, sv, tv, PATH_ENUM_CAP);
                if paths.is_empty() {
                    continue; // unreachable: no distance to record
                }
                let key = format!("{sv}-{tv}");
                hop.insert(key.clone(), paths[0].len() - 1);
                count.insert(key.clone(), paths.len());
                // A* with a zero heuristic is Dijkstra, which is what the
                // reference runs. A heuristic of our own would make this a
                // test of the heuristic rather than of the search.
                if let Some((_, cost)) = a_star(&view, sv, tv, &zero) {
                    wdist.insert(key, cost);
                }
            }
        }
        // Yen's on the widest-separated pair only: it is O(k n) shortest-path
        // runs per pair, and the file is read by a human.
        let mut simple: HashMap<String, Vec<f64>> = HashMap::new();
        // Greatest hop distance, ties broken by the smallest (s, t) -- stated
        // rather than left to whatever the iterator yields first, because the
        // reference has to pick the *same* pair. "Widest-separated" alone is
        // not a rule when several pairs share the maximum, and the two sides
        // silently ran Yen's on different pairs.
        let far = hop
            .iter()
            .map(|(k, &h)| {
                let mut it = k.split('-').map(|x| x.parse::<usize>().unwrap());
                (std::cmp::Reverse(h), it.next().unwrap(), it.next().unwrap(), k.clone())
            })
            .min();
        if let Some((_, _, _, far)) = far {
            let mut it = far.split('-').map(|x| x.parse::<usize>().unwrap());
            let (sv, tv) = (it.next().unwrap(), it.next().unwrap());
            simple.insert(
                far.clone(),
                yens_k_shortest(&view, sv, tv, 5).into_iter().map(|(_, c)| c).collect(),
            );
        }

        // Modularity of a partition fixed by node index, so both sides score
        // the *same* partition. Louvain is stochastic and seeded differently
        // in the two implementations, so comparing the partitions themselves
        // would say nothing about either modularity function.
        let parts: Vec<usize> = (0..r.n).map(|i| i % 3).collect();
        let q = modularity(&single, &parts);

        // ---- The twenty-five added for H2, in the same shapes
        // `record_reference.py` records. Only the deterministic ones: a greedy
        // matching or colouring agrees with NetworkX or not depending on which
        // edge each side visited first, so a check on those would pass by luck
        // and fail by luck. They are named in `NO_DETERMINISTIC_REFERENCE`
        // there rather than left as a silent hole.
        let idx = |v: &Vec<f64>| -> HashMap<String, f64> {
            v.iter().enumerate().map(|(i, x)| (i.to_string(), *x)).collect()
        };
        let katz = katz_centrality(&view, 0.05, 1.0, 5000, 1e-12).map(|v| idx(&v));
        let hits_pair = hits(&view, 5000, 1e-12);
        let ppr = idx(&personalised_page_rank(&view, &[0], 0.85, 500, 1e-12));
        let bf: Option<HashMap<String, f64>> = bellman_ford(&view, 0).map(|d| {
            d.iter().enumerate().filter_map(|(i, x)| x.map(|v| (i.to_string(), v))).collect()
        });
        let dag_len = dag_longest_path(&view).map(|p| p.len() as i64 - 1);
        // Ordered pairs excluding self, matching what the recorder counts.
        // The self-pairs the engine keeps for nodes on a cycle are a real fact
        // and are checked separately -- folding them into this count would
        // compare two different quantities.
        let tc_pairs = transitive_closure(&view).into_iter().filter(|(a, b)| a != b).count();
        let sq: HashMap<String, f64> = (0..r.n)
            .filter_map(|i| square_clustering(&single, i, true).map(|v| (i.to_string(), v)))
            .collect();
        let eff: HashMap<String, f64> = (0..r.n)
            .filter_map(|i| effective_size(&single, i).map(|v| (i.to_string(), v)))
            .collect();
        let cons: HashMap<String, f64> = (0..r.n)
            .filter_map(|i| constraint(&single, i).map(|v| (i.to_string(), v)))
            .collect();
        let mut truss = k_truss(&single, 3, true);
        truss.sort_unstable();

        // Assembled as a map and merged below rather than inlined into the
        // `json!` literal: with these added, the macro hits its recursion
        // limit. Splitting the object is the fix that does not require
        // `#![recursion_limit]` on a whole example for one literal.
        let mut h2 = serde_json::Map::new();
        let mut put = |k: &str, v: serde_json::Value| { h2.insert(k.to_string(), v); };
        put("katz", serde_json::to_value(&katz).unwrap());
        put("hits_hubs", serde_json::to_value(hits_pair.as_ref().map(|(h, _)| idx(h))).unwrap());
        put("hits_authorities", serde_json::to_value(hits_pair.as_ref().map(|(_, a)| idx(a))).unwrap());
        put("personalised_pagerank", serde_json::to_value(&ppr).unwrap());
        put("bellman_ford_from_0", serde_json::to_value(&bf).unwrap());
        put("wiener_index", serde_json::to_value(wiener_index(&view)).unwrap());
        put("dag_longest_path_length", serde_json::to_value(dag_len).unwrap());
        put("transitive_closure_pairs", serde_json::to_value(tc_pairs).unwrap());
        put("is_bipartite", serde_json::to_value(bipartite_sets(&single, true).is_some()).unwrap());
        put("k_truss_3_nodes", serde_json::to_value(&truss).unwrap());
        put("transitivity", serde_json::to_value(transitivity(&single, true)).unwrap());
        put("global_efficiency", serde_json::to_value(global_efficiency(&single, true)).unwrap());
        put("square_clustering", serde_json::to_value(&sq).unwrap());
        put("rich_club_1", serde_json::to_value(rich_club_coefficient(&single, 1, true)).unwrap());
        put("biconnected_component_count",
            serde_json::to_value(biconnected_components(&single, true).len()).unwrap());
        put("effective_size", serde_json::to_value(&eff).unwrap());
        put("constraint", serde_json::to_value(&cons).unwrap());
        // A seeded algorithm. The other 48 families are unseeded and
        // deterministic by construction; ALGO-11 asks for reproducibility
        // *given a seed*, and nothing in this export exercised a seed until
        // this line. It is the clause most likely to rot, because a caller
        // switching to an unseeded RNG breaks nothing that compiles.
        put("random_walk_seeded",
            serde_json::to_value(random_walk(&view, 0, 32, 0x5A_4D)
                .to_vec()).unwrap());
        put("reciprocity",
            serde_json::to_value(if r.directed { reciprocity(&view) } else { None }).unwrap());

        let mut entry = serde_json::json!({
            "name": r.name,
            "directed": r.directed,
            "nodes": r.n,
            "edges": r.edges.iter().map(|(a, b, w)| serde_json::json!([a, b, w])).collect::<Vec<_>>(),
            "results": {
                "pagerank": pr_vec.iter().map(|(k, v)| (k.clone(), *v)).collect::<HashMap<_, _>>(),
                "pagerank_tight": pr_tight_vec.iter().map(|(k, v)| (k.clone(), *v)).collect::<HashMap<_, _>>(),
                "clustering": lcc_vec.iter().map(|(k, v)| (k.clone(), *v)).collect::<HashMap<_, _>>(),
                "wcc_count": wcc.components.len(),
                "scc_count": scc.components.len(),
                "mst_total_weight": mst.total_weight,
                "triangles": tri,
                "bfs_cost": bfs_costs.iter().cloned().collect::<HashMap<_, _>>(),
                "dijkstra_cost": dij_costs.iter().cloned().collect::<HashMap<_, _>>(),
                "max_flow_0_to_last": flow,
                "cdlp_community_count": communities.len(),

                "degree_centrality": idx(&degree_centrality(&single, bidir)),
                "closeness_centrality": idx(&closeness_centrality(&single, bidir)),
                "betweenness_centrality": idx(&betweenness_centrality(&single, bidir)),
                "harmonic_centrality": idx(&harmonic_centrality(&single, bidir)),
                "core_number": idx(&core_number(&single, bidir)
                    .iter().map(|&c| c as f64).collect::<Vec<f64>>()),
                // `None` means power iteration did not converge, which is a
                // different statement from a vector of zeros and is recorded
                // as such.
                "eigenvector_centrality": eigenvector_centrality(&single, bidir, 1000, 1e-10)
                    .map(|v| idx(&v)),

                "eccentricity": ecc,
                "diameter": diameter(&single, true),
                "radius": radius(&single, true),
                "average_neighbour_degree": idx(&average_neighbour_degree(&single, true)),
                "degree_assortativity": degree_assortativity(&single, true),

                "jaccard": jaccard,
                "adamic_adar": adamic,
                "common_neighbours": common,
                "overlap": overlap,
                "cosine": cosine,

                // The three greedy results, exported to be checked against
                // their **invariant** rather than compared to NetworkX.
                //
                // The comment below still holds for comparison: a greedy
                // matching agrees with NetworkX or not depending on which edge
                // each side visited first, so a parity check would pass by luck
                // and fail by luck. It does not follow that nothing can be
                // checked. A matching is maximal if no edge can be added; a
                // colouring is proper if no edge is monochromatic; a dominating
                // set dominates if every node is in it or beside it. Those are
                // properties of the answer, not of the tie-break, and they are
                // *stronger* than parity -- they say the result is right rather
                // than that it matches someone else's arbitrary choice
                // (benchmarks#135).
                "maximal_matching": maximal_matching(&single, true),
                "greedy_colouring": greedy_colouring(&single, true),
                "dominating_set": dominating_set(&single, true),

                "hop_distance": hop,
                "shortest_path_count": count,
                "weighted_distance": wdist,
                "simple_path_lengths": simple,
                "modularity_of_index_mod_3": q,

            }
        });
        // Merge the H2 block into `results`, which the macro could not hold in
        // one literal.
        if let Some(res) = entry.get_mut("results").and_then(|r| r.as_object_mut()) {
            res.extend(h2);
        }
        graphs.push(entry);
    }

    let doc = serde_json::json!({
        "generator": "algo_parity_export",
        "pagerank_config": {
            "damping": 0.85,
            "default_iterations": 20,
            "default_tolerance": 1e-4,
            "note": "unweighted; compare against nx.pagerank(alpha=0.85, weight=None). \
                     nx defaults to the *weighted* form, which is a different algorithm \
                     on a weighted graph and disagrees by ~1e-2."
        },
        "graphs": graphs,
    });
    std::fs::write(&out_path, serde_json::to_string_pretty(&doc).unwrap()).unwrap();
    println!("wrote {} graphs to {out_path}", refs.len());
}
