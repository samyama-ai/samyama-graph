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
};

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

        graphs.push(serde_json::json!({
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
            }
        }));
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
