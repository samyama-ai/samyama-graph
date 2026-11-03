pub mod cdlp;
pub mod centrality;
pub mod centrality_extra;
pub mod common;
pub mod community;
pub mod community_detect;
pub mod cpu_fallback;
pub mod embeddings;
pub mod flow;
#[cfg(feature = "gpu")]
pub mod gpu_dispatch;
#[cfg(test)]
mod gpu_parity_coverage;
#[cfg(all(test, feature = "gpu"))]
mod gpu_parity_tests;
pub mod lcc;
pub mod link_prediction;
pub mod metrics;
pub mod mst;
pub mod pagerank;
pub mod pathfinding;
pub mod pathfinding_extra;
pub mod paths_extra;
pub mod pca;
pub mod progress;
pub mod similarity_extra;
pub mod structure_extra;
pub mod temporal;
pub mod topology;
pub mod traversal;

pub use cdlp::{cdlp, CdlpConfig, CdlpResult};
pub use centrality::{
    betweenness_centrality, closeness_centrality, core_number, degree_centrality,
    eigenvector_centrality, harmonic_centrality, ranked, Scores,
};
pub use centrality_extra::{
    hits, index_of, katz_centrality, personalised_page_rank, ranked_scores, vote_rank,
};
pub use common::{GraphView, NodeId};
pub use community::{
    strongly_connected_components, weakly_connected_components, SccResult, WccResult,
};
pub use community_detect::{louvain, modularity, with_ids, Communities};
pub use embeddings::{fastrp, node2vec, FastRpConfig, Node2VecConfig};
pub use flow::{edmonds_karp, FlowResult};
pub use lcc::{
    local_clustering_coefficient, local_clustering_coefficient_directed,
    local_clustering_coefficient_with, DirectedLcc, LccResult,
};
pub use link_prediction::{predict_links, score_one, LinkScore, PairScore};
pub use metrics::{
    average_neighbour_degree, degree_assortativity, diameter, eccentricity, radius, ranked_opt,
};
pub use mst::{prim_mst, MSTResult};
pub use pagerank::{page_rank, page_rank_with, PageRankConfig};
pub use pathfinding::{bfs, bfs_all_shortest_paths, dijkstra, PathResult};
pub use pathfinding_extra::{
    a_star, all_shortest_paths, article_rank, random_walk, yens_k_shortest,
};
pub use paths_extra::{
    all_pairs_hops, bellman_ford, dag_longest_path, transitive_closure, wiener_index,
};
pub use pca::{pca, PcaConfig, PcaResult, PcaSolver};
pub use progress::{NoProgress, Progress, Stopped};
pub use similarity_extra::{
    constraint, cosine_similarity, effective_size, node_similarity, overlap_coefficient,
    reciprocity,
};
pub use structure_extra::{
    biconnected_components, bipartite_sets, dominating_set, global_efficiency, greedy_colouring,
    k_truss, maximal_matching, rich_club_coefficient, square_clustering, transitivity,
    undirected_degree,
};
pub use temporal::{
    earliest_arrival, propagation_ranking, symptom_explanation, temporal_reachability,
    temporal_shortest_path, ArrivalTimes, Explanation, TemporalEdges, TemporalError, TemporalPath,
};
pub use topology::count_triangles;
pub use traversal::{articulation_points, bridges, find_cycle, topological_sort, TopoResult};

#[cfg(test)]
#[path = "lib_cov_tests.rs"]
mod cov_tests;
