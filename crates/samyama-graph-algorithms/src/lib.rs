pub mod common;
#[cfg(feature = "gpu")]
pub mod gpu_dispatch;
#[cfg(all(test, feature = "gpu"))]
mod gpu_parity_tests;
pub mod pagerank;
pub mod community;
pub mod pathfinding;
pub mod flow;
pub mod mst;
pub mod topology;
pub mod cdlp;
pub mod lcc;
pub mod pca;
pub mod centrality;
pub mod link_prediction;
pub mod temporal;

pub use common::{GraphView, NodeId};
pub use pagerank::{page_rank, PageRankConfig};
pub use community::{weakly_connected_components, WccResult, strongly_connected_components, SccResult};
pub use pathfinding::{bfs, dijkstra, bfs_all_shortest_paths, PathResult};
pub use flow::{edmonds_karp, FlowResult};
pub use mst::{prim_mst, MSTResult};
pub use topology::count_triangles;
pub use cdlp::{cdlp, CdlpResult, CdlpConfig};
pub use lcc::{
    local_clustering_coefficient, local_clustering_coefficient_directed,
    local_clustering_coefficient_with, DirectedLcc, LccResult,
};
pub use pca::{pca, PcaConfig, PcaResult, PcaSolver};
pub use centrality::{
    betweenness_centrality, closeness_centrality, core_number, degree_centrality,
    eigenvector_centrality, harmonic_centrality, ranked, Scores,
};
pub use link_prediction::{predict_links, score_one, LinkScore, PairScore};
pub use temporal::{
    earliest_arrival, propagation_ranking, symptom_explanation, temporal_reachability,
    temporal_shortest_path, ArrivalTimes, Explanation, TemporalEdges, TemporalError,
    TemporalPath,
};