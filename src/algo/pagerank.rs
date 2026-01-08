//! PageRank algorithm implementation
//!
//! Implements REQ-ALGO-001: Node centrality

use super::common::GraphView;
use crate::graph::{GraphStore, NodeId};
use std::collections::HashMap;

/// PageRank configuration
pub struct PageRankConfig {
    /// Damping factor (usually 0.85)
    pub damping_factor: f64,
    /// Number of iterations
    pub iterations: usize,
    /// Tolerance for convergence (optional - not used in fixed iteration version)
    pub tolerance: f64,
}

impl Default for PageRankConfig {
    fn default() -> Self {
        Self {
            damping_factor: 0.85,
            iterations: 20,
            tolerance: 0.0001,
        }
    }
}

/// Calculate PageRank for the graph (or a subgraph)
pub fn page_rank(
    store: &GraphStore,
    label: Option<&str>,
    edge_type: Option<&str>,
    config: PageRankConfig,
) -> HashMap<NodeId, f64> {
    // 1. Project graph to optimized view
    let view = GraphView::new(store, label, edge_type);
    let n = view.node_count;
    
    if n == 0 {
        return HashMap::new();
    }

    // 2. Initialize scores
    // Initial score is 1.0 for all nodes (some implementations use 1/N, but Neo4j uses 0.15 + ...)
    let initial_score = 1.0;
    let mut scores = vec![initial_score; n];
    let mut next_scores = vec![0.0; n];

    // 3. Iteration
    let d = config.damping_factor;
    let base_score = 1.0 - d;

    for _ in 0..config.iterations {
        let mut total_diff = 0.0;

        for i in 0..n {
            let mut sum_incoming = 0.0;
            
            // Iterate over incoming edges
            for &source_idx in &view.incoming[i] {
                let out_degree = view.out_degree(source_idx);
                if out_degree > 0 {
                    sum_incoming += scores[source_idx] / out_degree as f64;
                }
            }

            next_scores[i] = base_score + d * sum_incoming;
            total_diff += (next_scores[i] - scores[i]).abs();
        }

        // Swap buffers
        scores.copy_from_slice(&next_scores);

        // Check convergence (optional optimization)
        if total_diff < config.tolerance {
            break;
        }
    }

    // 4. Map back to NodeIds
    let mut result = HashMap::with_capacity(n);
    for (idx, score) in scores.into_iter().enumerate() {
        result.insert(view.index_to_node[idx], score);
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::GraphStore;

    #[test]
    fn test_pagerank_simple() {
        let mut store = GraphStore::new();
        // Star graph: Center (0) points to Leaves (1, 2, 3)
        // Leaves point back to Center
        // Center should have highest PageRank
        let center = store.create_node("Node");
        let l1 = store.create_node("Node");
        let l2 = store.create_node("Node");

        store.create_edge(center, l1, "LINK").unwrap();
        store.create_edge(center, l2, "LINK").unwrap();
        store.create_edge(l1, center, "LINK").unwrap();
        store.create_edge(l2, center, "LINK").unwrap();

        let scores = page_rank(&store, None, None, PageRankConfig::default());

        let center_score = *scores.get(&center).unwrap();
        let l1_score = *scores.get(&l1).unwrap();
        
        assert!(center_score > l1_score);
    }
}
