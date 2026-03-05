//! PageRank algorithm implementation
//!
//! Implements REQ-ALGO-001: Node centrality

use super::common::{GraphView, NodeId};
use std::collections::HashMap;

/// PageRank configuration
pub struct PageRankConfig {
    /// Damping factor (usually 0.85)
    pub damping_factor: f64,
    /// Number of iterations
    pub iterations: usize,
    /// Tolerance for convergence (0.0 = run all iterations)
    pub tolerance: f64,
    /// Whether to redistribute dangling node mass.
    /// Set to false for LDBC Graphalytics compatibility (reference outputs
    /// are generated without dangling redistribution).
    pub dangling_redistribution: bool,
}

impl Default for PageRankConfig {
    fn default() -> Self {
        Self {
            damping_factor: 0.85,
            iterations: 20,
            tolerance: 0.0001,
            dangling_redistribution: true,
        }
    }
}

/// Calculate PageRank for the graph view
pub fn page_rank(
    view: &GraphView,
    config: PageRankConfig,
) -> HashMap<NodeId, f64> {
    let n = view.node_count;
    
    if n == 0 {
        return HashMap::new();
    }

    // 2. Initialize scores
    // LDBC Graphalytics spec: initial score is 1/N
    let initial_score = 1.0 / n as f64;
    let mut scores = vec![initial_score; n];
    let mut next_scores = vec![0.0; n];

    // 3. Iteration
    // LDBC Graphalytics spec: PR(v) = (1-d)/N + d * sum(PR(u)/out_degree(u))
    let d = config.damping_factor;
    let base_score = (1.0 - d) / n as f64;

    for _ in 0..config.iterations {
        let mut total_diff = 0.0;

        // Compute dangling node mass if enabled
        let dangling_contrib = if config.dangling_redistribution {
            let dangling_sum: f64 = (0..n)
                .filter(|&i| view.out_degree(i) == 0)
                .map(|i| scores[i])
                .sum();
            dangling_sum / n as f64
        } else {
            0.0
        };

        for i in 0..n {
            let mut sum_incoming = 0.0;

            // Iterate over incoming edges
            for &source_idx in view.predecessors(i) {
                let out_degree = view.out_degree(source_idx);
                if out_degree > 0 {
                    sum_incoming += scores[source_idx] / out_degree as f64;
                }
            }

            next_scores[i] = base_score + d * (sum_incoming + dangling_contrib);
            total_diff += (next_scores[i] - scores[i]).abs();
        }

        // Swap buffers
        scores.copy_from_slice(&next_scores);

        // Check convergence
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
    use crate::common::GraphView;

    fn build_triangle_graph() -> GraphView {
        // Triangle: 1->2, 2->3, 3->1
        let node_count = 3;
        let index_to_node = vec![1, 2, 3];
        let mut node_to_index = HashMap::new();
        for (i, &id) in index_to_node.iter().enumerate() {
            node_to_index.insert(id, i);
        }
        let outgoing = vec![vec![1], vec![2], vec![0]]; // 1->2, 2->3, 3->1
        let incoming = vec![vec![2], vec![0], vec![1]]; // 1<-3, 2<-1, 3<-2
        GraphView::from_adjacency_list(node_count, index_to_node, node_to_index, outgoing, incoming, None)
    }

    fn build_star_graph() -> GraphView {
        // Star: 1->2, 1->3, 1->4 (node 1 is hub)
        let node_count = 4;
        let index_to_node = vec![1, 2, 3, 4];
        let mut node_to_index = HashMap::new();
        for (i, &id) in index_to_node.iter().enumerate() {
            node_to_index.insert(id, i);
        }
        let outgoing = vec![vec![1, 2, 3], vec![], vec![], vec![]];
        let incoming = vec![vec![], vec![0], vec![0], vec![0]];
        GraphView::from_adjacency_list(node_count, index_to_node, node_to_index, outgoing, incoming, None)
    }

    #[test]
    fn test_pagerank_empty_graph() {
        let view = GraphView::from_adjacency_list(
            0, vec![], HashMap::new(), vec![], vec![], None,
        );
        let result = page_rank(&view, PageRankConfig::default());
        assert!(result.is_empty());
    }

    #[test]
    fn test_pagerank_single_node() {
        let mut node_to_index = HashMap::new();
        node_to_index.insert(1u64, 0);
        let view = GraphView::from_adjacency_list(
            1, vec![1], node_to_index, vec![vec![]], vec![vec![]], None,
        );
        let result = page_rank(&view, PageRankConfig {
            damping_factor: 0.85,
            iterations: 20,
            tolerance: 0.0001,
            dangling_redistribution: true,
        });
        assert_eq!(result.len(), 1);
        // Single node with dangling redistribution: score should be ~1.0
        let score = result[&1];
        assert!((score - 1.0).abs() < 0.01, "Single node score should be ~1.0, got {}", score);
    }

    #[test]
    fn test_pagerank_triangle_symmetric() {
        let view = build_triangle_graph();
        let result = page_rank(&view, PageRankConfig {
            damping_factor: 0.85,
            iterations: 100,
            tolerance: 1e-10,
            dangling_redistribution: true,
        });

        assert_eq!(result.len(), 3);
        // In a symmetric triangle, all nodes should have equal PageRank
        let scores: Vec<f64> = vec![result[&1], result[&2], result[&3]];
        let avg = scores.iter().sum::<f64>() / 3.0;
        for s in &scores {
            assert!((s - avg).abs() < 0.001, "Triangle nodes should have equal rank, got {:?}", scores);
        }
    }

    #[test]
    fn test_pagerank_star_hub_has_lowest_rank() {
        // In a star where hub points outward, the hub sends rank but receives none
        let view = build_star_graph();
        let result = page_rank(&view, PageRankConfig {
            damping_factor: 0.85,
            iterations: 50,
            tolerance: 1e-10,
            dangling_redistribution: false,
        });

        assert_eq!(result.len(), 4);
        // Without dangling redistribution, hub (node 1) that only sends should have lowest rank
        // Leaf nodes (2,3,4) receive from hub but are dangling
        let hub_score = result[&1];
        let leaf_score = result[&2];
        // Hub sends all its rank out, leaf receives rank from hub
        assert!(leaf_score > hub_score,
            "Leaf ({}) should have higher rank than hub ({}) without dangling redistribution",
            leaf_score, hub_score);
    }

    #[test]
    fn test_pagerank_scores_sum_to_one() {
        let view = build_triangle_graph();
        let result = page_rank(&view, PageRankConfig {
            damping_factor: 0.85,
            iterations: 100,
            tolerance: 1e-10,
            dangling_redistribution: true,
        });

        let total: f64 = result.values().sum();
        assert!((total - 1.0).abs() < 0.01,
            "PageRank scores should sum to ~1.0 with dangling redistribution, got {}", total);
    }

    #[test]
    fn test_pagerank_convergence() {
        let view = build_triangle_graph();
        // With very high tolerance, should converge in 1 iteration
        let result_1 = page_rank(&view, PageRankConfig {
            damping_factor: 0.85,
            iterations: 1,
            tolerance: 0.0,
            dangling_redistribution: true,
        });
        let result_100 = page_rank(&view, PageRankConfig {
            damping_factor: 0.85,
            iterations: 100,
            tolerance: 0.0,
            dangling_redistribution: true,
        });

        // More iterations should give more accurate result
        // In a symmetric triangle, converged score is 1/3
        let target = 1.0 / 3.0;
        let diff_1 = (result_1[&1] - target).abs();
        let diff_100 = (result_100[&1] - target).abs();
        assert!(diff_100 <= diff_1,
            "100 iterations should be closer to target than 1: diff_1={}, diff_100={}", diff_1, diff_100);
    }

    #[test]
    fn test_pagerank_dangling_redistribution_flag() {
        let view = build_star_graph(); // Nodes 2,3,4 are dangling
        let with_dangling = page_rank(&view, PageRankConfig {
            damping_factor: 0.85,
            iterations: 50,
            tolerance: 1e-10,
            dangling_redistribution: true,
        });
        let without_dangling = page_rank(&view, PageRankConfig {
            damping_factor: 0.85,
            iterations: 50,
            tolerance: 1e-10,
            dangling_redistribution: false,
        });

        // With dangling redistribution, scores should sum to ~1.0
        let total_with: f64 = with_dangling.values().sum();
        assert!((total_with - 1.0).abs() < 0.01,
            "With dangling redistribution, sum should be ~1.0, got {}", total_with);

        // Without redistribution, total will be < 1.0 (rank leaks through dangling nodes)
        let total_without: f64 = without_dangling.values().sum();
        assert!(total_without < 0.9,
            "Without dangling redistribution, rank should leak, got sum={}", total_without);
    }

    #[test]
    fn test_pagerank_damping_factor_effect() {
        let view = build_triangle_graph();
        let low_damping = page_rank(&view, PageRankConfig {
            damping_factor: 0.5,
            iterations: 100,
            tolerance: 1e-10,
            dangling_redistribution: true,
        });
        let high_damping = page_rank(&view, PageRankConfig {
            damping_factor: 0.99,
            iterations: 100,
            tolerance: 1e-10,
            dangling_redistribution: true,
        });

        // Both should produce valid scores summing to 1
        let total_low: f64 = low_damping.values().sum();
        let total_high: f64 = high_damping.values().sum();
        assert!((total_low - 1.0).abs() < 0.01);
        assert!((total_high - 1.0).abs() < 0.01);
    }
}