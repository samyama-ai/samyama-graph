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