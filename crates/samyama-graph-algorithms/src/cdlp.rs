//! Community Detection via Label Propagation (CDLP)
//!
//! Synchronous label propagation algorithm for community detection.
//! Each node starts with its own ID as label, then iteratively sets its
//! label to the most frequent label among its neighbors.

use super::common::{GraphView, NodeId};
use std::collections::HashMap;

/// Result of CDLP algorithm
#[derive(Debug, Clone)]
pub struct CdlpResult {
    /// Mapping from NodeId to community label
    pub labels: HashMap<NodeId, NodeId>,
    /// Number of iterations until convergence
    pub iterations: usize,
}

/// Configuration for CDLP
pub struct CdlpConfig {
    /// Maximum number of iterations
    pub max_iterations: usize,
}

impl Default for CdlpConfig {
    fn default() -> Self {
        Self {
            max_iterations: 100,
        }
    }
}

/// Run synchronous CDLP on the given graph view
///
/// Returns community labels for each node. Uses undirected edges
/// (both successors and predecessors).
pub fn cdlp(view: &GraphView, config: &CdlpConfig) -> CdlpResult {
    let n = view.node_count;
    if n == 0 {
        return CdlpResult { labels: HashMap::new(), iterations: 0 };
    }

    // Initialize: each node gets its own NodeId as label
    let mut labels: Vec<NodeId> = (0..n).map(|i| view.index_to_node[i]).collect();
    let mut new_labels = labels.clone();
    let mut converged = false;
    let mut iterations = 0;

    for _iter in 0..config.max_iterations {
        converged = true;
        iterations += 1;

        for idx in 0..n {
            // Collect neighbor labels (undirected)
            let mut label_counts: HashMap<NodeId, usize> = HashMap::new();
            for &neighbor in view.successors(idx) {
                *label_counts.entry(labels[neighbor]).or_insert(0) += 1;
            }
            for &neighbor in view.predecessors(idx) {
                *label_counts.entry(labels[neighbor]).or_insert(0) += 1;
            }

            if label_counts.is_empty() {
                new_labels[idx] = labels[idx];
                continue;
            }

            // Pick the most frequent label; break ties by choosing smallest label
            let max_count = *label_counts.values().max().unwrap();
            let best_label = label_counts.into_iter()
                .filter(|(_, count)| *count == max_count)
                .map(|(label, _)| label)
                .min()
                .unwrap();

            if best_label != labels[idx] {
                converged = false;
            }
            new_labels[idx] = best_label;
        }

        std::mem::swap(&mut labels, &mut new_labels);

        if converged {
            break;
        }
    }

    let result_labels: HashMap<NodeId, NodeId> = (0..n)
        .map(|idx| (view.index_to_node[idx], labels[idx]))
        .collect();

    CdlpResult { labels: result_labels, iterations }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use crate::common::GraphView;

    fn make_triangle_graph() -> GraphView {
        // Triangle: 1-2, 2-3, 1-3
        let index_to_node = vec![1, 2, 3];
        let mut node_to_index = HashMap::new();
        node_to_index.insert(1, 0);
        node_to_index.insert(2, 1);
        node_to_index.insert(3, 2);

        let outgoing = vec![vec![1, 2], vec![0, 2], vec![0, 1]];
        let incoming = vec![vec![1, 2], vec![0, 2], vec![0, 1]];

        GraphView::from_adjacency_list(3, index_to_node, node_to_index, outgoing, incoming, None)
    }

    #[test]
    fn test_cdlp_triangle() {
        let view = make_triangle_graph();
        let result = cdlp(&view, &CdlpConfig::default());
        // In a triangle, all nodes should converge to the same label
        let labels: Vec<NodeId> = result.labels.values().cloned().collect();
        assert!(labels.iter().all(|l| *l == labels[0]),
            "All nodes in a triangle should have the same community label");
    }

    #[test]
    fn test_cdlp_disconnected() {
        // Two disconnected nodes: 1 and 2
        let index_to_node = vec![1, 2];
        let mut node_to_index = HashMap::new();
        node_to_index.insert(1, 0);
        node_to_index.insert(2, 1);

        let view = GraphView::from_adjacency_list(
            2, index_to_node, node_to_index,
            vec![vec![], vec![]], vec![vec![], vec![]], None,
        );
        let result = cdlp(&view, &CdlpConfig::default());
        assert_ne!(result.labels[&1], result.labels[&2]);
    }

    #[test]
    fn test_cdlp_empty() {
        let view = GraphView::from_adjacency_list(
            0, vec![], HashMap::new(), vec![], vec![], None,
        );
        let result = cdlp(&view, &CdlpConfig::default());
        assert!(result.labels.is_empty());
    }
}
