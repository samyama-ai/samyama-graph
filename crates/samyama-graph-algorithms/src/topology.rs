//! Graph topology analysis algorithms
//!
//! Implements REQ-ALGO-005 (Triangle Counting)

use super::common::GraphView;
use std::collections::HashSet;

/// Triangle Counting
///
/// Returns total number of triangles in the graph.
/// For undirected graphs, each triangle is counted once.
/// For directed, we treat as undirected for counting.
pub fn count_triangles(view: &GraphView) -> usize {
    let mut triangle_count = 0;
    
    // Using simple algorithm: for each edge (u, v), find common neighbors of u and v.
    // To avoid overcounting, we only consider nodes with indices i < j < k.
    
    for u in 0..view.node_count {
        let u_neighbors: HashSet<_> = view.successors(u).iter()
            .chain(view.predecessors(u).iter())
            .cloned()
            .collect();
            
        for &v in &u_neighbors {
            if v <= u { continue; } // Order u < v
            
            let v_neighbors: HashSet<_> = view.successors(v).iter()
                .chain(view.predecessors(v).iter())
                .cloned()
                .collect();
                
            for &w in &v_neighbors {
                if w <= v { continue; } // Order v < w
                
                // Check if w is also neighbor of u
                if u_neighbors.contains(&w) {
                    triangle_count += 1;
                }
            }
        }
    }
    
    triangle_count
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_triangle_counting() {
        // Complete graph K4: 4 nodes, all connected.
        // Triangles: (0,1,2), (0,1,3), (0,2,3), (1,2,3) -> 4 triangles.
        
        let node_count = 4;
        let mut outgoing = vec![vec![]; 4];
        let mut incoming = vec![vec![]; 4];
        
        for i in 0..4 {
            for j in (i+1)..4 {
                outgoing[i].push(j);
                incoming[j].push(i);
            }
        }
        
        let view = GraphView::from_adjacency_list(
            node_count,
            vec![0, 1, 2, 3],
            HashMap::new(),
            outgoing,
            incoming,
            None,
        );
        
        let count = count_triangles(&view);
        assert_eq!(count, 4);
    }
}
