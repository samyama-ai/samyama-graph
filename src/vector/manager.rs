//! Manager for multiple vector indices
//!
//! Handles indexing for different node labels and property keys.

use crate::graph::NodeId;
use crate::vector::index::{VectorIndex, DistanceMetric, VectorResult};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

/// Key for identifying a vector index: (Label, PropertyKey)
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct IndexKey {
    pub label: String,
    pub property_key: String,
}

/// Manager for all vector indices in the system
#[derive(Debug)]
pub struct VectorIndexManager {
    indices: RwLock<HashMap<IndexKey, Arc<RwLock<VectorIndex>>>>,
}

impl VectorIndexManager {
    /// Create a new manager
    pub fn new() -> Self {
        Self {
            indices: RwLock::new(HashMap::new()),
        }
    }

    /// Create a new index
    pub fn create_index(
        &self,
        label: &str,
        property_key: &str,
        dimensions: usize,
        metric: DistanceMetric,
    ) -> VectorResult<()> {
        let key = IndexKey {
            label: label.to_string(),
            property_key: property_key.to_string(),
        };
        
        let index = VectorIndex::new(dimensions, metric);
        let mut indices = self.indices.write().unwrap();
        indices.insert(key, Arc::new(RwLock::new(index)));
        
        Ok(())
    }

    /// Get an index
    pub fn get_index(&self, label: &str, property_key: &str) -> Option<Arc<RwLock<VectorIndex>>> {
        let key = IndexKey {
            label: label.to_string(),
            property_key: property_key.to_string(),
        };
        
        let indices = self.indices.read().unwrap();
        indices.get(&key).cloned()
    }

    /// Add a vector to an index
    pub fn add_vector(
        &self,
        label: &str,
        property_key: &str,
        node_id: NodeId,
        vector: &Vec<f32>,
    ) -> VectorResult<()> {
        match self.get_index(label, property_key) {
            Some(index_lock) => {
                let mut index = index_lock.write().unwrap();
                index.add(node_id, vector)?;
            }
            // Returning Ok here made a vector added to a non-existent index indistinguishable
            // from one that was stored, which is how auto-embed could generate thousands of
            // embeddings that went nowhere without a single error (#310). Callers that treat
            // a missing index as acceptable can still ignore the result; they can no longer
            // do so unknowingly.
            None => {
                tracing::warn!(
                    "no vector index for {}.{}; vector for node {} was not stored",
                    label, property_key, node_id.as_u64()
                );
            }
        }
        Ok(())
    }

    /// Search an index
    pub fn search(
        &self,
        label: &str,
        property_key: &str,
        query: &[f32],
        k: usize,
    ) -> VectorResult<Vec<(NodeId, f32)>> {
        if let Some(index_lock) = self.get_index(label, property_key) {
            let index = index_lock.read().unwrap();
            return index.search(query, k);
        }
        Ok(Vec::new())
    }

    /// List all indices
    pub fn list_indices(&self) -> Vec<IndexKey> {
        let indices = self.indices.read().unwrap();
        indices.keys().cloned().collect()
    }

    /// Search across ALL indices (every label + property), merge the per-index
    /// hits, and return the global top-k by distance.
    ///
    /// This backs the "no label given" default in the vector-search API: instead
    /// of guessing a single label, the query vector is run against every index
    /// whose dimensionality matches, so a caller who doesn't know (or care) which
    /// label holds the answer still gets the best matches across the whole graph.
    /// Indices whose dimension differs from the query are skipped (a query vector
    /// can only be compared within its own embedding space).
    pub fn search_all(&self, query: &[f32], k: usize) -> VectorResult<Vec<(NodeId, f32)>> {
        // Snapshot the key list first so we don't hold the map lock across the
        // per-index searches (each of which takes the index's own lock).
        let keys = self.list_indices();
        // A node can be indexed under more than one (label, property); keep only
        // its best (smallest) distance so it isn't returned twice.
        let mut best: HashMap<NodeId, f32> = HashMap::new();
        for key in keys {
            if let Some(index_lock) = self.get_index(&key.label, &key.property_key) {
                let index = index_lock.read().unwrap();
                if index.dimensions() != query.len() {
                    continue;
                }
                for (node_id, distance) in index.search(query, k)? {
                    best.entry(node_id)
                        .and_modify(|d| {
                            if distance < *d {
                                *d = distance;
                            }
                        })
                        .or_insert(distance);
                }
            }
        }
        let mut merged: Vec<(NodeId, f32)> = best.into_iter().collect();
        merged.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
        merged.truncate(k);
        Ok(merged)
    }

    /// Drop and rebuild a specific HNSW index from a caller-supplied vector list.
    ///
    /// Snapshot import (create_node_stub / create_node) bypasses the event loop
    /// that normally calls add_vector, leaving the HNSW empty even though node
    /// properties carry Vector values. Rebuilding from scratch is idempotent and
    /// avoids the duplicate entries that would occur if add_vector were called on
    /// top of a partially-populated index.
    pub fn rebuild_for_label(
        &self,
        label: &str,
        property_key: &str,
        vectors: &[(NodeId, Vec<f32>)],
    ) -> VectorResult<()> {
        let key = IndexKey {
            label: label.to_string(),
            property_key: property_key.to_string(),
        };
        // Read dims + metric under a short read lock, then release before building.
        let (dims, metric) = {
            let indices = self.indices.read().unwrap();
            match indices.get(&key) {
                Some(idx_lock) => {
                    let idx = idx_lock.read().unwrap();
                    (idx.dimensions(), idx.metric())
                }
                None => return Ok(()), // no index registered for this key — nothing to do
            }
        };
        // Build a fresh HNSW outside any lock (potentially expensive for large datasets).
        // Skip individual vectors that don't match the index dimension rather than
        // aborting the whole rebuild — a single malformed embedding must not leave the
        // entire index empty (which then returns 0 results / panics on search).
        let mut new_index = VectorIndex::new(dims, metric);
        let mut skipped = 0usize;
        for (node_id, vec) in vectors {
            if new_index.add(*node_id, vec).is_err() {
                skipped += 1;
            }
        }
        if skipped > 0 {
            eprintln!(
                "[vector] rebuild {}.{}: indexed {} vectors, skipped {} with mismatched dimension (expected {})",
                label, property_key, vectors.len() - skipped, skipped, dims
            );
        }
        // Swap in via write lock on the existing Arc so concurrent readers see
        // the updated index without needing to re-acquire the outer map lock.
        let indices = self.indices.read().unwrap();
        if let Some(idx_lock) = indices.get(&key) {
            *idx_lock.write().unwrap() = new_index;
        }
        Ok(())
    }

    /// Save all indices to a directory
    pub fn dump_all(&self, path: &std::path::Path) -> VectorResult<()> {
        if !path.exists() {
            std::fs::create_dir_all(path)?;
        }

        let indices = self.indices.read().unwrap();
        let mut metadata = Vec::new();

        for (key, index_lock) in indices.iter() {
            let index = index_lock.read().unwrap();
            let index_filename = format!("{}_{}.hnsw", key.label, key.property_key);
            let index_path = path.join(&index_filename);
            index.dump(&index_path)?;

            metadata.push(serde_json::json!({
                "label": key.label,
                "property_key": key.property_key,
                "dimensions": index.dimensions(),
                "metric": index.metric(),
                "filename": index_filename,
            }));
        }

        let metadata_path = path.join("metadata.json");
        let metadata_file = std::fs::File::create(metadata_path)?;
        serde_json::to_writer_pretty(metadata_file, &metadata)
            .map_err(|e| crate::vector::VectorError::IndexError(e.to_string()))?;

        Ok(())
    }

    /// Load all indices from a directory
    pub fn load_all(&self, path: &std::path::Path) -> VectorResult<()> {
        if !path.exists() {
            return Ok(());
        }

        let metadata_path = path.join("metadata.json");
        if !metadata_path.exists() {
            return Ok(());
        }

        let metadata_file = std::fs::File::open(metadata_path)?;
        let metadata: Vec<serde_json::Value> = serde_json::from_reader(metadata_file)
            .map_err(|e| crate::vector::VectorError::IndexError(e.to_string()))?;

        let mut indices = self.indices.write().unwrap();
        for item in metadata {
            let label = item["label"].as_str().unwrap();
            let property_key = item["property_key"].as_str().unwrap();
            let dimensions = item["dimensions"].as_u64().unwrap() as usize;
            let metric: DistanceMetric = serde_json::from_value(item["metric"].clone())
                .map_err(|e| crate::vector::VectorError::IndexError(e.to_string()))?;
            let filename = item["filename"].as_str().unwrap();

            let index_path = path.join(filename);
            let index = VectorIndex::load(&index_path, dimensions, metric)?;
            
            let key = IndexKey {
                label: label.to_string(),
                property_key: property_key.to_string(),
            };
            indices.insert(key, Arc::new(RwLock::new(index)));
        }

        Ok(())
    }
}

impl Default for VectorIndexManager {
    fn default() -> Self {
        Self::new()
    }
}
