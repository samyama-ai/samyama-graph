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
        if let Some(index_lock) = self.get_index(label, property_key) {
            let mut index = index_lock.write().unwrap();
            index.add(node_id, vector)?;
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
