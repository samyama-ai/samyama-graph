//! Manager for multiple vector indices
//!
//! Handles indexing for different node labels and property keys.

use crate::graph::NodeId;
use crate::vector::index::Quantization;
use crate::vector::index::{VectorIndex, DistanceMetric, VectorError, VectorResult};
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
    /// Index name -> the (label, property) it indexes.
    ///
    /// `CREATE VECTOR INDEX vidx FOR (n:N) ON (n.embedding)` names the index, and until
    /// #1041 that name was parsed and dropped: the query addressed the index by label and
    /// property instead, so the name a user was required to supply was never usable. It is
    /// how Neo4j's form of `db.index.vector.queryNodes` addresses an index, so it has to
    /// survive creation.
    names: RwLock<HashMap<String, IndexKey>>,
    /// Index -> the embedding model that produced its vectors (#275).
    ///
    /// Vectors from different models are not comparable, even at the same
    /// dimension: a query embedded by model B against an index built by model A
    /// returns confident, wrong neighbours and no error. Recording the build
    /// model is what lets a text query be checked before it searches. An index
    /// with no entry has an unknown model (created by DDL or a raw-vector
    /// caller, or loaded from a `metadata.json` written before this field), and
    /// is never refused on that account.
    model_ids: RwLock<HashMap<IndexKey, String>>,
}

impl VectorIndexManager {
    /// Create a new manager
    pub fn new() -> Self {
        Self {
            indices: RwLock::new(HashMap::new()),
            names: RwLock::new(HashMap::new()),
            model_ids: RwLock::new(HashMap::new()),
        }
    }

    /// Create a new index, remembering the name it was given.
    pub fn create_index_named(
        &self,
        name: Option<&str>,
        label: &str,
        property_key: &str,
        dimensions: usize,
        metric: DistanceMetric,
        quantization: Quantization,
    ) -> VectorResult<()> {
        self.create_index_quantized(label, property_key, dimensions, metric, quantization)?;
        if let Some(name) = name.map(str::trim).filter(|n| !n.is_empty()) {
            self.names.write().unwrap().insert(
                name.to_string(),
                IndexKey { label: label.to_string(), property_key: property_key.to_string() },
            );
        }
        Ok(())
    }

    /// The (label, property) an index name refers to.
    pub fn resolve_name(&self, name: &str) -> Option<(String, String)> {
        self.names.read().unwrap().get(name)
            .map(|k| (k.label.clone(), k.property_key.clone()))
    }

    /// Every index name known, for an error message that can be acted on.
    pub fn index_names(&self) -> Vec<String> {
        let mut v: Vec<String> = self.names.read().unwrap().keys().cloned().collect();
        v.sort();
        v
    }

    /// Create a new index
    pub fn create_index(
        &self,
        label: &str,
        property_key: &str,
        dimensions: usize,
        metric: DistanceMetric,
    ) -> VectorResult<()> {
        self.create_index_quantized(label, property_key, dimensions, metric, Quantization::None)
    }

    /// Create an index that stores its vectors at the given precision (NDS-09).
    pub fn create_index_quantized(
        &self,
        label: &str,
        property_key: &str,
        dimensions: usize,
        metric: DistanceMetric,
        quantization: Quantization,
    ) -> VectorResult<()> {
        let key = IndexKey {
            label: label.to_string(),
            property_key: property_key.to_string(),
        };
        
        let index = VectorIndex::with_quantization(dimensions, metric, quantization);
        let mut indices = self.indices.write().unwrap();
        // A (re)created index starts empty, so whatever model built the one it
        // replaces says nothing about it.
        self.model_ids.write().unwrap().remove(&key);
        indices.insert(key, Arc::new(RwLock::new(index)));
        
        Ok(())
    }

    /// Record the embedding model that builds this index (#275).
    ///
    /// Returns `false` (and records nothing) when no index exists for the key.
    /// An empty `model_id` clears the record, making the model unknown again.
    pub fn set_model_id(&self, label: &str, property_key: &str, model_id: &str) -> bool {
        let key = IndexKey { label: label.to_string(), property_key: property_key.to_string() };
        if !self.indices.read().unwrap().contains_key(&key) {
            return false;
        }
        let model_id = model_id.trim();
        let mut ids = self.model_ids.write().unwrap();
        if model_id.is_empty() {
            ids.remove(&key);
        } else {
            ids.insert(key, model_id.to_string());
        }
        true
    }

    /// Record `model_id` only if the index has no model recorded yet, and
    /// return the model the index is bound to afterwards (`None` when there is
    /// no such index). Used by auto-embed: the first model to write vectors
    /// into an unbound index is the one that built it.
    pub fn bind_model_if_unset(&self, label: &str, property_key: &str, model_id: &str) -> Option<String> {
        let key = IndexKey { label: label.to_string(), property_key: property_key.to_string() };
        if !self.indices.read().unwrap().contains_key(&key) {
            return None;
        }
        let model_id = model_id.trim();
        let mut ids = self.model_ids.write().unwrap();
        if model_id.is_empty() {
            return ids.get(&key).cloned();
        }
        Some(ids.entry(key).or_insert_with(|| model_id.to_string()).clone())
    }

    /// The embedding model recorded for an index, if any (#275).
    pub fn model_id(&self, label: &str, property_key: &str) -> Option<String> {
        let key = IndexKey { label: label.to_string(), property_key: property_key.to_string() };
        self.model_ids.read().unwrap().get(&key).cloned()
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
                index.add(node_id, vector)
            }
            // Returning Ok here made a vector added to a non-existent index indistinguishable
            // from one that was stored, which is how auto-embed could generate thousands of
            // embeddings that went nowhere without a single error (#310). Callers that treat
            // a missing index as acceptable can still ignore the result; they can no longer
            // do so unknowingly (#1569).
            None => Err(VectorError::IndexError(format!(
                "no vector index for {}.{}; vector for node {} was not stored",
                label,
                property_key,
                node_id.as_u64()
            ))),
        }
    }

    /// Search an index.
    ///
    /// No index is an error, not an empty answer. It used to return `[]`, so a
    /// vector search over an index that was never created, or that did not
    /// come back after a restart, read exactly like "nothing is similar" --
    /// the one failure in the index contract that nothing could see (#1660).
    /// The full-text procedure already says `no full-text index named ...`;
    /// this is the same rule for vectors.
    pub fn search(
        &self,
        label: &str,
        property_key: &str,
        query: &[f32],
        k: usize,
    ) -> VectorResult<Vec<(NodeId, f32)>> {
        let index_lock = self.get_index(label, property_key).ok_or_else(|| self.missing(label, property_key))?;
        let index = index_lock.read().unwrap();
        index.search(query, k)
    }

    /// The error a search over an index that does not exist raises. Names the
    /// indexes that do, so a typo in the label or property is visible.
    fn missing(&self, label: &str, property_key: &str) -> VectorError {
        let known: Vec<String> = self
            .indices
            .read()
            .unwrap()
            .keys()
            .map(|k| format!(":{}({})", k.label, k.property_key))
            .collect();
        VectorError::IndexError(format!(
            "no vector index on :{label}({property_key}). {}",
            if known.is_empty() {
                format!("None exists; CREATE VECTOR INDEX <name> FOR (n:{label}) ON (n.{property_key}) creates one.")
            } else {
                format!("Known vector indexes: {}", known.join(", "))
            }
        ))
    }

    /// Every index as a persistable declaration (#1477).
    ///
    /// Carries what `dump_all` drops: the **name** the DDL gave the index, and
    /// its quantization. Neither was in `metadata.json`, so a dump/load round
    /// trip restored an index that `db.index.vector.queryNodes('vidx', ...)`
    /// could not resolve — `index_names()` came back empty and the error could
    /// not even name the index that had in fact been loaded.
    pub fn definitions(&self) -> Vec<crate::index::catalog::IndexDefinition> {
        let names = self.names.read().unwrap();
        let indices = self.indices.read().unwrap();
        indices
            .iter()
            .map(|(key, index_lock)| {
                let index = index_lock.read().unwrap();
                crate::index::catalog::IndexDefinition::Vector {
                    name: names
                        .iter()
                        .find(|(_, k)| *k == key)
                        .map(|(n, _)| n.clone()),
                    label: key.label.clone(),
                    property: key.property_key.clone(),
                    dimensions: index.dimensions(),
                    metric: index.metric(),
                    quantization: index.quantization(),
                }
            })
            .collect()
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

    /// [`search`](Self::search), keeping only the nodes `keep` accepts, still
    /// returning up to `k` of them (#1605).
    ///
    /// The HNSW graph has no delete, so a node that lost the indexed label (or
    /// was deleted) is still in it. Asking for `k` and filtering would return
    /// fewer than `k`, so the search widens until it has `k` kept nodes or has
    /// looked at every vector. A node indexed twice -- it lost the label and
    /// regained it -- is returned once, at its best distance.
    pub fn search_filtered(
        &self,
        label: &str,
        property_key: &str,
        query: &[f32],
        k: usize,
        keep: impl Fn(NodeId) -> bool,
    ) -> VectorResult<Vec<(NodeId, f32)>> {
        // No index is an error, as in [`Self::search`] (#1660).
        let index_lock = self.get_index(label, property_key).ok_or_else(|| self.missing(label, property_key))?;
        let index = index_lock.read().unwrap();
        widen(index.len(), k, |fetch| index.search(query, fetch), &keep)
    }

    /// [`search_all`](Self::search_all), keeping only the `(label, node)` pairs
    /// `keep` accepts (#1605).
    pub fn search_all_filtered(
        &self,
        query: &[f32],
        k: usize,
        keep: impl Fn(&str, NodeId) -> bool,
    ) -> VectorResult<Vec<(NodeId, f32)>> {
        let mut best: HashMap<NodeId, f32> = HashMap::new();
        for key in self.list_indices() {
            if let Some(index_lock) = self.get_index(&key.label, &key.property_key) {
                let index = index_lock.read().unwrap();
                if index.dimensions() != query.len() {
                    continue;
                }
                let kept = widen(index.len(), k, |fetch| index.search(query, fetch), &|id| {
                    keep(&key.label, id)
                })?;
                for (node_id, distance) in kept {
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
        // Read dims + metric + quantization under a short read lock, then
        // release before building.
        //
        // Quantization has to come across with the rest. `CREATE VECTOR INDEX
        // ... OPTIONS {quantization: "fp16"}` registers the index and the
        // operator then backfills it through here, so a rebuild that rebuilt
        // at full precision silently undid the option one statement after it
        // was honoured -- the caller asked for half the memory, got an f32
        // index, and was told the statement succeeded (#1385).
        let (dims, metric, quantization) = {
            let indices = self.indices.read().unwrap();
            match indices.get(&key) {
                Some(idx_lock) => {
                    let idx = idx_lock.read().unwrap();
                    (idx.dimensions(), idx.metric(), idx.quantization())
                }
                None => return Ok(()), // no index registered for this key — nothing to do
            }
        };
        // Build a fresh HNSW outside any lock (potentially expensive for large datasets).
        // Skip individual vectors that don't match the index dimension rather than
        // aborting the whole rebuild — a single malformed embedding must not leave the
        // entire index empty (which then returns 0 results / panics on search).
        let mut new_index = VectorIndex::with_quantization(dims, metric, quantization);
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
        let model_ids = self.model_ids.read().unwrap();
        let mut metadata = Vec::new();

        for (key, index_lock) in indices.iter() {
            let index = index_lock.read().unwrap();
            let index_filename = format!("{}_{}.hnsw", key.label, key.property_key);
            let index_path = path.join(&index_filename);
            index.dump(&index_path)?;

            let mut entry = serde_json::json!({
                "label": key.label,
                "property_key": key.property_key,
                "dimensions": index.dimensions(),
                "metric": index.metric(),
                "filename": index_filename,
            });
            // Written only when known, so an index whose model is unknown
            // round-trips as unknown rather than as some placeholder (#275).
            if let Some(model_id) = model_ids.get(key) {
                entry["model_id"] = serde_json::Value::String(model_id.clone());
            }
            metadata.push(entry);
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
        let mut model_ids = self.model_ids.write().unwrap();
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
            // Optional: metadata written before #275 has no `model_id`, and an
            // index loaded from it has an unknown model -- searchable, never
            // refused.
            match item.get("model_id").and_then(|v| v.as_str()).map(str::trim) {
                Some(m) if !m.is_empty() => {
                    model_ids.insert(key.clone(), m.to_string());
                }
                _ => {
                    model_ids.remove(&key);
                }
            }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn model_ids_need_an_index_and_an_empty_one_clears() {
        let mgr = VectorIndexManager::default();
        assert!(!mgr.set_model_id("Doc", "v", "m1"), "no index yet");
        assert_eq!(mgr.bind_model_if_unset("Doc", "v", "m1"), None);

        mgr.create_index("Doc", "v", 2, DistanceMetric::Cosine)
            .unwrap();
        assert_eq!(
            mgr.bind_model_if_unset("Doc", "v", "  "),
            None,
            "blank binds nothing"
        );
        assert!(mgr.set_model_id("Doc", "v", " m1 "));
        assert_eq!(mgr.model_id("Doc", "v").as_deref(), Some("m1"));
        assert_eq!(
            mgr.bind_model_if_unset("Doc", "v", ""),
            Some("m1".to_string())
        );
        assert_eq!(
            mgr.bind_model_if_unset("Doc", "v", "m2"),
            Some("m1".to_string())
        );
        assert!(mgr.set_model_id("Doc", "v", ""));
        assert_eq!(mgr.model_id("Doc", "v"), None);
    }

    #[test]
    fn adding_to_or_searching_a_missing_index_is_an_error() {
        let mgr = VectorIndexManager::new();
        assert!(matches!(
            mgr.add_vector("Nope", "v", NodeId::new(1), &vec![1.0, 0.0]),
            Err(VectorError::IndexError(_))
        ));
        assert!(mgr.list_indices().is_empty());
        // Searching it is an error too, naming the index (#1660).
        let err = mgr.search("Nope", "v", &[1.0, 0.0], 3).unwrap_err().to_string();
        assert!(err.contains("no vector index on :Nope(v)"), "{err}");
        assert!(err.contains("CREATE VECTOR INDEX"), "{err}");
        // With one index present, the error says which exist.
        mgr.create_index("Doc", "v", 2, DistanceMetric::Cosine).unwrap();
        let err = mgr.search("Nope", "v", &[1.0, 0.0], 3).unwrap_err().to_string();
        assert!(err.contains(":Doc(v)"), "{err}");
    }

    #[test]
    fn search_all_skips_indexes_of_another_dimension_and_keeps_the_best_distance() {
        let mgr = VectorIndexManager::new();
        mgr.create_index("A", "v", 2, DistanceMetric::Cosine)
            .unwrap();
        mgr.create_index("B", "v", 2, DistanceMetric::Cosine)
            .unwrap();
        mgr.create_index("C", "v", 3, DistanceMetric::Cosine)
            .unwrap();
        mgr.add_vector("A", "v", NodeId::new(1), &vec![1.0, 0.0])
            .unwrap();
        mgr.add_vector("B", "v", NodeId::new(1), &vec![0.6, 0.8])
            .unwrap();
        mgr.add_vector("B", "v", NodeId::new(2), &vec![0.0, 1.0])
            .unwrap();
        mgr.add_vector("C", "v", NodeId::new(3), &vec![1.0, 0.0, 0.0])
            .unwrap();

        let hits = mgr.search_all(&[1.0, 0.0], 5).unwrap();
        let ids: Vec<u64> = hits.iter().map(|(n, _)| n.as_u64()).collect();
        assert_eq!(
            ids,
            vec![1, 2],
            "node 1 once, at its better distance; C skipped"
        );
        assert!(hits[0].1 < 1e-6);
    }

    #[test]
    fn rebuild_skips_mismatched_vectors_and_ignores_unknown_indexes() {
        let mgr = VectorIndexManager::new();
        assert!(mgr
            .rebuild_for_label("Nope", "v", &[(NodeId::new(1), vec![1.0])])
            .is_ok());
        mgr.create_index("A", "v", 2, DistanceMetric::Cosine)
            .unwrap();
        mgr.rebuild_for_label(
            "A",
            "v",
            &[
                (NodeId::new(1), vec![1.0, 0.0]),
                (NodeId::new(2), vec![1.0, 0.0, 0.0]),
            ],
        )
        .unwrap();
        let idx = mgr.get_index("A", "v").unwrap();
        assert_eq!(idx.read().unwrap().len(), 1);
    }

    #[test]
    fn dump_and_load_round_trip_model_ids() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("vectors");
        let mgr = VectorIndexManager::new();
        mgr.create_index("A", "v", 2, DistanceMetric::Cosine)
            .unwrap();
        mgr.create_index("B", "w", 2, DistanceMetric::Cosine)
            .unwrap();
        mgr.add_vector("A", "v", NodeId::new(1), &vec![1.0, 0.0])
            .unwrap();
        mgr.set_model_id("A", "v", "model-a");
        mgr.dump_all(&path).unwrap();

        let loaded = VectorIndexManager::new();
        // A stale binding for B is dropped, since the dump has none for it.
        loaded
            .create_index("B", "w", 2, DistanceMetric::Cosine)
            .unwrap();
        loaded.set_model_id("B", "w", "stale");
        loaded.load_all(&path).unwrap();
        assert_eq!(loaded.model_id("A", "v").as_deref(), Some("model-a"));
        assert_eq!(loaded.model_id("B", "w"), None);
        assert_eq!(
            loaded.search("A", "v", &[1.0, 0.0], 1).unwrap()[0].0,
            NodeId::new(1)
        );
    }

    #[test]
    fn load_all_without_a_directory_or_metadata_is_a_no_op() {
        let dir = tempfile::tempdir().unwrap();
        let mgr = VectorIndexManager::new();
        mgr.load_all(&dir.path().join("absent")).unwrap();
        mgr.load_all(dir.path()).unwrap();
        assert!(mgr.list_indices().is_empty());
    }

    #[test]
    fn load_all_rejects_malformed_metadata() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("metadata.json"), b"not json").unwrap();
        let mgr = VectorIndexManager::new();
        assert!(mgr.load_all(dir.path()).is_err());
    }
}

/// Search with a growing fetch size until `k` results pass `keep`, or the
/// index has nothing more to give. Results keep the search's order, which is
/// nearest first; a node already taken is skipped.
fn widen(
    len: usize,
    k: usize,
    search: impl Fn(usize) -> VectorResult<Vec<(NodeId, f32)>>,
    keep: &dyn Fn(NodeId) -> bool,
) -> VectorResult<Vec<(NodeId, f32)>> {
    // No shortcut for an empty index or `k == 0`: the search itself runs at
    // least once, so its checks -- a query of the wrong dimension -- still
    // refuse.
    let mut fetch = k.min(len);
    loop {
        let mut seen = std::collections::HashSet::new();
        let kept: Vec<(NodeId, f32)> = search(fetch)?
            .into_iter()
            .filter(|(id, _)| seen.insert(*id) && keep(*id))
            .take(k)
            .collect();
        if kept.len() >= k || fetch >= len {
            return Ok(kept);
        }
        fetch = (fetch * 2).min(len);
    }
}
