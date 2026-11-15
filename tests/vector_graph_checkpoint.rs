//! A checkpoint generation carries each index's HNSW graph, and recovery
//! reloads it without re-inserting a vector (#1973). A graph that is corrupt
//! or does not describe the index costs a rebuild from the vector list, never
//! a wrong answer.
//!
//! Whether a vector was re-inserted is read from the recovery report, not
//! from a clock.
use samyama::{
    graph::NodeId,
    vector::{
        index::Quantization, manager::CheckpointRecovery, DistanceMetric, HnswParams,
        VectorIndexManager,
    },
};
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};

const DIM: usize = 8;

/// Deterministic vectors, spread enough that distances do not tie.
fn vector(seed: u64) -> Vec<f32> {
    let mut x = seed
        .wrapping_mul(6364136223846793005)
        .wrapping_add(1442695040888963407);
    (0..DIM)
        .map(|_| {
            x = x
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            ((x >> 33) as f32 / (1u64 << 31) as f32) - 0.5
        })
        .collect()
}

/// An index large enough that searches walk the graph rather than scan, with
/// superseded and removed entries left in the graph (#1968), below the share
/// that would compact it.
fn populated(quantization: Quantization, n: u64) -> VectorIndexManager {
    let m = VectorIndexManager::new();
    m.create_index_with_params(
        Some("emb"),
        "Doc",
        "embedding",
        DIM,
        DistanceMetric::L2,
        quantization,
        HnswParams::default(),
    )
    .unwrap();
    m.set_model_id("Doc", "embedding", "model-v1");
    for i in 0..n {
        m.add_vector("Doc", "embedding", NodeId::new(i), &vector(i))
            .unwrap();
    }
    for i in 0..n / 100 {
        m.add_vector("Doc", "embedding", NodeId::new(i), &vector(n + i))
            .unwrap();
    }
    let index = m.get_index("Doc", "embedding").unwrap();
    for i in n / 100..n / 50 {
        assert!(index.write().unwrap().remove(NodeId::new(i)));
    }
    m
}

fn answers(m: &VectorIndexManager) -> Vec<Vec<(NodeId, f32)>> {
    (0..20)
        .map(|q| {
            m.search("Doc", "embedding", &vector(1_000_000 + q), 10)
                .unwrap()
        })
        .collect()
}

fn assert_same_answers(a: &VectorIndexManager, b: &VectorIndexManager) {
    for (x, y) in answers(a).iter().zip(answers(b).iter()) {
        let ids = |r: &Vec<(NodeId, f32)>| r.iter().map(|(n, _)| *n).collect::<Vec<_>>();
        assert_eq!(ids(x), ids(y));
        for ((_, dx), (_, dy)) in x.iter().zip(y.iter()) {
            assert!((dx - dy).abs() <= 1e-6, "{dx} vs {dy}");
        }
    }
    let ia = a.get_index("Doc", "embedding").unwrap();
    let ib = b.get_index("Doc", "embedding").unwrap();
    assert_eq!(ia.read().unwrap().len(), ib.read().unwrap().len());
    assert_eq!(
        ia.read().unwrap().superseded(),
        ib.read().unwrap().superseded()
    );
}

fn generation(root: &Path) -> PathBuf {
    let name = std::fs::read_to_string(root.join("CURRENT")).unwrap();
    root.join(name.trim())
}

#[test]
fn reload_uses_the_graph_and_answers_as_the_original() {
    for (quantization, n) in [(Quantization::None, 3_000), (Quantization::Fp16, 600)] {
        let dir = tempfile::tempdir().unwrap();
        let original = populated(quantization, n);
        original.checkpoint_all(dir.path()).unwrap();

        let restored = VectorIndexManager::new();
        let report = restored.recover_checkpoint(dir.path()).unwrap();
        assert_eq!(
            report,
            CheckpointRecovery {
                graphs_loaded: 1,
                rebuilt: 0,
                vectors_reinserted: 0
            }
        );
        assert_same_answers(&original, &restored);
        assert_eq!(
            restored.resolve_name("emb"),
            Some(("Doc".into(), "embedding".into()))
        );
        assert_eq!(
            restored.model_id("Doc", "embedding").as_deref(),
            Some("model-v1")
        );
        let index = restored.get_index("Doc", "embedding").unwrap();
        assert_eq!(index.read().unwrap().quantization(), quantization);

        // A removed node stays removed, and a superseded vector no longer
        // answers for its node.
        let removed = NodeId::new(n / 100);
        let hits = restored
            .search("Doc", "embedding", &vector(n / 100), 5)
            .unwrap();
        assert!(hits.iter().all(|(id, _)| *id != removed));

        // The reloaded graph takes new vectors.
        restored
            .add_vector("Doc", "embedding", NodeId::new(n + 7), &vector(9_999_999))
            .unwrap();
        let hits = restored
            .search("Doc", "embedding", &vector(9_999_999), 1)
            .unwrap();
        assert_eq!(hits[0].0, NodeId::new(n + 7));
    }
}

#[test]
fn a_corrupt_graph_file_falls_back_to_rebuilding() {
    for suffix in [
        ".topology.hnsw.graph",
        ".topology.hnsw.data",
        ".topology.ids",
    ] {
        let dir = tempfile::tempdir().unwrap();
        let original = populated(Quantization::None, 3_000);
        original.checkpoint_all(dir.path()).unwrap();
        let file = generation(dir.path()).join(format!("Doc_embedding.hnsw{suffix}"));
        assert!(file.exists(), "{file:?}");
        std::fs::write(&file, b"corrupt").unwrap();

        let restored = VectorIndexManager::new();
        let report = restored.recover_checkpoint(dir.path()).unwrap();
        assert_eq!(report.graphs_loaded, 0, "{suffix}");
        assert_eq!(report.rebuilt, 1, "{suffix}");
        let live = original
            .get_index("Doc", "embedding")
            .unwrap()
            .read()
            .unwrap()
            .len();
        assert_eq!(report.vectors_reinserted, live, "{suffix}");
        // Rebuilt from the current entries only, so nothing is superseded and
        // the answers come from a different graph: compare against exact
        // search rather than the original's graph.
        let index = restored.get_index("Doc", "embedding").unwrap();
        assert_eq!(index.read().unwrap().len(), live);
        for q in 0..5 {
            let query = vector(2_000_000 + q);
            let exact = original
                .get_index("Doc", "embedding")
                .unwrap()
                .read()
                .unwrap()
                .search_exact_where(&query, 1, &|_| true)
                .unwrap();
            let rebuilt = index
                .read()
                .unwrap()
                .search_exact_where(&query, 1, &|_| true)
                .unwrap();
            assert_eq!(exact[0].0, rebuilt[0].0);
        }
    }
}

#[test]
fn a_graph_that_does_not_describe_the_index_is_not_used() {
    let dir = tempfile::tempdir().unwrap();
    populated(Quantization::None, 3_000)
        .checkpoint_all(dir.path())
        .unwrap();
    let generation = generation(dir.path());

    // Change the index's declared graph shape and re-seal the manifest, so
    // every checksum passes and only the graph's own consistency check is
    // left to notice that it was built with another `m`.
    let metadata_path = generation.join("metadata.json");
    let mut metadata: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&metadata_path).unwrap()).unwrap();
    metadata[0]["params"]["m"] = serde_json::json!(8);
    let bytes = serde_json::to_vec(&metadata).unwrap();
    std::fs::write(&metadata_path, &bytes).unwrap();
    let manifest_path = generation.join("manifest.json");
    let mut manifest: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&manifest_path).unwrap()).unwrap();
    manifest["files"]["metadata.json"] = serde_json::json!(format!("{:x}", Sha256::digest(&bytes)));
    std::fs::write(&manifest_path, serde_json::to_vec(&manifest).unwrap()).unwrap();

    let restored = VectorIndexManager::new();
    let report = restored.recover_checkpoint(dir.path()).unwrap();
    assert_eq!(report.graphs_loaded, 0);
    assert_eq!(report.rebuilt, 1);
    let index = restored.get_index("Doc", "embedding").unwrap();
    assert_eq!(index.read().unwrap().params().m, 8);
}

#[test]
fn a_corrupt_vector_list_still_fails_the_generation() {
    let dir = tempfile::tempdir().unwrap();
    populated(Quantization::None, 300)
        .checkpoint_all(dir.path())
        .unwrap();
    std::fs::write(
        generation(dir.path()).join("Doc_embedding.hnsw"),
        b"corrupt",
    )
    .unwrap();
    let err = VectorIndexManager::new()
        .recover_checkpoint(dir.path())
        .unwrap_err();
    assert!(err.to_string().contains("checksum"), "{err}");
}

#[test]
fn empty_indexes_and_legacy_dumps_rebuild_with_nothing_to_insert_or_without_a_graph() {
    // An empty index has no graph to write; recovering it inserts nothing.
    let dir = tempfile::tempdir().unwrap();
    let empty = VectorIndexManager::new();
    empty
        .create_index("Doc", "embedding", DIM, DistanceMetric::Cosine)
        .unwrap();
    empty.checkpoint_all(dir.path()).unwrap();
    let report = VectorIndexManager::new()
        .recover_checkpoint(dir.path())
        .unwrap();
    assert_eq!(
        report,
        CheckpointRecovery {
            graphs_loaded: 0,
            rebuilt: 1,
            vectors_reinserted: 0
        }
    );

    // A legacy dump has no manifest to verify a graph against, so it is
    // rebuilt, as before.
    let dir = tempfile::tempdir().unwrap();
    let original = populated(Quantization::None, 300);
    original.dump_all(dir.path()).unwrap();
    let report = VectorIndexManager::new()
        .recover_checkpoint(dir.path())
        .unwrap();
    assert_eq!(report.graphs_loaded, 0);
    assert_eq!(report.rebuilt, 1);
    assert_eq!(
        report.vectors_reinserted,
        original
            .get_index("Doc", "embedding")
            .unwrap()
            .read()
            .unwrap()
            .len()
    );
}
