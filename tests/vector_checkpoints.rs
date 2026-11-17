use samyama::{
    graph::NodeId,
    vector::{index::Quantization, DistanceMetric, HnswParams, VectorIndexManager},
};
fn manager() -> VectorIndexManager {
    let m = VectorIndexManager::new();
    m.create_index_with_params(
        Some("named"),
        "V",
        "v",
        2,
        DistanceMetric::L2,
        Quantization::Fp16,
        HnswParams::default(),
    )
    .unwrap();
    m.set_model_id("V", "v", "model-v1");
    m.add_vector("V", "v", NodeId::new(1), &vec![0.0, 1.0])
        .unwrap();
    m
}
#[test]
fn checkpoint_restores_names_models_precision_and_latest_generation() {
    let dir = tempfile::tempdir().unwrap();
    let m = manager();
    m.checkpoint_all(dir.path()).unwrap();
    m.add_vector("V", "v", NodeId::new(2), &vec![1.0, 0.0])
        .unwrap();
    m.checkpoint_all(dir.path()).unwrap();
    let restored = VectorIndexManager::new();
    restored.recover_checkpoint(dir.path()).unwrap();
    assert_eq!(
        restored.resolve_name("named"),
        Some(("V".into(), "v".into()))
    );
    assert_eq!(restored.model_id("V", "v").as_deref(), Some("model-v1"));
    let index = restored.get_index("V", "v").unwrap();
    assert_eq!(index.read().unwrap().quantization(), Quantization::Fp16);
    assert_eq!(index.read().unwrap().len(), 2);
    assert_eq!(
        restored.search("V", "v", &[1.0, 0.0], 1).unwrap()[0].0,
        NodeId::new(2)
    );
}
#[test]
fn corrupted_generation_never_replaces_live_indexes() {
    let dir = tempfile::tempdir().unwrap();
    let source = manager();
    source.checkpoint_all(dir.path()).unwrap();
    let name = std::fs::read_to_string(dir.path().join("CURRENT")).unwrap();
    std::fs::write(dir.path().join(name.trim()).join("V_v.hnsw"), b"corrupt").unwrap();
    let live = manager();
    live.add_vector("V", "v", NodeId::new(7), &vec![7.0, 0.0])
        .unwrap();
    assert!(live
        .recover_checkpoint(dir.path())
        .unwrap_err()
        .to_string()
        .contains("checksum"));
    assert_eq!(live.get_index("V", "v").unwrap().read().unwrap().len(), 2);
}
#[test]
fn incomplete_unpublished_generation_is_ignored() {
    let dir = tempfile::tempdir().unwrap();
    manager().checkpoint_all(dir.path()).unwrap();
    std::fs::create_dir(dir.path().join("generation-interrupted")).unwrap();
    let restored = VectorIndexManager::new();
    restored.recover_checkpoint(dir.path()).unwrap();
    assert_eq!(
        restored.get_index("V", "v").unwrap().read().unwrap().len(),
        1
    );
}
#[test]
fn current_cannot_escape_checkpoint_directory() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("CURRENT"), "../outside").unwrap();
    assert!(VectorIndexManager::new()
        .recover_checkpoint(dir.path())
        .is_err());
}
