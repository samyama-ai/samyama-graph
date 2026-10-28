use samyama::graph::GraphStore;
use samyama::snapshot::{
    export_tenant, import_tenant, import_tenant_with_merge_policy, MergeEdges,
};
fn snapshot(evidence: bool) -> Vec<u8> {
    let mut g = GraphStore::new();
    let a = g.create_node("Drug");
    let b = g.create_node("Protein");
    g.get_node_mut(a).unwrap().set_property("id", "D1");
    g.get_node_mut(b).unwrap().set_property("id", "P1");
    let edge = g.create_edge(a, b, "TARGETS").unwrap();
    if evidence {
        g.set_edge_property(edge, "evidence", "study").unwrap();
    }
    let mut bytes = Vec::new();
    export_tenant(&g, &mut bytes).unwrap();
    bytes
}
fn check(policy: MergeEdges, evidence: bool, expected_edges: usize, skipped: u64, kept: u64) {
    let bytes = snapshot(evidence);
    let mut g = GraphStore::new();
    import_tenant(&mut g, &bytes[..]).unwrap();
    let stats =
        import_tenant_with_merge_policy(&mut g, &bytes[..], &["id"], false, policy).unwrap();
    assert_eq!(g.node_count(), 2);
    assert_eq!(g.edge_count(), expected_edges);
    assert_eq!(stats.edges_deduplicated, skipped);
    assert_eq!(stats.parallel_edges_kept, kept);
}
#[test]
fn default_policy_collapses_only_unpropertied_edges() {
    check(MergeEdges::Unpropertied, false, 1, 1, 0);
    check(MergeEdges::Unpropertied, true, 2, 0, 1);
}
#[test]
fn explicit_never_and_always_policies_are_respected() {
    check(MergeEdges::Never, false, 2, 0, 1);
    check(MergeEdges::Always, true, 1, 1, 0);
}
