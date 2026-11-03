//! A snapshot can be encrypted at rest and read back (REL-09).
//!
//! REL-09 asks for "TLS 1.3 for all protocols; at-rest encryption for storage
//! and snapshots; key rotation without downtime". The transport half landed
//! separately; this is the snapshot half.
//!
//! The unit tests in `src/snapshot/encryption.rs` cover the container — frames,
//! nonces, truncation, a wrong key. What these cover is the join: that a real
//! graph survives the trip, that an unencrypted snapshot still imports with the
//! feature present, and that the failure modes an operator will actually hit
//! say what went wrong.

use samyama::graph::{GraphStore, PropertyValue};
use samyama::snapshot::{export_tenant, export_tenant_encrypted, import_tenant_maybe_encrypted};
use samyama::snapshot::encryption::KEY_BYTES;

fn graph() -> GraphStore {
    let mut s = GraphStore::new();
    let mut ids = Vec::new();
    for i in 0..200 {
        let n = s.create_node("Person");
        let _ = s.set_node_property("default", n, "name", PropertyValue::String(format!("p{i}")));
        let _ = s.set_node_property("default", n, "n", PropertyValue::Integer(i));
        ids.push(n);
    }
    for w in ids.windows(2) {
        let _ = s.create_edge(w[0], w[1], "KNOWS");
    }
    s
}

fn census(s: &GraphStore) -> (usize, usize, Vec<String>) {
    let mut names: Vec<String> = s
        .all_nodes()
        .iter()
        .filter_map(|n| match s.node_properties_merged(n.id).get("name") {
            Some(PropertyValue::String(v)) => Some(v.clone()),
            _ => None,
        })
        .collect();
    names.sort();
    (s.all_nodes().len(), s.all_edges().len(), names)
}

#[test]
fn an_encrypted_snapshot_round_trips_to_the_same_graph() {
    let key = [42u8; KEY_BYTES];
    let source = graph();

    let mut sealed = Vec::new();
    export_tenant_encrypted(&source, &mut sealed, &key).expect("export");

    // The check that says this is encryption. `Person` is a label in every
    // node of the plaintext stream; if it survives into the file, nothing was
    // encrypted.
    assert!(
        !sealed.windows(6).any(|w| w == b"Person"),
        "the plaintext is visible in the encrypted snapshot"
    );

    let mut restored = GraphStore::new();
    import_tenant_maybe_encrypted(&mut restored, &sealed[..], Some(&key)).expect("import");
    assert_eq!(census(&restored), census(&source));
}

#[test]
fn an_unencrypted_snapshot_still_imports() {
    // Sniffing rather than requiring the caller to say which it is, so an
    // operator who turns encryption on does not have to migrate the snapshots
    // they already have.
    let source = graph();
    let mut plain = Vec::new();
    export_tenant(&source, &mut plain).expect("export");

    let mut restored = GraphStore::new();
    import_tenant_maybe_encrypted(&mut restored, &plain[..], None).expect("import without a key");
    assert_eq!(census(&restored), census(&source));

    // And with a key in hand, which is the normal state once encryption is on:
    // the old files must not suddenly need one.
    let mut restored = GraphStore::new();
    import_tenant_maybe_encrypted(&mut restored, &plain[..], Some(&[42u8; KEY_BYTES]))
        .expect("a key must not break a plaintext snapshot");
    assert_eq!(census(&restored), census(&source));
}

#[test]
fn an_encrypted_snapshot_without_a_key_says_so() {
    // Otherwise this surfaces as a gzip error, and the two look identical while
    // needing completely different actions from the operator.
    let source = graph();
    let mut sealed = Vec::new();
    export_tenant_encrypted(&source, &mut sealed, &[42u8; KEY_BYTES]).expect("export");

    let mut restored = GraphStore::new();
    let err = import_tenant_maybe_encrypted(&mut restored, &sealed[..], None)
        .expect_err("an encrypted snapshot with no key must fail");
    assert!(
        err.to_string().contains("encrypted") && err.to_string().contains("--snapshot-key"),
        "the error should name the cause and the flag, got: {err}"
    );
}

#[test]
fn the_wrong_key_does_not_produce_a_partial_graph() {
    // The failure that matters most: a wrong key must not leave rows behind.
    // The import path already unwinds what it created on error (#199) and this
    // is the case that exercises it through the decrypting reader.
    let source = graph();
    let mut sealed = Vec::new();
    export_tenant_encrypted(&source, &mut sealed, &[42u8; KEY_BYTES]).expect("export");

    let mut restored = GraphStore::new();
    assert!(
        import_tenant_maybe_encrypted(&mut restored, &sealed[..], Some(&[1u8; KEY_BYTES])).is_err(),
        "a wrong key must fail"
    );
    assert_eq!(
        restored.all_nodes().len(),
        0,
        "a failed import left {} nodes behind",
        restored.all_nodes().len()
    );
}

#[test]
fn a_truncated_encrypted_snapshot_fails_rather_than_importing_a_short_graph() {
    let source = graph();
    let mut sealed = Vec::new();
    export_tenant_encrypted(&source, &mut sealed, &[42u8; KEY_BYTES]).expect("export");

    let mut restored = GraphStore::new();
    let cut = sealed.len() * 3 / 4;
    assert!(
        import_tenant_maybe_encrypted(&mut restored, &sealed[..cut], Some(&[42u8; KEY_BYTES]))
            .is_err(),
        "a truncated snapshot must fail rather than import what it has"
    );
    assert_eq!(restored.all_nodes().len(), 0);
}
