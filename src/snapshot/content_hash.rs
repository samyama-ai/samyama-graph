//! A content hash of the graph a query reads (ML-08, #1747).
//!
//! `snapshot_version` is a counter: it is not derived from the data, only the
//! transaction API advances it, and two different graphs can share a value. So
//! nothing could say which data a dataset was built from. This hash is computed
//! from the data itself, and two stores holding the same graph -- the same
//! nodes, labels, properties and relationships under the same ids -- hash the
//! same however they were built, in whatever order their properties were set,
//! on whatever machine.
//!
//! What is hashed is what a query can see:
//!
//! * every live node: its id, its labels (sorted), and its properties as
//!   `node_properties_merged` resolves them, sorted by key;
//! * every live relationship, full or bulk-loaded ("stub"): its id, source,
//!   target, type and properties, sorted by key.
//!
//! Not hashed: indexes, constraints, creation and update times, the version
//! counter, and superseded MVCC versions. None of them changes a query's rows.
//! Ids **are** hashed, because `id(n)` is something a query can return.
//!
//! Values are encoded with a type tag, so `1`, `1.0` and `'1'` differ, and a
//! float is hashed by its bits.
//!
//! The cost is one pass over the graph. `cached_content_hash` keeps the last
//! one and reuses it until `GraphStore::data_changes` moves -- the counter
//! every data write advances and graph projections already trust to say "the
//! data changed". It is not keyed on `current_version`, which ordinary writes
//! do not move.

use sha2::{Digest, Sha256};

use crate::graph::property::PropertyValue;
use crate::graph::store::GraphStore;
use crate::graph::types::{EdgeId, NodeId};

/// One relationship as hashed: id, source, target, type, properties.
type EdgeRecord = (u64, u64, u64, String, Vec<(String, PropertyValue)>);

/// `sha256:<64 hex digits>` over the graph's content.
pub fn content_hash(store: &GraphStore) -> String {
    let mut h = Sha256::new();
    h.update(b"samyama-graph-content-v1\0");

    // Nodes, by slot: the current version of each live node.
    let mut live: Vec<NodeId> = Vec::new();
    for idx in 0..store.node_slot_count() {
        let id = NodeId::new(idx as u64);
        let Some(node) = store.get_node(id) else {
            continue;
        };
        live.push(id);
        h.update(b"N");
        h.update(id.as_u64().to_le_bytes());
        let mut labels: Vec<&str> = node.labels.iter().map(|l| l.as_str()).collect();
        labels.sort_unstable();
        h.update((labels.len() as u64).to_le_bytes());
        for l in labels {
            put_str(&mut h, l);
        }
        put_properties(
            &mut h,
            store.node_properties_merged(id).into_iter().collect(),
        );
    }

    // Relationships: the full ones with their properties, then the stub ones
    // only adjacency knows about, the same split the snapshot export makes.
    let mut edges: Vec<EdgeRecord> = Vec::new();
    let full = store.all_edges();
    let full_ids: std::collections::HashSet<u64> = full.iter().map(|e| e.id.as_u64()).collect();
    for e in full {
        edges.push((
            e.id.as_u64(),
            e.source.as_u64(),
            e.target.as_u64(),
            e.edge_type.as_str().to_string(),
            e.properties.into_iter().collect(),
        ));
    }
    let mut stub = |src: NodeId, tgt: NodeId, eid: EdgeId| {
        if full_ids.contains(&eid.as_u64()) {
            return;
        }
        // No type means deleted, as in the export (#1096).
        if let Some(t) = store.get_edge_type(eid) {
            edges.push((
                eid.as_u64(),
                src.as_u64(),
                tgt.as_u64(),
                t.as_str().to_string(),
                Vec::new(),
            ));
        }
    };
    for &id in &live {
        for (tgt, eid) in store.frozen_outgoing_neighbors(id.as_u64() as usize) {
            stub(id, tgt, eid);
        }
        for &(tgt, eid) in store.get_outgoing_neighbor_slice(id) {
            stub(id, tgt, eid);
        }
    }
    edges.sort_unstable_by_key(|e| e.0);
    edges.dedup_by_key(|e| e.0);
    for (id, src, tgt, edge_type, props) in edges {
        h.update(b"E");
        h.update(id.to_le_bytes());
        h.update(src.to_le_bytes());
        h.update(tgt.to_le_bytes());
        put_str(&mut h, &edge_type);
        put_properties(&mut h, props);
    }

    format!("sha256:{}", hex(&h.finalize()))
}

/// `content_hash`, computed once per state of the data and reused until a
/// write moves `data_changes`.
///
/// Called under the store's read lock, so no write can land between reading
/// the counter and hashing; the memo's own lock makes two concurrent exports
/// share one computation instead of each paying for it.
pub fn cached_content_hash(store: &GraphStore) -> String {
    let changes = store.data_changes();
    let mut memo = store.content_hash_memo().lock().unwrap_or_else(|p| p.into_inner());
    if let Some((at, hash)) = memo.as_ref() {
        if *at == changes {
            return hash.clone();
        }
    }
    let hash = content_hash(store);
    *memo = Some((changes, hash.clone()));
    hash
}

/// `sha256:<hex>` of arbitrary bytes: an export's body, for its manifest.
pub fn bytes_hash(bytes: &[u8]) -> String {
    format!("sha256:{}", hex(&Sha256::digest(bytes)))
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// Length-prefixed, so `("ab", "c")` and `("a", "bc")` differ.
fn put_str(h: &mut Sha256, s: &str) {
    h.update((s.len() as u64).to_le_bytes());
    h.update(s.as_bytes());
}

fn put_properties(h: &mut Sha256, mut props: Vec<(String, PropertyValue)>) {
    props.sort_unstable_by(|a, b| a.0.cmp(&b.0));
    h.update((props.len() as u64).to_le_bytes());
    for (k, v) in &props {
        put_str(h, k);
        put_value(h, v);
    }
}

/// One tag byte per type, then the value in a fixed byte form.
fn put_value(h: &mut Sha256, v: &PropertyValue) {
    match v {
        PropertyValue::Null => h.update([0u8]),
        PropertyValue::String(s) => {
            h.update([1u8]);
            put_str(h, s);
        }
        PropertyValue::Integer(i) => {
            h.update([2u8]);
            h.update(i.to_le_bytes());
        }
        PropertyValue::Float(f) => {
            h.update([3u8]);
            h.update(f.to_bits().to_le_bytes());
        }
        PropertyValue::Boolean(b) => h.update([4u8, *b as u8]),
        PropertyValue::DateTime(ms) => {
            h.update([5u8]);
            h.update(ms.to_le_bytes());
        }
        PropertyValue::Date(d) => {
            h.update([6u8]);
            h.update(d.to_le_bytes());
        }
        PropertyValue::LocalTime(n) => {
            h.update([7u8]);
            h.update(n.to_le_bytes());
        }
        PropertyValue::Time {
            nanos,
            offset_seconds,
        } => {
            h.update([8u8]);
            h.update(nanos.to_le_bytes());
            h.update(offset_seconds.to_le_bytes());
        }
        PropertyValue::LocalDateTime { secs, nanos } => {
            h.update([9u8]);
            h.update(secs.to_le_bytes());
            h.update(nanos.to_le_bytes());
        }
        PropertyValue::ZonedDateTime {
            secs,
            nanos,
            offset_seconds,
            zone,
        } => {
            h.update([10u8]);
            h.update(secs.to_le_bytes());
            h.update(nanos.to_le_bytes());
            h.update(offset_seconds.to_le_bytes());
            match zone {
                Some(z) => {
                    h.update([1u8]);
                    put_str(h, z);
                }
                None => h.update([0u8]),
            }
        }
        PropertyValue::Array(items) => {
            h.update([11u8]);
            h.update((items.len() as u64).to_le_bytes());
            for item in items {
                put_value(h, item);
            }
        }
        PropertyValue::Map(m) => {
            h.update([12u8]);
            put_properties(h, m.iter().map(|(k, v)| (k.clone(), v.clone())).collect());
        }
        PropertyValue::Vector(xs) => {
            h.update([13u8]);
            h.update((xs.len() as u64).to_le_bytes());
            for x in xs {
                h.update(x.to_bits().to_le_bytes());
            }
        }
        PropertyValue::Duration {
            months,
            days,
            seconds,
            nanos,
        } => {
            h.update([14u8]);
            h.update(months.to_le_bytes());
            h.update(days.to_le_bytes());
            h.update(seconds.to_le_bytes());
            h.update(nanos.to_le_bytes());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const T: &str = "default";

    /// Two people and a KNOWS edge, the properties set in the given order.
    fn people(reverse_property_order: bool) -> GraphStore {
        let mut s = GraphStore::new();
        let a = s.create_node("Person");
        let b = s.create_node("Person");
        let props: Vec<(&str, PropertyValue)> = vec![
            ("name", PropertyValue::String("Ada".into())),
            ("age", PropertyValue::Integer(36)),
            ("score", PropertyValue::Float(0.5)),
        ];
        let ordered: Vec<_> = if reverse_property_order {
            props.into_iter().rev().collect()
        } else {
            props
        };
        for (k, v) in ordered {
            s.set_node_property(T, a, k, v).unwrap();
        }
        s.set_node_property(T, b, "name", "Bob").unwrap();
        let e = s.create_edge(a, b, "KNOWS").unwrap();
        s.set_edge_property(e, "since", 2020i64).unwrap();
        s
    }

    #[test]
    fn the_same_graph_hashes_the_same_whatever_order_its_properties_were_set_in() {
        let h = content_hash(&people(false));
        assert!(h.starts_with("sha256:") && h.len() == 7 + 64, "{h}");
        assert_eq!(h, content_hash(&people(false)));
        assert_eq!(h, content_hash(&people(true)));
    }

    #[test]
    fn any_change_a_query_could_see_changes_the_hash() {
        let base = content_hash(&people(false));
        // Ada and Bob, by the ids the store gave them.
        let mut ids: Vec<NodeId> = people(false).all_nodes().iter().map(|n| n.id).collect();
        ids.sort_unstable();
        ids.dedup();
        let (a, b) = (ids[0], ids[1]);

        let mut s = people(false);
        s.set_node_property(T, a, "age", 37i64).unwrap();
        assert_ne!(content_hash(&s), base, "a property value");

        let mut s = people(false);
        s.set_node_property(T, a, "age", 36.0f64).unwrap();
        assert_ne!(content_hash(&s), base, "36 and 36.0 are different values");

        let mut s = people(false);
        s.create_edge(b, a, "KNOWS").unwrap();
        assert_ne!(content_hash(&s), base, "a new relationship");

        let mut s = people(false);
        s.create_node("Person");
        assert_ne!(
            content_hash(&s),
            base,
            "a new node, even with no properties"
        );

        let mut s = people(false);
        let e = s.all_edges()[0].id;
        s.set_edge_property(e, "since", 2021i64).unwrap();
        assert_ne!(content_hash(&s), base, "a relationship property");
    }

    #[test]
    fn a_snapshot_round_trip_into_a_fresh_store_keeps_the_hash() {
        let original = people(false);
        let mut buf = Vec::new();
        crate::snapshot::export_tenant(&original, &mut buf).unwrap();
        let mut restored = GraphStore::new();
        crate::snapshot::import_tenant(&mut restored, &buf[..]).unwrap();
        assert_eq!(content_hash(&restored), content_hash(&original));
    }

    #[test]
    fn the_cached_hash_is_reused_until_a_write_and_never_goes_stale() {
        let mut s = people(false);
        let first = cached_content_hash(&s);
        assert_eq!(first, content_hash(&s));
        assert_eq!(cached_content_hash(&s), first, "unchanged data, same hash");
        let mut ids: Vec<NodeId> = s.all_nodes().iter().map(|n| n.id).collect();
        ids.sort_unstable();
        ids.dedup();
        let (a, b) = (ids[0], ids[1]);
        let e = s.all_edges()[0].id;
        // Every kind of write: each must invalidate the memo, so the cached
        // hash always equals a fresh one.
        let writes: Vec<Box<dyn Fn(&mut GraphStore)>> = vec![
            Box::new(move |s| s.set_node_property(T, a, "age", 40i64).unwrap()),
            Box::new(move |s| {
                s.set_edge_property(e, "since", 1999i64).unwrap();
            }),
            Box::new(move |s| {
                s.create_edge(b, a, "LIKES").unwrap();
            }),
            Box::new(|s| {
                s.create_node("Person");
            }),
            Box::new(move |s| {
                s.delete_edge(e).unwrap();
            }),
            Box::new(move |s| {
                s.delete_node(T, b).unwrap();
            }),
        ];
        let mut seen = vec![first];
        for (k, write) in writes.iter().enumerate() {
            write(&mut s);
            let cached = cached_content_hash(&s);
            assert_eq!(cached, content_hash(&s), "write {k}: the memo went stale");
            assert!(!seen.contains(&cached), "write {k}: the hash did not move");
            seen.push(cached);
        }
    }

    #[test]
    fn values_of_different_types_never_collide() {
        let one = |v: PropertyValue| {
            let mut s = GraphStore::new();
            let n = s.create_node("X");
            s.set_node_property(T, n, "v", v).unwrap();
            content_hash(&s)
        };
        let hashes = [
            one(PropertyValue::Integer(1)),
            one(PropertyValue::Float(1.0)),
            one(PropertyValue::String("1".into())),
            one(PropertyValue::Boolean(true)),
            one(PropertyValue::Array(vec![PropertyValue::Integer(1)])),
        ];
        let distinct: std::collections::HashSet<_> = hashes.iter().collect();
        assert_eq!(distinct.len(), hashes.len());
    }
}
