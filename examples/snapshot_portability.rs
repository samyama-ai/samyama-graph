//! Write a `.sgsnap` on one machine, load it on another, compare (HW-07, KG-11).
//!
//! ```bash
//! cargo run --release --example snapshot_portability -- --write snap.sgsnap --json wrote.json
//! cargo run --release --example snapshot_portability -- --verify snap.sgsnap --against wrote.json
//! ```
//!
//! HW-07 asks that a snapshot written on any tier loads on any other with the
//! same results; KG-11 adds "across architectures and across engine minor
//! versions". Neither had ever been checked, because checking it needs two
//! machines — and CI already has two, x86-64 and aarch64, which have never
//! exchanged a file.
//!
//! # What "the same results" is taken to mean
//!
//! Not "the bytes match": a re-export can legitimately differ (map iteration
//! order, a compression level). What must match is the **graph a query sees**,
//! so the fingerprint is computed from the loaded store and covers:
//!
//! - node and edge counts
//! - every label and edge type, sorted, with their counts
//! - a checksum over each node's (sorted labels, sorted properties) and each
//!   edge's (type, endpoints, sorted properties)
//!
//! The checksum is [`fnv_then_splitmix`], written out here rather than taken
//! from `DefaultHasher` — SipHash's output is not stable across Rust releases,
//! so a fingerprint built on it would differ between two toolchains and report
//! a portability failure that is nothing of the kind. That is the trap this
//! whole example exists to avoid, in miniature.
//!
//! # What it does not prove
//!
//! One writer and one reader on two architectures is two data points. It
//! cannot show the format is endian-neutral in general; it shows that these
//! two agree, which is the thing CI can keep true. The engine-minor-version
//! half of KG-11 is not covered at all — that needs an older binary, and this
//! example says so rather than implying otherwise.

use std::collections::BTreeMap;

use samyama::graph::{GraphStore, PropertyValue};

/// FNV-1a then a splitmix64 finisher: fixed, written down, and stable across
/// toolchains, which `DefaultHasher` is explicitly not.
fn fnv_then_splitmix(bytes: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in bytes {
        h ^= *b as u64;
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    let mut z = h.wrapping_add(0x9e37_79b9_7f4a_7c15);
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

/// A value's bytes, type-tagged and float-normalised.
///
/// A float that is exactly an integer is hashed as the integer: Cypher
/// compares `1` and `1.0` equal, and two architectures that print a float
/// differently must not read as a portability failure.
fn value_bytes(v: &PropertyValue) -> Vec<u8> {
    let mut out = Vec::new();
    match v {
        PropertyValue::Float(f) if f.fract() == 0.0 && f.is_finite() => {
            out.push(2);
            out.extend_from_slice(&(*f as i64).to_le_bytes());
        }
        PropertyValue::Integer(i) => {
            out.push(2);
            out.extend_from_slice(&i.to_le_bytes());
        }
        PropertyValue::Float(f) => {
            out.push(3);
            out.extend_from_slice(&f.to_bits().to_le_bytes());
        }
        PropertyValue::Boolean(b) => {
            out.push(1);
            out.push(*b as u8);
        }
        PropertyValue::String(s) => {
            out.push(4);
            out.extend_from_slice(s.as_bytes());
        }
        other => {
            out.push(5);
            out.extend_from_slice(other.to_string().as_bytes());
        }
    }
    out
}

/// What a query would see, reduced to numbers that can cross a machine.
fn fingerprint(store: &GraphStore) -> BTreeMap<String, String> {
    let nodes = store.all_nodes();
    let edges = store.all_edges();

    let mut labels: BTreeMap<String, usize> = BTreeMap::new();
    let mut node_sum: u64 = 0;
    for n in &nodes {
        let mut ls: Vec<&str> = n.labels.iter().map(|l| l.as_str()).collect();
        ls.sort_unstable();
        for l in &ls {
            *labels.entry((*l).to_string()).or_default() += 1;
        }
        let mut buf = ls.join(",").into_bytes();
        // The merged view, not the row map: a property written through Cypher
        // lands on the columnar side and `node.properties` cannot see it
        // (#554). A fingerprint blind to half the properties would call two
        // different graphs identical.
        let props = store.node_properties_merged(n.id);
        let mut keys: Vec<&String> = props.keys().collect();
        keys.sort();
        for k in keys {
            buf.extend_from_slice(k.as_bytes());
            buf.extend_from_slice(&value_bytes(&props[k]));
        }
        // Summed, not folded in order: node ids need not be allocated in the
        // same order on the far side, and an order-sensitive digest would
        // report a difference that no query can observe.
        node_sum = node_sum.wrapping_add(fnv_then_splitmix(&buf));
    }

    let mut types: BTreeMap<String, usize> = BTreeMap::new();
    let mut edge_sum: u64 = 0;
    for e in &edges {
        *types.entry(e.edge_type.as_str().to_string()).or_default() += 1;
        let mut buf = e.edge_type.as_str().as_bytes().to_vec();
        buf.extend_from_slice(&e.source.as_u64().to_le_bytes());
        buf.extend_from_slice(&e.target.as_u64().to_le_bytes());
        let props = store.edge_properties_merged(e.id);
        let mut keys: Vec<&String> = props.keys().collect();
        keys.sort();
        for k in keys {
            buf.extend_from_slice(k.as_bytes());
            buf.extend_from_slice(&value_bytes(&props[k]));
        }
        edge_sum = edge_sum.wrapping_add(fnv_then_splitmix(&buf));
    }

    let mut out = BTreeMap::new();
    out.insert("nodes".into(), nodes.len().to_string());
    out.insert("edges".into(), edges.len().to_string());
    out.insert("node_digest".into(), format!("{node_sum:016x}"));
    out.insert("edge_digest".into(), format!("{edge_sum:016x}"));
    out.insert(
        "labels".into(),
        labels.iter().map(|(k, v)| format!("{k}:{v}")).collect::<Vec<_>>().join(","),
    );
    out.insert(
        "edge_types".into(),
        types.iter().map(|(k, v)| format!("{k}:{v}")).collect::<Vec<_>>().join(","),
    );
    out
}

/// A graph with every property type the format has to carry.
fn fixture() -> GraphStore {
    let engine = samyama::query::QueryEngine::new();
    let mut g = GraphStore::new();
    engine
        .execute_mut(
            "CREATE (a:Person:Employee {name: 'Alice', age: 34, score: 91.5, \
                     whole: 7.0, active: true, tags: ['x','y'], note: 'ünïcødé ✓'}) \
             CREATE (b:Person {name: 'Bob', age: 41}) \
             CREATE (c:Company {name: 'Acme', founded: 1998}) \
             CREATE (d {note: 'no labels, which Neo4j and we both allow'}) \
             CREATE (a)-[:KNOWS {since: 2019, weight: 0.75}]->(b) \
             CREATE (b)-[:WORKS_AT {role: 'engineer'}]->(c) \
             CREATE (a)-[:KNOWS {since: 2020}]->(b)",
            &mut g,
            "default",
        )
        .expect("fixture");
    g
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let flag = |n: &str| -> Option<String> {
        args.iter().position(|a| a == n).and_then(|i| args.get(i + 1)).cloned()
    };
    let arch = std::env::consts::ARCH;
    let os = std::env::consts::OS;

    if let Some(path) = flag("--write") {
        let g = fixture();
        let fp = fingerprint(&g);
        let file = std::fs::File::create(&path).expect("create snapshot");
        let stats = samyama::snapshot::export_tenant(&g, file).expect("export");
        let bytes = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);

        let mut doc = fp.clone();
        doc.insert("arch".into(), arch.to_string());
        doc.insert("os".into(), os.to_string());
        doc.insert("engine_version".into(), env!("CARGO_PKG_VERSION").to_string());
        doc.insert("snapshot_bytes".into(), bytes.to_string());
        doc.insert("exported_nodes".into(), format!("{:?}", stats));

        let json = serde_json::to_string_pretty(&doc).unwrap();
        if let Some(out) = flag("--json") {
            std::fs::write(&out, &json).expect("write json");
        }
        println!("wrote {path} on {arch}-{os} ({bytes} bytes)");
        println!("{json}");
        return;
    }

    if let Some(path) = flag("--verify") {
        let against = flag("--against").expect("--verify needs --against <wrote.json>");
        let expected: BTreeMap<String, String> =
            serde_json::from_str(&std::fs::read_to_string(&against).expect("read json"))
                .expect("parse json");

        let file = std::fs::File::open(&path).expect("open snapshot");
        let mut g = GraphStore::new();
        samyama::snapshot::import_tenant(&mut g, file).expect("import");
        let got = fingerprint(&g);

        let mut bad = Vec::new();
        for key in ["nodes", "edges", "node_digest", "edge_digest", "labels", "edge_types"] {
            let want = expected.get(key).map(String::as_str).unwrap_or("(absent)");
            let have = got.get(key).map(String::as_str).unwrap_or("(absent)");
            let mark = if want == have { "ok  " } else { "FAIL" };
            if want != have {
                bad.push(key);
            }
            println!("  {mark} {key:<12} wrote={want}  read={have}");
        }

        println!(
            "\nwritten on {}-{} (engine {}), read on {arch}-{os} (engine {})",
            expected.get("arch").map(String::as_str).unwrap_or("?"),
            expected.get("os").map(String::as_str).unwrap_or("?"),
            expected.get("engine_version").map(String::as_str).unwrap_or("?"),
            env!("CARGO_PKG_VERSION"),
        );
        let same_arch = expected.get("arch").map(String::as_str) == Some(arch);
        if same_arch {
            println!(
                "NOTE both halves ran on {arch}. That is a round trip, not a \
                 portability result -- HW-07 is about two tiers."
            );
        }

        if bad.is_empty() {
            println!("PASS  the graph a query sees is identical");
        } else {
            eprintln!("FAIL  differs in: {}", bad.join(", "));
            std::process::exit(1);
        }
        return;
    }

    eprintln!(
        "usage:\n  snapshot_portability --write <snap> [--json <out>]\n  \
         snapshot_portability --verify <snap> --against <out>"
    );
    std::process::exit(2);
}
