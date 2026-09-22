//! Derive a schema from a snapshot, and render it as a diagram (KG-02).
//!
//! KG-02 asks every published KG repository to carry "a README with schema
//! diagram". Zero of twelve do, and `CH-KG-CONF` says why: the diagram is
//! missing because in most of them *the schema is*. Its note is worth keeping
//! in mind here, because it is the objection this module has to answer —
//!
//! > Generating a diagram for a repository that has not written its schema
//! > down would be drawing what somebody remembers.
//!
//! That objection holds against a hand-drawn diagram and against a diagram
//! generated from a loader's intentions. It does not hold against one computed
//! from the data: what this reads is the snapshot that was actually published,
//! and every edge in the diagram is an edge that exists, with the count of how
//! many times it does.
//!
//! So the output is a **measurement**, not documentation. If a relationship a
//! README claims is absent from the diagram, the README is wrong.
//!
//! # What it reports
//!
//! - node labels, with counts and the property keys seen on them
//! - edge types, with counts
//! - the `(source label)-[type]->(target label)` triples that occur, with
//!   counts — which is the part a hand-drawn diagram gets wrong, because it is
//!   the part nobody remembers
//!
//! # Two passes, and why
//!
//! An edge names node ids, not labels, and a snapshot interleaves nodes and
//! edges. Resolving an edge's endpoints therefore needs the node table, so the
//! file is read twice rather than held in memory: the published snapshots run
//! to hundreds of megabytes, and a tool that needs the graph in memory to
//! describe it is one nobody runs on the graph that most needs describing.
//!
//! The label map is kept — one small integer per node — which is the one thing
//! that cannot be streamed away.

use std::collections::{BTreeMap, BTreeSet, HashMap};

/// Everything derived from one snapshot.
#[derive(Debug, Default, Clone)]
pub struct Schema {
    /// Label -> node count.
    pub labels: BTreeMap<String, u64>,
    /// Label -> property keys seen on nodes carrying it.
    pub properties: BTreeMap<String, BTreeSet<String>>,
    /// Edge type -> edge count.
    pub edge_types: BTreeMap<String, u64>,
    /// `(source label, type, target label)` -> count.
    pub triples: BTreeMap<(String, String, String), u64>,
    /// Edges whose endpoints are not in the file.
    pub dangling_edges: u64,
    pub nodes: u64,
    pub edges: u64,
}

/// A node with no labels, reported under this name rather than dropped.
const UNLABELLED: &str = "(unlabelled)";

fn first_label(v: &serde_json::Value) -> String {
    v.get("labels")
        .and_then(|l| l.as_array())
        .and_then(|a| a.first())
        .and_then(|s| s.as_str())
        .unwrap_or(UNLABELLED)
        .to_string()
}

/// Read a snapshot twice — nodes, then edges — and build the schema.
///
/// `open` is called once per pass. It is a closure rather than a path so the
/// caller decides how the bytes arrive, and so the tests can hand it a string.
pub fn derive<R, F>(mut open: F) -> Result<Schema, String>
where
    R: std::io::BufRead,
    F: FnMut() -> Result<R, String>,
{
    use std::io::BufRead;

    let mut schema = Schema::default();
    // Node id -> label index. Interned, because a label string per node is the
    // difference between describing a large graph and failing to.
    let mut label_ids: Vec<String> = Vec::new();
    let mut label_index: HashMap<String, u32> = HashMap::new();
    let mut node_label: HashMap<u64, u32> = HashMap::new();

    for line in open()?.lines() {
        let line = line.map_err(|e| e.to_string())?;
        if line.trim().is_empty() {
            continue;
        }
        let v: serde_json::Value =
            serde_json::from_str(&line).map_err(|e| format!("not JSON: {e}"))?;
        if v.get("t").and_then(|t| t.as_str()) != Some("n") {
            continue;
        }
        schema.nodes += 1;
        let label = first_label(&v);
        *schema.labels.entry(label.clone()).or_insert(0) += 1;

        let idx = match label_index.get(&label) {
            Some(i) => *i,
            None => {
                let i = label_ids.len() as u32;
                label_ids.push(label.clone());
                label_index.insert(label.clone(), i);
                i
            }
        };
        if let Some(id) = v.get("id").and_then(|i| i.as_u64()) {
            node_label.insert(id, idx);
        }

        if let Some(props) = v.get("props").and_then(|p| p.as_object()) {
            let keys = schema.properties.entry(label).or_default();
            for k in props.keys() {
                keys.insert(k.clone());
            }
        }
    }

    for line in open()?.lines() {
        let line = line.map_err(|e| e.to_string())?;
        if line.trim().is_empty() {
            continue;
        }
        let v: serde_json::Value =
            serde_json::from_str(&line).map_err(|e| format!("not JSON: {e}"))?;
        if v.get("t").and_then(|t| t.as_str()) != Some("e") {
            continue;
        }
        schema.edges += 1;
        let ty = v
            .get("type")
            .and_then(|t| t.as_str())
            .unwrap_or("(untyped)")
            .to_string();
        *schema.edge_types.entry(ty.clone()).or_insert(0) += 1;

        let src = v.get("src").and_then(|s| s.as_u64());
        let tgt = v.get("tgt").and_then(|s| s.as_u64());
        match (src.and_then(|s| node_label.get(&s)), tgt.and_then(|t| node_label.get(&t))) {
            (Some(s), Some(t)) => {
                let key = (
                    label_ids[*s as usize].clone(),
                    ty,
                    label_ids[*t as usize].clone(),
                );
                *schema.triples.entry(key).or_insert(0) += 1;
            }
            // An edge pointing at a node the file does not contain. Counted
            // rather than silently skipped: it means the snapshot is not
            // self-contained, which is worth more than the diagram.
            _ => schema.dangling_edges += 1,
        }
    }

    Ok(schema)
}

/// Mermaid identifiers must be alphanumeric-ish; labels are not.
fn ident(label: &str) -> String {
    let s: String = label
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
        .collect();
    if s.chars().next().is_some_and(|c| c.is_ascii_digit()) {
        format!("n{s}")
    } else {
        s
    }
}

impl Schema {
    /// A mermaid `graph LR`, ready to paste into a README.
    ///
    /// Mermaid rather than an image: GitHub, Gitea and this repository's own
    /// artifact viewer all render it, it survives in a diff, and `CH-KG-CONF`
    /// recognises a fenced mermaid block as structural — an embedded PNG it can
    /// only accept on the strength of its filename.
    pub fn to_mermaid(&self) -> String {
        let mut out = String::from("```mermaid\ngraph LR\n");
        for (label, count) in &self.labels {
            out.push_str(&format!("    {}[\"{label}<br/>{count}\"]\n", ident(label)));
        }
        for ((src, ty, tgt), count) in &self.triples {
            out.push_str(&format!(
                "    {} -->|\"{ty} ({count})\"| {}\n",
                ident(src),
                ident(tgt)
            ));
        }
        out.push_str("```\n");
        out
    }

    /// The same thing as a table, for the repositories whose docs use one.
    pub fn to_markdown(&self) -> String {
        let mut out = String::new();
        out.push_str("| Label | Nodes | Properties |\n|---|---:|---|\n");
        for (label, count) in &self.labels {
            let props = self
                .properties
                .get(label)
                .map(|p| p.iter().cloned().collect::<Vec<_>>().join(", "))
                .unwrap_or_default();
            out.push_str(&format!("| `{label}` | {count} | {props} |\n"));
        }
        out.push_str("\n| Relationship | From | To | Count |\n|---|---|---|---:|\n");
        for ((src, ty, tgt), count) in &self.triples {
            out.push_str(&format!("| `{ty}` | `{src}` | `{tgt}` | {count} |\n"));
        }
        if self.dangling_edges > 0 {
            out.push_str(&format!(
                "\n**{} edge(s) point at a node this snapshot does not contain.**\n",
                self.dangling_edges
            ));
        }
        out
    }
}

/// Derive the schema of a snapshot file, gzip or plain.
pub fn derive_from_path(path: &std::path::Path) -> Result<Schema, String> {
    use std::io::{BufReader, Read};

    let mut probe = std::fs::File::open(path).map_err(|e| format!("{}: {e}", path.display()))?;
    let mut magic = [0u8; 2];
    probe
        .read_exact(&mut magic)
        .map_err(|e| format!("{}: {e}", path.display()))?;
    let gzipped = magic == [0x1f, 0x8b];

    derive(|| {
        let f = std::fs::File::open(path).map_err(|e| format!("{}: {e}", path.display()))?;
        // Boxed so both arms have one type; the cost is one virtual call per
        // read of a 64 KiB buffer.
        let r: Box<dyn std::io::Read> = if gzipped {
            Box::new(flate2::read::GzDecoder::new(f))
        } else {
            Box::new(f)
        };
        Ok(BufReader::new(r))
    })
}
