//! Agent memory stays out of exports and out of anything published (#2242).
//!
//! The acceptance criteria, each against a real export:
//! - a default export has no memory; `include_agent_memory` exports all of it;
//! - a data node linked to memory exports cleanly, without the link;
//! - publishing (`pii::scan_snapshot`) refuses a snapshot carrying memory;
//! - the telemetry rename leaves domain `Question` / `Tool` nodes untouched;
//! - restoring a KG snapshot keeps the memory already in the graph.
//!
//! Relationships are made with `create_edge` and `create_edge_stub`, before and
//! after `compact_adjacency`, so the exclusion holds however the edges are
//! stored. (Today a stub edge keeps its endpoints and is exported with the full
//! edges; the export's separate adjacency-only branch is kept for stores
//! without them and applies the same rule.)

use std::io::{BufRead, BufReader};

use samyama::agent::memory_labels::{is_memory_node, MEMORY_LABELS};
use samyama::graph::{GraphStore, Label};
use samyama::snapshot::format::SnapshotHeader;
use samyama::snapshot::{export_tenant, export_tenant_with, import_tenant, ExportOptions, AGENT_MEMORY_EDGES, AGENT_MEMORY_NODES};

/// Two drugs, a domain `Question` and `Tool`, and one node of every memory
/// label, joined to the data in both directions.
///
/// Data-only relationships: 2 (one full, one stub).
/// Relationships touching memory: 3 full + 2 stub (one buffered, one frozen)
/// + 1 memory-to-memory = 6.
fn graph_with_memory(compact: bool) -> GraphStore {
    let mut g = GraphStore::new();
    let aspirin = g.create_node("Drug");
    g.get_node_mut(aspirin).unwrap().set_property("name", "aspirin");
    let ibuprofen = g.create_node("Drug");
    g.get_node_mut(ibuprofen).unwrap().set_property("name", "ibuprofen");
    let quiz = g.create_node("Question");
    g.get_node_mut(quiz).unwrap().set_property("text", "what treats a headache?");
    let hammer = g.create_node("Tool");
    g.get_node_mut(hammer).unwrap().set_property("name", "hammer");
    g.create_edge(aspirin, ibuprofen, "SIMILAR_TO").unwrap();
    g.create_edge_stub(quiz, aspirin, "ABOUT").unwrap();

    let memory: Vec<_> = MEMORY_LABELS.iter().map(|l| g.create_node(*l)).collect();
    let decision = memory[0];
    g.get_node_mut(decision).unwrap().set_property("rationale", "the user said they are allergic");
    g.create_edge(decision, aspirin, "EVIDENCED_BY").unwrap(); // memory -> data
    g.create_edge(aspirin, memory[1], "ASKED_IN").unwrap(); // data -> memory
    g.create_edge(memory[1], memory[2], "USED_TOOL").unwrap(); // memory -> memory
    g.create_edge(quiz, memory[3], "EXTRACTED_BY").unwrap();
    g.create_edge_stub(memory[4], ibuprofen, "READ").unwrap(); // stub out of memory
    if compact {
        g.compact_adjacency();
    }
    g.create_edge_stub(hammer, memory[5], "SEEN_IN").unwrap(); // stub into memory, buffered
    g
}

const DATA_NODES: u64 = 4;
const DATA_EDGES: u64 = 2;
const MEMORY_EDGES: u64 = 6;

fn export(g: &GraphStore, include_agent_memory: bool) -> (Vec<u8>, samyama::snapshot::format::ExportStats) {
    let mut buf = Vec::new();
    let stats = export_tenant_with(g, &mut buf, &ExportOptions { include_agent_memory, ..Default::default() }).unwrap();
    (buf, stats)
}

fn header(bytes: &[u8]) -> SnapshotHeader {
    let mut line = String::new();
    BufReader::new(flate2::read::GzDecoder::new(bytes)).read_line(&mut line).unwrap();
    serde_json::from_str(&line).unwrap()
}

fn restore(bytes: &[u8]) -> GraphStore {
    let mut g = GraphStore::new();
    import_tenant(&mut g, bytes).unwrap();
    g
}

fn memory_in(g: &GraphStore) -> usize {
    g.all_nodes().into_iter().filter(|n| is_memory_node(n)).count()
}

fn dropped(h: &SnapshotHeader, what: &str) -> Option<u64> {
    h.dropped.iter().find(|d| d.what == what).map(|d| d.count)
}

#[test]
fn a_default_export_has_no_memory_and_counts_what_it_left_out() {
    for compact in [false, true] {
        let g = graph_with_memory(compact);
        let (bytes, stats) = export(&g, false);
        assert_eq!(stats.node_count, DATA_NODES, "compact={compact}");
        assert_eq!(stats.edge_count, DATA_EDGES, "compact={compact}");
        assert!(!stats.labels.iter().any(|l| MEMORY_LABELS.contains(&l.as_str())), "{:?}", stats.labels);

        let h = header(&bytes);
        assert_eq!((h.node_count, h.edge_count), (DATA_NODES, DATA_EDGES));
        assert_eq!(dropped(&h, AGENT_MEMORY_NODES), Some(MEMORY_LABELS.len() as u64), "compact={compact}");
        assert_eq!(dropped(&h, AGENT_MEMORY_EDGES), Some(MEMORY_EDGES), "compact={compact}");
        assert_eq!(stats.dropped, h.dropped, "the caller is told what the file says");

        let back = restore(&bytes);
        assert_eq!(memory_in(&back), 0);
        assert_eq!(back.node_count() as u64, DATA_NODES);
        assert_eq!(back.edge_count() as u64, DATA_EDGES, "compact={compact}");
        let text = String::from_utf8_lossy(&{
            let mut s = Vec::new();
            std::io::Read::read_to_end(&mut flate2::read::GzDecoder::new(&bytes[..]), &mut s).unwrap();
            s
        })
        .into_owned();
        assert!(!text.contains("allergic"), "a memory property reached the file");
    }
}

#[test]
fn the_plain_entry_point_leaves_memory_out_too() {
    let g = graph_with_memory(false);
    let mut buf = Vec::new();
    let stats = export_tenant(&g, &mut buf).unwrap();
    assert_eq!(stats.node_count, DATA_NODES);
    assert_eq!(memory_in(&restore(&buf)), 0);
}

#[test]
fn including_memory_exports_all_of_it() {
    for compact in [false, true] {
        let g = graph_with_memory(compact);
        let (bytes, stats) = export(&g, true);
        assert_eq!(stats.node_count, DATA_NODES + MEMORY_LABELS.len() as u64);
        assert_eq!(stats.edge_count, DATA_EDGES + MEMORY_EDGES, "compact={compact}");
        let h = header(&bytes);
        assert_eq!(dropped(&h, AGENT_MEMORY_NODES), None);
        assert_eq!(dropped(&h, AGENT_MEMORY_EDGES), None);

        let back = restore(&bytes);
        assert_eq!(memory_in(&back), MEMORY_LABELS.len());
        assert_eq!(back.edge_count() as u64, DATA_EDGES + MEMORY_EDGES, "compact={compact}");
        for l in MEMORY_LABELS {
            assert_eq!(back.get_nodes_by_label(&Label::new(*l)).len(), 1, "{l}");
        }
    }
}

#[test]
fn a_data_node_linked_to_memory_exports_without_the_link() {
    let g = graph_with_memory(false);
    let (bytes, _) = export(&g, false);
    let back = restore(&bytes);
    let drugs = back.get_nodes_by_label(&Label::new("Drug"));
    assert_eq!(drugs.len(), 2);
    let aspirin = drugs
        .iter()
        .find(|n| back.node_property(n.id, "name").and_then(|v| v.as_string().map(str::to_string)).as_deref() == Some("aspirin"))
        .expect("aspirin survives");
    let types: Vec<String> = back
        .get_outgoing_edges(aspirin.id)
        .iter()
        .chain(back.get_incoming_edges(aspirin.id).iter())
        .map(|e| e.edge_type.as_str().to_string())
        .collect();
    assert!(types.contains(&"SIMILAR_TO".to_string()), "{types:?}");
    assert!(!types.iter().any(|t| t == "EVIDENCED_BY" || t == "ASKED_IN"), "{types:?}");
}

#[test]
fn domain_question_and_tool_nodes_are_not_memory() {
    let g = graph_with_memory(false);
    let (bytes, _) = export(&g, false);
    let back = restore(&bytes);
    assert_eq!(back.get_nodes_by_label(&Label::new("Question")).len(), 1);
    assert_eq!(back.get_nodes_by_label(&Label::new("Tool")).len(), 1);
}

#[test]
fn a_graph_without_memory_reports_nothing_left_out() {
    let mut g = GraphStore::new();
    let a = g.create_node("Question");
    let b = g.create_node("Tool");
    g.create_edge(a, b, "NEEDS").unwrap();
    let (bytes, stats) = export(&g, false);
    assert_eq!((stats.node_count, stats.edge_count), (2, 1));
    let h = header(&bytes);
    assert_eq!(dropped(&h, AGENT_MEMORY_NODES), None);
    assert_eq!(dropped(&h, AGENT_MEMORY_EDGES), None);
}

#[test]
fn publishing_refuses_memory_and_accepts_the_default_export() {
    let g = graph_with_memory(false);
    let (full, _) = export(&g, true);
    let report = samyama::pii::scan_snapshot(BufReader::new(flate2::read::GzDecoder::new(&full[..]))).unwrap();
    assert!(!report.is_clean(), "a snapshot with memory must not pass the publish gate");
    assert_eq!(report.agent_memory_nodes(), MEMORY_LABELS.len());
    for l in MEMORY_LABELS {
        assert_eq!(report.agent_memory.get(*l), Some(&1), "{l}");
    }

    let (kg, _) = export(&g, false);
    let report = samyama::pii::scan_snapshot(BufReader::new(flate2::read::GzDecoder::new(&kg[..]))).unwrap();
    assert_eq!(report.agent_memory_nodes(), 0);
    assert!(report.is_clean(), "{:?}", report.findings);
}

#[test]
fn restoring_a_kg_snapshot_keeps_the_memory_already_there() {
    let source = graph_with_memory(false);
    let (kg, _) = export(&source, false);

    let mut live = GraphStore::new();
    let decision = live.create_node("AgentDecision");
    live.get_node_mut(decision).unwrap().set_property("rationale", "kept");
    let question = live.create_node("AgentQuestion");
    live.create_edge(question, decision, "CAUSED_BY").unwrap();

    import_tenant(&mut live, &kg[..]).unwrap();
    assert_eq!(memory_in(&live), 2, "the restore removed memory");
    assert_eq!(
        live.get_node(decision).unwrap().get_property("rationale").and_then(|v| v.as_string()),
        Some("kept")
    );
    assert_eq!(live.get_outgoing_edges(question).len(), 1);
    assert_eq!(live.get_nodes_by_label(&Label::new("Drug")).len(), 2, "the KG arrived");
}
