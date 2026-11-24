//! The labels that are agent memory, defined once (#2242, Smriti Phase 0).
//!
//! Agent memory lives in the same tenant as the domain data (Smriti spec §6),
//! so everything that must treat it differently -- snapshot export, the
//! publish gate (`samyama pii-scan`), the memory procedures and, later, the RAM
//! metrics -- needs to know which nodes are memory. This is the one list they
//! all read; a label added here is excluded from export and refused at publish
//! everywhere at once.
//!
//! Memory is recognised **by label, never by guess**. The plan executor's
//! telemetry used to be written as `Question` and `Tool`, which are ordinary
//! domain labels too: a quiz graph has questions, a tooling graph has tools.
//! Filtering on those would have removed real data, so the telemetry now
//! writes `AgentQuestion` and `AgentTool`, and only the `Agent*` names and the
//! extraction-telemetry names below are memory. A domain `Question` node is
//! untouched by all of this.
//!
//! What is in memory can be personal: user conversations, agent decisions and
//! their free-text rationale. That is why it stays out of a default export and
//! out of anything published.

use crate::graph::{GraphStore, Label, Node, NodeId};

/// A recorded agent decision (`src/agent/memory.rs`, #2099).
pub const AGENT_DECISION: &str = "AgentDecision";
/// A question the plan executor ran (formerly `Question`).
pub const AGENT_QUESTION: &str = "AgentQuestion";
/// A tool the plan executor called (formerly `Tool`).
pub const AGENT_TOOL: &str = "AgentTool";
/// Extraction telemetry `graphrag-rs` writes: one extraction attempt.
pub const EXTRACTION_ATTEMPT: &str = "ExtractionAttempt";
/// Extraction telemetry: the source an extraction read.
pub const EXTRACTION_SOURCE: &str = "ExtractionSource";
/// Extraction telemetry: a verified extraction kept as a demonstration.
pub const VERIFIED_EXTRACTION_DEMO: &str = "VerifiedExtractionDemo";

/// Every label that marks a node as agent memory.
pub const MEMORY_LABELS: &[&str] = &[
    AGENT_DECISION,
    AGENT_QUESTION,
    AGENT_TOOL,
    EXTRACTION_ATTEMPT,
    EXTRACTION_SOURCE,
    VERIFIED_EXTRACTION_DEMO,
];

/// Is `label` one of the memory labels? Exact and case-sensitive, as labels
/// are: `agentquestion` is not `AgentQuestion`.
pub fn is_memory_label(label: &str) -> bool {
    MEMORY_LABELS.contains(&label)
}

/// Is `node` agent memory? True when **any** of its labels is a memory label,
/// so a node that is also given a domain label is still memory.
pub fn is_memory_node(node: &Node) -> bool {
    node.labels.iter().any(|l| is_memory_label(l.as_str()))
}

/// The ids of every memory node in `store`, read from the label index.
pub fn memory_node_ids(store: &GraphStore) -> std::collections::HashSet<NodeId> {
    MEMORY_LABELS
        .iter()
        .filter_map(|l| store.nodes_with_label(&Label::new(*l)))
        .flat_map(|ids| ids.iter().copied())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_list_holds_the_agent_and_extraction_labels_and_no_domain_ones() {
        for label in [
            "AgentDecision",
            "AgentQuestion",
            "AgentTool",
            "ExtractionAttempt",
            "ExtractionSource",
            "VerifiedExtractionDemo",
        ] {
            assert!(is_memory_label(label), "{label}");
        }
        // The old telemetry names are domain labels: never memory.
        for label in ["Question", "Tool", "Decision", "agentquestion", "Person", ""] {
            assert!(!is_memory_label(label), "{label}");
        }
        let mut sorted = MEMORY_LABELS.to_vec();
        sorted.sort_unstable();
        sorted.dedup();
        assert_eq!(sorted.len(), MEMORY_LABELS.len(), "a label is listed twice");
    }

    #[test]
    fn a_node_is_memory_when_any_label_is() {
        let mut store = GraphStore::new();
        let domain = store.create_node("Question");
        let memory = store.create_node("AgentQuestion");
        let both = store.create_node_with_labels([Label::new("Person"), Label::new("AgentDecision")]);
        let get = |id| store.get_node(id).unwrap();
        assert!(!is_memory_node(get(domain)));
        assert!(is_memory_node(get(memory)));
        assert!(is_memory_node(get(both)), "a domain label does not hide a memory one");
        let ids = memory_node_ids(&store);
        assert_eq!(ids.len(), 2);
        assert!(ids.contains(&memory) && ids.contains(&both) && !ids.contains(&domain));
    }

    #[test]
    fn an_empty_store_has_no_memory() {
        assert!(memory_node_ids(&GraphStore::new()).is_empty());
    }
}
