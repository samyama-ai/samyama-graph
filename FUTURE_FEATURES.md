# Samyama Graph Database - Future High-Value Features

This document outlines strategic features planned for Samyama to increase its value for developers, data scientists, and enterprise users.

---

## 1. Native Graph Visualizer (Web UI) 🎨
**Category**: Developer Experience (DX)
**Value**: **Critical for Adoption.** Graph data is inherently visual. A visual interface transforms the database from a "black box" into an interactive exploration platform.

### Key Capabilities:
*   **Force-Directed Graph**: Interactive rendering of nodes and edges in the browser.
*   **Interactive Exploration**: Double-click nodes to expand relationships in real-time.
*   **Cypher Workbench**: Integrated query editor with syntax highlighting and multi-tab support.
*   **Data Inspector**: Side panel to view full property sets and vector metadata.
*   **Analytics Dashboard**: Visual representation of algorithm results (e.g., node size based on PageRank).

---

## 2. Time-Travel / Temporal Queries ⏳
**Category**: Enterprise / Security
**Value**: **Powerful Differentiator.** Allows users to query the graph state at any point in history, which is essential for audit trails and forensic analysis.

### Key Capabilities:
*   **Versioned Storage**: Use MVCC (Multi-Version Concurrency Control) or Snapshot-based versioning in RocksDB.
*   **Temporal Syntax**: Support `MATCH ... AT TIME 'YYYY-MM-DD'` in Cypher.
*   **Delta Analysis**: Query only the changes between two points in time.
*   **Snapshot Recovery**: Instant "rollback" of the graph to a previous healthy state.

---

## 3. "Auto-RAG" Pipelines 🤖
**Category**: AI / Ease-of-Use
**Value**: **AI Stack Simplification.** Automates the heavy lifting of vector management, making Samyama the easiest database for building RAG applications.

### Key Capabilities:
*   **Embedding Policies**: Define indices that automatically trigger vectorization.
    *   `CREATE VECTOR INDEX ON :Document(content) USING 'openai-text-3-small'`
*   **Automatic Ingestion**: When a node is created/updated, Samyama calls the configured AI provider (OpenAI, HuggingFace, Ollama) asynchronously.
*   **Hybrid Search Abstraction**: Simplify the `CALL` syntax into native Cypher predicates for easier integration with LLM frameworks like LangChain.
*   **Context Chunking**: Built-in logic to split large text properties into optimized chunks for better embedding accuracy.
