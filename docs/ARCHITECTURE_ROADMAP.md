# Architectural Roadmap: Modularization & Expansion

This document outlines the strategic plan to transition Samyama from a monolithic structure to a modular ecosystem of specialized crates. This approach improves compilation times, enforces cleaner boundaries, and enables independent usage of components.

## 1. Modularization Strategy (Refactor)

The first phase involves extracting existing core components into standalone crates.

### `crates/samyama-vector` (Vector Search Engine)
*   **Current State**: Logic currently resides in `src/vector/`.
*   **Rationale**: Vector search (HNSW, DiskANN) is compute-intensive and has heavy dependencies (SIMD). Decoupling it prevents bloating the core graph engine.
*   **Planned Features**:
    *   **Pluggable Distance Metrics**: Cosine, L2 (Euclidean), Dot Product.
    *   **Index Implementation**: HNSW (Hierarchical Navigable Small World) graph.
    *   **Optimization**: Scalar/Product Quantization (PQ) for reduced memory footprint.

### `crates/samyama-raft` (Consensus Module)
*   **Current State**: Logic currently resides in `src/raft/`.
*   **Rationale**: Raft is the distributed "heartbeat" but is conceptually orthogonal to graph storage.
*   **Planned Features**:
    *   **Core Consensus**: Leader Election, Log Replication, Snapshotting.
    *   **Transport Abstraction**: Trait-based networking to allow swapping TCP/QUIC/In-Memory.
    *   **Verification**: Strictly tested state machine via randomized simulation (Jepsen-style).

### `crates/samyama-cypher` (Query Compiler)
*   **Current State**: Logic currently resides in `src/query/` (Parser, AST).
*   **Rationale**: The Cypher parser (Pest) and AST definitions are valuable as a standalone library (e.g., for client-side validation, linters, or other tools).
*   **Planned Features**:
    *   **Parser**: Raw String $\to$ Abstract Syntax Tree (AST).
    *   **Planner**: AST $\to$ Logical Plan (algebraic IR).
    *   **Optimizer**: Logical Plan $\to$ Physical Plan (cost-based optimization).

## 2. New Capabilities (Expansion)

The second phase introduces new domains to the "Decision Engine" ecosystem.

### `crates/samyama-ml` (Predictive Inference)
*   **Concept**: A bridge between "Data" (Graph) and "Decisions" (Optimization). While optimization finds the *best* choice given constraints, ML predicts the *future* constraints.
*   **Rationale**: To enable "Predictive Optimization" (e.g., predict next week's demand, *then* optimize supply).
*   **Planned Features**:
    *   **Inference Runtime**: Integration with `ort` (ONNX Runtime) to execute pre-trained PyTorch/TensorFlow models within Rust.
    *   **Graph Neural Networks (GNN)**: Native utilities for generating and managing node embeddings directly from graph topology.

### `crates/samyama-geo` (Spatial Indexing)
*   **Concept**: First-class support for geospatial data types and operations.
*   **Rationale**: Critical for logistics, supply chain, and routing use cases which are primary targets for Samyama.
*   **Planned Features**:
    *   **Indexing**: R-Tree or QuadTree implementation for 2D spatial data.
    *   **Integration**: GeoHash or H3 (Uber's hexagonal index) for discrete spatial bucketing.
    *   **Predicates**: `WITHIN`, `DISTANCE`, `INTERSECTS` supported natively in Cypher.

---

## Execution Plan

1.  **Phase 1 (Refactor)**: Extract `samyama-vector` and `samyama-raft` to `crates/`.
2.  **Phase 2 (New Feature)**: Build `samyama-ml` prototype with ONNX support.
3.  **Phase 3 (Utility)**: Extract `samyama-cypher` and implement `samyama-geo` as required by user demand.
