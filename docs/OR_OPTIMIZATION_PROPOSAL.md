# Samyama Optimization Engine (OR-Native Graph DB)

## 1. Executive Summary
This proposal outlines the architecture for transforming Samyama Graph into an **Active Decision Engine** by integrating native Operations Research (OR) capabilities. 

Instead of exporting data to external solvers (Gurobi/OR-Tools), Samyama will perform **In-Database Optimization** using a custom Rust implementation of metaheuristic algorithms (Jaya, Rao, TLBO).

## 2. Strategic Value
*   **Zero-Copy Solving**: Solvers run directly on graph memory. No serialization overhead.
*   **Reactive Intelligence**: Optimization is triggered by data changes (e.g., "Inventory Low" -> Trigger "Reorder Optimization").
*   **Unified Model**: The Graph *is* the optimization model. Nodes are variables; Edges are constraints.

## 3. Architecture: The "Hybrid Solver"

### 3.1 Core Components
We will implement a new crate: `samyama-optimization`.

| Component | Function |
| :--- | :--- |
| **Solver Core** | Rust implementations of **Rao**, **Jaya**, and **TLBO** algorithms. |
| **Graph Adapter** | Maps Graph Entities (Nodes/Edges) to Optimization Variables. |
| **Python Bindings** | `pyo3` wrappers to publish as a high-performance PyPI package. |

### 3.2 Workflow
1.  **Define**: User defines objective using Cypher.
    ```cypher
    CALL algo.or.solve({
      algorithm: 'Rao3',
      objective: 'MINIMIZE sum(n.cost) FOR (n:Resource)',
      constraints: ['n.load <= n.capacity']
    })
    ```
2.  **Map**: The `Graph Adapter` reads properties directly from the `Storage Engine`.
3.  **Solve**: The `Solver Core` runs the metaheuristic in parallel (using `rayon`).
4.  **Update**: Resulting values are written back to node properties (`n.allocation = 50`).

## 4. Implementation Plan (The Rewrite)

### Phase 1: Rust Kernel (Standalone)
*   Port `Rao1`, `Rao2`, `Rao3`, `Jaya`, `TLBO` from Python to Rust.
*   Implement `ObjectiveFunction` traits.
*   Benchmark against original Python implementation.

### Phase 2: Python Integration (`pyo3`)
*   Wrap the Rust Kernel.
*   Publish to PyPI as `sandeepkunkunuru/optimization-rs` (or similar).
*   Ensure drop-in compatibility with existing Python scripts.

### Phase 3: DB Integration
*   Create `OptimizationOperator` in Samyama's Query Engine.
*   Map Cypher AST to Solver Configuration.

## 5. Target Case Studies
1.  **Supply Chain**: Flow optimization across warehouse nodes.
2.  **Healthcare**: Patient-to-Bed allocation matching.
3.  **Smart Grid**: Load balancing across grid topology.

## 6. Comparison: Traditional vs. Samyama

| Feature | Traditional (Graph -> Python) | Samyama (In-Database) |
| :--- | :--- | :--- |
| **Data Movement** | High (Export/Import) | **Zero** |
| **Latency** | Minutes | **Milliseconds** |
| **Constraint Def** | Matrix / Equations | **Graph Structure** |
| **Complexity** | High (ETL Pipelines) | **Low (SQL/Cypher)** |
