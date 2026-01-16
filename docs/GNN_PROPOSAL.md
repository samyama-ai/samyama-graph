# GNN Implementation Strategy for Samyama Graph

## Recommendation: Implement GNN Inference, Not Training

Samyama Graph should implement GNN capabilities by focusing strictly on **Inference (Execution)** rather than **Training**.

---

## 1. Rationale

### Alignment with "AI-Native" Goal
As an AI-native Graph Vector Database, Samyama already provides:
- **Storage**: Property Graph.
- **Retrieval**: Vector Search/RAG.
- **Analytics**: Centrality and Pathfinding.

**The Missing Piece**: **Prediction**. GNNs provide the predictive layer (e.g., Fraud detection, Link prediction). Adding this completes the end-to-end AI story.

### Technical Feasibility (Rust Context)
- **Training Complexity**: Building a full GNN training loop (backpropagation, gradients) in Rust is high-effort and low-reward compared to established Python ecosystems (PyTorch/DGL).
- **Inference Performance**: Running a pre-trained model *inside* the database is a massive win. It eliminates the "data gravity" problem—moving massive graph data to external Python services for prediction is slow and expensive.

### Competitive Advantage
Most graph databases act as passive stores. By enabling in-database inference, Samyama becomes a dynamic **Feature Store**, capable of updating node properties (e.g., `n.risk_score`) in real-time as the graph evolves.

---

## 2. Proposed Strategy

### Support ONNX Runtime
- Integrate the `ort` (ONNX Runtime) crate.
- Allow users to upload pre-trained models exported from Python.
- **Interface**: A Cypher procedure such as:
  ```cypher
  CALL algo.gnn.predict('fraud_model', 'Person') YIELD node, score
  SET node.fraud_score = score
  ```

### Native GraphSAGE-style Aggregators
- Leverage existing Vector Search (HNSW) infrastructure.
- Implement a "message passing" operator that aggregates neighbor embeddings (vectors) to generate new node embeddings.
- This provides a "zero-config" GNN experience for users who don't want to manage external models.

---

## 3. Verdict
**Yes, but scoped.** Do not attempt to replace PyTorch. Instead, build the **inference engine** that allows users to apply GNNs to live data without leaving the database environment. This integrates perfectly with the existing roadmap for "Auto-RAG Pipelines."
