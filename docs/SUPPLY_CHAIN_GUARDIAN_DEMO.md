# SupplyChainGuardian: Comprehensive Samyama Demo

## 1. Scenario: Pharmaceutical Supply Chain Resilience

A global pharmaceutical company, **PharmaCorp**, produces a life-saving drug, **CardioFix**. The supply chain involves sourcing Raw Materials (RM), synthesizing Active Pharmaceutical Ingredients (API) in India, manufacturing tablets in Germany, and distributing to US/EU markets.

### The Problem (Pain Point)
A sudden **Port Strike in Hamburg** disrupts the flow.
*   **Operations Manager** needs to know: "Which shipments are affected?" (NLQ)
*   **Procurement** needs to find: "Alternative API suppliers with cardiac certification." (Vector Search)
*   **System** needs to: "Enrich supplier nodes with latest news." (Agents)
*   **Logistics** needs to: "Re-optimize delivery routes to minimize delay." (Optimization)
*   **Risk Officer** needs to: "Identify the most critical supplier nodes." (Graph Algorithms)

---

## 2. Solution Architecture

The demo combines all Samyama features into a unified workflow.

### Feature Usage Map
| Feature | Application in Demo |
| :--- | :--- |
| **Property Graph** | Modeling Suppliers, Factories, Ports, Routes, and Products. |
| **Auto-Embed** | Automatically vectorizing supplier descriptions ("FDA approved, specializes in...") and news articles. |
| **Agentic Enrichment** | Triggering a "News Agent" when a Port node status changes to "Disrupted" to fetch latest strike info. |
| **Vector Search** | Finding alternative suppliers based on semantic match of capabilities (e.g., "cardiac API"). |
| **NLQ** | Answering executive questions like "Show me all shipments routed through Hamburg." |
| **Graph Algorithms** | Using **PageRank** to score supplier criticality and **BFS** for shortest path analysis. |
| **Optimization** | Using **Jaya/Rao** algorithms to solve the *Logistics Resource Allocation* problem (minimizing cost/time). |

---

## 3. Data Model

### Nodes
*   `(:Supplier {name, location, capabilities_text, reliability_score})`
*   `(:Material {name, type})`
*   `(:Factory {name, location, capacity})`
*   `(:Port {name, location, status, news_summary})`
*   `(:Shipment {id, value, due_date})`

### Edges
*   `(:Supplier)-[:SUPPLIES]->(:Material)`
*   `(:Material)-[:USED_IN]->(:Factory)`
*   `(:Factory)-[:SHIPS_VIA]->(:Port)`
*   `(:Port)-[:CONNECTED_TO {distance_km, cost}]->(:Port)`
*   `(:Shipment)-[:LOCATED_AT]->(:Port)`

---

## 4. Demo Workflow (Step-by-Step)

### Step 1: Ingestion & Auto-Embed
*   Load the initial graph structure.
*   **Action**: Create `Supplier` nodes with text descriptions.
*   **Result**: Samyama automatically generates embeddings for `capabilities_text`.

### Step 2: The Disruption (Agent Trigger)
*   **Event**: Update `Port` "Hamburg" status to "Strike".
*   **Trigger**: The `NewsEnrichmentAgent` wakes up.
*   **Action**: Agent "searches" (mock) the web for "Hamburg Port Strike duration".
*   **Effect**: Updates `Port` node with `news_summary`: "Strike expected to last 7 days."

### Step 3: Impact Analysis (NLQ)
*   **User Query**: "Which high-value shipments are currently at Hamburg?"
*   **NLQ Pipeline**: Translates to:
    ```cypher
    MATCH (s:Shipment)-[:LOCATED_AT]->(p:Port {name: 'Hamburg'}) 
    WHERE s.value > 100000 
    RETURN s
    ```
*   **Result**: Returns list of at-risk shipments.

### Step 4: Finding Alternatives (Vector Search)
*   **User Query**: "Find alternative suppliers for 'Cardiac API' similar to 'IndoChem Labs'."
*   **Action**: Vector search on `Supplier` nodes using `capabilities_embedding`.
*   **Result**: Returns top 3 semantically similar suppliers not affected by the route.

### Step 5: Criticality Assessment (Graph Algo)
*   **Action**: Run `PageRank` on the supply network.
*   **Insight**: Identify that "Rotterdam Port" is the next single-point-of-failure if traffic diverts.

### Step 6: Route Optimization (Optimization Engine)
*   **Problem**: Allocate shipments to alternative routes (Rotterdam vs Antwerp) given capacity constraints.
*   **Solver**: Use `JayaSolver`.
*   **Objective**: Minimize `(Total Cost + Delay Penalty)`.
*   **Result**: Optimal distribution plan (e.g., 60% to Rotterdam, 40% to Antwerp).

---

## 5. Implementation Plan

We will create a single executable `examples/supply_chain_demo.rs` that orchestrates this entire flow.

1.  **Setup**: Initialize Graph, Tenants, Agents.
2.  **Simulation**: Script the workflow steps.
3.  **Visualization**: Print ASCII graph or structured logs for each step.
