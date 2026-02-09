# Agentic Graph Enrichment Proposal

> **Status: Implemented** (v0.5.0-alpha.1)
>
> Implementation: `src/agent/`, `src/nlq/`, `src/embed/`
> Demo: `examples/agentic_enrichment_demo.rs`
> Domain examples with NLQ pipeline: `banking_demo.rs`, `supply_chain_demo.rs`, `clinical_trials_demo.rs`, `enterprise_soc_demo.rs`, `knowledge_graph_demo.rs`, `smart_manufacturing_demo.rs`, `social_network_demo.rs`

## Overview

**Agentic Enrichment** transforms the database from a passive store of data into an active knowledge partner. Instead of merely retrieving existing data, the database acts as an agent to fetch, structure, and persist missing information on-demand using Large Language Models (LLMs).

This feature solves the "Cold Start Problem" in graph databases by allowing the graph to bootstrap and heal itself.

## Core Concept

**Generation-Augmented Knowledge (GAK)**: Using LLMs to build and repair the database state, as opposed to Retrieval-Augmented Generation (RAG) which uses the database to help the LLM.

## Use Cases

1.  **The "Live Data" Gap**
    *   *User Query:* "How does the current Fed interest rate impact my 'Mortgage' node?"
    *   *System Action:* Detects missing 'Fed Rate' node. Calls LLM with tool access to fetch current rate (e.g., 5.5%). Creates `Rate` node and links it to `Mortgage` node. Returns answer.

2.  **The "Research Assistant"**
    *   *User Query:* "Connect 'Diabetes' to recent papers about 'Semaglutide'."
    *   *System Action:* Queries external sources (PubMed/LLM), finds relevant papers, creates `Paper` nodes, and creates `[:MENTIONS]` edges to the `Diabetes` node.

3.  **Self-Healing / Auto-Completion**
    *   *System Action:* Detects a `Person` node with missing properties (e.g., `City`) but has context from neighbors. Asks LLM: "Based on these logs, where does this person live?" and fills the gap.

## Architecture (Implemented)

A new **Agent Loop** component, separate from the standard Query Engine.

### Workflow

1.  **Trigger**:
    *   Explicit: `CALL db.enrich.expand('Diabetes', 'Research Paper')`
    *   Implicit: Query returns partial/no results (configurable policy).

2.  **Prompt Engineering**:
    *   System templates a prompt: *"I have a node 'Diabetes'. Find related 'Research Paper' entities and return them as JSON matching this schema: { ... }."*

3.  **LLM Call**:
    *   Sends request to configured Provider (OpenAI, Ollama, etc.) via Chat Endpoint.

4.  **Parse & Persist**:
    *   Parses the JSON response.
    *   Generates and executes internal `CREATE (n:Paper ...)-[:RELATES]->(d)` commands.

5.  **Return**:
    *   The original query is re-run, now yielding results from the newly enriched graph.

## Terminology

*   **Agentic Enrichment**: The capability of the database to actively fetch and create data.
*   **JIT Knowledge Graph**: A graph that constructs itself Just-In-Time.
*   **Enrichment Policy**: Configuration defining when and how the database is allowed to reach out to external agents.
