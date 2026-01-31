# Natural Language Querying (NLQ) Proposal

## Overview

**Natural Language Querying (NLQ)** allows users to interact with the Samyama Graph Database using plain English (or other languages) instead of writing Cypher code. This lowers the barrier to entry for non-technical users and accelerates development for engineers.

**Key Philosophy:** **Optional & Tenant-Scoped.**
Samyama remains a high-performance, standalone graph database. NLQ is an *additive* layer. If no LLM is configured, the database functions purely as a standard graph database.

## Core Concept

**Text-to-Cypher Translation**: The database acts as an intermediary that translates user intent into executable database queries.

## Architecture

### 1. The Agent Layer
A lightweight module sitting between the API/Protocol layer and the Query Engine.

```mermaid
graph LR
    User[User/Client] -->|1. 'Who knows Alice?'| Agent[Samyama Agent Layer]
    Agent -->|2. Fetch Schema| Schema[Tenant Schema]
    Agent -->|3. Prompt + Schema| LLM[LLM Provider]
    LLM -->|4. Generate Cypher| Agent
    Agent -->|5. MATCH (a)-[:KNOWS]->(b)...| Engine[Query Engine]
    Engine -->|6. Results| User
```

### 2. Tenant Configuration
NLQ is configured per tenant. This ensures:
*   **Data Privacy:** Schema information is only sent to the provider authorized by that tenant.
*   **Cost Control:** Usage is billed/tracked per tenant settings.
*   **Opt-In:** Tenants with strict security requirements can leave this disabled.

Configuration Object (`NLQConfig`):
*   `enabled`: bool
*   `provider`: OpenAI, Ollama, Anthropic, etc.
*   `model`: "gpt-4o", "llama3", etc.
*   `system_prompt`: Custom instructions (e.g., "Always limit results to 10").

## Workflow

1.  **User Request:** Client sends a request via RESP or REST: `GRAPH.QUERY_NL "my_graph" "Show me the top 5 products purchased by users in London"`
2.  **Context Construction:** The system retrieves the graph schema (Labels, Edge Types, Property Keys) for the tenant.
3.  **Translation:**
    *   System constructs a prompt: *"You are a Cypher expert. Given this schema: {Schema}, translate this question: '{Question}' into a single read-only Cypher query."*
    *   LLM returns Cypher.
4.  **Validation:**
    *   System parses the returned Cypher to ensure it is valid syntax.
    *   **Security Check:** Ensures the query is read-only (rejects CREATE/DELETE/DROP).
5.  **Execution:** The validated Cypher is executed against the `QueryEngine`.
6.  **Response:** Standard RecordBatch results are returned to the user.

## Limitations & Safeguards

1.  **Read-Only Default:** NLQ should default to read-only operations to prevent accidental data loss from "hallucinated" delete commands.
2.  **Hallucination Risk:** The system returns the *generated query* alongside the results so users can verify accuracy.
3.  **Performance:** LLM calls add latency. This feature is for *exploration*, not high-frequency transactional lookups.

## Future Extensions

*   **CoT (Chain of Thought):** For complex analytical questions, allow the agent to break down the problem into multiple steps/queries.
*   **Feedback Loop:** Users can correct the generated Cypher, and the system "learns" (stores few-shot examples) for that tenant.
