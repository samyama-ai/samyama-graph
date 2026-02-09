# Samyama Graph Database Glossary

## Core Concepts

### Property Graph
A graph data model where data is organized as nodes (entities) and edges (relationships). Both nodes and edges can have properties (key-value pairs) and labels/types.

### Node
The fundamental unit of data in a property graph, representing an entity (e.g., Person, Product). Nodes can have multiple **Labels** and **Properties**.

### Edge
A directed connection between two nodes, representing a relationship (e.g., KNOWS, BOUGHT). Edges always have a single **Edge Type** and can have **Properties**.

### Label
A tag applied to a node to categorize it (e.g., `:Person`, `:Vehicle`). Nodes can have multiple labels. Used for indexing and query filtering.

### Properties
Key-value pairs attached to nodes or edges. Keys are strings, and values can be strings, integers, floats, booleans, vectors, etc.

## Architecture

### Raft Consensus
A distributed consensus algorithm used by Samyama to ensure data consistency and fault tolerance across the cluster. It manages leader election and log replication.

### WAL (Write-Ahead Log)
A persistence technique where modifications are written to a log file before they are applied to the database. Ensures durability and crash recovery.

### RocksDB
An embeddable persistent key-value store used by Samyama as the underlying storage engine for graph data.

## Multi-Tenancy

### Tenant
A logical isolation boundary within the database. Each tenant has its own data, indices, configuration, and resource quotas.

### Resource Quota
Limits set per tenant on resources like memory usage, storage size, number of nodes/edges, and concurrent connections.

## AI & Vector Search

### Vector Embedding
A list of floating-point numbers (e.g., `[0.1, 0.5, -0.9]`) representing the semantic meaning of text, images, or other data. Used for similarity search.

### Vector Index (HNSW)
Hierarchical Navigable Small World. A graph-based index structure used for efficient Approximate Nearest Neighbor (ANN) search on vector embeddings.

### RAG (Retrieval-Augmented Generation)
A technique that enhances LLM responses by retrieving relevant data from a knowledge base (like Samyama) and providing it as context.

### Auto-Embed (formerly Auto-RAG)
**"Write-Once, Embed-Automatically"**. A tenant-level feature where the database automatically generates vector embeddings for specific text properties upon write, using configured LLM providers. This automates the "Retrieval" preparation step of RAG.

### NLQ (Natural Language Querying)
A feature allowing users to query the graph using plain language (e.g., English). The system uses an LLM to translate the request into an executable OpenCypher query.

### Agentic Enrichment (Implemented)
**Generation-Augmented Knowledge (GAK)**. A feature where the database acts as an agent to fetch missing information from external sources (LLMs, APIs) to build or repair the graph on-demand. Implemented in `src/agent/`.

## Query Languages

### OpenCypher
A declarative graph query language used to query the property graph. Uses ASCII-art style patterns (e.g., `(a)-[:KNOWS]->(b)`).

### SPARQL (Parser Complete)
A semantic query language for databases, able to retrieve and manipulate data stored in Resource Description Framework (RDF) format. Parser implemented in `src/rdf/sparql/`.

## Protocols

### RESP (Redis Serialization Protocol)
The communication protocol used by Redis. Samyama implements this to be compatible with existing Redis clients.

## Performance & Internals

### Late Materialization
An optimization where scan and traversal operators pass lightweight references (`NodeRef(NodeId)` / `EdgeRef(EdgeId)`) through the execution pipeline instead of full object clones. Properties are resolved lazily only when projected or filtered. Yields 4-5x improvement in multi-hop query latency.

### CSR (Compressed Sparse Row)
A memory-efficient representation for graph adjacency. Stores all edge targets in a single contiguous array with an offset index per node, improving cache locality for traversals.

### MVCC (Multi-Version Concurrency Control)
A concurrency control method where each node/edge maintains version history. Readers access consistent snapshots via `get_node_at_version()` without blocking writers.

### GAK (Generation-Augmented Knowledge)
The inverse of RAG — using LLMs to *build* the database rather than *query* it. The database acts as an agent to fetch, structure, and persist missing information on-demand.

### ClaudeCode LLM Provider
An LLM provider integration that uses Anthropic's Claude models for NLQ (Natural Language Querying) and agentic enrichment tasks. Configured per-tenant alongside OpenAI and Ollama providers.

### NLQ Pipeline
The end-to-end flow for Natural Language Querying: user text → schema injection → LLM translation → Cypher query → execution → results. Supports optional agentic enrichment for missing data.

### HNSW (Hierarchical Navigable Small World)
A graph-based approximate nearest neighbor (ANN) index used for efficient vector similarity search. Provides sub-millisecond search latency on datasets up to 1M vectors.
