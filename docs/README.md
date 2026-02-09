# Samyama Graph Database Documentation

## Core Documentation
- [Architecture](./ARCHITECTURE.md) - High-level system architecture and component interactions.
- [Requirements](./REQUIREMENTS.md) - Functional and non-functional requirements.
- [Tech Stack](./TECH_STACK.md) - Technology choices (Rust, RocksDB, Tokio, etc.).
- [Technology Comparisons](./TECHNOLOGY_COMPARISONS.md) - Benchmarks and trade-off analysis vs alternatives.
- [ACID Guarantees](./ACID_GUARANTEES.md) - Transactional consistency and durability details.
- [Glossary](./GLOSSARY.md) - Key terms and definitions.

## Roadmap & Features
- [Architecture Roadmap](./ARCHITECTURE_ROADMAP.md) - Modularization and expansion plans.
- [Cypher Compatibility](./CYPHER_COMPATIBILITY.md) - Status of OpenCypher implementation vs Neo4j/FalkorDB.
- [GNN Proposal](./GNN_PROPOSAL.md) - Strategy for Graph Neural Network integration (Inference).
- [Agentic Enrichment](./AGENCY_ENRICHMENT_PROPOSAL.md) - AI-driven graph enrichment (Implemented).

## Performance
- [Benchmarks](./performance/BENCHMARKS.md) - Performance metrics and test results.
- [Benchmark Results v0.5.0](./performance/BENCHMARK_RESULTS_v0.5.0.md) - Latest benchmark numbers (2026-02-07).
- [Progress](./performance/PROGRESS.md) - Performance optimization progress log.
- [Performance Roadmap](./performance/PERFORMANCE_ROADMAP.md) - Optimization plans (CSR, JIT, etc.).
- [Benchmark Comparison](./performance/BENCHMARK_COMPARISON.md) - Comparison vs Neo4j and FalkorDB.

## Examples (15 demos)
- `banking_demo.rs` - Banking use case with NLQ
- `supply_chain_demo.rs` - Supply chain graph
- `clinical_trials_demo.rs` - Clinical trials knowledge graph
- `enterprise_soc_demo.rs` - Enterprise SOC security monitoring
- `knowledge_graph_demo.rs` - General knowledge graph
- `smart_manufacturing_demo.rs` - Smart manufacturing IoT
- `social_network_demo.rs` - Social network analysis
- `agentic_enrichment_demo.rs` - Agent-based graph enrichment
- `persistence_demo.rs` - RocksDB persistence & multi-tenancy
- `cluster_demo.rs` - Raft clustering
- `full_benchmark.rs` - Full performance benchmark
- `vector_benchmark.rs` - Vector search benchmark
- `mvcc_benchmark.rs` - MVCC concurrency benchmark
- `late_materialization_bench.rs` - Late materialization benchmark
- `graph_optimization_benchmark.rs` - Optimization solver benchmark

## Demos & Tutorials
- [Supply Chain Demo](./SUPPLY_CHAIN_GUARDIAN_DEMO.md) - Comprehensive demo scenario.

## Sub-Directories
- **[ADR/](./ADR/)**: Architecture Decision Records.
- **[product/](./product/)**: Product management artifacts (Personas, Workflows, Test Cases).
- **[test-results/](./test-results/)**: Detailed test execution reports.
- **[optimization/](./optimization/)**: Optimization case study and workflow.
- **[archive/](./archive/)**: Historical documents and earlier phase records.
