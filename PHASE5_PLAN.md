# Phase 5: RDF/SPARQL Support - Implementation Plan

## Overview

Phase 5 adds optional RDF (Resource Description Framework) and SPARQL query language support to Samyama Graph Database. This enables semantic web capabilities alongside the existing property graph model.

## Requirements Coverage

### RDF Support (REQ-RDF-001 to REQ-RDF-007)
- **REQ-RDF-001**: RDF data model (subject-predicate-object triples)
- **REQ-RDF-002**: RDF triples support
- **REQ-RDF-003**: RDF serialization formats (Turtle, RDF/XML, N-Triples, JSON-LD)
- **REQ-RDF-004**: Named graphs (quad store)
- **REQ-RDF-005**: RDF Schema (RDFS) semantics
- **REQ-RDF-006**: Property graph ↔ RDF mapping
- **REQ-RDF-007**: Basic OWL reasoning (optional)

### SPARQL Support (REQ-SPARQL-001 to REQ-SPARQL-008)
- **REQ-SPARQL-001**: SPARQL 1.1 query language
- **REQ-SPARQL-002**: SPARQL HTTP protocol
- **REQ-SPARQL-003**: SPARQL query forms (SELECT, CONSTRUCT, ASK, DESCRIBE)
- **REQ-SPARQL-004**: SPARQL UPDATE operations
- **REQ-SPARQL-005**: SPARQL filtering and constraints
- **REQ-SPARQL-006**: SPARQL aggregates
- **REQ-SPARQL-007**: SPARQL federation (SERVICE keyword)
- **REQ-SPARQL-008**: Query optimization

## Architecture Decision

### Hybrid Approach: Integration with Existing Libraries

Rather than implementing RDF/SPARQL from scratch, we'll leverage mature Rust libraries:

1. **RDF Core**: Use `oxrdf` for RDF primitives (IRIs, literals, triples)
2. **SPARQL Parser**: Use `spargebra` for SPARQL 1.1 parsing
3. **Serialization**: Use `rio` for RDF I/O (Turtle, RDF/XML, N-Triples, JSON-LD)
4. **Custom Integration**: Bridge between property graph and RDF models

### Rationale

- **Time to Market**: Reduces implementation time from 12 months to 3-4 months
- **Standards Compliance**: These libraries are battle-tested and spec-compliant
- **Maintenance**: Less code to maintain, community support
- **Quality**: Proven implementations with extensive testing

## Module Structure

```
src/
├── rdf/                    # Phase 5: RDF/SPARQL Module
│   ├── mod.rs              # Module entry point
│   ├── types.rs            # RDF types (IRI, Literal, Triple, Quad)
│   ├── store.rs            # RDF triple/quad store
│   ├── mapping.rs          # Property Graph ↔ RDF mapping
│   ├── namespace.rs        # Namespace management (prefixes)
│   ├── serialization/      # RDF serialization formats
│   │   ├── mod.rs
│   │   ├── turtle.rs       # Turtle format
│   │   ├── rdfxml.rs       # RDF/XML format
│   │   ├── ntriples.rs     # N-Triples format
│   │   └── jsonld.rs       # JSON-LD format
│   └── schema.rs           # RDFS reasoning (basic)
├── sparql/                 # Phase 5: SPARQL Module
│   ├── mod.rs              # Module entry point
│   ├── parser.rs           # SPARQL parser (wraps spargebra)
│   ├── executor/           # SPARQL execution engine
│   │   ├── mod.rs
│   │   ├── select.rs       # SELECT queries
│   │   ├── construct.rs    # CONSTRUCT queries
│   │   ├── ask.rs          # ASK queries
│   │   ├── describe.rs     # DESCRIBE queries
│   │   └── update.rs       # UPDATE operations
│   ├── algebra.rs          # SPARQL algebra evaluation
│   ├── optimizer.rs        # Query optimization
│   ├── results.rs          # Query result formats (JSON, XML, CSV)
│   └── http.rs             # SPARQL HTTP endpoint (protocol)
└── lib.rs                  # Update with RDF/SPARQL exports
```

## Implementation Phases

### Week 1-2: RDF Data Model (12 working days)

**Tasks:**
1. Add dependencies to Cargo.toml:
   - `oxrdf` (RDF primitives)
   - `rio_api`, `rio_turtle`, `rio_xml` (RDF I/O)
   - `spargebra` (SPARQL parser)
   - `sparesults` (SPARQL results)

2. Implement RDF types (`src/rdf/types.rs`):
   - Wrapper types for IRI, Literal, BlankNode
   - Triple structure (subject, predicate, object)
   - Quad structure (triple + named graph)
   - TriplePattern for queries

3. Implement RDF store (`src/rdf/store.rs`):
   - TripleStore: HashMap-based triple storage
   - QuadStore: Support for named graphs
   - Indices: SPO, POS, OSP for efficient queries
   - Insert, delete, query operations
   - Integration with persistence layer

4. Implement namespace management (`src/rdf/namespace.rs`):
   - Prefix → IRI mapping
   - Common prefixes (rdf, rdfs, xsd, owl)
   - Compact IRI notation

**Deliverables:**
- Basic RDF triple/quad storage
- In-memory RDF store
- Namespace management
- Unit tests (>80% coverage)

### Week 3: RDF Serialization (5 working days)

**Tasks:**
1. Turtle format (`src/rdf/serialization/turtle.rs`):
   - Parse Turtle to triples
   - Serialize triples to Turtle
   - Prefix handling

2. N-Triples format (`src/rdf/serialization/ntriples.rs`):
   - Simple line-based format
   - Parse and serialize

3. RDF/XML format (`src/rdf/serialization/rdfxml.rs`):
   - Use `rio_xml` for parsing/serialization

4. JSON-LD format (`src/rdf/serialization/jsonld.rs`):
   - JSON-based RDF representation

**Deliverables:**
- Import/export RDF data in 4 formats
- Integration tests with sample RDF files
- Documentation and examples

### Week 4-5: Property Graph ↔ RDF Mapping (10 working days)

**Tasks:**
1. Property Graph → RDF mapping (`src/rdf/mapping.rs`):
   ```
   Node(id=1, labels=[Person], props={name: "Alice"})
   →
   <http://example.org/node/1> rdf:type ex:Person .
   <http://example.org/node/1> ex:name "Alice" .
   ```

2. RDF → Property Graph mapping:
   ```
   <alice> rdf:type ex:Person .
   <alice> ex:name "Alice" .
   →
   Node(id=alice, labels=[Person], props={name: "Alice"})
   ```

3. Edge mapping:
   ```
   Edge(source=1, target=2, type=KNOWS, props={since: 2020})
   →
   <http://example.org/node/1> ex:KNOWS <http://example.org/node/2> .
   <http://example.org/edge/1-2-KNOWS> ex:since "2020"^^xsd:integer .
   ```

4. Bidirectional sync:
   - Property graph as primary
   - RDF as view layer
   - Automatic synchronization

**Deliverables:**
- Bidirectional mapping
- Automatic sync on mutations
- Comprehensive test suite
- Mapping documentation

### Week 6: RDFS Reasoning (5 working days)

**Tasks:**
1. RDFS entailment rules (`src/rdf/schema.rs`):
   - rdfs:subClassOf transitivity
   - rdfs:subPropertyOf transitivity
   - rdfs:domain and rdfs:range inference
   - rdf:type inheritance

2. Basic reasoning engine:
   - Forward chaining
   - Materialized view of inferred triples

**Deliverables:**
- Basic RDFS reasoning
- Inferred triple computation
- Tests with RDFS ontologies

### Week 7-8: SPARQL Parser and Algebra (10 working days)

**Tasks:**
1. SPARQL parser integration (`src/sparql/parser.rs`):
   - Wrap `spargebra::Query`
   - Error handling and validation
   - Parse SPARQL 1.1 queries

2. SPARQL algebra evaluation (`src/sparql/algebra.rs`):
   - BGP (Basic Graph Pattern) matching
   - FILTER evaluation
   - OPTIONAL, UNION operators
   - Property path evaluation
   - Subqueries

3. SPARQL optimizer (`src/sparql/optimizer.rs`):
   - Join reordering
   - Filter pushdown
   - Index selection

**Deliverables:**
- SPARQL 1.1 parsing
- Algebra evaluation framework
- Basic optimization
- Unit tests

### Week 9-10: SPARQL Query Execution (10 working days)

**Tasks:**
1. SELECT queries (`src/sparql/executor/select.rs`):
   - BGP matching on triple store
   - Solution mappings
   - Projection, sorting, limits
   - Aggregation (COUNT, SUM, AVG, MIN, MAX)
   - GROUP BY, HAVING

2. CONSTRUCT queries (`src/sparql/executor/construct.rs`):
   - Template instantiation
   - New graph construction

3. ASK queries (`src/sparql/executor/ask.rs`):
   - Boolean result

4. DESCRIBE queries (`src/sparql/executor/describe.rs`):
   - Resource description

5. UPDATE operations (`src/sparql/executor/update.rs`):
   - INSERT DATA
   - DELETE DATA
   - DELETE/INSERT WHERE

**Deliverables:**
- All SPARQL query forms
- UPDATE operations
- Comprehensive test suite
- SPARQL 1.1 test suite integration

### Week 11: SPARQL Results and HTTP Endpoint (5 working days)

**Tasks:**
1. Result formats (`src/sparql/results.rs`):
   - SPARQL JSON results
   - SPARQL XML results
   - SPARQL CSV/TSV results
   - Use `sparesults` library

2. SPARQL HTTP endpoint (`src/sparql/http.rs`):
   - HTTP POST for queries
   - Content negotiation
   - Query parameter handling
   - Integration with existing HTTP server

**Deliverables:**
- SPARQL result serialization
- HTTP SPARQL endpoint
- Integration tests
- API documentation

### Week 12: Integration, Testing, Documentation (5 working days)

**Tasks:**
1. Integration with existing components:
   - RESP protocol: GRAPH.SPARQL command
   - Persistence: RDF triple storage in RocksDB
   - Multi-tenancy: Per-tenant RDF stores

2. Comprehensive testing:
   - SPARQL 1.1 test suite
   - W3C compliance tests
   - Performance benchmarks
   - Integration tests

3. Documentation:
   - RDF/SPARQL user guide
   - API documentation
   - Examples and tutorials
   - Migration guide (Cypher ↔ SPARQL)

**Deliverables:**
- Full integration
- Test suite passing
- Complete documentation
- Example applications

## Dependencies (Cargo.toml additions)

```toml
[dependencies]
# RDF support
oxrdf = "0.2"              # RDF primitives (IRI, Literal, Triple, Quad)
oxiri = "0.2"              # IRI handling

# RDF I/O
rio_api = "0.8"            # RDF I/O API
rio_turtle = "0.8"         # Turtle parser/serializer
rio_xml = "0.8"            # RDF/XML parser/serializer

# SPARQL support
spargebra = "0.3"          # SPARQL 1.1 parser
sparesults = "0.2"         # SPARQL results formats
sparopt = "0.1"            # SPARQL optimizer (optional)

# HTTP server (for SPARQL endpoint)
axum = "0.7"               # HTTP framework (async)
tower = "0.4"              # Middleware
tower-http = "0.5"         # HTTP utilities

# Additional utilities
percent-encoding = "2.3"    # URL encoding
mime = "0.3"               # MIME types
```

## API Design

### RDF Store API

```rust
use samyama::rdf::{RdfStore, Triple, Quad, Iri, Literal};

// Create RDF store
let mut rdf_store = RdfStore::new();

// Insert triples
let subject = Iri::new("http://example.org/alice")?;
let predicate = Iri::new("http://xmlns.com/foaf/0.1/name")?;
let object = Literal::new_simple_literal("Alice");

let triple = Triple::new(subject, predicate, object);
rdf_store.insert(triple)?;

// Query triples
let pattern = TriplePattern {
    subject: Some(subject),
    predicate: None,
    object: None,
};
let results = rdf_store.query(pattern)?;

// Named graphs (quads)
let graph = Iri::new("http://example.org/graph/social")?;
let quad = Quad::new(triple, Some(graph));
rdf_store.insert_quad(quad)?;
```

### SPARQL Query API

```rust
use samyama::sparql::{SparqlEngine, SparqlResults};

let engine = SparqlEngine::new(rdf_store);

// SELECT query
let query = r#"
    PREFIX foaf: <http://xmlns.com/foaf/0.1/>
    SELECT ?name ?age
    WHERE {
        ?person foaf:name ?name .
        ?person foaf:age ?age .
        FILTER(?age > 25)
    }
    ORDER BY DESC(?age)
    LIMIT 10
"#;

let results = engine.query(query)?;
match results {
    SparqlResults::Bindings(bindings) => {
        for solution in bindings {
            println!("{:?}", solution);
        }
    }
    _ => {}
}

// CONSTRUCT query
let query = r#"
    PREFIX foaf: <http://xmlns.com/foaf/0.1/>
    CONSTRUCT {
        ?person foaf:knows ?friend .
    }
    WHERE {
        ?person foaf:friend ?friend .
    }
"#;

let results = engine.query(query)?;
match results {
    SparqlResults::Graph(graph) => {
        println!("Constructed {} triples", graph.len());
    }
    _ => {}
}
```

### Property Graph ↔ RDF Mapping

```rust
use samyama::{GraphStore, rdf::{RdfStore, GraphToRdfMapper}};

let mut graph_store = GraphStore::new();
let mut rdf_store = RdfStore::new();

// Create mapper
let mapper = GraphToRdfMapper::new(
    "http://example.org/",  // Base IRI
);

// Map property graph to RDF
mapper.sync_to_rdf(&graph_store, &mut rdf_store)?;

// Now query with SPARQL
let engine = SparqlEngine::new(rdf_store);
let results = engine.query("SELECT * WHERE { ?s ?p ?o } LIMIT 10")?;
```

### SPARQL HTTP Endpoint

```bash
# Start server with SPARQL endpoint
cargo run --features sparql

# Query via HTTP
curl -X POST http://localhost:6379/sparql \
  -H "Content-Type: application/sparql-query" \
  -H "Accept: application/sparql-results+json" \
  -d 'SELECT * WHERE { ?s ?p ?o } LIMIT 10'

# UPDATE via HTTP
curl -X POST http://localhost:6379/sparql \
  -H "Content-Type: application/sparql-update" \
  -d 'INSERT DATA { <http://example.org/alice> <http://xmlns.com/foaf/0.1/name> "Alice" }'
```

## Performance Targets

### RDF Store Performance
- **Insert**: > 100K triples/second
- **Query**: < 10ms for simple triple patterns
- **BGP matching**: < 50ms for 2-3 triple patterns
- **Serialization**: > 50K triples/second (Turtle)

### SPARQL Performance
- **Simple SELECT**: < 20ms (p99)
- **Complex JOIN**: < 100ms (p99)
- **CONSTRUCT**: < 50ms for small result sets
- **UPDATE**: < 10ms per operation

## Testing Strategy

### Unit Tests
- RDF types and triple store (30+ tests)
- Serialization formats (20+ tests)
- Mapping logic (25+ tests)
- SPARQL parsing (20+ tests)
- Query execution (40+ tests)

### Integration Tests
- End-to-end SPARQL queries (15+ tests)
- HTTP endpoint (10+ tests)
- Property graph sync (10+ tests)

### Compliance Tests
- SPARQL 1.1 test suite (W3C)
- RDF parsing test suite

### Performance Tests
- Triple store benchmarks
- SPARQL query benchmarks
- Comparison with existing systems

## Documentation Deliverables

1. **User Guide**:
   - RDF concepts introduction
   - SPARQL tutorial
   - Property graph ↔ RDF mapping guide
   - Migration from Cypher to SPARQL

2. **API Documentation**:
   - RDF store API reference
   - SPARQL engine API reference
   - HTTP endpoint reference

3. **Examples**:
   - Loading RDF data from files
   - SPARQL query examples
   - Federated queries
   - Integration with existing property graph

4. **Architecture Documentation**:
   - RDF/SPARQL module design
   - Mapping strategy
   - Performance characteristics

## Risk Mitigation

### Technical Risks

| Risk | Probability | Impact | Mitigation |
|------|-------------|--------|------------|
| Property graph ↔ RDF impedance mismatch | High | Medium | Document limitations; provide manual override |
| SPARQL performance slower than Cypher | Medium | Medium | Optimize triple store indices; cache frequently used patterns |
| Memory overhead of dual models | Medium | High | Make RDF optional; lazy materialization |
| W3C compliance gaps | Low | Medium | Extensive test suite; document deviations |

### Business Risks

| Risk | Probability | Impact | Mitigation |
|------|-------------|--------|------------|
| Low user demand for RDF/SPARQL | Medium | Low | Make completely optional; feature flag |
| Increased complexity | High | Medium | Strong module boundaries; clear documentation |
| Maintenance burden | Medium | Medium | Leverage existing libraries; minimize custom code |

## Success Criteria

### Phase 5 Complete
- [ ] RDF triple/quad store operational
- [ ] All 4 RDF serialization formats supported
- [ ] Property graph ↔ RDF mapping works bidirectionally
- [ ] SPARQL 1.1 SELECT, CONSTRUCT, ASK, DESCRIBE implemented
- [ ] SPARQL UPDATE operations working
- [ ] SPARQL HTTP endpoint operational
- [ ] Basic RDFS reasoning functional
- [ ] 100+ tests passing (>80% coverage)
- [ ] Documentation complete
- [ ] Performance targets met

### Production Ready
- [ ] SPARQL 1.1 test suite passing (>90%)
- [ ] Production deployment in test environment
- [ ] User acceptance testing complete
- [ ] Performance benchmarks published

## Timeline Summary

| Week | Duration | Tasks | Team Size |
|------|----------|-------|-----------|
| 1-2 | 10 days | RDF data model & store | 2 engineers |
| 3 | 5 days | RDF serialization | 1 engineer |
| 4-5 | 10 days | Property graph ↔ RDF mapping | 2 engineers |
| 6 | 5 days | RDFS reasoning | 1 engineer |
| 7-8 | 10 days | SPARQL parser & algebra | 2 engineers |
| 9-10 | 10 days | SPARQL query execution | 2 engineers |
| 11 | 5 days | SPARQL results & HTTP endpoint | 1 engineer |
| 12 | 5 days | Integration, testing, docs | 2 engineers |
| **Total** | **60 days** | **12 weeks** | **Peak: 2 engineers** |

**Estimated Cost**: $200K - $250K (2 engineers × 12 weeks)

## Alternative Approaches

### Option 1: Full Oxigraph Integration
- **Approach**: Use Oxigraph as-is for RDF/SPARQL
- **Pros**: Fully spec-compliant, battle-tested
- **Cons**: Separate database; no property graph integration
- **Timeline**: 2-3 weeks

### Option 2: Adapter Pattern
- **Approach**: Build SPARQL → Cypher translator
- **Pros**: Minimal code; leverages existing engine
- **Cons**: Limited SPARQL coverage; translation complexity
- **Timeline**: 4-6 weeks

### Option 3: Proposed Hybrid (RECOMMENDED)
- **Approach**: Custom integration with existing libraries
- **Pros**: Full control; tight integration; standards-compliant
- **Cons**: More work than Option 2
- **Timeline**: 12 weeks (this plan)

## Recommendation

**Proceed with Hybrid Approach (Option 3)** for the following reasons:

1. **Standards Compliance**: Using `oxrdf` and `spargebra` ensures W3C compliance
2. **Time to Market**: 12 weeks vs 12 months from-scratch implementation
3. **Integration**: Tight integration with property graph model
4. **Flexibility**: Full control over mapping and optimization
5. **Maintainability**: Leverages community-maintained libraries

## Next Steps

1. **Immediate**: Add dependencies to Cargo.toml
2. **Week 1**: Create module structure and implement RDF types
3. **Week 2**: Implement RDF triple store
4. **Week 3**: Add RDF serialization formats
5. **Go/No-Go Decision (Week 6)**: Evaluate progress and decide if to continue

---

**Document Version**: 1.0
**Created**: 2025-11-10
**Status**: Implementation Plan
**Timeline**: 12 weeks
**Budget**: $200K-$250K
