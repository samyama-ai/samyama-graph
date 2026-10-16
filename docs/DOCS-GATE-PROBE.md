# docs-gate probe

Throwaway. Verifies that `docs-gate` runs CH-DOC-EXEC on a PR that touches
only markdown, and that it catches #1860's defect: two statements in one
fenced block with no blank line between them, which `doc_check` splits on.

```cypher
CREATE VECTOR INDEX probe_idx FOR (p:Paper) ON (p.embedding) OPTIONS {dimensions: 384, similarity: 'cosine'}
CALL vector.search('Paper', 'embedding', [0.1, 0.2, 0.3], 10) YIELD node, score
```
