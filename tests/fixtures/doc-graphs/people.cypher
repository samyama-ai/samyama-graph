// The README's social examples (shortest path, PageRank): three people in a chain.
// Used by doc_check (DX-04); a block opts in with ```cypher graph=people.
CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'})-[:KNOWS]->(c:Person {name: 'Carol'})
