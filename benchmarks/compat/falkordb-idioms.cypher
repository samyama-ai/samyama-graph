// Cypher idioms, as a user arriving from FalkorDB or RedisGraph writes them.
//
// Written from the outside, as `neo4j-idioms.cypher` is: shapes from
// FalkorDB's documentation — its index, full-text, vector and `algo.*`
// procedures, its functions, and RedisGraph's legacy index syntax — not a list
// of what this engine supports.
//
// Every entry was **run on FalkorDB** before it was added (graph module
// 4.22.1, `falkordb/falkordb:latest`, 2026-10-17) and FalkorDB accepted it.
// Three shapes a Neo4j user would expect were tried and left out because
// FalkorDB itself refuses them: `shortestPath` inside MATCH (FalkorDB takes it
// only in WITH or RETURN), `EXISTS { … }` subqueries, and the `=~` operator.
// A query its own source engine rejects says nothing about this one.
//
// Read by `tests/falkordb_compatibility_corpus.rs` and by
// `cargo run --release --example compatibility_report -- --queries
// benchmarks/compat/falkordb-idioms.cypher`. Queries are separated by a `;` at
// the end of a line. Protocol-level differences (graph names, `--compact`,
// the `CYPHER` parameter prefix, `GRAPH.*` commands) are not Cypher and are
// covered in `docs/MIGRATING-FROM-FALKORDB.md` instead.

// --- indexes ---------------------------------------------------------------
CREATE INDEX FOR (p:Person) ON (p.age);
CREATE INDEX ON :Person(name);
CREATE INDEX FOR ()-[k:KNOWS]-() ON (k.since);
DROP INDEX ON :Person(name);

// --- full-text -------------------------------------------------------------
CALL db.idx.fulltext.createNodeIndex('Movie', 'title');
CALL db.idx.fulltext.queryNodes('Movie', 'Matrix') YIELD node, score RETURN node.title, score;
CALL db.idx.fulltext.drop('Movie');

// --- vectors ---------------------------------------------------------------
CREATE VECTOR INDEX FOR (p:Person) ON (p.embedding) OPTIONS {dimension: 3, similarityFunction: 'euclidean'};
MATCH (p:Person {name: 'Alice'}) SET p.embedding = vecf32([0.1, 0.2, 0.3]);
CALL db.idx.vector.queryNodes('Person', 'embedding', 2, vecf32([0.1, 0.2, 0.3])) YIELD node, score RETURN node.name, score;
RETURN vec.euclideanDistance(vecf32([1.0, 2.0, 3.0]), vecf32([1.0, 2.0, 4.0]));
RETURN vec.cosineDistance(vecf32([1.0, 2.0, 3.0]), vecf32([1.0, 2.0, 4.0]));

// --- introspection ---------------------------------------------------------
CALL db.labels() YIELD label RETURN label;
CALL db.relationshipTypes() YIELD relationshipType RETURN relationshipType;
CALL db.propertyKeys() YIELD propertyKey RETURN propertyKey;
CALL db.indexes();
CALL dbms.procedures() YIELD name RETURN name;

// --- algorithms ------------------------------------------------------------
CALL algo.pageRank('Person', 'KNOWS') YIELD node, score RETURN node.name, score;
MATCH (a:Person {name: 'Alice'}) CALL algo.BFS(a, 3, 'KNOWS') YIELD nodes, edges RETURN size(nodes);
MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) CALL algo.SPpaths({sourceNode: a, targetNode: b, relTypes: ['KNOWS'], pathCount: 1}) YIELD path RETURN length(path);
CALL algo.WCC({nodeLabels: ['Person']}) YIELD node, componentId RETURN componentId, count(node);
CALL algo.betweenness({nodeLabels: ['Person'], relationshipTypes: ['KNOWS']}) YIELD node, score RETURN node.name, score;

// --- paths -----------------------------------------------------------------
MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) RETURN shortestPath((a)-[:KNOWS*]->(b));
MATCH p = (a:Person)-[:KNOWS*1..3]->(b) RETURN nodes(p), relationships(p), length(p);

// --- functions -------------------------------------------------------------
MATCH (a:Person) RETURN a.name, indegree(a), outdegree(a);
MATCH (a:Person {name: 'Bob'}) RETURN toJSON(a {.name, .age});
RETURN typeOf(1);
MATCH (a:Person) RETURN hasLabels(a, ['Person']);
RETURN randomUUID();
RETURN timestamp();
RETURN intern('x');
RETURN string.join(['a', 'b'], '-');
RETURN point({latitude: 32.07, longitude: 34.78});
RETURN distance(point({latitude: 32.07, longitude: 34.78}), point({latitude: 30.65, longitude: 34.78}));
RETURN date('2024-01-01');
RETURN duration({days: 3});

// --- clauses ---------------------------------------------------------------
MERGE (p:Person {name: 'Carol'}) ON CREATE SET p.created = timestamp() ON MATCH SET p.seen = timestamp();
UNWIND [1, 2, 3] AS x CREATE (:Num {v: x});
MATCH (a:Person) CALL { WITH a MATCH (a)-[:KNOWS]->(f) RETURN count(f) AS friends } RETURN a.name, friends;
FOREACH (x IN [1, 2] | CREATE (:Tmp {v: x}));
LOAD CSV WITH HEADERS FROM 'file://people.csv' AS row MERGE (:Person {name: row.name});
MATCH (a:Person) WITH a ORDER BY a.age DESC LIMIT 1 RETURN a.name;
MATCH (a:Person) RETURN a.name AS name UNION MATCH (m:Movie) RETURN m.title AS name;
OPTIONAL MATCH (a:Person)-[:DIRECTED]->(m) RETURN a.name, m;

// --- expressions and predicates --------------------------------------------
MATCH (a:Person) RETURN [(a)-[:KNOWS]->(f) | f.name] AS friends;
MATCH (a:Person) RETURN a {.name, .age} AS m;
RETURN [x IN range(1, 5) WHERE x % 2 = 0 | x * x];
RETURN reduce(s = 0, x IN [1, 2, 3] | s + x);
MATCH (a:Person) WHERE a.name STARTS WITH 'A' RETURN a.name;

// --- updates ---------------------------------------------------------------
MATCH (a:Person {name: 'Alice'}) SET a += {city: 'Paris'};
MATCH (a:Person {name: 'Alice'}) REMOVE a.city;
CREATE (:Person:Employee {name: 'Dan'});
MATCH (n:Tmp) DELETE n;
