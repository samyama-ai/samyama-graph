// Common Cypher idioms, as a user arriving from Neo4j writes them.
//
// Written from the outside: these are shapes that appear in Neo4j's own
// documentation, in the openCypher grammar, and in application code — not a
// list of what this engine supports. A corpus derived from our feature matrix
// would report 100% of whatever we already do, which is the failure this file
// exists to avoid.
//
// Read by `cargo run --release --example compatibility_report -- --queries
// benchmarks/compat/neo4j-idioms.cypher`. Queries are separated by a `;` at
// the end of a line.
//
// Add a query here when you meet an idiom in the wild, whether or not it
// works. A corpus that only grows when something starts passing measures the
// wrong thing.
//
// Every entry must be **valid Cypher**, though. A malformed query is refused
// for a reason that says nothing about this engine, and it inflates the
// refusal count with our own mistake: the first version of this file had
// `MATCH (p:Person) RETURN p UNION MATCH (c:Company) RETURN c`, which Neo4j
// rejects too, because UNION requires matching column names.

// --- reading ---------------------------------------------------------------
MATCH (p:Person) RETURN p.name;
MATCH (p:Person) RETURN p.name AS name ORDER BY name SKIP 10 LIMIT 10;
MATCH (p:Person) WHERE p.age > 30 AND p.city = 'Delhi' RETURN p;
MATCH (p:Person) WHERE p.name STARTS WITH 'A' RETURN p;
MATCH (p:Person) WHERE p.name =~ '(?i)a.*' RETURN p;
MATCH (p:Person) WHERE p.email IS NOT NULL RETURN p;
MATCH (p:Person)-[:KNOWS]->(f:Person) RETURN p.name, f.name;
MATCH (p:Person)-[:KNOWS*1..3]->(f:Person) RETURN DISTINCT f.name;
MATCH (p:Person)-[r:RATED]->(m:Movie) WHERE r.stars >= 4 RETURN m.title, r.stars;
MATCH (a:Person)-[:KNOWS]-(b:Person) RETURN a.name, b.name;
OPTIONAL MATCH (p:Person)-[:OWNS]->(c:Car) RETURN p.name, c.model;
MATCH (p:Person) RETURN p { .name, .age };
MATCH (n) RETURN labels(n), keys(n);
MATCH ()-[r]->() RETURN type(r), count(*);

// --- aggregation -----------------------------------------------------------
MATCH (p:Person)-[:KNOWS]->(f) RETURN p.name, count(f) AS friends ORDER BY friends DESC;
MATCH (p:Person) RETURN avg(p.age), min(p.age), max(p.age), sum(p.age);
MATCH (p:Person) RETURN collect(p.name);
MATCH (p:Person) RETURN collect(DISTINCT p.city);
MATCH (p:Person) RETURN count(DISTINCT p.city);
MATCH (p:Person) RETURN percentileCont(p.age, 0.5);
MATCH (p:Person) WITH p.city AS city, count(*) AS n WHERE n > 5 RETURN city, n;

// --- writing ---------------------------------------------------------------
CREATE (p:Person {name: 'Alice', age: 30});
CREATE (a:Person {name: 'A'})-[:KNOWS {since: 2019}]->(b:Person {name: 'B'});
MERGE (p:Person {name: 'Carol'}) ON CREATE SET p.created = timestamp() ON MATCH SET p.seen = true;
MATCH (p:Person {name: 'Alice'}) SET p.age = 31;
MATCH (p:Person {name: 'Alice'}) SET p += {city: 'Pune', active: true};
MATCH (p:Person {name: 'Alice'}) REMOVE p.age;
MATCH (p:Person {name: 'Alice'}) SET p:Employee;
MATCH (p:Person {name: 'Alice'}) DETACH DELETE p;
UNWIND [1, 2, 3] AS x CREATE (:N {v: x});
UNWIND [{name: 'A'}, {name: 'B'}] AS row MERGE (:Person {name: row.name});
FOREACH (x IN [1, 2, 3] | CREATE (:N {v: x}));

// --- structure -------------------------------------------------------------
MATCH (p:Person) WITH p WHERE p.age > 30 RETURN p.name;
MATCH (p:Person) RETURN p.name AS name UNION MATCH (c:Company) RETURN c.name AS name;
MATCH (p:Person) WHERE EXISTS { (p)-[:KNOWS]->() } RETURN p;
MATCH (p:Person) RETURN CASE WHEN p.age > 30 THEN 'senior' ELSE 'junior' END AS band;
MATCH (p:Person) RETURN [x IN range(1, 3) | x * 2];
MATCH (p:Person) RETURN [(p)-[:KNOWS]->(f) | f.name] AS friends;
MATCH p = (a:Person)-[:KNOWS*..3]->(b:Person) RETURN nodes(p), relationships(p);
MATCH p = shortestPath((a:Person)-[:KNOWS*..5]->(b:Person)) RETURN p;
CALL { MATCH (p:Person) RETURN p LIMIT 1 } RETURN p;
MATCH (p:Person) CALL { WITH p MATCH (p)-[:KNOWS]->(f) RETURN count(f) AS n } RETURN p.name, n;

// --- procedures and schema -------------------------------------------------
CREATE INDEX ON :Person(name);
CREATE CONSTRAINT FOR (p:Person) REQUIRE p.email IS UNIQUE;
CALL db.labels();
CALL db.schema.visualization();
CALL algo.pageRank() YIELD node, score RETURN node, score ORDER BY score DESC LIMIT 10;
CALL algo.pageRank() YIELD node, score WITH node, score WHERE score > 0.1 RETURN count(*);
CALL algo.shortestPath(1, 2) YIELD path RETURN path;

// --- things a Neo4j user will have written that we may not take -------------
MATCH (n) RETURN apoc.text.join(labels(n), ',');
CALL apoc.periodic.iterate('MATCH (n) RETURN n', 'SET n.seen = true', {batchSize: 1000});
MATCH (`my node`:Person) RETURN `my node`;
MATCH (:`Research Paper`) RETURN count(*);
MATCH (p:Person) RETURN p.name AS n ORDER BY n DESC LIMIT 5 UNION ALL MATCH (c:Company) RETURN c.name AS n;
MATCH (p:Person) USING INDEX p:Person(name) WHERE p.name = 'Alice' RETURN p;
LOAD CSV WITH HEADERS FROM 'file:///people.csv' AS row CREATE (:Person {name: row.name});
MATCH (p:Person) WHERE p.created > datetime('2024-01-01T00:00:00Z') RETURN p;
MATCH (p:Person) RETURN duration.between(p.created, datetime()) AS age;
MATCH (p:Person) RETURN point({latitude: 12.9, longitude: 77.6}) AS loc;
