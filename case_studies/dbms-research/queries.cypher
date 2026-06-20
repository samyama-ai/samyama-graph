// DBMS Research KG — structure showcase queries (the vector-search finale lives
// in demo.py, which sources its query vector from the graph at runtime).
// Schema: Problem{title,statement,status,slug,embedding} Topic{name,embedding}
//         Paper{title,year} Person{name} Algorithm{name} Bound{direction,expr,kind} System{name}
// Edges:  IN_TOPIC AUTHORED_BY CITES APPEARED_AT STATE_OF_ART CLOSING_REQUIRES
//         HAS_BOUND IN_MODEL AFFILIATED_WITH ACTIVE_ON RESTS_ON RELATED_TO GENERALIZES

// @query Research topics by open-problem count | Where the open questions in databases cluster
MATCH (p:Problem)-[:IN_TOPIC]->(t:Topic)
RETURN t.name AS topic, count(p) AS open_problems
ORDER BY open_problems DESC
LIMIT 8;

// @query Most-cited papers in the corpus | Citation impact within the research graph
MATCH (p:Paper)<-[:CITES]-(citing:Paper)
RETURN p.title AS paper, p.year AS year, count(citing) AS citations
ORDER BY citations DESC
LIMIT 5;

// @query Most prolific authors | Researchers attached to the most papers in the corpus
MATCH (paper:Paper)-[:AUTHORED_BY]->(person:Person)
RETURN person.name AS author, count(paper) AS papers
ORDER BY papers DESC
LIMIT 5;

// @query Best-characterized problems | Open problems with the most formal upper/lower bounds recorded
MATCH (p:Problem)-[:HAS_BOUND]->(b:Bound)
RETURN p.title AS problem, count(b) AS bounds
ORDER BY bounds DESC
LIMIT 5;
