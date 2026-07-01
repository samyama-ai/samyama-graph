// IMDB Movies Knowledge Graph — showcase queries
// Schema: Movie{tconst,title,year,runtime_minutes,title_type}
//         Series{tconst,title,year,end_year}
//         Person{nconst,name,birth_year,death_year}
//         Genre{name}  Rating{average_rating,num_votes}
// Edges:  HAS_GENRE  HAS_RATING  ACTED_IN  DIRECTED  WROTE  PRODUCED
// All queries project properties so results are readable post-import.

// @query Top-rated movies | What audiences consistently rate highest — the all-time crowd favourites
MATCH (m:Movie)-[:HAS_RATING]->(r:Rating)
WHERE r.num_votes >= 50000
RETURN m.title AS title, m.year AS year, r.average_rating AS rating, r.num_votes AS votes
ORDER BY r.average_rating DESC
LIMIT 10;

// @query Most prolific directors | Who has directed the most highly-rated films
MATCH (p:Person)-[:DIRECTED]->(m:Movie)-[:HAS_RATING]->(r:Rating)
WHERE r.average_rating >= 7.0
RETURN p.name AS director, count(m) AS films, round(avg(r.average_rating) * 10) / 10.0 AS avg_rating
ORDER BY films DESC
LIMIT 10;

// @query Genre popularity | Which genres dominate by total audience votes
MATCH (m:Movie)-[:HAS_GENRE]->(g:Genre)
MATCH (m)-[:HAS_RATING]->(r:Rating)
RETURN g.name AS genre, count(m) AS movies, sum(r.num_votes) AS total_votes
ORDER BY total_votes DESC
LIMIT 10;

// @query Director–actor power pairs | The most repeated director–actor collaborations — a graph-native multi-hop count
MATCH (d:Person)-[:DIRECTED]->(m:Movie)<-[:ACTED_IN]-(a:Person)
RETURN d.name AS director, a.name AS actor, count(m) AS films_together
ORDER BY films_together DESC
LIMIT 10;

// @query Busiest actors by director breadth | Actors who have worked with the widest range of directors
MATCH (a:Person)-[:ACTED_IN]->(m:Movie)<-[:DIRECTED]-(d:Person)
RETURN a.name AS actor, count(DISTINCT d) AS distinct_directors, count(DISTINCT m) AS films
ORDER BY distinct_directors DESC
LIMIT 10;

// @query Decades of cinema | How movie volume and average quality shifted over time
MATCH (m:Movie)-[:HAS_RATING]->(r:Rating)
WHERE r.num_votes >= 1000
RETURN (toInteger(m.year) / 10) * 10 AS decade, count(m) AS movies, round(avg(r.average_rating) * 10) / 10.0 AS avg_rating
ORDER BY decade
LIMIT 10;

// @query Top-rated TV series | The series audiences kept watching and rating
MATCH (s:Series)-[:HAS_RATING]->(r:Rating)
WHERE r.num_votes >= 10000
RETURN s.title AS series, s.year AS start_year, r.average_rating AS rating, r.num_votes AS votes
ORDER BY r.average_rating DESC
LIMIT 10;

// @query Most written-for directors | Directors whose films attracted the most distinct writers — a proxy for project complexity
MATCH (w:Person)-[:WROTE]->(m:Movie)<-[:DIRECTED]-(d:Person)
RETURN d.name AS director, count(DISTINCT w) AS distinct_writers, count(DISTINCT m) AS films
ORDER BY distinct_writers DESC
LIMIT 10;
