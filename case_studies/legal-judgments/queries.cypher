// @query Most productive judges | who authored the most judgments in 2016
MATCH (j:Judge)-[:DECIDED]->(c:Case)
RETURN j.name AS judge, count(DISTINCT c) AS cases
ORDER BY cases DESC
LIMIT 10;

// @query Most-cited legal sections | the provisions the Court leaned on most — IPC 302 (murder) leads
MATCH (c:Case)-[r:CITES]->(a:Act)
RETURN a.name AS act, r.section AS section, count(DISTINCT c) AS cases
ORDER BY cases DESC
LIMIT 10;

// @query Judges who most often sit together | the strongest 2016 bench pairings
MATCH (j1:Judge)-[:DECIDED]->(c:Case)<-[:DECIDED]-(j2:Judge)
WHERE j1.name < j2.name
RETURN j1.name AS judge_a, j2.name AS judge_b, count(DISTINCT c) AS cases_together
ORDER BY cases_together DESC
LIMIT 10;

// @query Laws cited together | which statutes co-occur in the same judgment (CrPC + IPC lead)
MATCH (a1:Act)<-[:CITES]-(c:Case)-[:CITES]->(a2:Act)
WHERE a1.name < a2.name
RETURN a1.name AS act_a, a2.name AS act_b, count(DISTINCT c) AS cited_together
ORDER BY cited_together DESC
LIMIT 10;

// @query Laws spanning the widest range of topics | the most foundational statutes (a two-hop traversal)
MATCH (a:Act)<-[:CITES]-(c:Case)-[:ABOUT]->(t:Topic)
RETURN a.name AS act, count(DISTINCT t.category) AS topic_breadth, count(DISTINCT c) AS cases
ORDER BY topic_breadth DESC, cases DESC
LIMIT 10;

// @query Docket by topic category | where the Court's 2016 work concentrated
MATCH (c:Case)-[:ABOUT]->(t:Topic)
RETURN t.category AS category, count(*) AS mentions
ORDER BY mentions DESC;
