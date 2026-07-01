// @query Top World Cup winners | nations with most tournament victories
MATCH (t:Tournament)
WHERE t.winner IS NOT NULL AND t.winner <> ''
RETURN t.winner AS team, count(t) AS titles
ORDER BY titles DESC
LIMIT 10;

// @query Top goal scorers of all time | players who scored the most World Cup goals
MATCH (g:Goal)-[:SCORED_BY]->(p:Player)
WHERE g.own_goal = false
RETURN CASE WHEN p.given_name IS NULL OR p.given_name = '' OR p.given_name = 'not applicable'
            THEN p.family_name ELSE p.given_name + ' ' + p.family_name END AS player,
       count(g) AS goals
ORDER BY goals DESC
LIMIT 10;

// @query Highest scoring matches | most goals in a single match
MATCH (m:Match)
WHERE m.home_score IS NOT NULL AND m.away_score IS NOT NULL
RETURN m.name AS match, m.date AS date, m.home_score + m.away_score AS total_goals,
       m.home_score AS home, m.away_score AS away, m.stage AS stage
ORDER BY m.home_score + m.away_score DESC
LIMIT 10;

// @query Most prolific host nations | tournaments and whether host country won
MATCH (t:Tournament)-[:HOSTED_BY]->(c:Country)
RETURN c.name AS host, count(t) AS tournaments_hosted,
       collect(t.year) AS years
ORDER BY tournaments_hosted DESC
LIMIT 10;

// @query Most appearances by confederation | regional football power
MATCH (team:Team)
RETURN team.confederation AS confederation, count(team) AS teams
ORDER BY teams DESC
LIMIT 10;

// @query Busiest stadiums | venues that hosted most World Cup matches
MATCH (m:Match)-[:PLAYED_AT]->(s:Stadium)
RETURN s.name AS stadium, s.city AS city, s.country AS country,
       count(m) AS matches_hosted, s.capacity AS capacity
ORDER BY matches_hosted DESC
LIMIT 10;

// @query Multi-tournament players | veterans who played in most World Cups
MATCH (p:Player)
WHERE p.count_tournaments >= 3
RETURN CASE WHEN p.given_name IS NULL OR p.given_name = '' OR p.given_name = 'not applicable'
            THEN p.family_name ELSE p.given_name + ' ' + p.family_name END AS player,
       p.position AS position, p.count_tournaments AS tournaments
ORDER BY p.count_tournaments DESC
LIMIT 10;

// @query Goals by match stage | where in the tournament do most goals happen
MATCH (g:Goal)-[:SCORED_IN]->(m:Match)
WHERE g.own_goal = false
RETURN m.stage AS stage, count(g) AS goals
ORDER BY goals DESC
LIMIT 10;
