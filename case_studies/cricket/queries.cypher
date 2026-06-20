// Cricket Knowledge Graph — showcase queries
// Schema: Player{name,cricsheet_id} Match{date,season,match_type,gender,winner}
//         Tournament{name} Venue{name,city} Team{name} Season{year}
// Edges:  BATTED_IN BOWLED_IN DISMISSED FIELDED_DISMISSAL COMPETED_IN PLAYED_FOR
//         HOSTED_AT WON WON_TOSS IN_SEASON PART_OF PLAYER_OF_MATCH
// All queries are structure-based (counts/traversals) so they return real rows
// from the snapshot without depending on optional edge properties.

// @query Biggest dismissal rivalries | Which bowler has a batsman's number — the most one-sided match-ups in cricket history
MATCH (bowler:Player)-[:DISMISSED]->(batsman:Player)
RETURN bowler.name AS bowler, batsman.name AS batsman, count(*) AS dismissals
ORDER BY dismissals DESC
LIMIT 5;

// @query Most prolific wicket-takers | Career dismissals across every format in the dataset
MATCH (bowler:Player)-[:DISMISSED]->(:Player)
RETURN bowler.name AS bowler, count(*) AS wickets
ORDER BY wickets DESC
LIMIT 5;

// @query Hardest batsmen to pin down | Batsmen dismissed by the widest variety of bowlers — a proxy for longevity and reach
MATCH (bowler:Player)-[:DISMISSED]->(batsman:Player)
RETURN batsman.name AS batsman, count(DISTINCT bowler) AS distinct_bowlers, count(*) AS times_out
ORDER BY distinct_bowlers DESC
LIMIT 5;

// @query Player-of-the-Match leaders | Who the match officials rated best, most often
MATCH (p:Player)-[:PLAYER_OF_MATCH]->(m:Match)
RETURN p.name AS player, count(m) AS awards
ORDER BY awards DESC
LIMIT 5;

// @query Busiest venues | The grounds that have staged the most international cricket
MATCH (m:Match)-[:HOSTED_AT]->(v:Venue)
RETURN v.name AS venue, v.city AS city, count(m) AS matches
ORDER BY matches DESC
LIMIT 5;

// @query Largest tournaments by matches played | Where the volume of cricket concentrates
MATCH (m:Match)-[:PART_OF]->(t:Tournament)
RETURN t.name AS tournament, count(m) AS matches
ORDER BY matches DESC
LIMIT 5;
