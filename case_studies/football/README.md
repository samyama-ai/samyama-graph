# Football Knowledge Graph — Case Study

90 years of World Cup history — tournaments, teams, players, goals, stadiums and
managers modelled as a property graph. Questions any football fan would ask
("who scored the most World Cup goals?", "which stadium hosted the most matches?",
"which players competed in 4+ tournaments?") become single Cypher traversals
instead of multi-table SQL joins.

![Football World Cup demo](demo.gif)

```bash
cd case_studies/football && ./run.sh        # validate every query against the snapshot
RECORD=1 ./run.sh                           # also regenerate demo.gif
```

## The graph

**Scale:** 16,150 nodes · 12,384 edges (imported from a 0.4 MB snapshot in seconds)

| Node label | Count | Key properties |
|------------|-------|----------------|
| Player | 10,401 | player_id, family_name, given_name, position, count_tournaments |
| Manager | 475 | manager_id, family_name, given_name, country |
| Goal | 3,637 | goal_id, minute, own_goal, penalty, period |
| Stadium | 240 | stadium_id, name, city, country, capacity |
| Team | 88 | team_id, name, code, confederation, region |
| Match | 1,248 | match_id, name, date, stage, home_score, away_score, result |
| Country | 31 | name |
| Tournament | 30 | tournament_id, name, year, host_country, winner, count_teams |

**Relationships (8):**

| Relationship | Pattern | Count |
|---|---|---|
| `SCORED_IN` | Goal → Match | 3,637 |
| `SCORED_BY` | Goal → Player | 3,637 |
| `IN_TOURNAMENT` | Match → Tournament | 1,248 |
| `HOME_TEAM` | Match → Team | 1,248 |
| `AWAY_TEAM` | Match → Team | 1,248 |
| `PLAYED_AT` | Match → Stadium | 1,248 |
| `HOSTED_BY` | Tournament → Country | 30 |
| `FROM` | Team → Country | 88 |

## Showcase queries

See [`queries.cypher`](queries.cypher). The narrative: all-time tournament winners →
top goal scorers → highest-scoring matches → prolific hosts → confederation strength →
busiest stadiums → multi-tournament veterans → goal patterns by stage.

## Data & license

Source: [DataHub World Cup Datasets](https://datahub.io/football/worldcup)
— `tournaments.csv`, `matches.csv`, `teams.csv`, `players.csv`, `goals.csv`,
`stadiums.csv`, `managers.csv`.

**License:** Open Data Commons Public Domain Dedication and License (PDDL).
Data is freely available for any use.

Snapshot built by [`examples/football_loader.rs`](../../examples/football_loader.rs)
and published as `football.sgsnap` on release
[`kg-snapshots-v8`](https://github.com/samyama-ai/samyama-graph/releases/tag/kg-snapshots-v8)
(sha256 pinned in [`case.env`](case.env)).
