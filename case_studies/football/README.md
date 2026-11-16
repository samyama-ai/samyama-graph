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

## Question catalog

[`football.sgqueries`](./football.sgqueries) holds 56 questions (15 easy, 26 medium,
15 hard; four unanswerable), built with `catalog-build --release` against the pinned
snapshot. [`questions.json`](questions.json) is its source, and the NLQ accuracy
benchmark
([competitor-benchmarks#260](https://git.samyama.ai/Samyama.ai/samyama-graph-competitor-benchmarks/issues/260))
reads it as its question set. Every entry is checked
weekly against the published snapshot by
[`kg-catalogs.yml`](../../.github/workflows/kg-catalogs.yml).

```bash
samyama queries list case_studies/football/football.sgqueries
samyama queries run  case_studies/football/football.sgqueries --snapshot football.sgsnap \
    --entry <id> [--param name=value]...
samyama verify football.sgsnap --queries case_studies/football/football.sgqueries
```

The graph holds men's and women's World Cups together (22 men's, 8 women's), and a
`Team` node stands for both of a nation's sides, so team totals combine them unless
a question filters on the tournament name. A goal is linked to its scorer and match
but not to a team, so team goal totals come from `home_score`/`away_score`. Player
names are `given_name + ' ' + family_name`, except the 474 players recorded by one
name (`Pelé`, `Marta`), whose `given_name` is `'not applicable'`; player samples are
names held by one player only. `Manager` nodes have no relationships, so "who managed
X" is one of the unanswerable questions.

**Data caveats the catalog surfaces.** `Match.result` records the winner of a
penalty shootout, so 43 matches are level on goals but not a `draw`
(`fb_penalty_shootouts`); the win questions say "shootouts included". Stages are
spelled two ways (`semi-final`/`semi-finals`, `quarter-final`/`quarter-finals`), and
the 1950 deciding matches are `final round`, not `final`. Four US stadiums are
recorded twice under different cities (Rose Bowl: Los Angeles and Pasadena; Giants,
Foxboro and Stanford Stadium likewise), so a per-`Stadium` count splits their
matches; `fb_shared_stadium_names` lists them (with the two genuinely different
Olympiastadions), so a corrected snapshot shows up as a changed answer.

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
