# Cricket Knowledge Graph — Case Study

Ball-by-ball international cricket — 21,324 matches across Tests, ODIs, T20s, the
IPL and more — modelled as a graph of players, matches, venues, teams and
tournaments. Questions an analyst actually asks ("who owns whom?", "hardest
batsman to dismiss", "busiest grounds") become one-line traversals instead of
multi-table joins.

![Cricket demo](demo.gif)

```bash
cd case_studies/cricket && ./run.sh        # validate every query against the snapshot
RECORD=1 ./run.sh                          # also regenerate demo.gif
```

## The graph

**Scale:** 36,619 nodes · 1,392,017 edges (imported from a 21 MB snapshot in seconds)

| Node label | Count | Key properties |
|------------|-------|----------------|
| Match | 21,324 | date, season, match_type, gender, winner |
| Player | 12,933 | name, cricsheet_id |
| Tournament | 1,053 | name |
| Venue | 877 | name, city |
| Team | 383 | name |
| Season | 49 | year |

**Relationships (12):** `BATTED_IN` (401K), `DISMISSED` (308K), `BOWLED_IN`
(279K), `FIELDED_DISMISSAL` (216K), `COMPETED_IN` (43K), `PLAYED_FOR` (24K),
`HOSTED_AT`, `WON_TOSS`, `IN_SEASON`, `PART_OF`, `WON`, `PLAYER_OF_MATCH`.

The **dismissal network** — `(:Player)-[:DISMISSED]->(:Player)`, 308K edges — is
the star: a directed bowler→batsman graph that turns "rivalry" into a first-class
queryable structure.

## Showcase queries

See [`queries.cypher`](queries.cypher). The narrative: biggest dismissal
rivalries → most prolific wicket-takers → hardest batsmen to pin down (distinct
bowlers, a graph-native count) → Player-of-the-Match leaders → busiest venues →
largest tournaments. Every query returns real rows from the snapshot — the
[Definition of Done](../DEFINITION_OF_DONE.md) gate fails the build otherwise.

## Data & license

Source: [Cricsheet.org](https://cricsheet.org) ball-by-ball data, **CC BY 4.0**.
Built by the [`cricket-kg`](https://github.com/samyama-ai/cricket-kg) loader and
published as the `cricket.sgsnap` asset on release
[`kg-snapshots-v1`](https://github.com/samyama-ai/samyama-graph/releases/tag/kg-snapshots-v1)
(sha256 pinned in [`case.env`](case.env)).
