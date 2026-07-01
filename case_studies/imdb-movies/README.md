# IMDB Movies Knowledge Graph — Case Study

Movies, TV series, directors, actors, writers and genres from the IMDB
non-commercial dataset — modelled as a graph. Questions that feel natural to a
film buff ("who collaborates with whom?", "which director keeps picking the same
actors?", "how did ratings shift decade to decade?") become single Cypher
traversals instead of multi-table SQL joins.

![IMDB Movies demo](demo.gif)

```bash
cd case_studies/imdb-movies && ./run.sh        # validate every query against the snapshot
RECORD=1 ./run.sh                              # also regenerate demo.gif
```

## The graph

**Scale:** 1,940,360 nodes · 2,634,125 edges (imported from a 49.8 MB snapshot in seconds)

| Node label | Count | Key properties |
|------------|-------|----------------|
| Movie | 49,632 | tconst, title, year, runtime_minutes, title_type |
| Person | 293,550 | nconst, name, birth_year, death_year |
| AlternateTitle | 1,517,802 | title, region, language |
| Series | 14,858 | tconst, title, year, end_year |
| Rating | 64,490 | average_rating, num_votes |
| Genre | 28 | name |

**Relationships (7):**

| Relationship | Pattern | Count |
|---|---|---|
| `HAS_ALTERNATE_TITLE` | Movie/Series → AlternateTitle | ~1.5M |
| `ACTED_IN` | Person → Movie/Series | ~530K |
| `DIRECTED` | Person → Movie/Series | ~80K |
| `WROTE` | Person → Movie/Series | ~90K |
| `PRODUCED` | Person → Movie/Series | ~270K |
| `HAS_RATING` | Movie/Series → Rating | ~64K |
| `HAS_GENRE` | Movie → Genre | ~120K |

The **director–actor collaboration network** — `(:Person)-[:DIRECTED]->(:Movie)<-[:ACTED_IN]-(:Person)` — is the star: a bipartite graph that turns "power pairs" into a first-class queryable structure.

## Showcase queries

See [`queries.cypher`](queries.cypher). The narrative: crowd-favourite films →
most prolific directors → genre dominance by audience size → director–actor power
pairs (the multi-hop graph-native question) → actors with widest director reach →
cinema by decade → top TV series → writers per director.

## Data & license

Source: [IMDB Non-Commercial Datasets](https://developer.imdb.com/non-commercial-datasets/)
— `title.basics.tsv.gz`, `title.ratings.tsv.gz`, `name.basics.tsv.gz`, `title.principals.tsv.gz`, `title.akas.tsv.gz`.


**License:** IMDB Non-Commercial Use Only. This data is provided for personal,
non-commercial use only. See [IMDB terms](https://developer.imdb.com/non-commercial-datasets/)
for full conditions.

Snapshot built by [`examples/imdb_loader.rs`](../../examples/imdb_loader.rs)
with `--min-votes 1000 --min-votes-series 500 --min-year 1950 --akas title.akas.tsv.gz` and published as
`imdb.sgsnap` on release
[`kg-snapshots-v8`](https://github.com/samyama-ai/samyama-graph/releases/tag/kg-snapshots-v8)
(sha256 pinned in [`case.env`](case.env)).
