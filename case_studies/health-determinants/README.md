# Health Determinants KG — Case Study

The upstream "why" behind health outcomes: air pollution, water access, poverty
and demographics, country by country, from World Bank WDI, WHO air quality, FAO
AQUASTAT and UNDP HDI. Joins to the surveillance and health-systems graphs by ISO
country code — the third panel of the public-health trifecta.

![Health determinants demo](demo.gif)

```bash
cd case_studies/health-determinants && ./run.sh        # validate every query
RECORD=1 ./run.sh                                       # also regenerate demo.gif
```

## The graph

**Scale:** 239,802 nodes · 239,795 edges (from a 5 MB snapshot)

| Node label | Count | Key properties |
|------------|-------|----------------|
| DemographicProfile | 107,088 | indicator_name, value, year |
| SocioeconomicIndicator | 52,367 | indicator_name, value, year |
| EnvironmentalFactor | 33,847 | indicator_name, value, year |
| WaterResource | 32,209 | indicator_name, value, year |
| NutritionIndicator | 14,073 | indicator_name, value, year |
| Country | 211 | iso_code, name |
| Region | 7 | name, who_code |

**Relationships:** `DEMOGRAPHIC_OF` (107K), `HAS_INDICATOR` (52K), `ENVIRONMENT_OF`
(34K), `WATER_RESOURCE_OF` (32K), `NUTRITION_STATUS` (14K), `IN_REGION`.

## Showcase queries

See [`queries.cypher`](queries.cypher): heaviest air-pollution burden → richest
socioeconomic profiles → lowest water-resource availability → most-profiled
demographics → world regions. Every query returns real rows (DoD-gated).

`Country.iso_code` is the **federation key** shared with the
[surveillance](../surveillance) and [health-systems](../health-systems) graphs.

## Question catalog

[`health-determinants.sgqueries`](./health-determinants.sgqueries) holds 58 questions
(17 easy, 28 medium, 13 hard; four unanswerable), built with `catalog-build --release`
against the pinned snapshot (#1154). [`questions.json`](questions.json) is its source,
and the NLQ accuracy benchmark
([competitor-benchmarks#260](https://git.samyama.ai/Samyama.ai/samyama-graph-competitor-benchmarks/issues/260))
reads it as its question set. Every entry is checked
weekly against the published snapshot by
[`kg-catalogs.yml`](../../.github/workflows/kg-catalogs.yml).

```bash
samyama queries list case_studies/health-determinants/health-determinants.sgqueries
samyama queries run  case_studies/health-determinants/health-determinants.sgqueries \
    --snapshot health-determinants.sgsnap --entry <id> [--param name=value]...
samyama verify health-determinants.sgsnap --queries case_studies/health-determinants/health-determinants.sgqueries
```

Every indicator node holds one value for one country and one year, so each question
pins the indicator (by its World Bank code in the Cypher, by its plain name in the
question) and the year. The graph's 211 `Country` nodes are World Bank economies and
include territories such as Bermuda and the Cayman Islands, which top some rankings;
there are no aggregate rows (no "World" or "Sub-Saharan Africa" country). Two
economies, Ethiopia and Venezuela, have the income level `Not classified`, and that
value appears as its own group in the income-group questions. Freshwater withdrawal is
a percentage of *internal* resources, so it can run far above 100% (Egypt: 7,750% in
2020). The questions avoid those already asked of the WHO data in the
[surveillance](../surveillance) and [health-systems](../health-systems) catalogs.

## Data & license

Sources: World Bank WDI (CC BY 4.0), WHO Ambient Air Quality, FAO AQUASTAT, UNDP
HDI. Snapshot `health-determinants.sgsnap` on release
[`kg-snapshots-v6`](https://github.com/samyama-ai/samyama-graph/releases/tag/kg-snapshots-v6)
(sha256 pinned in [`case.env`](case.env)).
