# Disease Surveillance KG (WHO GHO) — Case Study

WHO Global Health Observatory outbreak reports and immunization coverage, country
by country. The graph answers public-health questions — disease burden, immunity
gaps, regional structure — and joins cleanly to the health-systems and
health-determinants graphs via ISO country code.

![Surveillance demo](demo.gif)

```bash
cd case_studies/surveillance && ./run.sh        # validate every query
RECORD=1 ./run.sh                               # also regenerate demo.gif
```

## The graph

**Scale:** 216,553 nodes · 241,084 edges (from a 6 MB snapshot)

| Node label | Count | Key properties |
|------------|-------|----------------|
| HealthIndicator | 163,950 | name, value, year, indicator_code |
| DiseaseReport | 42,136 | year, value |
| VaccineCoverage | 10,212 | antigen, coverage_pct, year |
| Country | 234 | name, iso_code |
| Disease | 15 | name, indicator_code |
| Region | 6 | name, who_code |

**Relationships:** `HAS_INDICATOR` (149K), `REPORT_OF` (42K), `REPORTED` (40K),
`HAS_COVERAGE` (9.7K), `IN_REGION`.

## Showcase queries

See [`queries.cypher`](queries.cypher): which diseases are under surveillance →
highest reported burden (summed case counts) → most-reporting countries → lowest
average immunization coverage (the immunity gaps) → WHO regional structure. Every
query returns real rows (DoD-gated).

The `Country.iso_code` property is the **federation key**: the same node identity
lets you join this graph to health-systems (workforce, preparedness) and
health-determinants (pollution, water) — cross-domain public-health analysis
without a single hand-written join key.

## Data & license

Source: [WHO Global Health Observatory](https://www.who.int/data/gho). Snapshot
`surveillance.sgsnap` on release
[`kg-snapshots-v4`](https://github.com/samyama-ai/samyama-graph/releases/tag/kg-snapshots-v4)
(sha256 pinned in [`case.env`](case.env)). Built by the
[`surveillance-kg`](https://github.com/samyama-ai/surveillance-kg) loader.
