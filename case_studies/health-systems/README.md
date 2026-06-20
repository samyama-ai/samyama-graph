# Health Systems KG — Case Study

Emergency preparedness, country by country: WHO SPAR (IHR core-capacity) scores
that say how ready each nation is for a health emergency. The third panel of the
public-health trifecta — joins to [surveillance](../surveillance) (what's
happening) and [health-determinants](../health-determinants) (why) by ISO code.

![Health systems demo](demo.gif)

```bash
cd case_studies/health-systems && ./run.sh        # validate every query
RECORD=1 ./run.sh                                  # also regenerate demo.gif
```

## The graph

**Scale:** 8,663 nodes · 8,430 edges (from a 0.2 MB snapshot — runs in a blink)

| Node label | Count | Key properties |
|------------|-------|----------------|
| EmergencyResponse | 8,430 | indicator_name, score, year |
| Country | 233 | iso_code, name |

**Relationships:** `CAPACITY_FOR` (8,430) — each preparedness assessment links a
capacity score to the country it scores.

## Showcase queries

See [`queries.cypher`](queries.cypher): least-prepared countries (lowest average
SPAR score) → best-prepared → global preparedness trend over time → most
comprehensively assessed countries. Every query returns real rows (DoD-gated).

Cross-reference: a country that is **under-prepared here** *and* shows a **weak
outbreak signal in [surveillance](../surveillance)** *and* **poor determinants in
[health-determinants](../health-determinants)** is a triple-risk nation — one
join key (`Country.iso_code`) across three graphs.

## Data & license

Source: [WHO SPAR](https://extranet.who.int/e-spar) (IHR State Party
Self-Assessment) + WHO NHWA. Snapshot `health-systems.sgsnap` on release
[`kg-snapshots-v6`](https://github.com/samyama-ai/samyama-graph/releases/tag/kg-snapshots-v6)
(sha256 pinned in [`case.env`](case.env)).
