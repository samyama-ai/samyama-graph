# Drug Interactions & Pharmacogenomics KG — Case Study

Drugs, their gene targets, side effects, indications and bioactivity, integrated
from five open pharmacology sources. The graph turns clinical-decision-support
questions — "what's the side-effect burden of this drug?", "which two drugs
compete at the same target?" — into single traversals.

![Drug interactions demo](demo.gif)

```bash
cd case_studies/drug-interactions && ./run.sh        # validate every query
RECORD=1 ./run.sh                                    # also regenerate demo.gif
```

## The graph

**Scale:** 244,783 nodes · 387,577 edges (from an 8 MB snapshot)

| Node label | Count | Key properties |
|------------|-------|----------------|
| Bioactivity | 208,025 | target_name, pchembl_value, standard_type |
| Drug | 19,842 | name, drugbank_id, cas_number |
| Gene | 6,449 | gene_name |
| SideEffect | 5,858 | name, meddra_id |
| Indication | 2,844 | name, meddra_id |
| AdverseEvent | 1,765 | term |

**Relationships:** `BIOACTIVITY_TARGET` (180K), `HAS_SIDE_EFFECT` (139K),
`INTERACTS_WITH_GENE` (35K), `HAS_ADVERSE_EVENT` (18K), `HAS_INDICATION` (15K).

## Showcase queries

See [`queries.cypher`](queries.cypher): side-effect burden → busiest drug-target
genes (CYP enzymes surface naturally) → **polypharmacy risk** (drug pairs sharing
the most gene targets — a two-hop graph join that's the backbone of interaction
checking) → most widespread side effects → most-indicated drugs → adverse-event
volume. Every query returns real rows (DoD-gated).

## Data & license

Sources: DrugBank, DGIdb, SIDER, ChEMBL, OpenFDA (see the
[`druginteractions-kg`](https://github.com/samyama-ai/druginteractions-kg)
loader for per-source licensing). Snapshot `druginteractions.sgsnap` on release
[`kg-snapshots-v5`](https://github.com/samyama-ai/samyama-graph/releases/tag/kg-snapshots-v5)
(sha256 pinned in [`case.env`](case.env)).
