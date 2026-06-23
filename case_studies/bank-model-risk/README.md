# Bank Model-Risk Knowledge Graph

**A synthetic bank's entire model-risk inventory as a graph — 520 nodes, ~2.4K edges.**
Models, the data and assumptions behind them, their validations and findings, the
regulations that govern them (SR 11-7, Basel, IFRS 9, ECB TRIM, EU AI Act, …), and the
regulatory submissions and decisions they drive.

Model risk is a graph problem: regulators demand that you trace a model's full lineage —
data → assumptions → validation → regulation → the submissions and decisions it drives —
and explain *why*. Tables and documents don't make that a first-class, auditable query.
A graph does.

> ⚠️ **Fully synthetic.** Generated from a fixed seed — no real institution, no PII.
> A demonstration of the engine and the schema, not a deployed bank system.
> Generator + loader: [github.com/samyama-ai/bank-model-risk-kg](https://github.com/samyama-ai/bank-model-risk-kg).

## Run it

```bash
cd case_studies/bank-model-risk && ./run.sh      # fetch snapshot → import → validate 8 queries
RECORD=1 ./run.sh                                 # also (re)generate demo.gif
```

## Schema

**12 node labels** — Model (80), Validation (136), ValidationFinding (173), Person (28),
Feature (24), RegulatoryRequirement (16), DataSource (15), Assumption (12), Control (12),
Decision (10), BusinessUnit (8), Submission (6)

**17 edge types** — OWNED_BY, DEVELOPED_BY, BELONGS_TO, MEMBER_OF, DEPENDS_ON,
USES_FEATURE, DERIVED_FROM, MAKES_ASSUMPTION, GOVERNED_BY, SATISFIES, CONTROLLED_BY,
EVIDENCES, VALIDATED_BY, PERFORMED_BY, RAISED, FEEDS, USED_IN

## Showcase queries (all in `queries.cypher`)

```cypher
-- Data-source blast radius: if the Core Banking Ledger changes, which submissions are exposed?
MATCH (ds:DataSource)<-[:DEPENDS_ON]-(m:Model)-[:FEEDS]->(s:Submission)
WHERE ds.name = "Core Banking Ledger"
RETURN s.name AS submission, count(DISTINCT m) AS models_affected
ORDER BY models_affected DESC
-- ICAAP 2026 (20), DFAST 2026 (13), Pillar 3 (13), CCAR 2026 (7), IFRS9 (5), AML (3)

-- Model explainability: features and source systems behind an AML model's decisions
MATCH (m:Model)-[:USES_FEATURE]->(f:Feature)-[:DERIVED_FROM]->(ds:DataSource)
WHERE m.category = "AML Transaction Monitoring"
RETURN m.name, collect(DISTINCT f.name) AS features, collect(DISTINCT ds.name) AS sources
```

Eight governance queries ship here — blast-radius, what-feeds-CCAR, open High findings on
Tier-1 production models, regulatory coverage, explainability, SR 11-7 audit readiness,
data-concentration risk, and owner accountability. Each is gated to return rows.

## License

Apache 2.0. All data is synthetic and generated; it represents no real institution.
