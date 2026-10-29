# Knowledge graph catalog

Every public knowledge graph built for Samyama, in one place: what it holds, how big it is,
what licence its data carries, where to get it and a query to start with (KG-13, #1799).

Each KG lives in its own `samyama-ai/<name>` repository with its loader and source-data
notes. Before this page, a reader who did not already know a KG's name could not find it.

**How this page was made.** Every figure, licence and query below is taken from that KG's
own repository (its README, dataset card or query docs) as of **2026-10-28**, and the
source file is named where it matters. Nothing is rounded or filled in from elsewhere. Where
a repository's files disagree with each other, both figures are given. Every download link
was checked on that date: each HuggingFace dataset answered, and each `.sgsnap` link was
matched against the assets of the release it names.

When a KG's numbers change, its own repository is the source of truth; update the row here
from it.

## At a glance

| KG | What it holds | Scale (nodes / edges) | Data licence | Get the built graph |
|---|---|---|---|---|
| [ai-research-kg](#ai-research-kg) | 1,000 open problems in AI/ML | 29,707 / 62,842 | not decided yet | not published |
| [assetops-kg](#assetops-kg) | Industrial assets, sensors, failure modes, work orders | 12,647 / 12,662 | Apache 2.0 | build it |
| [bank-model-risk-kg](#bank-model-risk-kg) | Model governance, lineage and regulation (synthetic) | 520 / 2,391 | Apache 2.0 | HuggingFace |
| [banking-kg](#banking-kg) | A city's 2023 lending market plus one fictional bank's customers | 75,179 / 241,222 | public sources + synthetic; repo is proprietary | build it |
| [clinicaltrials-kg](#clinicaltrials-kg) | ClinicalTrials.gov with MeSH, RxNorm, FAERS, PubMed | 7.8M / 27M | mixed, per source | HuggingFace, snapshot |
| [cricket-kg](#cricket-kg) | Ball-by-ball cricket, 21,324 matches | 36,619 / 1,392,017 | CC BY 4.0 | HuggingFace, snapshot |
| [dbms-research-kg](#dbms-research-kg) | 1,053 open problems in database systems | 20,211 / 53,721 | CC BY 4.0 | snapshot |
| [druginteractions-kg](#druginteractions-kg) | Drugs, genes, side effects, indications | 245K / 388K | mixed, not permissive | snapshot |
| [edge-ai-kg](#edge-ai-kg) | Edge boards, accelerators, ONNX kernels, models | 25,162 / 77,743 | Apache 2.0 | HuggingFace, snapshot |
| [edtech-kg](#edtech-kg) | Course prerequisites and career pathways | 1,098 / 1,417 | mixed, per source | HuggingFace |
| [finance-kg](#finance-kg) | Companies, banks, legal entities, exchange rates | 69,437 / 149,387 | per source, open | build it |
| [football-kg](#football-kg) | World Cup tournaments, teams, players, goals | ~30K / ~59K | ODC PDDL | HuggingFace, snapshot |
| [health-determinants-kg](#health-determinants-kg) | World Bank, UNDP and WHO health determinants | 285,635 / not stated | mixed; part non-commercial | HuggingFace, snapshot |
| [health-systems-kg](#health-systems-kg) | WHO SPAR, NHWA, Gavi, Global Fund, IHME | not stated | mixed; part non-commercial | snapshot |
| [imdb-kg](#imdb-kg) | Movies, series, people, genres | 1.94M / 2.63M | IMDb non-commercial | snapshot |
| [legal-judgments-graph-kg](#legal-judgments-graph-kg) | 589 Indian Supreme Court judgments (2016) | 4,462 / 8,363 | CC BY 4.0 | HuggingFace, snapshot |
| [maths-research-kg](#maths-research-kg) | 1,161 open problems in mathematics | 34,073 / 78,162 | open question | build it |
| [market-structure-kg](#market-structure-kg) | A market maker's entities, venues, clearing and 1,764 ETFs | 45,227 / 71,640 | SEC public, GLEIF CC0 | build it |
| [marketing-attribution-kg](#marketing-attribution-kg) | Media measurement claims with lineage (synthetic) | 20,102 / 78,752 | Apache 2.0 | build it |
| [mental-health-kg](#mental-health-kg) | US behavioural-health facilities and clinicians | 113,710 / 1,665,153 | public domain | HuggingFace |
| [pathways-kg](#pathways-kg) | Reactome, STRING, GO, WikiPathways, UniProt | 119K / 835K | CC BY 4.0 | snapshot |
| [pubmed-kg](#pubmed-kg) | 37M PubMed articles, MeSH, citations | 66.2M / 1.04B | NLM terms, attribution | build it |
| [regulatory-affairs-kg](#regulatory-affairs-kg) | FDA device classifications and clearances | 28,496 / 25,310 | public domain | HuggingFace |
| [supplychain-logistics-kg](#supplychain-logistics-kg) | Ports, chokepoints, airports, suppliers | 23,336 / 133,289 | public domain or CC BY 4.0 | build it |
| [surveillance-kg](#surveillance-kg) | WHO GHO disease surveillance | not stated | CC BY-NC-SA 3.0 IGO | snapshot |
| [wildfire-evac-kg](#wildfire-evac-kg) | Road network of Paradise, California | 13,787 / 26,831 | ODbL 1.0 | HuggingFace |

**Reading the licence column.** It is the licence of the *data*, which is what governs
reusing the graph. The loader code in each repository is Apache 2.0 where the repository
says so. Several KGs mix sources with different terms; "non-commercial" and "share-alike"
there are conditions on you, not on the engine.

**Getting a graph.** *HuggingFace* means a versioned dataset under
`huggingface.co/datasets/VaidhyaMegha/`. *Snapshot* means a `.sgsnap` file on a
[samyama-graph release](https://github.com/samyama-ai/samyama-graph/releases), imported in
one call:

```bash
curl -X POST localhost:8080/api/tenants/default/snapshot/import -F "file=@cricket.sgsnap"
```

*Build it* means running the repository's own ETL against a local engine.

## The graphs

### ai-research-kg

[samyama-ai/ai-research-kg](https://github.com/samyama-ai/ai-research-kg). 1,000 open problems
in AI/ML with their papers, people, labs, assumptions and gaps.

- **Scale:** 29,707 nodes, 62,842 edges (README.md, DATASET_CARD.md).
- **Schema:** 11 labels (Problem, Topic, Paper, Person, Venue, Institution, Assumption,
  OpenQuestion, Obstruction, FutureDirection, DataSource); 11 edge types, including IN_TOPIC,
  CITES, AUTHORED_BY and BLOCKED_BY.
- **Data licence:** not decided. The source states none; CC BY 4.0 is proposed.
- **Get it:** not published until the licence is decided. Build it with the repository's ETL.

```cypher
MATCH (t:Topic)<-[:IN_TOPIC]-(p:Problem)-[:CITES]->(pa:Paper) RETURN pa.title AS paper, count(DISTINCT t.id) AS topics ORDER BY topics DESC LIMIT 5
```

### assetops-kg

[samyama-ai/assetops-kg](https://github.com/samyama-ai/assetops-kg). Industrial asset
operations, extending IBM AssetOpsBench.

- **Scale:** 12,647 nodes, 12,662 edges.
- **Schema:** 9 labels (Equipment, Sensor, FailureMode, WorkOrder, Location, Site, Event,
  AnomalyEvent, AlertEvent); 5 edge types (CONTAINS_LOCATION, CONTAINS_EQUIPMENT, HAS_SENSOR,
  FOR_EQUIPMENT, MONITORS).
- **Data licence:** Apache 2.0, as IBM AssetOpsBench is; carry IBM's copyright notice.
- **Get it:** build it. The repository's README notes that its old snapshot link
  (`kg-snapshots-v5/assetops.sgsnap`) returns 404.

The query below follows `DEPENDS_ON`, which the README describes as an added analytical
layer, not one of the 5 base edge types.

```cypher
MATCH (e:Equipment {name: 'Chiller-6'})<-[:DEPENDS_ON*1..3]-(downstream:Equipment) RETURN downstream.name, downstream.criticality_score ORDER BY downstream.criticality_score DESC
```

### bank-model-risk-kg

[samyama-ai/bank-model-risk-kg](https://github.com/samyama-ai/bank-model-risk-kg). Model
governance, lineage, regulation and explainability. All data is synthetic and represents no
real institution.

- **Scale:** 520 nodes, 2,391 edges (the headline says "~2.4K edges").
- **Schema:** 12 labels, including Model, Validation, ValidationFinding, Person, Feature,
  RegulatoryRequirement and DataSource; 17 edge types, including OWNED_BY, DEPENDS_ON,
  GOVERNED_BY and FEEDS.
- **Data licence:** Apache 2.0.
- **Get it:** [HuggingFace](https://huggingface.co/datasets/VaidhyaMegha/bank-model-risk-kg)
  (v1.0, CSVs and `bank-model-risk.sgsnap`).

```cypher
MATCH (ds:DataSource)<-[:DEPENDS_ON]-(m:Model)-[:FEEDS]->(s:Submission) WHERE ds.name = "Core Banking Ledger" RETURN s.name AS submission, count(DISTINCT m) AS models_affected ORDER BY models_affected DESC
```

### banking-kg

[samyama-ai/banking-kg](https://github.com/samyama-ai/banking-kg). A bank and its market in one
graph: Washington DC's 2023 lending market from four public regulatory filings, and one
fictional bank's 5,000 customers.

- **Scale:** 75,179 nodes, 241,222 relationships. The market layer is real and public (31,128 /
  83,640); the bank layer is synthetic (44,044 / 152,795), and every synthetic node carries
  `synthetic: true` (DATASET_CARD.md).
- **Schema:** 29 node types, including Card, Application, Account, Loan, Customer, Lender,
  SBALoan and Filing; 33 relationship types, including SPENDS_AT, FILED_WITH, ORIGINATED_BY and
  SOLD_TO.
- **Data licence:** the sources are US government public data (HMDA, FDIC BankFind, SBA FOIA)
  and GLEIF LEI data (CC0 1.0); the bank layer is synthetic, with no real person or
  institution. The repository's own LICENSE marks its code, schema and compiled graph
  proprietary to Samyama.ai.
- **Get it:** build it with the repository's ETL. No snapshot is published.

```cypher
MATCH (l:Lender {name: 'Citibank, National Association'})<-[:ORIGINATED_BY]-(n:Loan) WHERE NOT (n)-[:SOLD_TO]->() RETURN count(n)
```

### clinicaltrials-kg

[samyama-ai/clinicaltrials-kg](https://github.com/samyama-ai/clinicaltrials-kg). Clinical trials
from ClinicalTrials.gov, joined to MeSH, RxNorm, OpenFDA FAERS and PubMed.

- **Scale:** 7.8 million nodes, 27 million edges (headline). The published HuggingFace v1.0
  holds 7,628,735 nodes and 15,531,427 edges; it leaves out sources that cannot be
  redistributed (README.md).
- **Schema:** 11 labels (ClinicalTrial, Condition, Intervention, ArmGroup, Outcome, Sponsor,
  Site, AdverseEvent, MeSHDescriptor, Drug, Publication); 11 edge types.
- **Data licence:** mixed, per source. ClinicalTrials.gov is US public domain; MedDRA is a
  paid subscription and not redistributable.
- **Get it:** [HuggingFace](https://huggingface.co/datasets/VaidhyaMegha/clinicaltrials-kg)
  (v1.0, Parquet), or
  [`kg-snapshots-v5/clinical-trials.sgsnap`](https://github.com/samyama-ai/samyama-graph/releases/download/kg-snapshots-v5/clinical-trials.sgsnap)
  (746 MB).

`AdverseEvent` is one of the parts the HuggingFace dataset leaves out, so this query needs
the full build.

```cypher
MATCH (t:ClinicalTrial)-[:REPORTED]->(ae:AdverseEvent) RETURN ae.term, count(DISTINCT t) AS trials ORDER BY trials DESC LIMIT 5
```

### cricket-kg

[samyama-ai/cricket-kg](https://github.com/samyama-ai/cricket-kg). Every ball of 21,324 matches
from Cricsheet: Tests, ODIs, T20s, IPL, BBL and more.

- **Scale:** 36,619 nodes, 1,392,017 edges.
- **Schema:** 6 labels (Player, Match, Tournament, Venue, Team, Season); 12 edge types,
  including BATTED_IN, BOWLED_IN, DISMISSED and PLAYED_FOR.
- **Data licence:** CC BY 4.0. Using the data means crediting Cricsheet.
- **Get it:** [HuggingFace](https://huggingface.co/datasets/VaidhyaMegha/cricket-kg) (v1.0),
  or [`kg-snapshots-v1/cricket.sgsnap`](https://github.com/samyama-ai/samyama-graph/releases/download/kg-snapshots-v1/cricket.sgsnap)
  (22 MB).

```cypher
MATCH (p:Player)-[b:BATTED_IN]->(m:Match) RETURN p.name AS player, sum(b.runs) AS runs, sum(b.sixes) AS sixes ORDER BY runs DESC LIMIT 5
```

### dbms-research-kg

[samyama-ai/dbms-research-kg](https://github.com/samyama-ai/dbms-research-kg). 1,053 open
problems of database systems research, with papers, people, algorithms and gaps.

- **Scale:** 20,211 nodes, 53,721 edges.
- **Schema:** 16 labels, including Problem, Topic, Paper, Person, Algorithm, System and
  Bound; 17 edge types, including IN_TOPIC, CITES, GENERALIZES and STATE_OF_ART.
- **Data licence:** CC BY 4.0. The catalog calls itself a synthesized study aid, not a
  primary source.
- **Get it:** [`kg-snapshots-v10/dbms-research.sgsnap`](https://github.com/samyama-ai/samyama-graph/releases/download/kg-snapshots-v10/dbms-research.sgsnap)
  (13 MB). The repository's own dataset card says its snapshot is shared within the team
  only; this release carries one all the same.

```cypher
MATCH (p:Problem)-[:CITES]->(paper:Paper) WITH paper, count(p) AS problems RETURN paper.title AS paper, paper.year AS year, problems ORDER BY problems DESC, paper LIMIT 5
```

### druginteractions-kg

[samyama-ai/druginteractions-kg](https://github.com/samyama-ai/druginteractions-kg). Drug
interactions and pharmacogenomics.

- **Scale:** 245K nodes, 388K edges.
- **Schema:** 6 labels (Drug, Gene, SideEffect, Indication, Bioactivity, AdverseEvent); 5 edge
  types (INTERACTS_WITH_GENE, HAS_SIDE_EFFECT, HAS_INDICATION, HAS_ADVERSE_EVENT,
  BIOACTIVITY_TARGET).
- **Data licence:** mixed and not permissive. ChEMBL is CC BY-SA 3.0, SIDER is
  non-commercial, and DGIdb carries per-source terms. The dataset card advises shipping the
  loader, not the graph.
- **Get it:** [`kg-snapshots-v5/druginteractions.sgsnap`](https://github.com/samyama-ai/samyama-graph/releases/download/kg-snapshots-v5/druginteractions.sgsnap)
  (8 MB), or build it.

```cypher
MATCH (d:Drug)-[:HAS_SIDE_EFFECT]->(se:SideEffect) RETURN d.name, count(se) AS side_effects ORDER BY side_effects DESC LIMIT 5
```

### edge-ai-kg

[samyama-ai/edge-ai-kg](https://github.com/samyama-ai/edge-ai-kg). Edge AI deployment: boards,
accelerators, ONNX operator kernels and quantized models. Real ONNX, ONNX Runtime and MLPerf
Tiny data plus a generated fleet.

- **Scale:** 25,162 nodes, 77,743 edges (headline). The HuggingFace dataset holds 25,152 /
  76,306 and the release snapshot 25,145 / 76,291.
- **Schema:** 17 labels, including Kernel, Deployment, Operator, ModelVariant, Board and
  Accelerator; 23 edge types, including USES_OPERATOR, IMPLEMENTS and RUNS_ON.
- **Data licence:** Apache 2.0.
- **Get it:** [HuggingFace](https://huggingface.co/datasets/VaidhyaMegha/edge-ai-kg) (v1.0), or
  [`kg-snapshots-v9/edge-ai-kg.sgsnap`](https://github.com/samyama-ai/samyama-graph/releases/download/kg-snapshots-v9/edge-ai-kg.sgsnap)
  (1 MB).

```cypher
MATCH (m:Model)-[:USES_OPERATOR]->(op:Operator) WHERE m.name = "depthwise-cnn-neuro-043" OPTIONAL MATCH (k:Kernel)-[:IMPLEMENTS]->(op), (k)-[:RUNS_ON]->(a:Accelerator) WHERE a.kind = "NPU-Lite" WITH op.name AS operator, op.category AS category, op.since_version AS opset, count(k) AS kernels WHERE kernels = 0 RETURN operator, category, opset ORDER BY category
```

### edtech-kg

[samyama-ai/edtech-kg](https://github.com/samyama-ai/edtech-kg). Education-to-career pathways:
course prerequisites from a school district's catalogue.

- **Scale:** 1,098 nodes, 1,417 edges. The HuggingFace v1.0 holds 1,098 / 1,287.
- **Schema:** 4 labels loaded (Course, Requirement, Subject, Pathway) of 16 declared; edge
  types IN_SUBJECT, REQUIRES, INCLUDES, HAS_REQUIREMENT.
- **Data licence:** mixed, per source. US-government sources are public domain; the
  school-district catalogue's reuse terms are unsettled (DATASET-CARD.md).
- **Get it:** [HuggingFace](https://huggingface.co/datasets/VaidhyaMegha/edtech-kg) (v1.0).

```cypher
MATCH (blocked:Course)-[:REQUIRES*1..8]->(:Course {name: 'Algebra 1'}) MATCH (blocked)-[:IN_SUBJECT]->(s:Subject) RETURN s.name AS subject, count(DISTINCT blocked) AS closed_off ORDER BY closed_off DESC LIMIT 5
```

### finance-kg

[samyama-ai/finance-kg](https://github.com/samyama-ai/finance-kg). Real companies, banks, legal
entities and exchange rates from public sources, with generated banking activity kept apart.

- **Scale:** 69,437 nodes, 149,387 edges: a real layer of 56,043 / 83,041 and a generated
  layer of 13,394 / 66,346. Every node and edge carries its provenance.
- **Schema:** 20 labels, including Country, Currency, Company, Filing, Regulator and
  LegalEntity; 26 edge types.
- **Data licence:** per source, all open (SEC and FDIC public domain, GLEIF and Wikidata CC0,
  World Bank CC BY 4.0, ECB free reuse with credit).
- **Get it:** build it.

```cypher
MATCH (e:LegalEntity)-[:CONSOLIDATED_BY*1..6]->(p:LegalEntity) WHERE e.lei = "335800DJLEUSVXIZV875" MATCH (p)-[:LOCATED_IN]->(k:Country) WITH DISTINCT p.name AS parent, k.name AS country, p.lei AS lei RETURN parent, country, lei
```

### football-kg

[samyama-ai/football-kg](https://github.com/samyama-ai/football-kg). World Cup tournaments,
teams, players, goals and stadiums from the DataHub datasets.

- **Scale:** ~30K nodes, ~59K edges (headline). The HuggingFace v1.0 is a subset: 16,150 nodes,
  12,384 edges.
- **Schema:** 12 labels, including Player, Match, Goal, Booking, Substitution, Team and
  Stadium; the README states 19 edge types.
- **Data licence:** ODC Public Domain Dedication and License (PDDL).
- **Get it:** [HuggingFace](https://huggingface.co/datasets/VaidhyaMegha/football-kg) (v1.0), or
  [`kg-snapshots-v8/football.sgsnap`](https://github.com/samyama-ai/samyama-graph/releases/download/kg-snapshots-v8/football.sgsnap)
  (0.5 MB).

```cypher
MATCH (g:Goal)-[:SCORED_BY]->(p:Player) WHERE g.own_goal = false RETURN p.name AS player, count(g) AS goals ORDER BY goals DESC LIMIT 5
```

### health-determinants-kg

[samyama-ai/health-determinants-kg](https://github.com/samyama-ai/health-determinants-kg).
Health determinants from the World Bank, UNDP and WHO.

- **Scale:** 285,635 nodes in the full build (edges not stated). The published HuggingFace
  subset holds 239,802 nodes, 239,795 edges.
- **Schema:** 7 labels (Country, Region, SocioeconomicIndicator, DemographicProfile,
  EnvironmentalFactor, WaterResource, NutritionIndicator); 6 edge types.
- **Data licence:** mixed. World Bank data is CC BY 4.0; the WHO part (16% of the full build)
  is non-commercial. The HuggingFace subset is World Bank data only, CC BY 4.0.
- **Get it:** [HuggingFace](https://huggingface.co/datasets/VaidhyaMegha/health-determinants-kg)
  (the CC BY 4.0 subset), or
  [`kg-snapshots-v6/health-determinants.sgsnap`](https://github.com/samyama-ai/samyama-graph/releases/download/kg-snapshots-v6/health-determinants.sgsnap)
  (6 MB).

```cypher
MATCH (c:Country)-[:HAS_INDICATOR]->(i:SocioeconomicIndicator) WHERE i.year = 2023 RETURN c.name, count(i) AS indicators ORDER BY indicators DESC LIMIT 3
```

### health-systems-kg

[samyama-ai/health-systems-kg](https://github.com/samyama-ai/health-systems-kg). Health system
capacity from WHO SPAR and NHWA, Gavi, the Global Fund and IHME.

- **Scale:** not stated in the repository.
- **Schema:** Country and EmergencyResponse (`EmergencyResponse -[:CAPACITY_FOR]-> Country`),
  plus workforce, funding and vaccine-supply indicators.
- **Data licence:** WHO SPAR and NHWA are CC BY-NC-SA 3.0 IGO; IHME's terms may not permit
  redistribution. The dataset card advises shipping the loader, not the graph.
- **Get it:** [`kg-snapshots-v6/health-systems.sgsnap`](https://github.com/samyama-ai/samyama-graph/releases/download/kg-snapshots-v6/health-systems.sgsnap)
  (0.2 MB), or build it.

```cypher
MATCH (e:EmergencyResponse) WHERE e.year = 2021 AND e.score <= 100 RETURN e.capacity_name, avg(e.score) AS avg ORDER BY avg DESC LIMIT 5
```

### imdb-kg

[samyama-ai/imdb-kg](https://github.com/samyama-ai/imdb-kg). Movies, series, people, genres and
alternate titles from the IMDb non-commercial datasets.

- **Scale:** 1.94M nodes, 2.63M edges.
- **Schema:** 6 labels (Movie, Person, AlternateTitle, Series, Rating, Genre); 7 edge types
  (HAS_ALTERNATE_TITLE, ACTED_IN, DIRECTED, WROTE, PRODUCED, HAS_RATING, HAS_GENRE).
- **Data licence:** IMDb's non-commercial datasets are for personal, non-commercial use only.
- **Get it:** [`kg-snapshots-v8/imdb.sgsnap`](https://github.com/samyama-ai/samyama-graph/releases/download/kg-snapshots-v8/imdb.sgsnap)
  (52 MB), or build it.

```cypher
MATCH (d:Person)-[:DIRECTED]->(m:Movie)<-[:ACTED_IN]-(a:Person) RETURN d.name AS director, a.name AS actor, count(m) AS films_together ORDER BY films_together DESC LIMIT 5
```

### legal-judgments-graph-kg

[samyama-ai/legal-judgments-graph-kg](https://github.com/samyama-ai/legal-judgments-graph-kg).
589 Indian Supreme Court judgments from 2016: cases, judges, parties, cited acts and topics.

- **Scale:** 4,462 nodes, 8,363 edges.
- **Schema:** 5 labels (Topic, Party, Case, Act, Judge); 4 edge types (ABOUT, CITES,
  PARTY_IN, DECIDED).
- **Data licence:** CC BY 4.0, with attribution.
- **Get it:** [HuggingFace](https://huggingface.co/datasets/VaidhyaMegha/legal-judgments-kg)
  (v1.0; note the dataset name drops `-graph`), or
  [`kg-snapshots-v1/legal-judgments.sgsnap`](https://github.com/samyama-ai/samyama-graph/releases/download/kg-snapshots-v1/legal-judgments.sgsnap)
  (0.1 MB).

```cypher
MATCH (c:Case)-[r:CITES]->(a:Act) RETURN a.name AS act, r.section AS section, count(DISTINCT c) AS cases ORDER BY cases DESC LIMIT 5
```

### maths-research-kg

[samyama-ai/maths-research-kg](https://github.com/samyama-ai/maths-research-kg). 1,161 open and
hard problems in mathematics, what is proved toward each, and what blocks it.

- **Scale:** 34,073 nodes, 78,162 edges.
- **Schema:** 12 labels, including Problem, Topic, Result, Obstacle, Gap, Paper and Concept;
  14 edge types, including IN_TOPIC, HAS_RESULT, CITES and INVOLVES.
- **Data licence:** an open question. The upstream catalog states no licence.
- **Get it:** build it; the dataset card says the snapshot is not published.

```cypher
MATCH (p:Problem {status: 'open'})-[:INVOLVES]->(c:Concept), (p)-[:IN_TOPIC]->(t:Topic) RETURN c.name, count(DISTINCT t) AS topics, count(DISTINCT p) AS open_problems ORDER BY topics DESC, open_problems DESC LIMIT 5
```

### market-structure-kg

[samyama-ai/market-structure-kg](https://github.com/samyama-ai/market-structure-kg). One market
maker's legal entities, venues, regulators and clearing, the 1,764 ETFs it is authorised
participant for, and what the ten largest of them hold. Public data plus a synthetic layer.

- **Scale:** 45,227 nodes, 71,640 relationships: public 44,638 / 69,773, synthetic 589 / 1,867
  (DATASET_CARD.md).
- **Schema:** 21 labels (14 base and 7 sub-labels), including LegalEntity, Issuer,
  Counterparty, Instrument, ETF, Venue and Regulator; 28 relationship types, 15 public (such as
  SUBSIDIARY_OF, ISSUES, HOLDS, AUTHORIZED_PARTICIPANT_FOR, CLEARS_VIA) and 13 synthetic.
- **Data licence:** per source (DATA-LICENSES.md). SEC N-CEN and N-PORT data is public and
  redistributable with credit to the SEC; GLEIF data is CC0; only a 15-venue subset of the
  ISO MIC list is kept, which its terms allow; the synthetic layer is Apache 2.0 and marked
  `synthetic: true`.
- **Get it:** build it. The repository asks that a snapshot or CSV export not be sent out
  before a legal review, so none is published.

```cypher
MATCH (i:Issuer {name: 'NVIDIA Corp.'})-[:ISSUES]->(:Instrument)<-[h:HOLDS]-(e:ETF) <-[:AUTHORIZED_PARTICIPANT_FOR]-(:Counterparty {name: 'MARKET MAKER CAPITAL, LLC'}) RETURN e.name, round(h.weight * 10000) / 100 AS pct ORDER BY pct DESC LIMIT 5
```

### marketing-attribution-kg

[samyama-ai/marketing-attribution-kg](https://github.com/samyama-ai/marketing-attribution-kg).
Every measurement a brand takes about its own media, held as claims with lineage. Fully
synthetic.

- **Scale:** 20,102 nodes, 78,752 edges.
- **Schema:** 16 labels, including Claim, Market, Product, Run, Channel, Period and Method;
  16 edge types, including ABOUT, FOR, PRODUCED and EXECUTED_AS.
- **Data licence:** Apache 2.0; there is no third-party data in it.
- **Get it:** build it (`python data/generate.py`, then the loader). No snapshot is published yet.

```cypher
MATCH (a:Claim)-[:ABOUT]->(ch:Channel) WHERE ch.name = "Paid Social" AND a.granularity = "channel" AND a.metric = "contribution" WITH a, ch MATCH (a)-[:FOR]->(p:Period) WHERE p.id = "2026-Q1" WITH a, ch, p MATCH (ma:Method)-[:EXECUTED_AS]->(:Run)-[:PRODUCED]->(a) WHERE ma.kind = "mmm" WITH a, ch, p, ma MATCH (b:Claim)-[:ABOUT]->(ch) WHERE b.granularity = "channel" AND b.metric = "contribution" WITH a, ch, p, ma, b MATCH (b)-[:FOR]->(pb:Period) WHERE pb.id = "2026-Q1" WITH a, p, ma, b MATCH (mb:Method)-[:EXECUTED_AS]->(:Run)-[:PRODUCED]->(b) WHERE mb.kind = "geo" RETURN p.id, ma.kind, a.value, mb.kind, b.value, abs(a.value - b.value) AS gap, round(100.0 * abs(a.value - b.value) / a.value) AS gap_pct
```

### mental-health-kg

[samyama-ai/mental-health-kg](https://github.com/samyama-ai/mental-health-kg). US
behavioural-health facilities, licensed clinicians, shortage designations and a simulated
population.

- **Scale:** 113,710 nodes, 1,665,153 edges.
- **Schema:** 13 labels, including Provider, Facility, ShortageArea, Patient, County and
  Service; 13 edge types, including OFFERS, PRACTICES_IN and COVERS.
- **Data licence:** US federal government works, public domain; Synthea (the simulated
  patients) is Apache 2.0.
- **Get it:** [HuggingFace](https://huggingface.co/datasets/VaidhyaMegha/mental-health-kg)
  (v1.0, CSVs). No snapshot is shipped.

```cypher
MATCH (f:Facility)-[:OFFERS]->(a:Service), (f)-[:OFFERS]->(b:Service), (f)-[:OFFERS]->(c:Service) WHERE a.value = "Clients who have experienced intimate partner violence, domestic violence" AND b.value = "Sliding fee scale (fee is based on income and other factors)" AND c.value = "Spanish" RETURN f.name, f.city, f.intake
```

### pathways-kg

[samyama-ai/pathways-kg](https://github.com/samyama-ai/pathways-kg). Biological pathways from
Reactome, STRING, Gene Ontology, WikiPathways and UniProt (human).

- **Scale:** 119K nodes, 835K edges.
- **Schema:** 5 labels (Protein, GOTerm, Complex, Reaction, Pathway); 9 edge types, including
  ANNOTATED_WITH, INTERACTS_WITH and PARTICIPATES_IN.
- **Data licence:** CC BY 4.0 (DATA-LICENSES.md).
- **Get it:** [`kg-snapshots-v3/pathways.sgsnap`](https://github.com/samyama-ai/samyama-graph/releases/download/kg-snapshots-v3/pathways.sgsnap)
  (10 MB).

```cypher
MATCH (p:Protein)-[:INTERACTS_WITH]-(other:Protein) RETURN p.name AS protein, count(DISTINCT other) AS partners ORDER BY partners DESC LIMIT 5
```

### pubmed-kg

[samyama-ai/pubmed-kg](https://github.com/samyama-ai/pubmed-kg). The PubMed/MEDLINE baseline:
37M articles with their authors, MeSH terms, chemicals, journals, grants and citations.

- **Scale:** 66.2 million nodes, 1.04 billion edges.
- **Schema:** 6 labels (Article, Author, MeSHTerm, Chemical, Journal, Grant); 6 edge types
  (AUTHORED_BY, ANNOTATED_WITH, MENTIONS_CHEMICAL, PUBLISHED_IN, CITES, FUNDED_BY).
- **Data licence:** NLM terms: free to use and redistribute with attribution ("Courtesy of the
  U.S. National Library of Medicine"). Abstract text is often publisher-copyrighted
  (DATA-LICENSES.md).
- **Get it:** build it. The README links `kg-snapshots-v5/pubmed.sgsnap`, but that release
  holds no such file.

```cypher
MATCH (a:Article)-[:ANNOTATED_WITH]->(m1:MeSHTerm {name: 'Neoplasms'}) MATCH (a)-[:ANNOTATED_WITH]->(m2:MeSHTerm) WHERE m2.name <> 'Neoplasms' RETURN m2.name, count(DISTINCT a) AS articles ORDER BY articles DESC LIMIT 5
```

### regulatory-affairs-kg

[samyama-ai/regulatory-affairs-kg](https://github.com/samyama-ai/regulatory-affairs-kg). FDA
medical-device classifications and cardiovascular clearances, joined to the regulations that
govern them.

- **Scale:** 28,496 nodes, 25,310 edges (the loaded slice). The HuggingFace v1.0 holds 28,520 /
  25,332, because openFDA is live.
- **Schema:** an ontology of 34 labels and 39 edge types, of which Submission, ProductCode,
  Regulation, Manufacturer and six more are loadable today.
- **Data licence:** openFDA sources are US public domain.
- **Get it:** [HuggingFace](https://huggingface.co/datasets/VaidhyaMegha/regulatory-affairs-kg)
  (v1.0, CSVs and `regulatory-affairs.sgsnap`).

```cypher
MATCH (r:Regulation)<-[:GOVERNED_BY]-(:ProductCode)<-[:CLASSIFIED_AS]-(s:Submission) WHERE r.cfr_section = '870.5150' RETURN s.applicant AS applicant, count(s) AS clearances ORDER BY clearances DESC LIMIT 5
```

### supplychain-logistics-kg

[samyama-ai/supplychain-logistics-kg](https://github.com/samyama-ai/supplychain-logistics-kg).
Countries, ports, chokepoints, airports and a generated supplier network.

- **Scale:** 23,336 nodes, 133,289 edges: a real layer of 2,524 / 2,244 and a generated layer
  of 20,812 / 131,045.
- **Schema:** 14 labels, including Country, Port, Chokepoint, Airport, Product and Company;
  27 edge types, including LOCATED_IN, CONNECTS_TO, SUPPLIES and TRANSITS.
- **Data licence:** every source is public domain or CC BY 4.0; credit the World Bank.
- **Get it:** build it.

```cypher
MATCH (p:Port)-[:LOCATED_IN]->(k:Country) WHERE k.lpi_overall IS NOT NULL WITH k, count(p) AS ports MATCH (k)<-[:LOCATED_IN]-(a:Airport) WITH k.name AS country, k.lpi_overall AS lpi_overall, k.lpi_overall_year AS lpi_year, ports, count(a) AS large_airports RETURN country, lpi_overall, lpi_year, ports, large_airports ORDER BY lpi_overall DESC, country LIMIT 10
```

### surveillance-kg

[samyama-ai/surveillance-kg](https://github.com/samyama-ai/surveillance-kg). Disease
surveillance from the WHO Global Health Observatory.

- **Scale:** not stated in the repository.
- **Schema:** 7 labels (Country, Region, Disease, DiseaseReport, VaccineCoverage, AMRProfile,
  HealthIndicator); 7 edge types (IN_REGION, REPORTED, REPORT_OF, HAS_COVERAGE, COVERAGE_FOR,
  HAS_AMR, HAS_INDICATOR).
- **Data licence:** CC BY-NC-SA 3.0 IGO (WHO): non-commercial and share-alike, and the graph
  inherits it whole.
- **Get it:** [`kg-snapshots-v4/surveillance.sgsnap`](https://github.com/samyama-ai/samyama-graph/releases/download/kg-snapshots-v4/surveillance.sgsnap)
  (6 MB), or load the repository's checked-in data.

```cypher
MATCH (c:Country)-[:REPORTED]->(r:DiseaseReport)-[:REPORT_OF]->(d:Disease) RETURN d.name AS disease, count(r) AS reports ORDER BY reports DESC LIMIT 3
```

### wildfire-evac-kg

[samyama-ai/wildfire-evac-kg](https://github.com/samyama-ai/wildfire-evac-kg). The road network
of Paradise, California, for wildfire evacuation modelling.

- **Scale:** the raw extract has 12,610 OSM nodes and 1,177 ways (13,787 elements). The
  HuggingFace graph form has 13,787 nodes and 26,831 edges.
- **Schema:** not stated in the repository, which is an extract only (no loader).
- **Data licence:** ODbL 1.0, "© OpenStreetMap contributors"; share-alike.
- **Get it:** [HuggingFace](https://huggingface.co/datasets/VaidhyaMegha/wildfire-evac-kg)
  (node/edge CSVs and the raw extract).

The repository has no sample query yet.

## Not listed

- **Private repositories** are left out, because a reader outside the organisation cannot
  open them.
