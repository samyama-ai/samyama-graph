# Biological Pathways KG — Case Study

Proteins, complexes, reactions, pathways and Gene Ontology terms from Reactome,
STRING, GO and UniProt. Systems-biology questions — protein hubs, pleiotropy,
pathway crosstalk — become graph traversals over a 835K-edge interaction network.

![Pathways demo](demo.gif)

```bash
cd case_studies/pathways && ./run.sh        # validate every query
RECORD=1 ./run.sh                           # also regenerate demo.gif
```

## The graph

**Scale:** 118,686 nodes · 834,785 edges (from a 9 MB snapshot)

| Node label | Count | Key properties |
|------------|-------|----------------|
| GOTerm | 51,897 | go_id, name, namespace |
| Protein | 37,990 | uniprot_id, name |
| Complex | 15,963 | name, complex_id |
| Reaction | 9,988 | reaction_id, reaction_type |
| Pathway | 2,848 | name, pathway_id, organism |

**Relationships:** `ANNOTATED_WITH` (265K), `INTERACTS_WITH` (228K, with
`combined_score`), `PARTICIPATES_IN` (140K), `CATALYZES` (121K), `IS_A` (59K),
`COMPONENT_OF`, `PART_OF`, `REGULATES`, `CHILD_OF`.

## Showcase queries

See [`queries.cypher`](queries.cypher): most-connected protein hubs (degree
centrality — master regulators surface) → most pleiotropic proteins → largest
pathways → **pathway crosstalk** (pathway pairs sharing the most proteins, a
two-hop join across 140K participations) → most heavily GO-annotated proteins.
Every query returns real rows (DoD-gated).

## Question catalog

[`pathways.sgqueries`](./pathways.sgqueries) holds 52 questions (15 easy, 25 medium,
12 hard; four unanswerable), built with `catalog-build --release` against the pinned
snapshot. [`questions.json`](questions.json) is its source, and the NLQ accuracy
benchmark
([competitor-benchmarks#260](https://git.samyama.ai/Samyama.ai/samyama-graph-competitor-benchmarks/issues/260))
reads it as its question set. Every entry is checked
weekly against the published snapshot by
[`kg-catalogs.yml`](../../.github/workflows/kg-catalogs.yml).

```bash
samyama queries list case_studies/pathways/pathways.sgqueries
samyama queries run  case_studies/pathways/pathways.sgqueries --snapshot pathways.sgsnap \
    --entry <id> [--param name=value]...
samyama verify pathways.sgsnap --queries case_studies/pathways/pathways.sgqueries
```

Interactions are stored once per pair (`a -> b`, never also `b -> a`), so partner
questions match them in either direction. A protein's pathway list includes the
ancestors of its pathways (BAX takes part in `Apoptosis` and in `Programmed Cell
Death` above it), and 34 pathways sit under more than one parent.

**Data caveat the catalog surfaces.** One protein is often two `Protein` nodes. The
STRING load gave 15,958 nodes a gene name (`TP53`) and all of the interactions, but
their `uniprot_id` is often not the canonical accession (`TP53` is on `Q9UQ61`). The
Reactome and GO loads hang pathways and annotations on the canonical accession
(`P04637`), and 8,771 of those nodes have no name. Only 3,339 named nodes carry
interactions, pathways and GO terms together. So the catalog asks for a protein's
pathways and GO terms by UniProt accession, asks for its partners by gene name, and
uses samples (CDK1, EP300, MAPK1) that are merged. A further 13,257 `Protein` nodes have an
interaction type as their name (`-`, `physical association`, `enzymatic reaction`)
and hold nearly every `COMPONENT_OF` and all `CATALYZES` edges, so no question
names a catalyst or a complex member. 3,646 `GOTerm` nodes are bare ids with no name or
namespace.

## Data & license

Sources: [Reactome](https://reactome.org) (CC0), [STRING](https://string-db.org)
(CC BY 4.0), [Gene Ontology](http://geneontology.org) (CC BY 4.0),
[UniProt](https://www.uniprot.org) (CC BY 4.0). Snapshot `pathways.sgsnap` on
release
[`kg-snapshots-v3`](https://github.com/samyama-ai/samyama-graph/releases/tag/kg-snapshots-v3)
(sha256 pinned in [`case.env`](case.env)). Built by the
[`pathways-kg`](https://github.com/samyama-ai/pathways-kg) loader.
