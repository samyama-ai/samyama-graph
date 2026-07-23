# Legal Judgments Knowledge Graph — Case Study

589 judgments of the Supreme Court of India (2016) — judges, parties, cited legal
sections and topics modelled as a property graph. Questions a legal researcher would
ask ("which sections are cited most?", "which judges sit together most often?", "which
laws span the widest range of subjects?") become single Cypher traversals.

Reproduces a public reference demo (PostgreSQL + Apache AGE + pgvector) by Shreyas Rao
on Samyama — one engine instead of three.

![Legal Judgments demo](demo.gif)

```bash
cd case_studies/legal-judgments && ./run.sh   # validate every query against the snapshot
RECORD=1 ./run.sh                              # also regenerate demo.gif
```

## The graph

**Scale:** 4,462 nodes · 8,363 edges (imported from a small snapshot in seconds)

| Node label | Count | Key properties |
|------------|-------|----------------|
| Topic | 2,291 | text, category |
| Party | 1,102 | name |
| Case | 589 | id, title, year, month |
| Act | 446 | name |
| Judge | 34 | name |

**Relationships (4):**

| Relationship | Pattern | Count |
|---|---|---|
| `ABOUT` | Case → Topic | 3,041 |
| `CITES` | Case → Act (property: `section`) | 2,749 |
| `PARTY_IN` | Party → Case (property: `role`) | 1,309 |
| `DECIDED` | Judge → Case | 1,264 |

The `section` lives on the `CITES` edge, so section-level questions
("how many judgments cite IPC §302?") are answerable — reproducing the reference's
headline result exactly.

## Showcase queries

See [`queries.cypher`](queries.cypher). The narrative: most productive judges
(Dipak Misra, 104) → most-cited sections (IPC §302 = 57, Constitution Art 32 = 36) →
strongest bench pairings (Kurian Joseph & Rohinton F. Nariman, 55) → laws cited
together → laws spanning the widest topic range (Constitution of India, 11 categories)
→ the docket by topic category.

These match the reference demo's published numbers exactly.

## Data & license

Source: [`Shreyasrao/Indian-law-supreme-court-judgements-2016`](https://huggingface.co/datasets/Shreyasrao/Indian-law-supreme-court-judgements-2016)
(revision `e928c72019d6`). Originally from the Indian Supreme Court Judgments registry
on AWS Open Data, managed by Dattam Labs.

**License:** CC-BY-4.0.

Snapshot built by [`examples/legal_judgments_loader.rs`](../../examples/legal_judgments_loader.rs)
from the 9 node/edge CSVs, and published as `legal-judgments.sgsnap` on a release.
