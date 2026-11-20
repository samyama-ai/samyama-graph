# Vector search at biomedical scale (#1281)

`cargo bench --bench biomedical_vectors` measures vector search on the
shape of a biomedical knowledge graph: typed concept nodes and long-text
articles, searched both across every type and filtered to one type. It
reuses the ANN harness (`benches/ann_common`), the cost-profile helpers
(`benches/vector_cost_common`), fp16 quantization, IVF and the checkpoint
generations of #1973. It adds no engine code.

**No numbers are recorded here.** This page is the protocol. Figures belong
in a results section once a run on real embeddings, on a quiet host, exists.

## What is measured

Two levels:

| level | label | one vector per | synthetic default |
|---|---|---|---|
| concept | `Concept`, or one label per type | Drug, Disease, Gene, Protein, Target, Pathway, MeSHTerm, ClinicalCondition node (name + synonyms) | 10,000 × 384 |
| article | `Article` | article (title + abstract) | 3,000 × 768 |

For each level the bench crosses:

- **layout**: `shared`, one index over every type with the type applied as a
  filter (`search_filtered_ef`), or `per-type`, one index per type label,
  where an any-type query merges every index's top k;
- **index kind**: `hnsw`, `ivf`;
- **quantization**: every value this build has, today `f32` and `fp16`;
- **type subset**: `all`, plus each `--filter-types` entry (default `Drug`
  and `Pathway`: a mid-sized and the smallest type). Every subset has its
  own exact ground truth, computed over that type's vectors only.

Each configuration reports:

| figure | from |
|---|---|
| build time | inserting every vector, then IVF training if due |
| vector / graph / resident bytes, per vector, against the theoretical floor | `VectorIndex::vector_bytes`, `graph_bytes`, `resident_bytes` |
| process RSS (peak during the configuration) | `/proc`, Linux only |
| checkpoint size on disk, split into vector lists, HNSW graphs, IVF centroids and metadata | `checkpoint_all`, then the generation's files |
| checkpoint write and recovery time, first query after recovery, graphs loaded vs rebuilt | `checkpoint_all`, `recover_checkpoint` |
| recall@k against exact search, p50/p99 latency, QPS | serial sweep over `--ef` (HNSW) or `--nprobe` (IVF) |
| **recall delta against f32** at the same level, layout, kind, subset and width | the accuracy impact of each quantization |
| share of queries answered by an exact fallback, distances per query | `samyama::vector::work` counters |

A summary row per configuration gives the fastest width that reaches
`--recall-target` (0.95 by default), or says none does.

The type-filtered rows answer the question the issue asks about filtering:
with a shared index a selective type pushes the HNSW search into its exact
fallback (see [filtered-ann.md](./filtered-ann.md)), which shows in the
exact-fallback share; with a per-type index the filter costs nothing but
every index is paid for in memory and on disk. Both are measured, neither is
assumed.

## How to run

```bash
# No data: a seeded synthetic corpus. This is what the nightly runs.
cargo bench --bench biomedical_vectors

# Larger synthetic run; one quantization and kind per process for clean RSS.
cargo bench --bench biomedical_vectors -- --concepts 1000000 --articles 200000 \
    --quantization fp16 --kind hnsw --json bio-fp16-hnsw.json

# Real embeddings.
pip install numpy sentence-transformers
python3 scripts/prepare_biomedical_vectors.py \
    --mesh-xml desc2025.xml --pubmed-xml pubmed25n0001.xml.gz \
    --out data/biomedical-vectors
cargo bench --bench biomedical_vectors -- --data data/biomedical-vectors
```

The module doc of `benches/biomedical_vectors.rs` lists every flag. The prepared directory holds, per level,
`base.fvecs`, `base_types.txt` (one type per base vector, the node label),
`query.fvecs`, and optionally `metric.txt`. A level whose directory is
missing is skipped with a message.

`scripts/prepare_biomedical_vectors.py` embeds with one open model
(`NeuML/pubmedbert-base-embeddings` by default, 768-d, normalised) for both
levels. Concepts come from MeSH descriptors, typed by tree number, or from a
`type<TAB>text` TSV exported from the graph; articles from PubMed baseline
XML or a TSV. MeSH has no Target or Pathway branch, so the script's mapping
(receptors as targets, metabolic networks and signal transduction as
pathways) is a heuristic; a TSV from the graph's own labels is the better
source when one exists. `--texts-only` writes what would be embedded without
loading a model.

### int8

int8 quantization is in #2307 and not on `dev`. `--quantization` takes its
names from the build's list (`vector_cost_common::QUANTIZATIONS`), so once
#2307 merges and adds `int8` to that list, `--quantization int8` measures it
and its recall delta against f32 with no change here. Until then the bench
rejects `int8` and names what it has.

## Caveats

- The synthetic corpus has the shape of a biomedical one (typed clusters,
  related types near each other, unit vectors at sentence-model
  dimensions) and none of its content. Its recall says how the index
  behaves on clustered data, not how a given model's embeddings search.
- Latency is serial, one client thread. Throughput across cores is
  `ann_benchmark`'s job.
- RSS is unknown off Linux, and a freed index does not always return its
  pages, so later configurations in one process carry earlier ones'
  residue. Compare RSS across separate runs, one `--quantization` and
  `--kind` each.
- In-process, no network: this engine's ceiling, not comparable to a system
  measured over its API.
- Run with `--release` on a quiet host and compare the calibration line
  before comparing timings (#529).

## What is not done

- No run on real biomedical embeddings has been made, and the prep script
  has not been run against a model or a MeSH/PubMed download.
- No optimisation of the engine: the bench measures the choices the engine
  already has (layout, kind, quantization, width) so that one can be made
  from evidence.
- int8 (#2307), as above.
- Scale beyond one host's memory; the defaults fit the nightly's 30-minute
  budget, and million-vector runs are by flag.
