# Samyama Scripts

Utility scripts for data processing, enrichment, and testing.

## Verification

### `verify-sweep.sh`

Runs the full sweep — **tests, examples, benches, case studies** — and writes a
one-line-per-item summary of each, so two runs can be diffed directly.

```sh
scripts/verify-sweep.sh                 # against this checkout
scripts/verify-sweep.sh --provision     # also install toolchain + system deps
scripts/verify-sweep.sh --out DIR       # where results and logs go
scripts/verify-sweep.sh --jobs 4        # cargo -j (default: nproc-2)
```

This catches the classes a plain `cargo test` does not: an example that panics
instead of saying what data it needs, a bench that reports success while
measuring nothing, a case study whose snapshot URL was never filled in.

It distinguishes **breakage from absence**, and the exit code follows that
distinction — an example exiting non-zero because it needs a dataset
(`NEEDS-INPUT`) and a case study whose snapshot is unpublished (`SKIP`) are not
failures. A panic, a failed bench, or a failed test is.

Resumable: each stage drops a marker, so a run interrupted partway resumes
where it stopped. On a cloud host, launch it under `setsid nohup` so a dropped
SSH connection does not take the run with it.


## Knowledge Graph Enrichment

### `enrich_clinical_trials.py`

Enriches AACT clinical trials data using knowledge graph techniques.

**Purpose:**
- Links medical conditions to standardized disease ontologies
- Normalizes drug/intervention names
- Creates inferred relationships
- Calculates confidence scores for entity linkage

**Features:**
- **Entity Linking**: Maps conditions to Hetionet diseases using:
  - Exact matching
  - Alias/synonym matching
  - Fuzzy string matching (with high threshold)
- **Confidence Scoring**:
  - High (≥0.95): Exact or near-exact matches
  - Medium (0.85-0.94): Alias matches
  - Low (0.70-0.84): Fuzzy matches
- **Relationship Inference**: Creates Trial → Disease edges via condition mappings

**Usage:**

```bash
# Basic usage (uses /tmp for input/output)
python scripts/enrich_clinical_trials.py

# Custom directories
python scripts/enrich_clinical_trials.py \
    --input-dir /path/to/data \
    --output-dir /path/to/enriched
```

**Input Files** (in `--input-dir`):
- `aact_conditions.tsv` - Medical conditions from AACT
- `aact_edges_studies.tsv` - Trial → Condition relationships
- `clinical_nodes.tsv` - Hetionet diseases (optional, for reference)

**Output Files** (in `--output-dir`):
- `enriched_condition_mappings.tsv` - Condition → Disease mappings with confidence scores
- `enriched_trial_disease_edges.tsv` - Inferred Trial → Disease relationships
- `enriched_stats.json` - Statistics and quality metrics

**Example:**

```bash
# 1. Export data from CSDLC VM
python parse_clinical_trials.py  # Creates aact_*.tsv files

# 2. Run enrichment pipeline
python scripts/enrich_clinical_trials.py

# 3. Load enriched data into Samyama
# Update main.rs to load from /tmp/enriched/enriched_trial_disease_edges.tsv
```

**Statistics Example:**

```json
{
  "total_conditions": 32111,
  "linked_conditions": 8945,
  "high_confidence": 2134,
  "medium_confidence": 4231,
  "low_confidence": 2580,
  "enriched_edges": 15234,
  "link_rate": 0.278
}
```

**Disease Mappings:**

The enricher includes built-in mappings for common diseases:
- Diabetes Mellitus → ["diabetes", "type 2 diabetes", "t2dm", "diabetic", ...]
- Hypertension → ["high blood pressure", "htn", "elevated bp", ...]
- Cancer → ["carcinoma", "neoplasm", "tumor", "malignancy", ...]
- And 15+ more common conditions

**Extending Mappings:**

Edit `get_disease_mappings()` in the script to add new disease aliases:

```python
def get_disease_mappings(self) -> Dict[str, List[str]]:
    return {
        'your_disease': ['alias1', 'alias2', 'abbreviation'],
        # ... existing mappings
    }
```

## Future Scripts

Planned scripts for the `scripts/` directory:

- `normalize_drugs.py` - Drug name normalization (RxNorm, DrugBank)
- `deduplicate_entities.py` - Entity deduplication and merging
- `infer_relationships.py` - Relationship inference from co-occurrence
- `quality_metrics.py` - Data quality assessment
- `export_for_ml.py` - Export graph data for ML training

## Requirements

```bash
pip install -r requirements.txt  # If dependencies needed
```

Currently uses only Python standard library (no external dependencies).
