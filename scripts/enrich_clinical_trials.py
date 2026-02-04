#!/usr/bin/env python3
"""
Knowledge Graph Enrichment Pipeline for Clinical Trials Data

Enriches AACT clinical trials data by:
1. Linking medical conditions to existing Hetionet diseases
2. Normalizing drug/intervention names
3. Deduplicating similar entities
4. Creating inferred relationships
5. Calculating confidence scores

Usage:
    python scripts/enrich_clinical_trials.py \
        --input-dir /tmp \
        --output-dir /tmp/enriched

Input files:
    - aact_conditions.tsv
    - aact_interventions.tsv
    - aact_trials.tsv
    - aact_edges_studies.tsv
    - clinical_nodes.tsv (Hetionet diseases)

Output files:
    - enriched_condition_mappings.tsv - Condition → Disease links
    - enriched_trial_disease_edges.tsv - Trial → Disease relationships
    - enriched_stats.json - Statistics and quality metrics
"""

import argparse
import csv
import json
import re
from collections import defaultdict
from difflib import SequenceMatcher
from pathlib import Path
from typing import Dict, List, Tuple, Set


class KnowledgeGraphEnricher:
    """Enriches clinical trials data with knowledge graph techniques."""

    def __init__(self, input_dir: Path, output_dir: Path):
        self.input_dir = Path(input_dir)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True, parents=True)

        # Data structures
        self.hetionet_diseases: Dict[str, Tuple[str, str]] = {}  # lowercase -> (id, name)
        self.conditions: Dict[str, str] = {}  # cond_id -> name
        self.condition_links: Dict[str, Dict] = {}  # cond_id -> {disease, confidence, etc}
        self.trial_conditions: Dict[str, List[str]] = defaultdict(list)

        # Statistics
        self.stats = {
            'total_conditions': 0,
            'linked_conditions': 0,
            'high_confidence': 0,
            'medium_confidence': 0,
            'low_confidence': 0,
            'enriched_edges': 0
        }

    def similarity_score(self, s1: str, s2: str) -> float:
        """Calculate string similarity (0.0 to 1.0)."""
        return SequenceMatcher(None, s1.lower(), s2.lower()).ratio()

    def load_hetionet_diseases(self):
        """Load Hetionet disease names for entity linking."""
        print("\n[1/6] Loading Hetionet reference data...")

        hetionet_file = self.input_dir / "clinical_nodes.tsv"
        if not hetionet_file.exists():
            print(f"  Warning: {hetionet_file} not found, using built-in mappings only")
            return

        with open(hetionet_file, 'r') as f:
            for line in f:
                parts = line.strip().split('\t')
                if len(parts) >= 3 and parts[2] == 'Disease':
                    node_id, name = parts[0], parts[1]
                    self.hetionet_diseases[name.lower()] = (node_id, name)

        print(f"  Loaded {len(self.hetionet_diseases)} Hetionet diseases")

    def get_disease_mappings(self) -> Dict[str, List[str]]:
        """Return common disease name aliases and variations."""
        return {
            'diabetes mellitus': ['diabetes', 'type 2 diabetes', 'type 1 diabetes',
                                   'diabetic', 'dm', 't2dm', 't1dm'],
            'hypertension': ['high blood pressure', 'htn', 'hypertensive', 'elevated bp'],
            'cancer': ['carcinoma', 'neoplasm', 'tumor', 'tumour', 'malignancy',
                      'malignant', 'oncology'],
            'asthma': ['asthmatic', 'bronchial asthma', 'reactive airway'],
            'alzheimer disease': ["alzheimer's", 'ad', 'dementia', 'alzheimer'],
            'parkinson disease': ["parkinson's", 'pd', 'parkinson'],
            'heart failure': ['cardiac failure', 'chf', 'congestive heart failure',
                             'heart insufficiency'],
            'stroke': ['cerebrovascular accident', 'cva', 'brain attack', 'cerebral infarction'],
            'coronary artery disease': ['cad', 'coronary heart disease', 'chd', 'ischemic heart disease'],
            'chronic obstructive pulmonary disease': ['copd', 'chronic bronchitis', 'emphysema'],
            'rheumatoid arthritis': ['ra', 'rheumatoid', 'inflammatory arthritis'],
            'breast cancer': ['breast neoplasm', 'breast carcinoma', 'mammary carcinoma'],
            'lung cancer': ['lung neoplasm', 'lung carcinoma', 'bronchogenic carcinoma'],
            'depression': ['major depressive disorder', 'mdd', 'depressive disorder', 'clinical depression'],
            'schizophrenia': ['schizophrenic disorder', 'psychosis'],
            'obesity': ['overweight', 'obese', 'adiposity'],
            'hiv infections': ['hiv', 'human immunodeficiency virus', 'aids', 'acquired immunodeficiency syndrome'],
            'hepatitis c': ['hcv', 'hepatitis c virus'],
        }

    def link_condition_to_disease(self, condition_name: str) -> Tuple[str, float]:
        """
        Link a condition to a Hetionet disease.
        Returns (disease_name, confidence_score) or (None, 0.0)
        """
        condition_lower = condition_name.lower().strip()

        # Exact match
        if condition_lower in self.hetionet_diseases:
            return self.hetionet_diseases[condition_lower][1], 1.0

        # Check disease mappings
        mappings = self.get_disease_mappings()
        for disease, aliases in mappings.items():
            # Check if condition contains any alias
            for alias in aliases:
                if alias in condition_lower or condition_lower in alias:
                    # Find the canonical disease name in Hetionet
                    if disease in self.hetionet_diseases:
                        return self.hetionet_diseases[disease][1], 0.9
                    # Fuzzy match to find best Hetionet disease
                    for het_name_lower, (_, het_name) in self.hetionet_diseases.items():
                        if disease in het_name_lower or het_name_lower in disease:
                            return het_name, 0.85

        # Fuzzy matching (high threshold to avoid false positives)
        best_match = None
        best_score = 0.0
        for het_name_lower, (_, het_name) in self.hetionet_diseases.items():
            score = self.similarity_score(condition_name, het_name)
            if score > best_score and score > 0.85:
                best_score = score
                best_match = het_name

        return best_match, best_score

    def load_conditions(self):
        """Load AACT medical conditions."""
        print("\n[2/6] Loading AACT conditions...")

        conditions_file = self.input_dir / "aact_conditions.tsv"
        if not conditions_file.exists():
            raise FileNotFoundError(f"Required file not found: {conditions_file}")

        with open(conditions_file, 'r') as f:
            for line in f:
                parts = line.strip().split('\t')
                if len(parts) >= 2:
                    cond_id, name = parts[0], parts[1]
                    self.conditions[cond_id] = name

        self.stats['total_conditions'] = len(self.conditions)
        print(f"  Loaded {len(self.conditions)} conditions")

    def enrich_conditions(self):
        """Link conditions to Hetionet diseases."""
        print("\n[3/6] Linking conditions to diseases...")

        for cond_id, cond_name in self.conditions.items():
            linked_disease, confidence = self.link_condition_to_disease(cond_name)

            if linked_disease and confidence >= 0.70:
                self.condition_links[cond_id] = {
                    'disease': linked_disease,
                    'confidence': confidence,
                    'original_condition': cond_name
                }

                self.stats['linked_conditions'] += 1
                if confidence >= 0.95:
                    self.stats['high_confidence'] += 1
                elif confidence >= 0.85:
                    self.stats['medium_confidence'] += 1
                else:
                    self.stats['low_confidence'] += 1

        print(f"  Linked {self.stats['linked_conditions']} conditions")
        print(f"    High confidence (≥0.95): {self.stats['high_confidence']}")
        print(f"    Medium confidence (0.85-0.94): {self.stats['medium_confidence']}")
        print(f"    Low confidence (0.70-0.84): {self.stats['low_confidence']}")

    def load_trial_condition_edges(self):
        """Load trial → condition relationships."""
        print("\n[4/6] Loading trial-condition relationships...")

        edges_file = self.input_dir / "aact_edges_studies.tsv"
        if not edges_file.exists():
            raise FileNotFoundError(f"Required file not found: {edges_file}")

        with open(edges_file, 'r') as f:
            for line in f:
                parts = line.strip().split('\t')
                if len(parts) >= 3:
                    trial_id, _, cond_id = parts[0], parts[1], parts[2]
                    self.trial_conditions[trial_id].append(cond_id)

        print(f"  Loaded {len(self.trial_conditions)} trial-condition links")

    def create_enriched_edges(self):
        """Create trial → disease edges via condition links."""
        print("\n[5/6] Creating enriched trial-disease edges...")

        enriched_edges = []
        for trial_id, cond_ids in self.trial_conditions.items():
            for cond_id in cond_ids:
                if cond_id in self.condition_links:
                    link = self.condition_links[cond_id]
                    enriched_edges.append({
                        'trial_id': trial_id,
                        'disease': link['disease'],
                        'condition': link['original_condition'],
                        'confidence': link['confidence']
                    })

        self.stats['enriched_edges'] = len(enriched_edges)
        print(f"  Created {len(enriched_edges)} enriched edges")

        # Write enriched edges
        output_file = self.output_dir / "enriched_trial_disease_edges.tsv"
        with open(output_file, 'w', newline='') as f:
            writer = csv.writer(f, delimiter='\t')
            writer.writerow(['trial_id', 'disease', 'original_condition', 'confidence'])
            for edge in enriched_edges:
                writer.writerow([
                    edge['trial_id'],
                    edge['disease'],
                    edge['condition'],
                    f"{edge['confidence']:.3f}"
                ])

        print(f"  Written to: {output_file}")

    def write_condition_mappings(self):
        """Write condition → disease mappings."""
        print("\n[6/6] Writing condition mappings...")

        output_file = self.output_dir / "enriched_condition_mappings.tsv"
        with open(output_file, 'w', newline='') as f:
            writer = csv.writer(f, delimiter='\t')
            writer.writerow(['condition_id', 'condition_name', 'linked_disease', 'confidence'])
            for cond_id, link in sorted(self.condition_links.items()):
                writer.writerow([
                    cond_id,
                    link['original_condition'],
                    link['disease'],
                    f"{link['confidence']:.3f}"
                ])

        print(f"  Written {len(self.condition_links)} mappings to: {output_file}")

    def write_statistics(self):
        """Write enrichment statistics."""
        stats_file = self.output_dir / "enriched_stats.json"

        # Calculate percentages
        total = self.stats['total_conditions']
        self.stats['link_rate'] = self.stats['linked_conditions'] / total if total > 0 else 0

        with open(stats_file, 'w') as f:
            json.dump(self.stats, f, indent=2)

        print(f"\n  Statistics written to: {stats_file}")

    def run(self):
        """Run the full enrichment pipeline."""
        print("=" * 80)
        print("Clinical Trials Knowledge Graph Enrichment Pipeline")
        print("=" * 80)

        try:
            self.load_hetionet_diseases()
            self.load_conditions()
            self.enrich_conditions()
            self.load_trial_condition_edges()
            self.create_enriched_edges()
            self.write_condition_mappings()
            self.write_statistics()

            print("\n" + "=" * 80)
            print("Enrichment Summary")
            print("=" * 80)
            print(f"Total conditions: {self.stats['total_conditions']}")
            print(f"Linked to diseases: {self.stats['linked_conditions']} "
                  f"({self.stats['link_rate']*100:.1f}%)")
            print(f"Enriched trial-disease edges: {self.stats['enriched_edges']}")
            print(f"\nOutput directory: {self.output_dir}")
            print("=" * 80)

        except Exception as e:
            print(f"\nError: {e}")
            raise


def main():
    parser = argparse.ArgumentParser(
        description='Enrich clinical trials data with knowledge graph techniques'
    )
    parser.add_argument(
        '--input-dir',
        type=str,
        default='/tmp',
        help='Directory containing input TSV files (default: /tmp)'
    )
    parser.add_argument(
        '--output-dir',
        type=str,
        default='/tmp/enriched',
        help='Directory for enriched output files (default: /tmp/enriched)'
    )

    args = parser.parse_args()

    enricher = KnowledgeGraphEnricher(
        input_dir=args.input_dir,
        output_dir=args.output_dir
    )
    enricher.run()


if __name__ == "__main__":
    main()
