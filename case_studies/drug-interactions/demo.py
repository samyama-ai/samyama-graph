#!/usr/bin/env python3
"""Narrated Drug Interactions & Pharmacogenomics KG demo. Run via `RECORD=1 ./run.sh`."""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "_lib"))
from demo_lib import run_demo

run_demo(
    domain="Drug Interactions & Pharmacogenomics KG",
    tagline="Drugs, gene targets, side effects and bioactivity from 5 pharmacology sources",
    source="data: DrugBank · DGIdb · SIDER · ChEMBL · OpenFDA",
    takeaway_text="Polypharmacy risk and shared drug targets surface as graph patterns — the\nbackbone of clinical decision support.",
)
