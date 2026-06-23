#!/usr/bin/env python3
"""Narrated Bank Model-Risk KG demo. Run via `RECORD=1 ./run.sh` (server + snapshot
must already be up — run_case_study.sh handles that)."""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "_lib"))
from demo_lib import run_demo

run_demo(
    domain="Bank Model-Risk Knowledge Graph",
    tagline="A bank's model-risk inventory as a graph — lineage, regulation, validation, explainability",
    source="data: fully synthetic, generated from a fixed seed · no real institution · Apache-2.0",
    takeaway_text="Blast-radius, regulatory coverage, open findings on critical models, model\n"
                  "explainability — one query family over models, data, validations and regs.",
)
