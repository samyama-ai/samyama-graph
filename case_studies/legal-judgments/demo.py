#!/usr/bin/env python3
"""Narrated Indian Supreme Court Judgments KG demo. Run via `RECORD=1 ./run.sh` (server + snapshot must
already be up — run_case_study.sh handles that)."""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "_lib"))
from demo_lib import run_demo

run_demo(
    domain="Legal Judgments Knowledge Graph",
    tagline="589 Supreme Court of India judgments (2016) · judges, parties, cited sections, topics as a graph",
    source="data: Shreyasrao/Indian-law-supreme-court-judgements-2016 · HuggingFace · CC-BY-4.0",
    takeaway_text="Most-cited sections (IPC 302 = 57), strongest bench pairings, laws by topic breadth —\n"
                  "reproduces a 3-component Postgres + AGE + pgvector demo in one engine.",
)
