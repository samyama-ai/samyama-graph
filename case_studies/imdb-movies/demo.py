#!/usr/bin/env python3
"""Narrated IMDB Movies KG demo. Run via `RECORD=1 ./run.sh` (server + snapshot must
already be up — run_case_study.sh handles that)."""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "_lib"))
from demo_lib import run_demo

run_demo(
    domain="IMDB Movies Knowledge Graph",
    tagline="50K movies · 15K series · 294K persons · directors, actors, writers as a graph",
    source="data: IMDB Non-Commercial Datasets · imdb.com/non-commercial-datasets",
    takeaway_text="Top-rated films, director–actor power pairs, genre trends, decade arcs —\n"
                  "all answered as graph traversals, no joins to hand-write.",
)
