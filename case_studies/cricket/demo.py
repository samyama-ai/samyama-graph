#!/usr/bin/env python3
"""Narrated Cricket KG demo. Run via `RECORD=1 ./run.sh` (server + snapshot must
already be up — run_case_study.sh handles that)."""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "_lib"))
from demo_lib import run_demo

run_demo(
    domain="Cricket Knowledge Graph",
    tagline="Ball-by-ball international cricket as a graph — rivalries, venues, awards",
    source="data: Cricsheet.org (ODI/Test/T20/IPL, 21K+ matches) · CC BY 4.0",
    takeaway_text="Dismissal rivalries, venue specialists, award tallies — one query "
                  "family\nover players, matches and venues, no joins to hand-write.",
)
