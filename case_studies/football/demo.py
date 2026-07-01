#!/usr/bin/env python3
"""Narrated FIFA World Cup KG demo. Run via `RECORD=1 ./run.sh` (server + snapshot must
already be up — run_case_study.sh handles that)."""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "_lib"))
from demo_lib import run_demo

run_demo(
    domain="Football Knowledge Graph",
    tagline="30 tournaments · 1,200+ matches · 10K players · goals, stadiums, managers as a graph",
    source="data: DataHub World Cup Datasets · datahub.io/football/worldcup",
    takeaway_text="Top scorers, winning nations, busiest stadiums, multi-tournament veterans —\n"
                  "all answered as graph traversals across 90 years of World Cup history.",
)
