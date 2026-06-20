#!/usr/bin/env python3
"""Narrated Health Determinants KG demo. Run via `RECORD=1 ./run.sh`."""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "_lib"))
from demo_lib import run_demo
run_demo(
    domain="Health Determinants Knowledge Graph",
    tagline="Why populations are vulnerable — air, water, poverty, demographics",
    source="data: World Bank WDI · WHO air quality · FAO AQUASTAT · UNDP HDI",
    takeaway_text="Air pollution, water access and social drivers per country —\nthe upstream 'why' behind health outcomes, joinable by ISO code.",
)
