#!/usr/bin/env python3
"""Narrated Health Systems KG demo. Run via `RECORD=1 ./run.sh`."""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "_lib"))
from demo_lib import run_demo
run_demo(
    domain="Health Systems Knowledge Graph",
    tagline="Emergency preparedness & health-workforce capacity, country by country",
    source="data: WHO SPAR (IHR core capacity) + WHO NHWA (workforce density)",
    takeaway_text="Preparedness scores and workforce density expose the highest-risk\nnations — and join surveillance + determinants by ISO country code.",
)
