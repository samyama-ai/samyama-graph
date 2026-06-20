#!/usr/bin/env python3
"""Narrated Disease Surveillance Knowledge Graph demo. Run via `RECORD=1 ./run.sh`."""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "_lib"))
from demo_lib import run_demo

run_demo(
    domain="Disease Surveillance Knowledge Graph",
    tagline="WHO outbreak reports and immunization coverage, country by country",
    source="data: WHO Global Health Observatory (GHO)",
    takeaway_text="Outbreak hotspots meet immunity gaps in one query — and join cleanly to the\nhealth-systems and health-determinants graphs by ISO country code.",
)
