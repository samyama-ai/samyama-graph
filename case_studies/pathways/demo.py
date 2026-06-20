#!/usr/bin/env python3
"""Narrated Biological Pathways Knowledge Graph demo. Run via `RECORD=1 ./run.sh`."""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "_lib"))
from demo_lib import run_demo

run_demo(
    domain="Biological Pathways Knowledge Graph",
    tagline="Proteins, complexes, reactions and pathways from Reactome, STRING, GO, UniProt",
    source="data: Reactome · STRING · Gene Ontology · UniProt",
    takeaway_text="Protein hubs (TP53), pathway crosstalk and two-hop interaction neighbourhoods —\nsystems biology questions answered as graph traversals.",
)
