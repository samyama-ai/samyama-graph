#!/usr/bin/env python3
"""Generate the HIER query corpus (ADR-035).

The corpus is data, not code: this script writes `queries.json`, the runner reads it. It
lives next to the JSON so the *rules* behind 100+ near-identical queries are reviewable —
reading one generator is easier than reviewing a hundred hand-written strings, and it makes
the coverage argument checkable (which axes, which subtree sizes, which monoids).

Every query carries a `baseline`: the same question asked without index assistance. Three
kinds of baseline appear, in decreasing order of independence:

1. **Prefix** — this dataset encodes each ontology/geography code as its own path from the
   root, so "under X" is exactly "code starts with X". That is ground truth computed with
   no hierarchy machinery at all, neither index nor traversal.
2. **Traversal** — a variable-length expansion. Used where the code is not a prefix
   (calendar quarters) and as the honest in-engine before/after.
3. **Implicit** — no `baseline` key: the runner executes the *same* Cypher against a store
   with no hierarchy declared, which is the cleanest index-on/index-off comparison.

Regenerate with:  python3 benchmarks/hier/generate_corpus.py
"""
import json

Q = []


def add(qid, cls, name, cypher, baseline=None, skip=None):
    q = {"id": qid, "class": cls, "name": name, "cypher": cypher}
    if baseline:
        q["baseline"] = baseline
    if skip:
        q["skip"] = skip
    Q.append(q)


# Subtree sizes in this dataset, for the record:
#   ontology  T=9331  T0=1555  T05=259  T053=43  T0531=7  T05314=1
#   calendar  Y2019=353  Y2019Q1=88  Y2019M01=29  day=1
#   geography CO0=446  CO0S0=89  CO0S0T0=11  zip=1
ONTO = [("T", 9331), ("T0", 1555), ("T05", 259), ("T053", 43), ("T0531", 7), ("T05314", 1)]
GEO = [("Country", "CO0", 446), ("State", "CO0S0", 89), ("City", "CO0S0T0", 11)]
CAL_PREFIX = [("Year", "Y2019", 353), ("Month", "Y2019M01", 29)]
CAL_TRAVERSE = [("Quarter", "Y2019Q1", 88)]
THREAT = ["K0_0", "K0_5", "K1_0", "K2_3"]

# ---------------------------------------------------------------- H1 order test
n = 0
for code, size in ONTO[:5]:
    n += 1
    add(f"H1-{n:02d}", "H1", f"order test: terms under {code} (subtree {size})",
        f'MATCH (d:Term), (r:Term {{code: "{code}"}}) WHERE subsumes(d, r) RETURN count(d) AS n',
        f'MATCH (d:Term) WHERE d.code STARTS WITH "{code}" RETURN count(d) AS n')
for label, code, size in CAL_PREFIX:
    n += 1
    add(f"H1-{n:02d}", "H1", f"order test: days under {label} {code}",
        f'MATCH (d:Day), (r:{label} {{code: "{code}"}}) WHERE subsumes(d, r) RETURN count(d) AS n',
        f'MATCH (d:Day) WHERE d.code STARTS WITH "{code}" RETURN count(d) AS n')
for label, code, size in CAL_TRAVERSE:
    n += 1
    add(f"H1-{n:02d}", "H1", f"order test: days under {label} {code} (code is not a prefix)",
        f'MATCH (d:Day), (r:{label} {{code: "{code}"}}) WHERE subsumes(d, r) RETURN count(d) AS n',
        f'MATCH (d:Day)-[:IN_PERIOD*0..]->(r:{label} {{code: "{code}"}}) RETURN count(d) AS n')
for label, code, size in GEO:
    n += 1
    add(f"H1-{n:02d}", "H1", f"order test: zips under {label} {code}",
        f'MATCH (d:Zip), (r:{label} {{code: "{code}"}}) WHERE subsumes(d, r) RETURN count(d) AS n',
        f'MATCH (d:Zip) WHERE d.code STARTS WITH "{code}" RETURN count(d) AS n')
for code in THREAT[:2]:
    n += 1
    add(f"H1-{n:02d}", "H1", f"order test on a multi-parent DAG: techniques under {code}",
        f'MATCH (d:Technique), (r:Technique {{code: "{code}"}}) WHERE subsumes(d, r) RETURN count(d) AS n',
        f'MATCH (r:Technique {{code: "{code}"}})<-[:MAPS_TO*0..]-(d:Technique) RETURN count(DISTINCT d) AS n')
for code in ["T1", "T2"]:
    n += 1
    add(f"H1-{n:02d}", "H1", f"order test: terms under {code}",
        f'MATCH (d:Term), (r:Term {{code: "{code}"}}) WHERE subsumes(d, r) RETURN count(d) AS n',
        f'MATCH (d:Term) WHERE d.code STARTS WITH "{code}" RETURN count(d) AS n')

# ------------------------------------------------------- H2 single roll-up (index on/off)
n = 0
for code, size in ONTO:
    n += 1
    add(f"H2-{n:02d}", "H2", f"sum over the {code} subtree ({size} nodes)",
        f'MATCH (d)-[:IS_A*0..]->(r:Term {{code: "{code}"}}) RETURN sum(d.units) AS v')
for label, code, size in CAL_PREFIX + CAL_TRAVERSE:
    n += 1
    add(f"H2-{n:02d}", "H2", f"sum over the {label} {code} subtree ({size} nodes)",
        f'MATCH (d)-[:IN_PERIOD*0..]->(r:{label} {{code: "{code}"}}) RETURN sum(d.units) AS v')
for label, code, size in GEO:
    n += 1
    add(f"H2-{n:02d}", "H2", f"sum over the {label} {code} subtree ({size} nodes)",
        f'MATCH (d)-[:LOCATED_IN*0..]->(r:{label} {{code: "{code}"}}) RETURN sum(d.units) AS v')
for code in THREAT:
    n += 1
    add(f"H2-{n:02d}", "H2", f"sum over the multi-parent subtree of {code}",
        f'MATCH (d)-[:MAPS_TO*0..]->(r:Technique {{code: "{code}"}}) RETURN sum(d.units) AS v')
for code in ["T1", "T2", "T3", "T4"]:
    n += 1
    add(f"H2-{n:02d}", "H2", f"sum over the {code} subtree",
        f'MATCH (d)-[:IS_A*0..]->(r:Term {{code: "{code}"}}) RETURN sum(d.units) AS v')
for code, size in ONTO[1:5]:
    n += 1
    add(f"H2-{n:02d}", "H2", f"count over the {code} subtree ({size} nodes)",
        f'MATCH (d)-[:IS_A*0..]->(r:Term {{code: "{code}"}}) RETURN count(d) AS n')

# --------------------------------------------------------------- H3 level roll-up
n = 0
for level in [1, 2, 3, 4]:
    n += 1
    add(f"H3-{n:02d}", "H3", f"roll-up for every ontology node at level {level}",
        f'MATCH (t:Term) WHERE t.level = {level} RETURN t.code AS code, hierarchy_rollup(t, "sum") AS v',
        f'MATCH (t:Term)<-[:IS_A*0..]-(d:Term) WHERE t.level = {level} RETURN t.code AS code, sum(d.units) AS v')
for label, edge in [("Month", "IN_PERIOD"), ("Quarter", "IN_PERIOD"), ("Year", "IN_PERIOD")]:
    n += 1
    add(f"H3-{n:02d}", "H3", f"roll-up for every {label}",
        f'MATCH (t:{label}) RETURN t.code AS code, hierarchy_rollup(t, "sum") AS v',
        f'MATCH (t:{label})<-[:{edge}*0..]-(d) RETURN t.code AS code, sum(d.units) AS v')
for label in ["State", "City"]:
    n += 1
    add(f"H3-{n:02d}", "H3", f"roll-up for every {label}",
        f'MATCH (t:{label}) RETURN t.code AS code, hierarchy_rollup(t, "sum") AS v',
        f'MATCH (t:{label})<-[:LOCATED_IN*0..]-(d) RETURN t.code AS code, sum(d.units) AS v')

# ------------------------------------------------ H4 cross-hierarchy conjunction
n = 0
TWO_AXIS = [
    ("T0", "Y2019M01"), ("T0", "Y2019"), ("T05", "Y2019M01"),
    ("T", "Y2020M06"), ("T1", "Y2021"), ("T05", "Y2019"),
]
for term, month in TWO_AXIS:
    n += 1
    label = "Year" if len(month) == 5 else "Month"
    add(f"H4-{n:02d}", "H4", f"events about anything under {term}, in any period under {month}",
        f'MATCH (e:Event)-[:ABOUT]->(t), (e)-[:ON]->(day), (rt:Term {{code: "{term}"}}), '
        f'(rd:{label} {{code: "{month}"}}) WHERE subsumes(t, rt) AND subsumes(day, rd) '
        f'RETURN count(e) AS n',
        f'MATCH (e:Event)-[:ABOUT]->(t)-[:IS_A*0..]->(rt:Term {{code: "{term}"}}), '
        f'(e)-[:ON]->(day)-[:IN_PERIOD*0..]->(rd:{label} {{code: "{month}"}}) RETURN count(e) AS n')
THREE_AXIS = [
    ("T0", "Y2019", "CO0"), ("T0", "Y2019", "CO0S0"), ("T", "Y2020", "CO1"),
    ("T05", "Y2019", "CO0"), ("T1", "Y2021", "CO2"), ("T", "Y2019", "CO0S0T0"),
]
for term, period, geo in THREE_AXIS:
    n += 1
    glabel = {3: "Country", 5: "State", 7: "City"}[len(geo)]
    add(f"H4-{n:02d}", "H4",
        f"ontology x time x geography: events under {term}, {period}, {geo}",
        f'MATCH (e:Event)-[:ABOUT]->(t), (e)-[:ON]->(day), (e)-[:AT]->(z), '
        f'(rt:Term {{code: "{term}"}}), (rd:Year {{code: "{period}"}}), (rz:{glabel} {{code: "{geo}"}}) '
        f'WHERE subsumes(t, rt) AND subsumes(day, rd) AND subsumes(z, rz) RETURN count(e) AS n',
        f'MATCH (e:Event)-[:ABOUT]->(t)-[:IS_A*0..]->(rt:Term {{code: "{term}"}}), '
        f'(e)-[:ON]->(day)-[:IN_PERIOD*0..]->(rd:Year {{code: "{period}"}}), '
        f'(e)-[:AT]->(z)-[:LOCATED_IN*0..]->(rz:{glabel} {{code: "{geo}"}}) RETURN count(e) AS n')

# ------------------------------------------------------ H5 hierarchy x traversal
n = 0
for code in THREAT:
    n += 1
    add(f"H5-{n:02d}", "H5", f"events using any technique under {code} (DAG axis)",
        f'MATCH (e:Event)-[:USES]->(k), (rk:Technique {{code: "{code}"}}) WHERE subsumes(k, rk) RETURN count(e) AS n',
        f'MATCH (e:Event)-[:USES]->(k)-[:MAPS_TO*0..]->(rk:Technique {{code: "{code}"}}) RETURN count(DISTINCT e) AS n')
for code, _ in ONTO[:4]:
    n += 1
    add(f"H5-{n:02d}", "H5", f"events about anything under {code}, summed by measure",
        f'MATCH (e:Event)-[:ABOUT]->(t), (rt:Term {{code: "{code}"}}) WHERE subsumes(t, rt) RETURN sum(e.units) AS v',
        f'MATCH (e:Event)-[:ABOUT]->(t)-[:IS_A*0..]->(rt:Term {{code: "{code}"}}) RETURN sum(e.units) AS v')
for code in ["CO0", "CO1"]:
    n += 1
    add(f"H5-{n:02d}", "H5", f"events located anywhere under {code}",
        f'MATCH (e:Event)-[:AT]->(z), (rz:Country {{code: "{code}"}}) WHERE subsumes(z, rz) RETURN count(e) AS n',
        f'MATCH (e:Event)-[:AT]->(z)-[:LOCATED_IN*0..]->(rz:Country {{code: "{code}"}}) RETURN count(e) AS n')

# --------------------------------------------------------- H6 anti-subsumption
n = 0
for code, _ in ONTO[:5]:
    n += 1
    add(f"H6-{n:02d}", "H6", f"terms NOT under {code}",
        f'MATCH (d:Term), (r:Term {{code: "{code}"}}) WHERE NOT subsumes(d, r) RETURN count(d) AS n',
        f'MATCH (d:Term) WHERE NOT (d.code STARTS WITH "{code}") RETURN count(d) AS n')
DIFFS = [("T0", "T01"), ("T0", "T05"), ("T", "T0"), ("T05", "T053"), ("T1", "T12")]
for outer, inner in DIFFS:
    n += 1
    add(f"H6-{n:02d}", "H6", f"set difference: under {outer} but not under {inner}",
        f'MATCH (d:Term), (r:Term {{code: "{outer}"}}), (s:Term {{code: "{inner}"}}) '
        f'WHERE subsumes(d, r) AND NOT subsumes(d, s) RETURN count(d) AS n',
        f'MATCH (d:Term) WHERE (d.code STARTS WITH "{outer}") AND NOT (d.code STARTS WITH "{inner}") '
        f'RETURN count(d) AS n')

# --------------------------------------------------------------------- H7 LCA
n = 0
PAIRS = [
    ("T012", "T034"), ("T0120", "T0121"), ("T01", "T02"), ("T0", "T1"),
    ("T01234", "T01235"), ("T0123", "T0143"), ("T5", "T0"), ("T012", "T012"),
    ("T00000", "T55555"), ("T010", "T011"),
]
for a, b in PAIRS:
    n += 1
    add(f"H7-{n:02d}", "H7", f"lowest common ancestor of {a} and {b}",
        f'MATCH (a:Term {{code: "{a}"}}), (b:Term {{code: "{b}"}}), (c:Term) '
        f'WHERE id(c) IN hierarchy_lca(a, b) RETURN max(c.level) AS lvl',
        f'MATCH (a:Term {{code: "{a}"}})-[:IS_A*0..]->(c:Term)<-[:IS_A*0..]-(b:Term {{code: "{b}"}}) '
        f'RETURN max(c.level) AS lvl')

# ------------------------------------------------------- H8 top-k over roll-up
# ORDER BY without LIMIT: `ORDER BY sum(...) DESC` does not sort in this engine (issue
# #345), so a LIMIT would truncate an unsorted result and the two sides of the comparison
# would disagree for reasons that have nothing to do with the index. The class exists to
# measure roll-up called in a loop, which ordering the full result still does.
n = 0
for level in [1, 2, 3]:
    n += 1
    add(f"H8-{n:02d}", "H8", f"rank level-{level} ontology subtrees by rolled-up measure",
        f'MATCH (t:Term) WHERE t.level = {level} RETURN t.code AS code, hierarchy_rollup(t, "sum") AS v '
        f'ORDER BY hierarchy_rollup(t, "sum") DESC',
        f'MATCH (t:Term)<-[:IS_A*0..]-(d:Term) WHERE t.level = {level} RETURN t.code AS code, '
        f'sum(d.units) AS v ORDER BY sum(d.units) DESC')
for label, edge in [("Month", "IN_PERIOD"), ("Quarter", "IN_PERIOD"), ("Year", "IN_PERIOD")]:
    n += 1
    add(f"H8-{n:02d}", "H8", f"rank every {label} by rolled-up measure",
        f'MATCH (t:{label}) RETURN t.code AS code, hierarchy_rollup(t, "sum") AS v '
        f'ORDER BY hierarchy_rollup(t, "sum") DESC',
        f'MATCH (t:{label})<-[:{edge}*0..]-(d) RETURN t.code AS code, sum(d.units) AS v '
        f'ORDER BY sum(d.units) DESC')
for label in ["State", "City"]:
    n += 1
    add(f"H8-{n:02d}", "H8", f"rank every {label} by rolled-up measure",
        f'MATCH (t:{label}) RETURN t.code AS code, hierarchy_rollup(t, "sum") AS v '
        f'ORDER BY hierarchy_rollup(t, "sum") DESC',
        f'MATCH (t:{label})<-[:LOCATED_IN*0..]-(d) RETURN t.code AS code, sum(d.units) AS v '
        f'ORDER BY sum(d.units) DESC')

# ---------------------------------------------- H9 hierarchy-filtered vector search
# Unblocked by samyama-graph#439: a CALL's YIELD variables are now in scope for a
# following MATCH's WHERE, which is what this composition needs. The ANN call and the
# subsumption predicate always worked in isolation; only joining them was missing.
for i, code in enumerate(["T0", "T05", "T1", "T"], start=1):
    add(f"H9-{i:02d}", "H9", f"200-NN vector search restricted to the {code} subtree",
        f'CALL db.index.vector.queryNodes("Term", "emb", [0.5, 0.5, 0.5, 0.5], 200) YIELD node, score '
        f'MATCH (r:Term {{code: "{code}"}}) WHERE subsumes(node, r) RETURN count(node) AS n')

# ------------------------------------------------------ H10 temporal roll-up windows
n = 0
for year in [2019, 2020, 2021, 2022]:
    n += 1
    add(f"H10-{n:02d}", "H10", f"monthly roll-up window across {year}",
        f'MATCH (m:Month) WHERE m.y = {year} RETURN m.code AS code, hierarchy_rollup(m, "sum") AS v',
        f'MATCH (m:Month)<-[:IN_PERIOD*0..]-(d) WHERE m.y = {year} RETURN m.code AS code, sum(d.units) AS v')
for year in [2019, 2020, 2021]:
    n += 1
    add(f"H10-{n:02d}", "H10", f"quarterly roll-up window across {year}",
        f'MATCH (q:Quarter) WHERE q.y = {year} RETURN q.code AS code, hierarchy_rollup(q, "sum") AS v',
        f'MATCH (q:Quarter)<-[:IN_PERIOD*0..]-(d) WHERE q.y = {year} RETURN q.code AS code, sum(d.units) AS v')
for year in [2019, 2020, 2021]:
    n += 1
    add(f"H10-{n:02d}", "H10", f"whole-year roll-up for {year}",
        f'MATCH (d)-[:IN_PERIOD*0..]->(r:Year {{code: "Y{year}"}}) RETURN sum(d.units) AS v')

corpus = {
    "corpus": "HIER — hierarchy-heavy complex queries",
    "adr": "ADR-035",
    "paper": "arXiv:2606.24677",
    "generated_by": "benchmarks/hier/generate_corpus.py",
    "classes": {
        "H1": "order test (is x under y?)",
        "H2": "single roll-up over a subtree, sizes spanning four orders of magnitude",
        "H3": "level roll-up (group by hierarchy level)",
        "H4": "cross-hierarchy conjunction: ontology x time x geography in one query",
        "H5": "hierarchy predicate composed with graph traversal",
        "H6": "anti-subsumption and subtree set difference",
        "H7": "lowest common ancestor",
        "H8": "top-k over roll-up (roll-up called in a loop)",
        "H9": "hierarchy-filtered vector search",
        "H10": "temporal roll-up windows",
    },
    "queries": Q,
}
with open("benchmarks/hier/queries.json", "w") as f:
    json.dump(corpus, f, indent=1)
    f.write("\n")

runnable = sum(1 for q in Q if "skip" not in q)
print(f"{len(Q)} queries ({runnable} runnable, {len(Q)-runnable} blocked)")
from collections import Counter
for cls, count in sorted(Counter(q["class"] for q in Q).items()):
    print(f"  {cls}: {count}")
