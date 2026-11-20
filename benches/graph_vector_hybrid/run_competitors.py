#!/usr/bin/env python3
"""Run the graph + vector hybrid templates (#1987) on Neo4j 5 and on a
Qdrant + Neo4j two-system pipeline, over the data the Rust bench exports.

    cargo bench --bench graph_vector_hybrid_benchmark -- --export data/hybrid
    docker compose -f benches/graph_vector_hybrid/docker-compose.yml up -d
    pip install -r benches/graph_vector_hybrid/requirements.txt
    python3 benches/graph_vector_hybrid/run_competitors.py --data data/hybrid --csv competitors.csv

Systems:

  neo4j         the exported Cypher text, unchanged, against Neo4j's vector
                index. One round trip per query.
  qdrant+neo4j  vectors live only in Qdrant and the graph only in Neo4j; the
                client glues them. Timed end to end, round trips included:
                  seeded_expansion    Qdrant k-NN -> Neo4j 2-hop CITES expansion
                                      -> Qdrant search restricted to those ids
                  traversal_filtered  Neo4j 2-hop COAUTHOR traversal -> Qdrant
                                      search restricted to the papers reached
                  vector_join         Neo4j linked problems + Qdrant method vector
                                      -> Qdrant search excluding the linked ones

Every answer is scored against the exported brute-force reference: recall
(share of the reference returned) and exact (same set). Latency is per query,
client-side, single client, sequential; QPS is queries over the wall time of
the timed pass. Not run in CI; numbers depend on the host and on Docker.
"""

import argparse
import csv
import json
import math
import os
import sys
import time

try:
    from neo4j import GraphDatabase
    from qdrant_client import QdrantClient
    from qdrant_client.models import (
        Distance,
        Filter,
        HasIdCondition,
        PointStruct,
        VectorParams,
    )
except ImportError as e:  # pragma: no cover - environment check
    sys.exit(f"{e}: pip install -r benches/graph_vector_hybrid/requirements.txt")

BATCH = 2000

PIPELINE_EXPAND = (
    "MATCH (seed:Paper) WHERE seed.uid IN $seeds "
    "MATCH (seed)-[:CITES*0..2]-(c:Paper) "
    "RETURN DISTINCT c.uid AS uid"
)
PIPELINE_COAUTHORS = (
    "MATCH (x:Person {uid: $person})-[:COAUTHOR*0..2]-(a:Person)-[:AUTHORED]->(p:Paper) "
    "RETURN DISTINCT p.uid AS uid"
)
PIPELINE_LINKED = (
    "MATCH (m:Method {uid: $method})<-[:USES_METHOD]-(:Paper)-[:ADDRESSES]->(p:Problem) "
    "RETURN DISTINCT p.uid AS uid"
)


def read_vectors(path):
    with open(path, newline="") as f:
        rows = list(csv.DictReader(f))
    return [
        {"uid": int(r["uid"]), "emb": [float(x) for x in r["emb"].split(";")]}
        for r in rows
        if r["emb"]
    ]


def read_edges(path):
    with open(path, newline="") as f:
        return [{"src": int(r["src"]), "dst": int(r["dst"])} for r in csv.DictReader(f)]


def batches(items):
    for i in range(0, len(items), BATCH):
        yield items[i : i + BATCH]


# ------------------------------------------------------------------ loading


def load_neo4j(session, data, meta):
    session.run("MATCH (n) CALL { WITH n DETACH DELETE n } IN TRANSACTIONS OF 10000 ROWS").consume()
    for label in ("Paper", "Person", "Method", "Problem"):
        session.run(
            f"CREATE CONSTRAINT {label.lower()}_uid IF NOT EXISTS "
            f"FOR (n:{label}) REQUIRE n.uid IS UNIQUE"
        ).consume()
    for label, file in (("Paper", "papers.csv"), ("Method", "methods.csv"), ("Problem", "problems.csv")):
        for chunk in batches(read_vectors(os.path.join(data, file))):
            session.run(
                f"UNWIND $rows AS r CREATE (:{label} {{uid: r.uid, emb: r.emb}})", rows=chunk
            ).consume()
    for chunk in batches([{"uid": i} for i in range(meta["people"])]):
        session.run("UNWIND $rows AS r CREATE (:Person {uid: r.uid})", rows=chunk).consume()
    for rel, file, a, b in (
        ("CITES", "cites.csv", "Paper", "Paper"),
        ("AUTHORED", "authored.csv", "Person", "Paper"),
        ("COAUTHOR", "coauthor.csv", "Person", "Person"),
        ("USES_METHOD", "uses_method.csv", "Paper", "Method"),
        ("ADDRESSES", "addresses.csv", "Paper", "Problem"),
    ):
        for chunk in batches(read_edges(os.path.join(data, file))):
            session.run(
                f"UNWIND $rows AS r MATCH (a:{a} {{uid: r.src}}), (b:{b} {{uid: r.dst}}) "
                f"CREATE (a)-[:{rel}]->(b)",
                rows=chunk,
            ).consume()
    session.run(
        "CREATE VECTOR INDEX paper_emb IF NOT EXISTS FOR (p:Paper) ON (p.emb) "
        f"OPTIONS {{indexConfig: {{`vector.dimensions`: {int(meta['dim'])}, "
        "`vector.similarity_function`: 'cosine'}}"
    ).consume()
    session.run("CALL db.awaitIndexes(3600)").consume()


def load_qdrant(client, data, meta):
    for name, file in (("papers", "papers.csv"), ("methods", "methods.csv"), ("problems", "problems.csv")):
        rows = read_vectors(os.path.join(data, file))
        if client.collection_exists(name):
            client.delete_collection(name)
        client.create_collection(
            name, vectors_config=VectorParams(size=int(meta["dim"]), distance=Distance.COSINE)
        )
        for chunk in batches(rows):
            client.upsert(
                name,
                points=[PointStruct(id=r["uid"], vector=r["emb"]) for r in chunk],
                wait=True,
            )


# ------------------------------------------------------------------ the systems


def uids(result):
    return [r["uid"] for r in result]


def neo4j_query(session, cypher):
    def run(params):
        return uids(session.run(cypher, params))

    return run


def qdrant_ids(client, collection, vector, k, only=None, exclude=None):
    flt = None
    if only is not None:
        flt = Filter(must=[HasIdCondition(has_id=list(only))])
    elif exclude:
        flt = Filter(must_not=[HasIdCondition(has_id=list(exclude))])
    hits = client.query_points(collection, query=vector, limit=k, query_filter=flt).points
    return [int(h.id) for h in hits]


def pipeline(template, session, client, spec):
    k = spec["k"]
    if template == "seeded_expansion":
        seeds_k = spec["seeds"]

        def run(params):
            seeds = qdrant_ids(client, "papers", params["q"], seeds_k)
            reached = uids(session.run(PIPELINE_EXPAND, {"seeds": seeds}))
            return qdrant_ids(client, "papers", params["q"], k, only=reached) if reached else []

    elif template == "traversal_filtered":

        def run(params):
            reached = uids(session.run(PIPELINE_COAUTHORS, {"person": params["person"]}))
            return qdrant_ids(client, "papers", params["q"], k, only=reached) if reached else []

    else:

        def run(params):
            linked = uids(session.run(PIPELINE_LINKED, {"method": params["method"]}))
            vector = client.retrieve("methods", ids=[params["method"]], with_vectors=True)[0].vector
            return qdrant_ids(client, "problems", vector, k, exclude=linked)

    return run


# ------------------------------------------------------------------ measuring


def params_of(template, inst):
    if template == "seeded_expansion":
        return {"q": inst["q"]}
    if template == "traversal_filtered":
        return {"person": inst["person"], "q": inst["q"]}
    return {"method": inst["method"]}


def percentile(sorted_ms, p):
    if not sorted_ms:
        return 0.0
    rank = math.ceil(p / 100.0 * len(sorted_ms))
    return sorted_ms[min(max(rank, 1), len(sorted_ms)) - 1]


def measure(run, template, instances, warmup):
    for inst in instances[:warmup]:
        run(params_of(template, inst))
    times, recall, exact, rows = [], 0.0, 0, 0
    started = time.perf_counter()
    for inst in instances:
        at = time.perf_counter()
        got = run(params_of(template, inst))
        times.append((time.perf_counter() - at) * 1000.0)
        want = set(inst["reference"])
        got_set = set(got)
        recall += (len(got_set & want) / len(want)) if want else (1.0 if not got_set else 0.0)
        exact += int(got_set == want and len(got) == len(inst["reference"]))
        rows += len(got)
    elapsed = time.perf_counter() - started
    times.sort()
    n = max(len(instances), 1)
    return {
        "queries": len(instances),
        "recall": recall / n,
        "exact": exact,
        "qps": len(instances) / elapsed if elapsed > 0 else 0.0,
        "p50_ms": percentile(times, 50),
        "p99_ms": percentile(times, 99),
        "mean_rows": rows / n,
    }


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--data", required=True, help="directory written by the bench's --export")
    ap.add_argument("--neo4j-uri", default="bolt://localhost:7687")
    ap.add_argument("--neo4j-user", default="neo4j")
    ap.add_argument("--neo4j-password", default="benchpassword")
    ap.add_argument("--qdrant-url", default="http://localhost:6333")
    ap.add_argument("--systems", default="neo4j,qdrant+neo4j")
    ap.add_argument("--skip-load", action="store_true", help="reuse what the servers already hold")
    ap.add_argument("--warmup", type=int, default=10)
    ap.add_argument("--csv", help="write the results to this CSV file")
    args = ap.parse_args()

    with open(os.path.join(args.data, "meta.json")) as f:
        meta = json.load(f)
    with open(os.path.join(args.data, "queries.json")) as f:
        queries = json.load(f)
    systems = [s.strip() for s in args.systems.split(",") if s.strip()]
    unknown = set(systems) - {"neo4j", "qdrant+neo4j"}
    if unknown:
        sys.exit(f"unknown systems: {', '.join(sorted(unknown))}")

    driver = GraphDatabase.driver(args.neo4j_uri, auth=(args.neo4j_user, args.neo4j_password))
    client = QdrantClient(url=args.qdrant_url) if "qdrant+neo4j" in systems else None
    results = []
    with driver.session(database="neo4j") as session:
        if not args.skip_load:
            t = time.perf_counter()
            load_neo4j(session, args.data, meta)
            print(f"neo4j load + index: {time.perf_counter() - t:.1f} s")
            if client is not None:
                t = time.perf_counter()
                load_qdrant(client, args.data, meta)
                print(f"qdrant load: {time.perf_counter() - t:.1f} s")
        for system in systems:
            for template, spec in queries.items():
                instances = spec["instances"]
                if not instances:
                    print(f"{system} {template}: no instances, skipped")
                    continue
                if system == "neo4j":
                    run = neo4j_query(session, spec["cypher"])
                else:
                    run = pipeline(template, session, client, spec)
                r = measure(run, template, instances, args.warmup)
                r.update(system=system, template=template)
                results.append(r)
    driver.close()

    print(
        f"\n{'system':<14} {'template':<20} {'queries':>7} {'recall':>8} {'exact':>9} "
        f"{'qps':>9} {'p50_ms':>9} {'p99_ms':>9} {'rows':>6}"
    )
    for r in results:
        print(
            f"{r['system']:<14} {r['template']:<20} {r['queries']:>7} {r['recall']:>8.4f} "
            f"{str(r['exact']) + '/' + str(r['queries']):>9} {r['qps']:>9.1f} "
            f"{r['p50_ms']:>9.3f} {r['p99_ms']:>9.3f} {r['mean_rows']:>6.1f}"
        )
    if args.csv:
        cols = ["system", "papers", "people", "dim", "template", "queries", "recall", "exact",
                "qps", "p50_ms", "p99_ms", "mean_rows"]
        with open(args.csv, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=cols)
            w.writeheader()
            for r in results:
                w.writerow({**r, "papers": meta["papers"], "people": meta["people"], "dim": meta["dim"]})


if __name__ == "__main__":
    main()
