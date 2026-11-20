#!/usr/bin/env python3
"""Run retrieval_quality_benchmark's configurations against Elasticsearch,
OpenSearch, Weaviate and Qdrant (#1988).

Reads the same prepared BEIR directory as the Rust bench
(scripts/prepare_beir_datasets.py), loads it into each system, runs the same
queries and writes the same CSV columns, so the rows can be concatenated with
the bench's. Not run in CI: it needs the services from docker-compose.yml.

    docker compose -f benches/retrieval_quality/docker-compose.yml up -d
    python3 benches/retrieval_quality/competitors.py --dataset data/beir/scifact \\
        --systems elasticsearch,opensearch,weaviate,qdrant --csv competitors.csv

What is the same for every system, Samyama included:
  - the document text (title + " " + text) and the vectors (one model, dense/);
  - the query text: its alphanumeric runs joined by spaces, so no system's
    query syntax is triggered by a stray quote or asterisk;
  - HNSW m / ef_construction, and the search width swept as `ef`;
  - retrieval depth, the metrics (formulas identical to benches/retrieval_common)
    and the RRF fusion, which is done HERE, client-side, from each system's own
    BM25 and dense lists.

What is not the same, and is the system under test: each system's analyzer
(Elasticsearch/OpenSearch `english` by default, Weaviate `word` tokenisation
without stemming, Samyama lowercase + a light suffix stemmer) and its HNSW.
Qdrant has no BM25 of its own -- it ships sparse vectors and leaves the BM25
term weighting to the client -- so it reports dense rows only.

What `ef` means per system: Elasticsearch `num_candidates`, OpenSearch
`method_parameters.ef_search`, Weaviate the class's `ef`, Qdrant `hnsw_ef`.

Latency is one client over HTTP on a persistent connection, so it includes
JSON encoding and the loopback network; the Rust bench's in-process latency
does not. Compare quality columns across systems freely; compare latency only
with that in mind.

Needs `numpy`.
"""

import argparse
import http.client
import json
import math
import os
import re
import sys
import time

CSV_HEADER = (
    "dataset,system,config,path,embedding_model,metric,corpus,queries,k,depth,"
    "m,ef_construction,ef,rrf_k,ndcg_at_k,recall_at_depth,mrr_at_k,p50_us,p99_us,mean_us,errors,calibration_ms"
)

# ---------------------------------------------------------------- dataset
# Mirrors benches/retrieval_common and the loader in
# benches/retrieval_quality_benchmark.rs. Keep them in step.


def parse_beir_line(line):
    if not line.strip():
        return None
    row = json.loads(line)
    title = (row.get("title") or "").strip()
    text = (row.get("text") or "").strip()
    return str(row["_id"]), " ".join(t for t in (title, text) if t)


def read_jsonl(path):
    with open(path, encoding="utf-8") as f:
        return [r for r in (parse_beir_line(l) for l in f) if r is not None]


def parse_qrels(path):
    out = {}
    with open(path, encoding="utf-8") as f:
        for n, line in enumerate(f):
            line = line.rstrip("\r\n")
            if not line.strip():
                continue
            cols = line.split("\t")
            if len(cols) != 3:
                sys.exit(f"{path} line {n + 1}: expected 3 tab-separated columns")
            try:
                grade = int(cols[2].strip())
            except ValueError:
                if n == 0:
                    continue
                sys.exit(f"{path} line {n + 1}: grade {cols[2]!r} is not an integer")
            out.setdefault(cols[0].strip(), {})[cols[1].strip()] = grade
    return out


_WORD = re.compile(r"[^\W_]+")


def bm25_query_text(q):
    return " ".join(_WORD.findall(q))


def read_fvecs(path):
    import numpy as np

    raw = np.fromfile(path, dtype="<i4")
    if raw.size == 0:
        return np.zeros((0, 0), dtype="<f4")
    d = int(raw[0])
    rows = raw.reshape(-1, d + 1)
    if (rows[:, 0] != d).any():
        sys.exit(f"{path}: records have different dimensions")
    return rows[:, 1:].copy().view("<f4")


def read_ids(path):
    with open(path, encoding="utf-8") as f:
        return [l.rstrip("\r\n") for l in f if l.strip()]


class Dataset:
    def __init__(self, d, split, limit):
        self.name = os.path.basename(os.path.normpath(d))
        corpus = read_jsonl(os.path.join(d, "corpus.jsonl"))
        self.doc_ids = [i for i, _ in corpus]
        self.doc_texts = [t for _, t in corpus]
        qtext = dict(read_jsonl(os.path.join(d, "queries.jsonl")))
        self.qrels = parse_qrels(os.path.join(d, "qrels", f"{split}.tsv"))
        scored = sorted(q for q, j in self.qrels.items() if any(g > 0 for g in j.values()))
        self.queries = [(q, qtext[q]) for q in scored if q in qtext][: limit or None]
        self.dense = None
        dd = os.path.join(d, "dense")
        if os.path.exists(os.path.join(dd, "model.txt")):
            import numpy as np

            dv = read_fvecs(os.path.join(dd, "corpus.fvecs"))
            at = {i: n for n, i in enumerate(read_ids(os.path.join(dd, "corpus_ids.txt")))}
            docs = dv[[at[i] for i in self.doc_ids]]
            qv = read_fvecs(os.path.join(dd, "queries.fvecs"))
            qat = {i: n for n, i in enumerate(read_ids(os.path.join(dd, "query_ids.txt")))}
            qs = qv[[qat[q] for q, _ in self.queries]]
            with open(os.path.join(dd, "metric.txt")) as f:
                metric = f.read().strip()
            with open(os.path.join(dd, "model.txt")) as f:
                model = f.read().strip()
            self.dense = dict(model=model, metric=metric, docs=np.asarray(docs), queries=np.asarray(qs))


# ---------------------------------------------------------------- metrics


def ndcg_at_k(ranked, judged, k):
    dcg = sum(max(judged.get(d, 0), 0) / math.log2(i + 2) for i, d in enumerate(ranked[:k]))
    ideal = sorted((g for g in judged.values() if g > 0), reverse=True)[:k]
    idcg = sum(g / math.log2(i + 2) for i, g in enumerate(ideal))
    return dcg / idcg if idcg else 0.0


def recall_at_k(ranked, judged, k):
    relevant = sum(1 for g in judged.values() if g > 0)
    if not relevant:
        return 0.0
    return len({d for d in ranked[:k] if judged.get(d, 0) > 0}) / relevant


def mrr_at_k(ranked, judged, k):
    for i, d in enumerate(ranked[:k]):
        if judged.get(d, 0) > 0:
            return 1.0 / (i + 1)
    return 0.0


def rrf(lists, k, depth):
    acc = {}
    order = 0
    for lst in lists:
        seen = set()
        for i, d in enumerate(lst):
            if d in seen:
                continue
            seen.add(d)
            if d not in acc:
                order += 1
                acc[d] = [0.0, i + 1, order]
            acc[d][0] += 1.0 / (k + i + 1)
            acc[d][1] = min(acc[d][1], i + 1)
    return [d for d, _ in sorted(acc.items(), key=lambda e: (-e[1][0], e[1][1], e[1][2]))[:depth]]


def percentile(sorted_vals, p):
    if not sorted_vals:
        return None
    rank = math.ceil(p / 100.0 * len(sorted_vals))
    return sorted_vals[min(max(rank, 1), len(sorted_vals)) - 1]


# ---------------------------------------------------------------- HTTP


class Http:
    """One persistent connection; latency is measured around `call`."""

    def __init__(self, host, port):
        self.host, self.port = host, port
        self.conn = http.client.HTTPConnection(host, port, timeout=600)

    def call(self, method, path, body=None, ndjson=False):
        if body is None:
            data, ctype = None, "application/json"
        elif ndjson:
            data, ctype = body.encode(), "application/x-ndjson"
        else:
            data, ctype = json.dumps(body).encode(), "application/json"
        for attempt in (0, 1):
            try:
                self.conn.request(method, path, body=data, headers={"Content-Type": ctype})
                r = self.conn.getresponse()
                raw = r.read()
                break
            except (http.client.HTTPException, ConnectionError, OSError):
                self.conn.close()
                self.conn = http.client.HTTPConnection(self.host, self.port, timeout=600)
                if attempt:
                    raise
        if r.status >= 400:
            raise RuntimeError(f"{method} {path}: HTTP {r.status}: {raw[:500]!r}")
        return json.loads(raw) if raw else None

    def wait(self, path, seconds=180):
        deadline = time.time() + seconds
        while True:
            try:
                return self.call("GET", path)
            except Exception:
                if time.time() > deadline:
                    raise
                time.sleep(2)


# ---------------------------------------------------------------- systems
# Each exposes: name, load(ds, a), bm25(text, depth) -> ids or None,
# set_ef(ef), dense(vec, depth, ef) -> ids, exact(vec, depth) -> ids or None.


class Elastic:
    space = {"cosine": "cosine", "dot": "dot_product", "l2": "l2_norm"}

    def __init__(self, name, port, analyzer):
        self.name, self.http, self.analyzer = name, Http("localhost", port), analyzer
        self.index = "beir_rq"

    def mapping(self, ds, a):
        props = {"text": {"type": "text", "analyzer": self.analyzer}}
        if ds.dense is not None:
            props["embedding"] = {
                "type": "dense_vector",
                "dims": int(ds.dense["docs"].shape[1]),
                "index": True,
                "similarity": self.space[ds.dense["metric"]],
                # float HNSW: 8.15's default is int8_hnsw, a quantized index.
                "index_options": {"type": "hnsw", "m": a.m, "ef_construction": a.ef_construction},
            }
        return {"settings": {"number_of_shards": 1, "number_of_replicas": 0, "refresh_interval": "-1"},
                "mappings": {"properties": props}}

    def load(self, ds, a):
        self.http.wait("/_cluster/health?wait_for_status=yellow&timeout=60s")
        try:
            self.http.call("DELETE", f"/{self.index}")
        except RuntimeError:
            pass
        self.http.call("PUT", f"/{self.index}", self.mapping(ds, a))
        batch = []
        for i, (did, text) in enumerate(zip(ds.doc_ids, ds.doc_texts)):
            doc = {"text": text}
            if ds.dense is not None:
                doc["embedding"] = ds.dense["docs"][i].tolist()
            batch.append(json.dumps({"index": {"_index": self.index, "_id": did}}))
            batch.append(json.dumps(doc))
            if len(batch) >= 1000:
                self.bulk(batch)
                batch = []
        if batch:
            self.bulk(batch)
        self.http.call("POST", f"/{self.index}/_refresh")
        # One segment, so HNSW search is one graph rather than one per segment.
        self.http.call("POST", f"/{self.index}/_forcemerge?max_num_segments=1")
        self.http.call("POST", f"/{self.index}/_refresh")

    def bulk(self, lines):
        r = self.http.call("POST", "/_bulk", "\n".join(lines) + "\n", ndjson=True)
        if r.get("errors"):
            raise RuntimeError(f"{self.name}: bulk load reported errors")

    def ids(self, r):
        return [h["_id"] for h in r["hits"]["hits"]]

    def bm25(self, text, depth):
        return self.ids(self.http.call("POST", f"/{self.index}/_search", {
            "size": depth, "_source": False, "query": {"match": {"text": text}}}))

    def set_ef(self, ef):
        pass

    def dense(self, vec, depth, ef):
        return self.ids(self.http.call("POST", f"/{self.index}/_search", {
            "size": depth, "_source": False,
            "knn": {"field": "embedding", "query_vector": vec, "k": depth,
                    "num_candidates": min(max(ef, depth), 10000)}}))

    def exact(self, vec, depth):
        return None


class OpenSearch(Elastic):
    space = {"cosine": "cosinesimil", "dot": "innerproduct", "l2": "l2"}

    def mapping(self, ds, a):
        props = {"text": {"type": "text", "analyzer": self.analyzer}}
        if ds.dense is not None:
            props["embedding"] = {
                "type": "knn_vector",
                "dimension": int(ds.dense["docs"].shape[1]),
                "method": {"name": "hnsw", "engine": "lucene", "space_type": self.space[ds.dense["metric"]],
                           "parameters": {"m": a.m, "ef_construction": a.ef_construction}},
            }
        return {"settings": {"number_of_shards": 1, "number_of_replicas": 0, "refresh_interval": "-1",
                             "index.knn": True},
                "mappings": {"properties": props}}

    def dense(self, vec, depth, ef):
        return self.ids(self.http.call("POST", f"/{self.index}/_search", {
            "size": depth, "_source": False,
            "query": {"knn": {"embedding": {"vector": vec, "k": depth,
                                            "method_parameters": {"ef_search": ef}}}}}))


class Weaviate:
    cls = "BeirRq"
    space = {"cosine": "cosine", "dot": "dot", "l2": "l2-squared"}

    def __init__(self, port):
        self.name, self.http = "weaviate", Http("localhost", port)

    def load(self, ds, a):
        self.http.wait("/v1/.well-known/ready")
        try:
            self.http.call("DELETE", f"/v1/schema/{self.cls}")
        except RuntimeError:
            pass
        metric = ds.dense["metric"] if ds.dense is not None else "cosine"
        self.http.call("POST", "/v1/schema", {
            "class": self.cls,
            "vectorizer": "none",
            "vectorIndexType": "hnsw",
            "vectorIndexConfig": {"distance": self.space[metric], "maxConnections": a.m,
                                  "efConstruction": a.ef_construction, "ef": a.ef[0]},
            "invertedIndexConfig": {"bm25": {"k1": 1.2, "b": 0.75}},
            "properties": [
                {"name": "doc_id", "dataType": ["text"], "tokenization": "field", "indexSearchable": False},
                {"name": "text", "dataType": ["text"], "tokenization": "word"},
            ],
        })
        objs = []
        for i, (did, text) in enumerate(zip(ds.doc_ids, ds.doc_texts)):
            o = {"class": self.cls, "properties": {"doc_id": did, "text": text}}
            if ds.dense is not None:
                o["vector"] = ds.dense["docs"][i].tolist()
            objs.append(o)
            if len(objs) >= 500:
                self.batch(objs)
                objs = []
        if objs:
            self.batch(objs)

    def batch(self, objs):
        r = self.http.call("POST", "/v1/batch/objects", {"objects": objs})
        bad = [x for x in r if x.get("result", {}).get("errors")]
        if bad:
            raise RuntimeError(f"weaviate: batch import errors: {bad[0]['result']['errors']}")

    def get(self, args, depth):
        q = f"{{ Get {{ {self.cls}({args}, limit: {depth}) {{ doc_id }} }} }}"
        r = self.http.call("POST", "/v1/graphql", {"query": q})
        if r.get("errors"):
            raise RuntimeError(f"weaviate: {r['errors']}")
        return [o["doc_id"] for o in r["data"]["Get"][self.cls]]

    def bm25(self, text, depth):
        # `text` holds only alphanumerics and spaces, so it is a safe literal.
        return self.get(f'bm25: {{query: "{text}", properties: ["text"]}}', depth)

    def set_ef(self, ef):
        c = self.http.call("GET", f"/v1/schema/{self.cls}")
        c["vectorIndexConfig"]["ef"] = ef
        self.http.call("PUT", f"/v1/schema/{self.cls}", c)

    def dense(self, vec, depth, ef):
        return self.get(f"nearVector: {{vector: {json.dumps(vec)}}}", depth)

    def exact(self, vec, depth):
        return None


class Qdrant:
    coll = "beir_rq"
    space = {"cosine": "Cosine", "dot": "Dot", "l2": "Euclid"}

    def __init__(self, port):
        self.name, self.http = "qdrant", Http("localhost", port)

    def load(self, ds, a):
        if ds.dense is None:
            raise RuntimeError("qdrant: the dataset has no dense/ vectors and Qdrant has no BM25")
        self.http.wait("/readyz")
        try:
            self.http.call("DELETE", f"/collections/{self.coll}")
        except RuntimeError:
            pass
        self.http.call("PUT", f"/collections/{self.coll}", {
            "vectors": {"size": int(ds.dense["docs"].shape[1]), "distance": self.space[ds.dense["metric"]]},
            # Below these thresholds (KB) Qdrant answers by full scan and the
            # dense row would be exact search, not HNSW. Set low so small BEIR
            # sets are searched through the graph, as on every other system.
            "hnsw_config": {"m": a.m, "ef_construct": a.ef_construction, "full_scan_threshold": 10},
            "optimizers_config": {"indexing_threshold": 10},
        })
        points = []
        for i, did in enumerate(ds.doc_ids):
            points.append({"id": i, "vector": ds.dense["docs"][i].tolist(), "payload": {"doc_id": did}})
            if len(points) >= 500:
                self.http.call("PUT", f"/collections/{self.coll}/points?wait=true", {"points": points})
                points = []
        if points:
            self.http.call("PUT", f"/collections/{self.coll}/points?wait=true", {"points": points})
        # Wait for the optimiser to finish building the index.
        while self.http.call("GET", f"/collections/{self.coll}")["result"]["status"] != "green":
            time.sleep(2)

    def bm25(self, text, depth):
        return None

    def set_ef(self, ef):
        pass

    def search(self, vec, depth, params):
        r = self.http.call("POST", f"/collections/{self.coll}/points/search", {
            "vector": vec, "limit": depth, "params": params, "with_payload": ["doc_id"]})
        return [p["payload"]["doc_id"] for p in r["result"]]

    def dense(self, vec, depth, ef):
        return self.search(vec, depth, {"hnsw_ef": ef, "exact": False})

    def exact(self, vec, depth):
        return self.search(vec, depth, {"exact": True})


# ---------------------------------------------------------------- runner


def timed(n, search):
    for i in range(min(n, 20)):
        try:
            search(i)
        except Exception:
            pass
    lat, ans = [], []
    for i in range(n):
        t = time.perf_counter()
        try:
            got = search(i)
        except Exception as e:  # scored as 0 and counted, as the Rust bench does
            print(f"  query {i}: {e}", file=sys.stderr)
            got = None
        lat.append(time.perf_counter() - t)
        ans.append(got)
    return lat, ans


def fuse(a, b, rrf_k, depth):
    lat, ans = [], []
    for i in range(len(a[1])):
        t = time.perf_counter()
        got = rrf([a[1][i], b[1][i]], rrf_k, depth) if a[1][i] is not None and b[1][i] is not None else None
        took = time.perf_counter() - t
        lat.append(None if a[0][i] is None or b[0][i] is None else a[0][i] + b[0][i] + took)
        ans.append(got)
    return lat, ans


def score(ds, a, run):
    lat, ans = run
    nd, rc, mr, errors = [], [], [], 0
    for i, (qid, _) in enumerate(ds.queries):
        j = ds.qrels[qid]
        if ans[i] is None:
            errors += 1
            nd.append(0.0), rc.append(0.0), mr.append(0.0)
            continue
        ranked = [d for d in ans[i] if d != qid]
        nd.append(ndcg_at_k(ranked, j, a.k))
        rc.append(recall_at_k(ranked, j, a.depth))
        mr.append(mrr_at_k(ranked, j, a.k))
    ts = sorted(x for x in lat if x is not None)
    us = lambda v: "" if v is None else f"{v * 1e6:.1f}"
    mean = lambda v: sum(v) / len(v) if v else 0.0
    return dict(ndcg=mean(nd), recall=mean(rc), mrr=mean(mr), p50=us(percentile(ts, 50)),
                p99=us(percentile(ts, 99)), mean_us=us(mean(ts) if ts else None), errors=errors)


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--dataset", required=True)
    p.add_argument("--split", default="test")
    p.add_argument("--name")
    p.add_argument("--systems", default="elasticsearch,opensearch,weaviate,qdrant")
    p.add_argument("--k", type=int, default=10)
    p.add_argument("--depth", type=int, default=100)
    p.add_argument("--ef", default="100,200,400")
    p.add_argument("--m", type=int, default=16)
    p.add_argument("--ef-construction", type=int, default=200)
    p.add_argument("--rrf-k", type=float, default=60.0)
    p.add_argument("--exact", action="store_true", help="add exact dense rows where the system has them (Qdrant)")
    p.add_argument("--queries", type=int)
    p.add_argument("--analyzer", default="english", help="Elasticsearch/OpenSearch analyzer for the text field")
    p.add_argument("--es-port", type=int, default=9200)
    p.add_argument("--os-port", type=int, default=9201)
    p.add_argument("--weaviate-port", type=int, default=8080)
    p.add_argument("--qdrant-port", type=int, default=6333)
    p.add_argument("--csv")
    a = p.parse_args()
    a.ef = [int(x) for x in a.ef.split(",")]

    ds = Dataset(a.dataset, a.split, a.queries)
    if a.name:
        ds.name = a.name
    if not ds.queries:
        sys.exit("no judged query has text; nothing to score")
    texts = [bm25_query_text(t) for _, t in ds.queries]
    n = len(ds.queries)
    print(f"Dataset {ds.name}: {len(ds.doc_ids)} documents, {n} scored queries, dense: "
          f"{ds.dense['model'] if ds.dense else 'none'}")

    makers = {
        "elasticsearch": lambda: Elastic("elasticsearch", a.es_port, a.analyzer),
        "opensearch": lambda: OpenSearch("opensearch", a.os_port, a.analyzer),
        "weaviate": lambda: Weaviate(a.weaviate_port),
        "qdrant": lambda: Qdrant(a.qdrant_port),
    }
    rows = []
    for sname in a.systems.split(","):
        sname = sname.strip()
        if sname not in makers:
            sys.exit(f"unknown system {sname!r}; known: {', '.join(makers)}")
        s = makers[sname]()
        print(f"\n== {sname}: loading ...")
        t = time.perf_counter()
        s.load(ds, a)
        print(f"   loaded in {time.perf_counter() - t:.1f} s")

        def row(config, ef, run, dense):
            r = score(ds, a, run)
            r.update(system=sname, config=config, ef="" if ef is None else ef, dense=dense,
                     fused=config.startswith("rrf"))
            rows.append(r)
            print(f"   {config:<30} {r['ef']!s:>5} nDCG@{a.k} {r['ndcg']:.4f} recall@{a.depth} "
                  f"{r['recall']:.4f} MRR@{a.k} {r['mrr']:.4f} p50 {r['p50']} us errors {r['errors']}")

        bm25 = None
        if s.bm25(texts[0] or "a", a.depth) is not None:
            bm25 = timed(n, lambda i: s.bm25(texts[i], a.depth) if texts[i] else [])
            row("bm25", None, bm25, False)
        if ds.dense is not None:
            qv = [ds.dense["queries"][i].tolist() for i in range(n)]
            for ef in a.ef:
                s.set_ef(ef)
                run = timed(n, lambda i: s.dense(qv[i], a.depth, ef))
                row("dense-hnsw", ef, run, True)
                if bm25 is not None:
                    row("rrf-client:bm25+dense-hnsw", ef, fuse(bm25, run, a.rrf_k, a.depth), True)
            if a.exact and s.exact(qv[0], a.depth) is not None:
                run = timed(n, lambda i: s.exact(qv[i], a.depth))
                run = ([None] * n, run[1])  # not timed, as in the Rust bench
                row("dense-exact", None, run, True)
                if bm25 is not None:
                    row("rrf-client:bm25+dense-exact", None, fuse(bm25, run, a.rrf_k, a.depth), True)

    if a.csv:
        new = not os.path.exists(a.csv)
        model = ds.dense["model"] if ds.dense else ""
        metric = ds.dense["metric"] if ds.dense else ""
        with open(a.csv, "a", encoding="utf-8") as f:
            if new:
                f.write(CSV_HEADER + "\n")
            for r in rows:
                d = r["dense"]
                f.write(",".join(str(x) for x in [
                    ds.name, r["system"], r["config"], "http", model if d else "", metric if d else "",
                    len(ds.doc_ids), n, a.k, a.depth, a.m if d else "", a.ef_construction if d else "",
                    r["ef"], a.rrf_k if r["fused"] else "", f"{r['ndcg']:.6f}", f"{r['recall']:.6f}",
                    f"{r['mrr']:.6f}", r["p50"], r["p99"], r["mean_us"], r["errors"], "",
                ]) + "\n")
        print(f"\nWrote {len(rows)} rows to {a.csv}")


if __name__ == "__main__":
    main()
