#!/usr/bin/env python3
"""Download BEIR datasets and embed them for `retrieval_quality_benchmark` (#1988).

Each BEIR dataset is a zip holding `corpus.jsonl`, `queries.jsonl` and
`qrels/<split>.tsv`. The bench reads those three as they are. This script
unpacks them and then, unless `--no-embed`, embeds every document and every
query with ONE model, so every system in the comparison searches the same
vectors:

    <data-dir>/<name>/corpus.jsonl
    <data-dir>/<name>/queries.jsonl
    <data-dir>/<name>/qrels/test.tsv        # dev.tsv for msmarco
    <data-dir>/<name>/dense/corpus.fvecs    # TEXMEX layout, as benches/ann_common reads
    <data-dir>/<name>/dense/corpus_ids.txt  # one corpus id per line, row order of corpus.fvecs
    <data-dir>/<name>/dense/queries.fvecs
    <data-dir>/<name>/dense/query_ids.txt
    <data-dir>/<name>/dense/metric.txt      # cosine (vectors are also unit-normalised)
    <data-dir>/<name>/dense/model.txt       # the model name, copied into every CSV row
    <data-dir>/<name>/dense/query_prefix.txt # what was prepended to each query, often empty

A document is embedded as `title + " " + text`, the same string the bench
indexes for BM25. Queries get `--query-prefix` prepended (bge models expect
"Represent this sentence for searching relevant passages: ").

Usage:
    python3 scripts/prepare_beir_datasets.py                  # scifact, all-MiniLM-L6-v2
    python3 scripts/prepare_beir_datasets.py nfcorpus fiqa trec-covid
    python3 scripts/prepare_beir_datasets.py --all            # the six #1988 names
    python3 scripts/prepare_beir_datasets.py scifact --model BAAI/bge-base-en-v1.5 \\
        --query-prefix "Represent this sentence for searching relevant passages: "
    python3 scripts/prepare_beir_datasets.py scifact --no-embed   # BM25 only

Then:
    cargo bench --bench retrieval_quality_benchmark -- --dataset data/beir/scifact
    cargo bench --bench retrieval_quality_benchmark -- --dataset data/beir/msmarco --split dev

Embedding needs `numpy` and `sentence-transformers`
(`pip install numpy sentence-transformers`). NQ (2.7M passages) and MS MARCO
(8.8M) take hours to embed even on a GPU; start with nfcorpus or scifact.
"""

import argparse
import json
import os
import sys
import urllib.request
import zipfile

# The datasets #1988 names, with their BEIR zip name and evaluation split.
DATASETS = {
    "nfcorpus": ("NFCorpus, 3.6K docs, 323 test queries", "test"),
    "scifact": ("SciFact, 5.2K docs, 300 test queries", "test"),
    "fiqa": ("FiQA-2018, 57K docs, 648 test queries", "test"),
    "trec-covid": ("TREC-COVID, 171K docs, 50 test queries, graded qrels", "test"),
    "nq": ("Natural Questions, 2.68M docs, 3,452 test queries", "test"),
    "msmarco": ("MS MARCO passage, 8.8M docs, 6,980 dev queries", "dev"),
}
BASE_URL = "https://public.ukp.informatik.tu-darmstadt.de/thakur/BEIR/datasets"
DEFAULT_MODEL = "sentence-transformers/all-MiniLM-L6-v2"


def need(module, hint):
    try:
        return __import__(module)
    except ImportError:
        sys.exit(f"{module} is required for embedding: pip install {hint}  (or pass --no-embed)")


def download(name, data_dir):
    """Fetch and unpack `<name>.zip` unless `<data-dir>/<name>/corpus.jsonl` exists."""
    target = os.path.join(data_dir, name)
    if os.path.exists(os.path.join(target, "corpus.jsonl")):
        print(f"{name}: already unpacked at {target}")
        return target
    os.makedirs(data_dir, exist_ok=True)
    zpath = os.path.join(data_dir, f"{name}.zip")
    if not os.path.exists(zpath):
        url = f"{BASE_URL}/{name}.zip"
        print(f"{name}: downloading {url}")
        tmp = zpath + ".part"
        urllib.request.urlretrieve(url, tmp)
        os.replace(tmp, zpath)
    root = os.path.realpath(data_dir)
    with zipfile.ZipFile(zpath) as z:
        # The archive is untrusted input: refuse any member that would land
        # outside the data directory.
        for member in z.namelist():
            dest = os.path.realpath(os.path.join(root, member))
            if not dest.startswith(root + os.sep):
                sys.exit(f"{zpath}: member {member!r} escapes {data_dir}; refusing")
        z.extractall(root)
    if not os.path.exists(os.path.join(target, "corpus.jsonl")):
        sys.exit(f"{zpath} did not unpack to {target}/corpus.jsonl")
    return target


def read_jsonl(path):
    """(id, text) per line; a document's text is title + " " + text."""
    out = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            row = json.loads(line)
            title = (row.get("title") or "").strip()
            text = (row.get("text") or "").strip()
            out.append((str(row["_id"]), " ".join(t for t in (title, text) if t)))
    return out


def write_fvecs(path, rows):
    """Each row as a little-endian int32 dimension followed by its float32 values."""
    np = need("numpy", "numpy")
    rows = np.ascontiguousarray(rows, dtype="<f4")
    n, d = rows.shape
    out = np.empty((n, d + 1), dtype="<i4")
    out[:, 0] = d
    out[:, 1:] = rows.view("<i4")
    out.tofile(path)


def embed(target, model_name, query_prefix, batch_size, device):
    dense = os.path.join(target, "dense")
    if os.path.exists(os.path.join(dense, "model.txt")):
        with open(os.path.join(dense, "model.txt")) as f:
            have = f.read().strip()
        prefix_file = os.path.join(dense, "query_prefix.txt")
        have_prefix = open(prefix_file).read() if os.path.exists(prefix_file) else ""
        if have == model_name and have_prefix == query_prefix:
            print(f"{target}: already embedded with {model_name}")
            return
        sys.exit(
            f"{dense} holds {have} vectors (query prefix {have_prefix!r}); move it aside to "
            f"embed with {model_name} (prefix {query_prefix!r}). Every system must search one "
            "model's vectors."
        )
    st = need("sentence_transformers", "sentence-transformers")
    model = st.SentenceTransformer(model_name, device=device)
    docs = read_jsonl(os.path.join(target, "corpus.jsonl"))
    queries = read_jsonl(os.path.join(target, "queries.jsonl"))
    print(f"{target}: embedding {len(docs)} documents and {len(queries)} queries with {model_name}")

    def encode(texts):
        return model.encode(
            texts,
            batch_size=batch_size,
            normalize_embeddings=True,
            show_progress_bar=True,
            convert_to_numpy=True,
        )

    part = dense + ".part"
    os.makedirs(part, exist_ok=True)
    write_fvecs(os.path.join(part, "corpus.fvecs"), encode([t for _, t in docs]))
    with open(os.path.join(part, "corpus_ids.txt"), "w", encoding="utf-8") as f:
        f.writelines(f"{i}\n" for i, _ in docs)
    write_fvecs(
        os.path.join(part, "queries.fvecs"), encode([query_prefix + t for _, t in queries])
    )
    with open(os.path.join(part, "query_ids.txt"), "w", encoding="utf-8") as f:
        f.writelines(f"{i}\n" for i, _ in queries)
    with open(os.path.join(part, "metric.txt"), "w") as f:
        f.write("cosine\n")
    with open(os.path.join(part, "model.txt"), "w") as f:
        f.write(model_name + "\n")
    with open(os.path.join(part, "query_prefix.txt"), "w") as f:
        f.write(query_prefix)
    # Rename last, so an interrupted run leaves no half-written dense/ for the
    # bench to read.
    os.replace(part, dense)


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("datasets", nargs="*", help=f"any of {', '.join(DATASETS)}")
    p.add_argument("--all", action="store_true", help="every dataset #1988 names")
    p.add_argument("--data-dir", default="data/beir")
    p.add_argument("--no-embed", action="store_true", help="download only; the bench then runs BM25")
    p.add_argument("--model", default=DEFAULT_MODEL, help=f"sentence-transformers model (default {DEFAULT_MODEL})")
    p.add_argument("--query-prefix", default="", help="text prepended to every query before embedding")
    p.add_argument("--batch-size", type=int, default=128)
    p.add_argument("--device", default=None, help="cpu, cuda, mps; default: sentence-transformers' choice")
    a = p.parse_args()

    names = list(DATASETS) if a.all else (a.datasets or ["scifact"])
    unknown = [n for n in names if n not in DATASETS]
    if unknown:
        sys.exit(f"unknown dataset(s) {unknown}; known: {', '.join(DATASETS)}")
    for name in names:
        desc, split = DATASETS[name]
        print(f"== {name}: {desc}; evaluate with --split {split}")
        target = download(name, a.data_dir)
        if not a.no_embed:
            embed(target, a.model, a.query_prefix, a.batch_size, a.device)


if __name__ == "__main__":
    main()
