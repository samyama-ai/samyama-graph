#!/usr/bin/env python3
"""Download VectorDBBench datasets and convert them for `vector_scale_benchmark` (#1984).

VectorDBBench (github.com/zilliztech/VectorDBBench) hosts each case's data as
parquet files under one public prefix (its `AWS_S3_URL`, readable over HTTPS):

    https://assets.zilliz.com/benchmark/<dir>/shuffle_train.parquet             # 1 file
    https://assets.zilliz.com/benchmark/<dir>/shuffle_train-00-of-10.parquet    # .. -09-of-10
    https://assets.zilliz.com/benchmark/<dir>/test.parquet                       # queries
    https://assets.zilliz.com/benchmark/<dir>/neighbors.parquet                  # ground truth

`<dir>` is `<name>_<size label>_<size>` as VectorDBBench's `BaseDataset.dir_name`
builds it, e.g. `cohere_medium_1m`. Columns: train and test are `id` (int64)
and `emb` (list<float>); neighbors is `id` and `neighbors_id` (list<int64>,
1,000 per query for Cohere), where `neighbors_id` are train `id`s. The train
files are shuffled, so a row's position is not its id.

This script writes the layout `ann_benchmark` and `vector_scale_benchmark` read:

    <data-dir>/<case>/base.fvecs          # row i is train id i
    <data-dir>/<case>/query.fvecs         # in test id order
    <data-dir>/<case>/groundtruth.ivecs   # row i is test id i; values are train ids
    <data-dir>/<case>/metric.txt          # cosine

Base vectors are written straight into a memory-mapped output at their id's
offset, so a 10M case converts without holding 30 GB in memory. The ids must
be exactly 0..N-1; anything else is refused rather than silently renumbered,
since the ground truth names train ids.

Usage:
    python3 scripts/download_vectordbbench_datasets.py cohere-1m
    python3 scripts/download_vectordbbench_datasets.py cohere-10m openai-5m --data-dir /mnt/vdbb
    python3 scripts/download_vectordbbench_datasets.py --list
    python3 scripts/download_vectordbbench_datasets.py cohere-1m --keep-parquet

Then:
    cargo bench --bench vector_scale_benchmark -- --dataset data/vdbb/cohere-1m

Needs `numpy` and `pyarrow` (`pip install numpy pyarrow`). Downloads are large:
cohere-10m is ten 4.7 GB train files, openai-5m ten 4.5 GB files. Files already
in `<data-dir>/<case>/parquet/` are reused, so they can be fetched with another
tool (`curl -O`, `aws s3 cp --no-sign-request s3://assets.zilliz.com/benchmark/...`)
and then converted here.
"""

import argparse
import importlib
import os
import shutil
import sys
import urllib.request

BASE_URL = "https://assets.zilliz.com/benchmark"

# case -> (VectorDBBench dir, vectors, dim, train file count, description)
CASES = {
    "cohere-100k": ("cohere_small_100k", 100_000, 768, 1, "Cohere SMALL, 768-d cosine (smoke test)"),
    "cohere-1m": ("cohere_medium_1m", 1_000_000, 768, 1, "Cohere MEDIUM, 768-d cosine"),
    "cohere-10m": ("cohere_large_10m", 10_000_000, 768, 10, "Cohere LARGE, 768-d cosine (NDS-05 size)"),
    "openai-50k": ("openai_small_50k", 50_000, 1536, 1, "OpenAI SMALL, 1536-d cosine (smoke test)"),
    "openai-500k": ("openai_medium_500k", 500_000, 1536, 1, "OpenAI MEDIUM, 1536-d cosine"),
    "openai-5m": ("openai_large_5m", 5_000_000, 1536, 10, "OpenAI LARGE, 1536-d cosine"),
}


def need(module):
    try:
        return importlib.import_module(module)
    except ImportError:
        sys.exit(f"{module} is required: pip install numpy pyarrow")


def train_files(count):
    """VectorDBBench's `compose_train_files` with `USE_SHUFFLED_DATA` (its default)."""
    if count == 1:
        return ["shuffle_train.parquet"]
    return [f"shuffle_train-{i:02d}-of-{count}.parquet" for i in range(count)]


def fetch(remote_dir, name, raw_dir):
    path = os.path.join(raw_dir, name)
    if os.path.exists(path):
        print(f"  using cached {path}")
        return path
    url = f"{BASE_URL}/{remote_dir}/{name}"
    print(f"  downloading {url}")
    tmp = path + ".part"
    urllib.request.urlretrieve(url, tmp)
    os.replace(tmp, path)
    return path


def emb_matrix(table, column, dim, path):
    """A pyarrow list<float> column as an (n, dim) float32 array, checked for width."""
    np = need("numpy")
    pa = need("pyarrow")
    col = table.column(column).combine_chunks()
    if col.null_count:
        sys.exit(f"{path}: {col.null_count} null rows in `{column}`")
    if isinstance(col, (pa.ListArray, pa.LargeListArray)):
        lengths = np.diff(col.offsets.to_numpy())
        if len(lengths) and (lengths.min() != dim or lengths.max() != dim):
            sys.exit(f"{path}: `{column}` rows have {lengths.min()}..{lengths.max()} values, expected {dim}")
        flat = col.flatten().to_numpy(zero_copy_only=False)
    else:
        sys.exit(f"{path}: `{column}` is {col.type}, expected a list")
    return np.asarray(flat, dtype="<f4").reshape(-1, dim)


def write_vecs(path, rows, dtype):
    """Each row as a little-endian int32 dimension followed by its values."""
    np = need("numpy")
    rows = np.ascontiguousarray(rows, dtype=dtype)
    n, d = rows.shape
    out = np.empty((n, d + 1), dtype="<i4")
    out[:, 0] = d
    out[:, 1:] = rows.view("<i4") if dtype == "<f4" else rows
    out.tofile(path)


def convert_base(paths, n, dim, out_path):
    np = need("numpy")
    pq = need("pyarrow.parquet")
    mm = np.memmap(out_path, dtype="<i4", mode="w+", shape=(n, dim + 1))
    seen = np.zeros(n, dtype=bool)
    for path in paths:
        pf = pq.ParquetFile(path)
        for rg in range(pf.num_row_groups):
            t = pf.read_row_group(rg, columns=["id", "emb"])
            ids = t.column("id").to_numpy()
            if len(ids) and (ids.min() < 0 or ids.max() >= n):
                sys.exit(f"{path}: train id outside 0..{n - 1}")
            if seen[ids].any() or len(np.unique(ids)) != len(ids):
                sys.exit(f"{path}: duplicate train id")
            seen[ids] = True
            vecs = emb_matrix(t, "emb", dim, path)
            mm[ids, 0] = dim
            mm[ids, 1:] = vecs.view("<i4")
        print(f"  {os.path.basename(path)}: {int(seen.sum())}/{n} base vectors placed")
    if not seen.all():
        sys.exit(f"train ids are not 0..{n - 1}: {int((~seen).sum())} missing")
    mm.flush()
    del mm


def convert_case(case, data_dir, keep_parquet):
    np = need("numpy")
    pq = need("pyarrow.parquet")
    remote_dir, n, dim, count, desc = CASES[case]
    print(f"{case}: {desc}, {n} vectors")
    out_dir = os.path.join(data_dir, case)
    raw_dir = os.path.join(out_dir, "parquet")
    os.makedirs(raw_dir, exist_ok=True)

    test_path = fetch(remote_dir, "test.parquet", raw_dir)
    gt_path = fetch(remote_dir, "neighbors.parquet", raw_dir)

    test = pq.read_table(test_path, columns=["id", "emb"])
    order = np.argsort(test.column("id").to_numpy(), kind="stable")
    queries = emb_matrix(test, "emb", dim, test_path)[order]
    write_vecs(os.path.join(out_dir, "query.fvecs"), queries, "<f4")

    gt = pq.read_table(gt_path, columns=["id", "neighbors_id"])
    gt_ids = gt.column("id").to_numpy()
    if sorted(gt_ids.tolist()) != sorted(test.column("id").to_numpy().tolist()):
        sys.exit(f"{gt_path}: ground-truth ids do not match the test ids")
    nb = gt.column("neighbors_id").combine_chunks()
    widths = np.diff(nb.offsets.to_numpy())
    if widths.min() != widths.max():
        sys.exit(f"{gt_path}: ground-truth rows have {widths.min()}..{widths.max()} neighbours")
    neighbours = nb.flatten().to_numpy().reshape(len(gt_ids), int(widths[0]))
    neighbours = neighbours[np.argsort(gt_ids, kind="stable")]
    if neighbours.min() < 0 or neighbours.max() >= n:
        sys.exit(f"{gt_path}: neighbour ids outside 0..{n - 1}")
    write_vecs(os.path.join(out_dir, "groundtruth.ivecs"), neighbours.astype("<i4"), "<i4")
    print(f"  {len(queries)} queries, {neighbours.shape[1]} neighbours each")

    paths = [fetch(remote_dir, f, raw_dir) for f in train_files(count)]
    convert_base(paths, n, dim, os.path.join(out_dir, "base.fvecs"))

    with open(os.path.join(out_dir, "metric.txt"), "w") as m:
        m.write("cosine\n")
    if not keep_parquet:
        shutil.rmtree(raw_dir)
    print(f"  wrote {out_dir}")


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("cases", nargs="*", help=f"cases: {', '.join(CASES)}")
    p.add_argument("--data-dir", default="data/vdbb")
    p.add_argument("--keep-parquet", action="store_true", help="keep the downloaded parquet files")
    p.add_argument("--list", action="store_true", help="list the cases and exit")
    a = p.parse_args()
    if a.list or not a.cases:
        for name, (d, n, dim, count, desc) in CASES.items():
            print(f"  {name:12} {desc}; {BASE_URL}/{d}/ ({count} train file(s))")
        return
    for case in a.cases:
        if case not in CASES:
            sys.exit(f"unknown case `{case}`; known: {', '.join(CASES)}")
    os.makedirs(a.data_dir, exist_ok=True)
    for case in a.cases:
        convert_case(case, a.data_dir, a.keep_parquet)


if __name__ == "__main__":
    main()
