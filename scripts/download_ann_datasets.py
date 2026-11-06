#!/usr/bin/env python3
"""Download ann-benchmarks datasets and convert them for `ann_benchmark` (#1983).

ann-benchmarks publishes each dataset as one HDF5 file holding `train` (the
base set), `test` (the queries), `neighbors` (ground truth, 100 per query) and
a `distance` attribute. The bench reads the TEXMEX `.fvecs`/`.ivecs` layout
instead, so it needs no HDF5 library; this script writes, per dataset:

    <data-dir>/<name>/base.fvecs
    <data-dir>/<name>/query.fvecs
    <data-dir>/<name>/groundtruth.ivecs
    <data-dir>/<name>/metric.txt        # l2 or cosine, read by the bench

Usage:
    python3 scripts/download_ann_datasets.py                       # sift-128-euclidean
    python3 scripts/download_ann_datasets.py glove-100-angular gist-960-euclidean
    python3 scripts/download_ann_datasets.py --all
    python3 scripts/download_ann_datasets.py --convert local.hdf5  # no download
    python3 scripts/download_ann_datasets.py --data-dir /mnt/ann ...

Then:
    cargo bench --bench ann_benchmark -- --dataset data/ann/sift-128-euclidean

Needs `numpy` and `h5py` (`pip install numpy h5py`).
"""

import argparse
import os
import sys
import urllib.request

# The four the issue names, by their ann-benchmarks file names.
DATASETS = {
    "sift-128-euclidean": "SIFT-1M, 128-d, L2",
    "gist-960-euclidean": "GIST-1M, 960-d, L2",
    "glove-100-angular": "GloVe-100, 1.18M x 100-d, angular",
    "deep-image-96-angular": "DEEP-10M, 96-d, angular",
}
BASE_URL = "http://ann-benchmarks.com"

# ann-benchmarks' `distance` attribute -> the bench's `--metric` name.
METRICS = {"euclidean": "l2", "angular": "cosine"}


def need(module):
    try:
        return __import__(module)
    except ImportError:
        sys.exit(f"{module} is required: pip install numpy h5py")


def write_vecs(path, rows, dtype):
    """Each row as a little-endian int32 dimension followed by its values."""
    np = need("numpy")
    rows = np.ascontiguousarray(rows, dtype=dtype)
    n, d = rows.shape
    out = np.empty((n, d + 1), dtype="<i4")
    out[:, 0] = d
    out[:, 1:] = rows.astype(dtype).view("<i4") if dtype == "<f4" else rows
    out.tofile(path)


def convert(hdf5_path, out_dir):
    h5py = need("h5py")
    os.makedirs(out_dir, exist_ok=True)
    with h5py.File(hdf5_path, "r") as f:
        distance = f.attrs.get("distance")
        if isinstance(distance, bytes):
            distance = distance.decode()
        metric = METRICS.get(distance)
        if metric is None:
            sys.exit(f"{hdf5_path}: distance `{distance}` has no bench metric")
        print(f"  train {f['train'].shape}, test {f['test'].shape}, "
              f"neighbors {f['neighbors'].shape}, distance {distance}")
        write_vecs(os.path.join(out_dir, "base.fvecs"), f["train"][:], "<f4")
        write_vecs(os.path.join(out_dir, "query.fvecs"), f["test"][:], "<f4")
        write_vecs(os.path.join(out_dir, "groundtruth.ivecs"), f["neighbors"][:], "<i4")
    with open(os.path.join(out_dir, "metric.txt"), "w") as m:
        m.write(metric + "\n")
    print(f"  wrote {out_dir}")


def download(name, data_dir):
    hdf5 = os.path.join(data_dir, f"{name}.hdf5")
    if not os.path.exists(hdf5):
        url = f"{BASE_URL}/{name}.hdf5"
        print(f"Downloading {url}")
        tmp = hdf5 + ".part"
        urllib.request.urlretrieve(url, tmp)
        os.replace(tmp, hdf5)
    else:
        print(f"Using cached {hdf5}")
    return hdf5


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("names", nargs="*", help=f"datasets: {', '.join(DATASETS)}")
    p.add_argument("--all", action="store_true", help="all four datasets")
    p.add_argument("--convert", metavar="HDF5", help="convert a local file instead of downloading")
    p.add_argument("--data-dir", default="data/ann")
    a = p.parse_args()

    os.makedirs(a.data_dir, exist_ok=True)
    if a.convert:
        name = os.path.splitext(os.path.basename(a.convert))[0]
        convert(a.convert, os.path.join(a.data_dir, name))
        return

    names = list(DATASETS) if a.all else (a.names or ["sift-128-euclidean"])
    for name in names:
        if name not in DATASETS:
            sys.exit(f"unknown dataset `{name}`; known: {', '.join(DATASETS)}")
        print(f"{name}: {DATASETS[name]}")
        convert(download(name, a.data_dir), os.path.join(a.data_dir, name))


if __name__ == "__main__":
    main()
