#!/usr/bin/env bash
#
# Download LDBC Graphalytics datasets.
#
# By default downloads the two tiny LDBC example graphs, the correctness set.
# --size names an LDBC size class (scale = log10(vertices + edges)); the
# graphs listed for it below are of that class, checked by
# tests/graphalytics_size_class.rs (ALGO-08, #1687).
#
# Usage:
#   ./scripts/download_graphalytics.sh                     # example graphs
#   ./scripts/download_graphalytics.sh --size S            # S: datagen-7_5-fb, graph500-22
#   ./scripts/download_graphalytics.sh --size M            # M: datagen-8_0-fb, graph500-23
#   ./scripts/download_graphalytics.sh --size all          # examples and every class below
#   ./scripts/download_graphalytics.sh --data-dir /path    # Custom output dir
#
# Archives come from https://datasets.ldbcouncil.org/graphalytics/ and need
# `zstd`. Each holds the graph, its .properties and the reference output of
# every algorithm the graph defines. Sizes (compressed): S 170 and 212 MB,
# M 527 and 430 MB; extracted, about 1, 1, 3.4 and 2.4 GB.

set -euo pipefail

# ── Parse arguments ──────────────────────────────────────────────────
DATA_DIR="data/graphalytics"
SIZE="examples"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --size)
            SIZE="${2:-examples}"
            shift 2
            ;;
        --data-dir)
            DATA_DIR="${2}"
            shift 2
            ;;
        *)
            # Legacy positional arg for data dir
            DATA_DIR="$1"
            shift
            ;;
    esac
done

echo "================================================================"
echo "  LDBC Graphalytics Dataset Downloader"
echo "================================================================"
echo ""
echo "  Target directory: ${DATA_DIR}"
echo "  Size:             ${SIZE}"
echo ""

mkdir -p "${DATA_DIR}"

# ── XS (example) dataset helper ─────────────────────────────────────
download_xs_dataset() {
    local name="$1"
    local dir="${DATA_DIR}/${name}"

    if [ -f "${dir}/${name}.v" ] && [ -f "${dir}/${name}.e" ]; then
        echo "  [SKIP] ${name} — already exists"
        return
    fi

    echo "  [DOWNLOAD] ${name}..."
    mkdir -p "${dir}"

    local base_url="https://raw.githubusercontent.com/ldbc/ldbc_graphalytics/main/graphalytics-validation/src/main/resources/validation-graphs/example"

    # Download vertex file
    if curl -fsSL "${base_url}/${name}.v" -o "${dir}/${name}.v" 2>/dev/null; then
        local vcount
        vcount=$(wc -l < "${dir}/${name}.v" | tr -d ' ')
        echo "    Vertices: ${vcount}"
    else
        echo "    WARNING: Could not download ${name}.v"
    fi

    # Download edge file
    if curl -fsSL "${base_url}/${name}.e" -o "${dir}/${name}.e" 2>/dev/null; then
        local ecount
        ecount=$(wc -l < "${dir}/${name}.e" | tr -d ' ')
        echo "    Edges:    ${ecount}"
    else
        echo "    WARNING: Could not download ${name}.e"
    fi

    # Download properties file from config-template
    local props_url="https://raw.githubusercontent.com/ldbc/ldbc_graphalytics/main/config-template/graphs/${name}.properties"
    if curl -fsSL "${props_url}" -o "${dir}/${name}.properties" 2>/dev/null; then
        echo "    Properties file downloaded"
    fi

    # Download algorithm-specific input/output files for validation
    for algo in BFS CDLP LCC PR SSSP WCC; do
        local algo_url="${base_url}/${name}-${algo}"
        if curl -fsSL "${algo_url}" -o "${dir}/${name}-${algo}" 2>/dev/null; then
            : # silently download
        fi
    done

    # Download algorithm-specific input parameters
    local input_url="${base_url}/${name}-input"
    if curl -fsSL "${input_url}" -o "${dir}/${name}-input" 2>/dev/null; then
        echo "    Input parameters downloaded"
    fi

    echo "    Done: ${dir}/"
}

# ── Datasets by LDBC size class ─────────────────────────────────────
#
# scale = log10(vertices + edges), to one decimal; 2XS [6.5, 7), XS [7, 7.5),
# S [7.5, 8), M [8, 8.5). This list used to call wiki-Talk (2XS) and
# cit-Patents (XS) "S".
#
#   wiki-Talk        2.4M vertices,   5.0M edges  scale 6.9  2XS  directed
#   cit-Patents      3.8M vertices,  16.5M edges  scale 7.3  XS   directed
#   datagen-7_5-fb   633K vertices,  34.2M edges  scale 7.5  S    undirected, weighted
#   graph500-22      2.4M vertices,  64.2M edges  scale 7.8  S    undirected
#   datagen-8_0-fb   1.7M vertices, 107.5M edges  scale 8.0  M    undirected, weighted
#   graph500-23      4.6M vertices, 129.3M edges  scale 8.1  M    undirected
#
CLASS_2XS=("wiki-Talk")
CLASS_XS=("cit-Patents")
CLASS_S=("datagen-7_5-fb" "graph500-22")
CLASS_M=("datagen-8_0-fb" "graph500-23")
ARCHIVE_URL="https://datasets.ldbcouncil.org/graphalytics"

download_archive() {
    local name="$1"
    local dir="${DATA_DIR}/${name}"

    if [ -f "${dir}/${name}.v" ] && [ -f "${dir}/${name}.e" ]; then
        echo "  [SKIP] ${name} — already exists"
        return
    fi

    # Check for zstd
    if ! command -v zstd &>/dev/null; then
        echo "  [ERROR] zstd not found. Install with: brew install zstd (macOS) or apt install zstd (Linux)"
        return 1
    fi

    echo "  [DOWNLOAD] ${name}..."
    mkdir -p "${dir}"

    local url="${ARCHIVE_URL}/${name}.tar.zst"

    # The archives hold their files at the top level, so they are unpacked
    # into the dataset's own directory, where the runner looks for them.
    if ! curl -fSL --progress-bar "${url}" | zstd -dc | tar -x -C "${dir}"; then
        echo "    ERROR: Could not download or unpack ${url}"
        return 1
    fi

    # Verify extraction
    if [ -f "${dir}/${name}.v" ] && [ -f "${dir}/${name}.e" ]; then
        local vcount ecount
        vcount=$(wc -l < "${dir}/${name}.v" | tr -d ' ')
        ecount=$(wc -l < "${dir}/${name}.e" | tr -d ' ')
        echo "    Vertices: ${vcount}"
        echo "    Edges:    ${ecount}"
    else
        echo "    WARNING: Expected files not found after extraction"
        echo "    Contents of ${dir}:"
        ls -la "${dir}/" 2>/dev/null || echo "      (directory not found)"
    fi

    echo "    Done: ${dir}/"
}

# ── Download datasets ────────────────────────────────────────────────

SIZE_UPPER=$(echo "${SIZE}" | tr '[:lower:]' '[:upper:]')
case "${SIZE_UPPER}" in
    EXAMPLES) WANT=() ;;
    2XS) WANT=("${CLASS_2XS[@]}") ;;
    XS)  WANT=("${CLASS_XS[@]}") ;;
    S)   WANT=("${CLASS_S[@]}") ;;
    M)   WANT=("${CLASS_M[@]}") ;;
    ALL) WANT=("${CLASS_2XS[@]}" "${CLASS_XS[@]}" "${CLASS_S[@]}" "${CLASS_M[@]}") ;;
    *)
        echo "Unknown size '${SIZE}'. Valid: examples, 2XS, XS, S, M, all"
        exit 2
        ;;
esac

if [[ "${SIZE_UPPER}" == "EXAMPLES" || "${SIZE_UPPER}" == "ALL" ]]; then
    echo "Downloading the example graphs..."
    echo ""
    download_xs_dataset "example-directed"
    download_xs_dataset "example-undirected"
    echo ""
fi

FAILED=0
for ds in "${WANT[@]+"${WANT[@]}"}"; do
    download_archive "${ds}" || FAILED=1
done

# ── Verify ───────────────────────────────────────────────────────────
echo "Verifying datasets..."
echo ""

# Check XS datasets
for ds in example-directed example-undirected; do
    dir="${DATA_DIR}/${ds}"
    if [ -f "${dir}/${ds}.v" ] && [ -f "${dir}/${ds}.e" ]; then
        vcount=$(wc -l < "${dir}/${ds}.v" | tr -d ' ')
        ecount=$(wc -l < "${dir}/${ds}.e" | tr -d ' ')
        echo "  ${ds} (example):"
        echo "    Vertex file: ${dir}/${ds}.v  (${vcount} lines)"
        echo "    Edge file:   ${dir}/${ds}.e  (${ecount} lines)"
    fi
done

# Check class datasets
for ds in "${CLASS_2XS[@]}" "${CLASS_XS[@]}" "${CLASS_S[@]}" "${CLASS_M[@]}"; do
    dir="${DATA_DIR}/${ds}"
    if [ -f "${dir}/${ds}.v" ] && [ -f "${dir}/${ds}.e" ]; then
        vcount=$(wc -l < "${dir}/${ds}.v" | tr -d ' ')
        ecount=$(wc -l < "${dir}/${ds}.e" | tr -d ' ')
        echo "  ${ds}:"
        echo "    Vertex file: ${dir}/${ds}.v  (${vcount} lines)"
        echo "    Edge file:   ${dir}/${ds}.e  (${ecount} lines)"
    fi
done

echo ""
echo "================================================================"
echo "  Download complete!"
echo ""
echo "  Run benchmarks:"
echo "    cargo bench --release --bench graphalytics_benchmark -- --all"
if [[ "${SIZE_UPPER}" != "EXAMPLES" ]]; then
    echo "    cargo bench --release --bench graphalytics_benchmark -- --size ${SIZE} --all"
fi
echo "================================================================"
exit "${FAILED}"
