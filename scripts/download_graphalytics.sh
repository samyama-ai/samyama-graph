#!/usr/bin/env bash
#
# Download LDBC Graphalytics example datasets.
#
# These are tiny (XS) datasets suitable for development and correctness testing.
# For larger scale factors, visit: https://ldbcouncil.org/benchmarks/graphalytics/
#
# Usage:
#   ./scripts/download_graphalytics.sh
#   ./scripts/download_graphalytics.sh /custom/data/dir

set -euo pipefail

DATA_DIR="${1:-data/graphalytics}"

echo "================================================================"
echo "  LDBC Graphalytics Dataset Downloader"
echo "================================================================"
echo ""
echo "  Target directory: ${DATA_DIR}"
echo ""

mkdir -p "${DATA_DIR}"

# ── Helper ───────────────────────────────────────────────────────────
download_dataset() {
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

# ── Download datasets ────────────────────────────────────────────────

echo "Downloading example datasets..."
echo ""

download_dataset "example-directed"
download_dataset "example-undirected"

echo ""

# ── Verify ───────────────────────────────────────────────────────────
echo "Verifying datasets..."
echo ""

for ds in example-directed example-undirected; do
    dir="${DATA_DIR}/${ds}"
    if [ -f "${dir}/${ds}.v" ] && [ -f "${dir}/${ds}.e" ]; then
        vcount=$(wc -l < "${dir}/${ds}.v" | tr -d ' ')
        ecount=$(wc -l < "${dir}/${ds}.e" | tr -d ' ')
        echo "  ${ds}:"
        echo "    Vertex file: ${dir}/${ds}.v  (${vcount} lines)"
        echo "    Edge file:   ${dir}/${ds}.e  (${ecount} lines)"
    else
        echo "  ${ds}: MISSING FILES"
    fi
done

echo ""
echo "================================================================"
echo "  Download complete!"
echo ""
echo "  Run the benchmark:"
echo "    cargo run --release --example graphalytics_benchmark -- --all"
echo "================================================================"
