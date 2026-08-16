#!/usr/bin/env bash
#
# Download the LDBC SNB Interactive SF1 dataset (CsvBasic, LongDateFormatter).
#
# Used by:
#   examples/ldbc_loader.rs
#   benches/ldbc_benchmark.rs     (SNB Interactive: IS1-IS7, IC1-IC14, INS1-INS8, DEL1-DEL8)
#   benches/ldbc_bi_benchmark.rs  (SNB Business Intelligence: BI-1 through BI-20)
#
# All three expect the extracted dataset at:
#   data/ldbc-sf1/social_network-sf1-CsvBasic-LongDateFormatter/
#
# Usage:
#   ./scripts/download_ldbc_snb.sh                     # Download to default location
#   ./scripts/download_ldbc_snb.sh /custom/data/dir     # Download to custom location
#
# After downloading, run:
#   cargo run --release --example ldbc_loader -- --query
#   cargo bench --bench ldbc_benchmark -- --runs 3
#   cargo bench --bench ldbc_bi_benchmark -- --runs 3

set -euo pipefail

DATA_DIR="${1:-data/ldbc-sf1}"
DATASET_NAME="social_network-sf1-CsvBasic-LongDateFormatter"
ARCHIVE_URL="https://datasets.ldbcouncil.org/snb-interactive-v1/${DATASET_NAME}.tar.zst"
ARCHIVE_PATH="${DATA_DIR}/${DATASET_NAME}.tar.zst"
EXTRACT_DIR="${DATA_DIR}/${DATASET_NAME}"

echo "================================================================"
echo "  LDBC SNB Interactive SF1 Dataset Downloader — Samyama"
echo "================================================================"
echo ""
echo "  Target directory: ${EXTRACT_DIR}"
echo ""

# ── Check if data already exists ─────────────────────────────────────
if [ -d "${EXTRACT_DIR}/static" ] && [ -d "${EXTRACT_DIR}/dynamic" ]; then
    echo "  [SKIP] SNB SF1 data already exists at ${EXTRACT_DIR}"
    echo ""
    echo "  To re-download, remove the directory first:"
    echo "    rm -rf ${EXTRACT_DIR}"
    echo ""
    echo "  To run the benchmarks:"
    echo "    cargo bench --bench ldbc_benchmark -- --runs 3"
    echo "    cargo bench --bench ldbc_bi_benchmark -- --runs 3"
    exit 0
fi

mkdir -p "${DATA_DIR}"

# ── Download ─────────────────────────────────────────────────────────
if ! command -v curl &>/dev/null; then
    echo "  ERROR: 'curl' not found. Install curl and re-run."
    exit 1
fi

echo "  Downloading ${DATASET_NAME}.tar.zst (~220MB compressed)..."
echo "    ${ARCHIVE_URL}"
echo ""

curl --location \
     --continue-at - \
     --retry 5 \
     --retry-delay 10 \
     --retry-connrefused \
     --retry-all-errors \
     -o "${ARCHIVE_PATH}" \
     "${ARCHIVE_URL}"

echo ""
echo "  Downloaded: $(du -h "${ARCHIVE_PATH}" | cut -f1)"
echo ""

# ── Extract ──────────────────────────────────────────────────────────
echo "  Extracting..."

if command -v zstd &>/dev/null && command -v tar &>/dev/null; then
    # Standard path: native zstd + tar
    tar -xv --use-compress-program=unzstd -f "${ARCHIVE_PATH}" -C "${DATA_DIR}"
elif command -v 7z &>/dev/null; then
    # Fallback for systems without zstd (e.g. Windows/Git Bash): 7z handles
    # .zst decompression; pipe the decompressed tar stream into a second 7z
    # invocation to unpack it, since 7z doesn't do both in one step.
    ( cd "${DATA_DIR}" && 7z x -aoa "$(basename "${ARCHIVE_PATH}")" -so | 7z x -aoa -si -ttar -o. )
else
    echo "  ERROR: Need either 'zstd' (with 'tar') or '7z' to extract this archive."
    echo "    Install zstd: apt install zstd (Linux) / brew install zstd (macOS) / scoop install zstd (Windows)"
    echo "    Or install 7-Zip: https://www.7-zip.org/"
    exit 1
fi

# ── Verify ───────────────────────────────────────────────────────────
echo ""
echo "================================================================"
echo "  Dataset Summary"
echo "================================================================"
echo ""

if [ -d "${EXTRACT_DIR}/static" ] && [ -d "${EXTRACT_DIR}/dynamic" ]; then
    total_size=$(du -sh "${EXTRACT_DIR}" | cut -f1)
    echo "  Extracted to: ${EXTRACT_DIR}"
    echo "  Total size:   ${total_size}"
    echo ""
    echo "  Cleaning up archive..."
    rm -f "${ARCHIVE_PATH}"
    echo ""
    echo "================================================================"
    echo "  Download complete!"
    echo ""
    echo "  Run the loader / benchmarks:"
    echo "    cargo run --release --example ldbc_loader -- --query"
    echo "    cargo bench --bench ldbc_benchmark -- --runs 3"
    echo "    cargo bench --bench ldbc_bi_benchmark -- --runs 3"
    echo "================================================================"
else
    echo "  WARNING: Expected 'static/' and 'dynamic/' not found after extraction."
    echo "  Contents of ${DATA_DIR}:"
    ls -la "${DATA_DIR}/" 2>/dev/null || echo "    (directory not found)"
    exit 1
fi
