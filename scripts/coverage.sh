#!/usr/bin/env bash
# Samyama Graph — Code Coverage Script
# Usage:
#   ./scripts/coverage.sh              # HTML report (opens in browser)
#   ./scripts/coverage.sh --summary    # Terminal summary only
#   ./scripts/coverage.sh --json       # JSON output for CI
#   ./scripts/coverage.sh --lcov       # LCOV format for external tools

set -euo pipefail

PROJ_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$PROJ_ROOT"

# Check tool availability
if ! command -v cargo-llvm-cov &>/dev/null; then
    echo "cargo-llvm-cov not found. Installing..."
    cargo install cargo-llvm-cov
fi

# Ensure llvm-tools are available
rustup component add llvm-tools-preview 2>/dev/null || true

MODE="${1:---html}"
REPORT_DIR="$PROJ_ROOT/coverage"
mkdir -p "$REPORT_DIR"

echo "=== Samyama Code Coverage ==="
echo "Mode: $MODE"
echo ""

case "$MODE" in
    --summary)
        # Terminal summary — quick overview
        cargo llvm-cov test \
            --workspace \
            --exclude samyama-python \
            -- --test-threads=4 2>/dev/null
        ;;
    --json)
        # JSON output for CI pipelines
        cargo llvm-cov test \
            --workspace \
            --exclude samyama-python \
            --json \
            --output-path "$REPORT_DIR/coverage.json" \
            -- --test-threads=4
        echo "JSON report: $REPORT_DIR/coverage.json"
        ;;
    --lcov)
        # LCOV format for external tools (Codecov, Coveralls)
        cargo llvm-cov test \
            --workspace \
            --exclude samyama-python \
            --lcov \
            --output-path "$REPORT_DIR/lcov.info" \
            -- --test-threads=4
        echo "LCOV report: $REPORT_DIR/lcov.info"
        ;;
    --html|*)
        # HTML report with per-file detail
        cargo llvm-cov test \
            --workspace \
            --exclude samyama-python \
            --html \
            --output-dir "$REPORT_DIR" \
            -- --test-threads=4
        echo ""
        echo "HTML report: $REPORT_DIR/html/index.html"
        # Open in browser on macOS
        if command -v open &>/dev/null; then
            open "$REPORT_DIR/html/index.html"
        fi
        ;;
esac

echo ""
echo "=== Done ==="
