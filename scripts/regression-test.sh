#!/usr/bin/env bash
# Samyama Graph (OSS) — Regression Test Suite
# Runs: unit tests (default + GPU feature matrix), builds all examples + benches,
# runs the GPU examples, clippy + fmt. Adapted from SGE's regression-test.sh and
# enhanced with the GPU/CUDA feature matrix (the gpu-oss-port work).
#
# Usage: ./scripts/regression-test.sh [--quick]
#   --quick: skip benchmarks and the long example runs
set -uo pipefail
export PATH="$HOME/.cargo/bin:$PATH"
cd "$(dirname "$0")/.."

QUICK=false
[[ "${1:-}" == "--quick" ]] && QUICK=true

PASS=0; FAIL=0; SKIP=0; ERRORS=()
log()  { echo -e "\n\033[1;36m=== $1 ===\033[0m"; }
pass() { PASS=$((PASS+1)); echo "  ✅ $1"; }
fail() { FAIL=$((FAIL+1)); ERRORS+=("$1"); echo "  ❌ $1"; }
skip() { SKIP=$((SKIP+1)); echo "  ⏭️  $1 (skipped)"; }

# Detect GPU + a cudarc-compatible CUDA (<=12.5) so the cuda lane runs where possible.
HAVE_GPU=false; command -v nvidia-smi >/dev/null 2>&1 && nvidia-smi -L >/dev/null 2>&1 && HAVE_GPU=true
CUDA_OK=false
if $HAVE_GPU; then
  for v in 12.4 12.5 12.3 12.2 12.1 12.0; do
    if [ -d "/usr/local/cuda-$v" ]; then export PATH="/usr/local/cuda-$v/bin:$PATH"; export CUDA_ROOT="/usr/local/cuda-$v"; CUDA_OK=true; break; fi
  done
  command -v nvcc >/dev/null 2>&1 && nvcc --version 2>/dev/null | grep -qiE "release 12\.[0-5]" && CUDA_OK=true
fi
echo "GPU: $HAVE_GPU | CUDA<=12.5: $CUDA_OK"

# Helper: run `cargo test <args>`, pass if it prints a test-result with 0 failed.
test_suite() { # <label> <cargo test args...>
  local label="$1"; shift
  local logf; logf="/tmp/sg-test-${label//[^a-zA-Z0-9]/_}.log"
  cargo test "$@" > "$logf" 2>&1
  if grep -q "test result:" "$logf" && ! grep -q "test result: FAILED" "$logf"; then
    local p; p=$(grep -oE '[0-9]+ passed' "$logf" | grep -oE '[0-9]+' | paste -sd+ | bc 2>/dev/null || echo "?")
    pass "$label ($p passed)"
  else
    fail "$label"; tail -8 "$logf"
  fi
}

# ─── 1. Unit tests: default (whole workspace) ───
log "Unit tests — default (workspace)"
test_suite "workspace-default" --workspace

# ─── 2. Unit tests: GPU feature matrix (the port) ───
if $HAVE_GPU; then
  log "Unit tests — --features gpu (wgpu)"
  test_suite "gpu-smoke"   -p samyama-gpu --test gpu_smoke
  test_suite "gpu-parity"  -p samyama-graph-algorithms --features gpu cpu_gpu_parity_all_ops
  test_suite "gpu-algos"   -p samyama-graph-algorithms --features gpu
  if $CUDA_OK; then
    log "Unit tests — --features cuda (unified memory)"
    test_suite "cuda-unified" -p samyama-gpu --features cuda unified
    test_suite "cuda-smoke"   -p samyama-gpu --features cuda --test gpu_smoke
  else
    skip "CUDA lane (no CUDA<=12.5 toolkit)"
  fi
else
  skip "GPU feature tests (no GPU)"
fi

# ─── 3. Cypher GPU procedures (engine) ───
log "Cypher algo.cdlp / algo.lcc procedures"
test_suite "cypher-algo" -p samyama --lib test_algo

# ─── 4. Build all examples + benches ───
log "Build examples + benches (default)"
if cargo build --release --examples --benches > /tmp/sg-build.log 2>&1; then
  pass "examples+benches build (default)"
else
  fail "examples+benches build (default)"; tail -8 /tmp/sg-build.log
fi
if $HAVE_GPU && $CUDA_OK; then
  if cargo build --release -p samyama-graph-algorithms --features cuda --examples > /tmp/sg-build-cuda.log 2>&1; then
    pass "GPU examples build (--features cuda)"
  else
    fail "GPU examples build (--features cuda)"; tail -8 /tmp/sg-build-cuda.log
  fi
fi

# ─── 5. Run the GPU examples (smoke) ───
if ! $QUICK && $HAVE_GPU && $CUDA_OK; then
  log "Run GPU examples"
  for ex in e1_ablation e3a_um_vs_copy; do
    if cargo run --release -q -p samyama-graph-algorithms --features cuda --example "$ex" > "/tmp/sg-ex-$ex.log" 2>&1; then
      pass "example $ex"
    else
      fail "example $ex"; tail -6 "/tmp/sg-ex-$ex.log"
    fi
  done
else
  skip "GPU example runs"
fi

# ─── 6. Clippy (changed GPU crates) + fmt ───
log "Clippy + fmt (GPU crates)"
if cargo clippy -p samyama-gpu -p samyama-graph-algorithms --features gpu > /tmp/sg-clippy.log 2>&1; then
  pass "clippy (gpu crates)"
else
  fail "clippy (gpu crates)"; tail -8 /tmp/sg-clippy.log
fi
if cargo fmt --check -p samyama-gpu -p samyama-graph-algorithms > /tmp/sg-fmt.log 2>&1; then
  pass "fmt (gpu crates)"
else
  skip "fmt (gpu crates) — run cargo fmt"
fi

# ─── Summary ───
echo -e "\n════════════════════════════════════════"
echo "  Regression Test Results"
echo "════════════════════════════════════════"
echo "  ✅ Pass: $PASS   ❌ Fail: $FAIL   ⏭️  Skip: $SKIP"
echo "════════════════════════════════════════"
if [[ $FAIL -gt 0 ]]; then
  echo -e "\nFailures:"; for e in "${ERRORS[@]}"; do echo "  - $e"; done
  exit 1
fi
echo -e "\nAll checks passed! ✅"
