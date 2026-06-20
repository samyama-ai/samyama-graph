#!/usr/bin/env bash
# Generic case-study runner. Each domain ships a `case.env` and a one-line run.sh
# that execs this script. End to end: build server (if needed) → fetch & verify
# snapshot → start server → import → DoD-validate every query → (optionally)
# record the narrated GIF. Single command, idempotent, no external services.
#
#   cd case_studies/<domain> && ./run.sh            # validate (+ record if RECORD=1)
#   RECORD=1 ./run.sh                                # also (re)generate demo.gif
#   ./run.sh --skip-validate                         # demo only, no gate
#
set -uo pipefail

LIB_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$LIB_DIR/../.." && pwd)"
CASE_DIR="$(pwd)"

# ---- config from ./case.env -------------------------------------------------
[[ -f "$CASE_DIR/case.env" ]] || { echo "ERROR: no case.env in $CASE_DIR"; exit 2; }
# shellcheck disable=SC1091
source "$CASE_DIR/case.env"
: "${DOMAIN:?case.env must set DOMAIN}"
: "${SNAPSHOT_URL:?case.env must set SNAPSHOT_URL}"
: "${SNAPSHOT_SHA256:=-}"     # "-" to skip hash check
: "${GRAPH:=default}"
: "${DEDUP_KEY:=}"
: "${PORT_RESP:=6379}"            # RESP port (configurable). HTTP is fixed at 8080 in the server.
PACE="${DEMO_PACE:-1.0}"

PORT_RESP="${SG_RESP_PORT:-$PORT_RESP}"
PORT_HTTP=8080                    # hardcoded in src/main.rs (HttpServer::new(.., 8080))
BASE_URL="http://127.0.0.1:${PORT_HTTP}"
SKIP_VALIDATE=0
[[ "${1:-}" == "--skip-validate" ]] && SKIP_VALIDATE=1

CACHE_DIR="${SG_SNAPSHOT_CACHE:-$REPO_ROOT/case_studies/.snapshots}"
SNAP="$CACHE_DIR/${DOMAIN}.sgsnap"
DATA_DIR="$(mktemp -d -t sg-cs-XXXX)"
SERVER_PID=""

cleanup() { [[ -n "$SERVER_PID" ]] && kill "$SERVER_PID" 2>/dev/null; rm -rf "$DATA_DIR"; }
trap cleanup EXIT

say() { printf '\033[0;36m[%s]\033[0m %s\n' "$DOMAIN" "$*"; }
die() { printf '\033[0;31m[%s] ERROR:\033[0m %s\n' "$DOMAIN" "$*" >&2; exit 1; }

# ---- 1. server binary -------------------------------------------------------
BIN="${SAMYAMA_BIN:-$REPO_ROOT/target/release/samyama}"
if [[ ! -x "$BIN" ]]; then
  say "building release server (one-time)…"
  ( cd "$REPO_ROOT" && cargo build --release ) || die "cargo build failed"
fi

# ---- 2. fetch + verify snapshot --------------------------------------------
"$LIB_DIR/fetch_snapshot.sh" "$SNAPSHOT_URL" "$SNAPSHOT_SHA256" "$SNAP" || die "snapshot fetch failed"

# ---- 3. start server --------------------------------------------------------
# Free port 8080 if a stale server holds it (e.g. AMI's bundled demo server).
if curl -fsS "$BASE_URL/api/status" >/dev/null 2>&1; then
  say "port $PORT_HTTP already serving — stopping the existing samyama process"
  pkill -f "release/samyama" 2>/dev/null; sleep 2
fi
say "starting server (HTTP :$PORT_HTTP, RESP :$PORT_RESP)…"
# --ephemeral guarantees an empty in-memory store (no CWD-relative ./samyama_data
# recovery). Run from the temp CWD too, as a fallback for older binaries.
( cd "$DATA_DIR" && RUST_LOG=warn exec "$BIN" --host 127.0.0.1 --port "$PORT_RESP" --ephemeral ) \
  >"$DATA_DIR/server.log" 2>&1 &
SERVER_PID=$!
for _ in $(seq 1 60); do
  curl -fsS "$BASE_URL/api/status" >/dev/null 2>&1 && break
  kill -0 "$SERVER_PID" 2>/dev/null || { cat "$DATA_DIR/server.log"; die "server died on startup"; }
  sleep 1
done
curl -fsS "$BASE_URL/api/status" >/dev/null 2>&1 || die "server not healthy after 60s"

# ---- 4. import snapshot -----------------------------------------------------
say "importing $(du -h "$SNAP" | cut -f1) snapshot…"
IMPORT_ARGS=(-fsS -X POST "$BASE_URL/api/snapshot/import" -F "file=@$SNAP")
[[ -n "$DEDUP_KEY" ]] && IMPORT_ARGS+=(--url-query "dedup_key=$DEDUP_KEY")
RESP="$(curl "${IMPORT_ARGS[@]}")" || die "import request failed"
echo "$RESP" | (command -v jq >/dev/null && jq . || cat)
for _ in $(seq 1 30); do
  N=$(curl -fsS "$BASE_URL/api/status" | (command -v jq >/dev/null && jq -r '.storage.nodes' || grep -o '"nodes":[0-9]*' | head -1 | grep -o '[0-9]*'))
  [[ "${N:-0}" -gt 0 ]] && { say "ready: $N nodes"; break; }
  sleep 1
done
# Settle after import: the HNSW rebuild runs synchronously in the import handler,
# but async edge compaction (DS-07) and indexing drain just after — a query that
# races them can still disrupt the server (backlog SK-29), so wait briefly.
sleep 3

# ---- 5. DoD gate: every query must return rows ------------------------------
if [[ "$SKIP_VALIDATE" -eq 0 ]]; then
  say "validating showcase queries (DoD gate)…"
  PYTHONPATH="$LIB_DIR" python3 "$LIB_DIR/validate_queries.py" "$CASE_DIR/queries.cypher" \
    --base-url "$BASE_URL" --graph "$GRAPH" || die "DoD gate failed — fix queries before recording"
fi

# ---- 6. optional GIF --------------------------------------------------------
if [[ "${RECORD:-0}" == "1" ]]; then
  say "recording demo GIF…"
  DEMO_PACE="$PACE" SG_BASE_URL="$BASE_URL" SG_GRAPH="$GRAPH" \
    "$LIB_DIR/record_gif.sh" "$CASE_DIR/demo.py" "$CASE_DIR/demo.gif" "$LIB_DIR" \
    || die "GIF recording failed"
  say "wrote demo.gif"
fi

say "done."
