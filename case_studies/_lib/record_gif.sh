#!/usr/bin/env bash
# Record a narrated demo to an animated GIF: asciinema captures the rich-Console
# run, agg renders it. Requires a server already running + snapshot imported
# (run_case_study.sh handles that). vhs is intentionally not used (asciinema+agg
# is the pipeline proven on the powergrid/telecom demos).
#
#   record_gif.sh <demo.py> <out.gif> <lib_dir>
set -uo pipefail

DEMO="${1:?demo.py}"; OUT="${2:?out.gif}"; LIB_DIR="${3:?lib dir}"
CAST="${OUT%.gif}.cast"

for t in asciinema agg; do
  command -v "$t" >/dev/null || { echo "[gif] '$t' not installed — skipping GIF"; exit 0; }
done

export PYTHONPATH="$LIB_DIR${PYTHONPATH:+:$PYTHONPATH}"
export SG_BASE_URL="${SG_BASE_URL:-http://127.0.0.1:8080}"
export SG_GRAPH="${SG_GRAPH:-default}"

echo "[gif] recording $DEMO → $CAST"
# 100x30 reads well in a README. asciinema 2.x takes the recording size from
# COLUMNS/LINES (verified), so set them for both asciinema and the inner python.
rm -f "$CAST"
COLUMNS=100 LINES=30 TERM=xterm-256color asciinema rec --overwrite -q -i 1.5 \
  -c "env COLUMNS=100 LINES=30 TERM=xterm-256color python3 $DEMO" "$CAST" \
  || { echo "[gif] asciinema failed"; exit 1; }

echo "[gif] rendering $CAST → $OUT"
agg --speed 1.3 --idle-time-limit 1.5 --font-size 18 --theme asciinema "$CAST" "$OUT" \
  || { echo "[gif] agg failed"; exit 1; }

SZ=$(du -h "$OUT" | cut -f1)
echo "[gif] wrote $OUT ($SZ)"
