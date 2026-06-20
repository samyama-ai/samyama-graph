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
# Record idle up to 6s so the demo's read-pauses survive into the cast (the cast
# is committed and is the pausable artifact: `asciinema play demo.cast`).
rm -f "$CAST"
COLUMNS=100 LINES=30 TERM=xterm-256color asciinema rec --overwrite -q -i 6 \
  -c "env COLUMNS=100 LINES=30 TERM=xterm-256color python3 $DEMO" "$CAST" \
  || { echo "[gif] asciinema failed"; exit 1; }

echo "[gif] rendering $CAST → $OUT"
# speed 1.0 (real-time) + a 4s idle cap so the post-result read pauses render in
# full — a looping GIF can't be paused, so the reading time must be in the frames.
agg --speed 1.0 --idle-time-limit 4.0 --font-size 18 --theme asciinema "$CAST" "$OUT" \
  || { echo "[gif] agg failed"; exit 1; }

SZ=$(du -h "$OUT" | cut -f1)
echo "[gif] wrote $OUT ($SZ)"
