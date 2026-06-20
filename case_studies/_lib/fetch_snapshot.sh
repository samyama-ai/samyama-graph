#!/usr/bin/env bash
# Download a .sgsnap snapshot and verify its sha256. Cached: a present file with
# a matching hash is reused. Pass "-" as the hash to skip verification (not
# recommended — the DoD requires a pinned hash).
#
#   fetch_snapshot.sh <url> <sha256|-> <out_path>
set -uo pipefail

URL="${1:?url}"; WANT="${2:--}"; OUT="${3:?out path}"
mkdir -p "$(dirname "$OUT")"

sha() { if command -v sha256sum >/dev/null; then sha256sum "$1" | awk '{print $1}';
        else shasum -a 256 "$1" | awk '{print $1}'; fi; }

if [[ -f "$OUT" && "$WANT" != "-" ]]; then
  [[ "$(sha "$OUT")" == "$WANT" ]] && { echo "[fetch] cached + verified: $OUT"; exit 0; }
  echo "[fetch] cached file hash mismatch — re-downloading"
fi

echo "[fetch] downloading $URL"
curl -fL --retry 3 --progress-bar -o "$OUT.part" "$URL" || { echo "[fetch] download failed"; exit 1; }
mv "$OUT.part" "$OUT"

if [[ "$WANT" != "-" ]]; then
  GOT="$(sha "$OUT")"
  if [[ "$GOT" != "$WANT" ]]; then
    echo "[fetch] SHA256 MISMATCH"; echo "  want $WANT"; echo "  got  $GOT"; exit 1
  fi
  echo "[fetch] sha256 verified"
else
  echo "[fetch] WARNING: hash check skipped (pin SNAPSHOT_SHA256 for the DoD)"
  echo "[fetch] sha256($OUT) = $(sha "$OUT")"
fi
