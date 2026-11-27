#!/bin/sh
# Starts the server for `snap.samyama.daemon` (#2203). Data in $SNAP_COMMON,
# shared by every revision; extra flags from `snap set samyama args=...`.
set -e
mkdir -p "$SNAP_COMMON/data"
ARGS="$(snapctl get args)"
# shellcheck disable=SC2086 # ARGS is a list of flags, split on purpose
exec "$SNAP/bin/samyama" --data-path "$SNAP_COMMON/data" $ARGS
