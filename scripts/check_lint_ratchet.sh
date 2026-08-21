#!/usr/bin/env bash
# #487: stop the lint debt growing, without a big-bang reformat.
#
# The tree has ~5,958 rustfmt diffs and ~769 clippy warnings. Turning either
# into a hard gate today means reformatting nearly every file in one commit:
# `git blame` stops being useful, and every branch in flight conflicts. That is
# a decision with real costs, and it is not this script's to make.
#
# What this does instead is a ratchet, the same shape as the TCK one: the
# warning count may not rise. New code arrives clean, the debt shrinks whenever
# someone touches a file, and the big-bang stays available as a separate,
# deliberate choice rather than a prerequisite.
#
#   usage: check_lint_ratchet.sh [clippy-ceiling]
set -uo pipefail

CEILING="${1:-769}"

count=$(cargo clippy --workspace --all-targets 2>&1 | grep -cE "^warning")
echo "clippy warnings: $count (ceiling $CEILING)"

if [ "$count" -gt "$CEILING" ]; then
  echo "FAIL: $((count - CEILING)) more clippy warnings than the ceiling."
  echo "  New code should not add to the backlog. Fix the new warnings, or"
  echo "  raise the ceiling in the same commit and say why."
  exit 1
fi

if [ "$count" -lt "$CEILING" ]; then
  echo "OK: $((CEILING - count)) below the ceiling — lower it to $count and lock the gain in."
else
  echo "OK: at the ceiling."
fi
