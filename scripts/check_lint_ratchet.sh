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

# Two different things start with "warning:" — individual lints, and cargo's
# per-crate summary ("warning: `samyama` (lib) generated 64 warnings"). The count
# below includes both, which is what the ceiling was set against, so it stays that
# way until the ceiling is re-measured on CI in the same commit.
#
# It is worth knowing that the two move independently. The summary count is one
# line per *crate that emitted anything*, so it shifts with toolchain version and
# with how the workspace is split, neither of which is lint debt. Measured on
# 2026-09-07 with rustc 1.96.1: 945 total = 799 lints + 146 summaries, while CI on
# `stable` passed the 769 ceiling on the same commit. A number that differs between
# a developer's machine and CI teaches people to ignore it, so both are printed.
raw=$(cargo clippy --workspace --all-targets 2>&1)
count=$(echo "$raw" | grep -cE "^warning")
lints=$(echo "$raw" | grep -E "^warning" | grep -vc "generated")
summaries=$((count - lints))
echo "clippy warnings: $count (ceiling $CEILING)"
echo "  of which lints: $lints, per-crate summaries: $summaries"

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
