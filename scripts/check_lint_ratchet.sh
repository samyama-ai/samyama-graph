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

CEILING="${1:-796}"

# --- the measurement must be a measurement (#1134) --------------------------
#
# This gate passed on CI while reporting **0 clippy warnings** — run 34305145896,
# 2026-09-09, on the same tree that reports 952 locally. A ceiling of 769 is
# trivially satisfied by 0, so the step went green while measuring nothing, and
# the script's own advice was "lower it to 0 and lock the gain in", which would
# have made the gate permanently green and then broken the build the moment a
# real count came back.
#
# Zero warnings from a workspace that has 952 of them is not a clean tree. It is
# a failed measurement, and a gate that cannot tell the two apart is worse than
# no gate: #1134 records that a local red that CI calls green trains people to
# ignore the script.
#
# So: capture cargo's exit status and its output, and refuse to report a count
# unless cargo actually said it did something. The previous version discarded
# both — `raw=$(cargo clippy ... 2>&1)` with no status check — which is why the
# CI log for that run contains no cargo output at all and the 0 could not be
# diagnosed from it.
raw=$(cargo clippy --workspace --all-targets --message-format=short 2>&1)
status=$?

# Strip ANSI colour before counting anything. This is the whole bug: the
# workflow sets CARGO_TERM_COLOR=always, so on CI every diagnostic line begins
# with an escape sequence rather than the literal word, and the original
# `grep -cE "^warning"` matched **nothing**. The ceiling of 769 was therefore
# never enforced on CI at all -- the step reported 0 and passed on every commit
# since it was added. Locally the pipe turns colour off, which is exactly why
# the two disagreed (#1134).
raw=$(printf '%s\n' "$raw" | sed -e 's/\x1b\[[0-9;]*[A-Za-z]//g')

# `Finished` is cargo's own statement that the check completed. `Checking` and
# `Compiling` say units were actually analysed rather than served whole from a
# warm target dir.
finished=$(printf '%s\n' "$raw" | grep -cE "^\s*(Finished|Checking|Compiling)")

if [ "$status" -ne 0 ]; then
  echo "FAIL: cargo clippy exited $status — the count below would be an artefact"
  echo "  of a failed run, not a lint count. Last 40 lines:"
  printf '%s\n' "$raw" | tail -40
  exit 1
fi

if [ "$finished" -eq 0 ]; then
  echo "FAIL: cargo clippy produced no Finished/Checking/Compiling line, so"
  echo "  nothing was measured. A count from this run means only that the"
  echo "  output was empty (#1134). Last 40 lines:"
  printf '%s\n' "$raw" | tail -40
  exit 1
fi

# `--message-format=short` puts one diagnostic per line as
# `path:line:col: warning: ...`, so the per-crate summary
# ("warning: `samyama` (lib) generated 64 warnings") is the only thing that can
# still start the line with `warning`. Counting both was #1134's original
# complaint: the summary count is one line per crate that emitted anything, so
# it moves with the toolchain and with how the workspace splits into units,
# neither of which is lint debt.
lints=$(printf '%s\n' "$raw" | grep -cE "^[^ ].*: warning: ")
summaries=$(printf '%s\n' "$raw" | grep -cE "^warning: .* generated .* warning")

# The ceiling is on lints, not on lints plus summaries. A per-crate summary is
# one line per crate that emitted anything, so it moves with the toolchain and
# with how the workspace splits into compilation units, neither of which is lint
# debt (#1134). It is still printed, because it is what the old combined number
# was made of and leaving it out would make the two eras look incomparable.
count=$lints

echo "clippy lints: $count (ceiling $CEILING)"
echo "  per-crate summaries, not counted: $summaries"
echo "  cargo reported $finished Finished/Checking/Compiling lines"

# 796 is the lint-only count measured **on CI**, on `main`, run 34314866037.
#
# The first value here was 795, read from the CI run of the pull request that
# introduced this counting (34313013867). That was wrong by one, and the reason
# is worth keeping: GitHub builds a PR as a **merge of the branch into main**, so
# a figure read there is only valid while main stands still. Three PRs landed
# between that measurement and the merge, and one of them carried a lint, so main
# broke on its own gate the moment it went green on the PR.
#
# So: when raising or lowering this, read the number from a CI run **on main**,
# not from a pull request. A PR figure is a forecast.
if [ "$count" -gt "$CEILING" ]; then
  echo "FAIL: $((count - CEILING)) more clippy lints than the ceiling."
  echo "  New code should not add to the backlog. Fix the new warnings, or"
  echo "  raise the ceiling in the same commit and say why."
  echo
  # Which ones. A gate that says "one too many" without saying which sends the
  # reader to reproduce it locally, and a lint set is not identical across
  # toolchains -- CI reads two more than this machine on the same commit -- so
  # reproducing it locally is exactly what does not work. Grouped by file, since
  # a diff against the ceiling is usually a handful of files.
  echo "  lints by file:"
  printf '%s\n' "$raw" | grep -E "^[^ ].*: warning: " \
    | sed -E 's/:[0-9]+:[0-9]+:.*//' | sort | uniq -c | sort -rn \
    | sed 's/^/    /'
  exit 1
fi

if [ "$count" -lt "$CEILING" ]; then
  echo "OK: $((CEILING - count)) below the ceiling — lower it to $count and lock the gain in."
else
  echo "OK: at the ceiling."
fi
