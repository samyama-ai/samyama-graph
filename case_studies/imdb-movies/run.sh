#!/usr/bin/env bash
# Fetch snapshot → import → validate every showcase query → (RECORD=1) record GIF.
exec "$(cd "$(dirname "$0")/../_lib" && pwd)/run_case_study.sh" "$@"
