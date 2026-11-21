#!/bin/sh
# Fast native gate: 36 fixtures + dev answer + exact diff against a reference output on 20M.
# Usage: scripts/hostcheck.sh [REF_OUT_20M]   (BIN overrides the binary)
set -eu
ROOT=$(cd "$(dirname "$0")/.." && pwd)
PY="$ROOT/.venv/bin/python"
BIN="${BIN:-$ROOT/rust/target/release/shopsess}"; export BIN
T=$(mktemp -d)
fx=0; "$PY" "$ROOT/scripts/run_fixtures.py" "$ROOT/fixtures" -- "$ROOT/scripts/host_run.sh" > "$T/fx.log" 2>&1 || fx=1
tail -1 "$T/fx.log"
"$ROOT/scripts/host_run.sh" "$ROOT/data/dev" "$T/dev"
dv=0; "$PY" "$ROOT/scripts/compare.py" "$T/dev" "$ROOT/ref/dev-answer/dev-answer" > "$T/dev.log" || dv=1
grep -E 'rows' "$T/dev.log"
g=0
if [ -n "${1:-}" ]; then
  "$ROOT/scripts/host_run.sh" "$ROOT/data/gen20m" "$T/g20"
  "$PY" "$ROOT/scripts/compare.py" "$T/g20" "$1" > "$T/g20.log" || g=1
  grep -E 'rows' "$T/g20.log"
fi
if [ "$fx$dv$g" = 000 ]; then echo "HOSTCHECK PASSED"; rm -rf "$T"; else echo "HOSTCHECK FAILED fx=$fx dev=$dv g20=$g (logs in $T)"; grep -iv pass "$T/fx.log" | head -20; exit 1; fi
