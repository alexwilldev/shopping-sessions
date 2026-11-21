#!/bin/sh
# Submission gate: all 36 fixtures + the dev set, run with scorer flags.
# Usage: scripts/check.sh [IMAGE]   (env: PLATFORM, CPUS, MEM)
# Exit 0 only if every fixture and the dev set match exactly.
set -eu
ROOT=$(cd "$(dirname "$0")/.." && pwd)
export IMAGE="${1:-${IMAGE:-shopping-sessions:latest}}"
export PLATFORM="${PLATFORM:-linux/arm64}" CPUS="${CPUS:-8}" MEM="${MEM:-7g}"
PY="$ROOT/.venv/bin/python"
OUT="$ROOT/out/dev"

echo "== fixtures ($IMAGE, $PLATFORM)" >&2
fx=0; "$PY" "$ROOT/scripts/run_fixtures.py" "$ROOT/fixtures" -- "$ROOT/scripts/docker_run.sh" || fx=1

echo "== dev set" >&2
rm -rf "$OUT"; mkdir -p "$OUT"
start=$("$PY" -c 'import time;print(time.time())')
"$ROOT/scripts/docker_run.sh" "$ROOT/data/dev" "$OUT"
end=$("$PY" -c 'import time;print(time.time())')
"$PY" -c "print(f'dev wall time: {$end-$start:.2f}s')"
dv=0; "$PY" "$ROOT/scripts/compare.py" "$OUT" "$ROOT/ref/dev-answer/dev-answer" || dv=1
if [ "$fx$dv" = 00 ]; then echo "CHECK PASSED" >&2; else echo "CHECK FAILED (fixtures=$fx dev=$dv)" >&2; exit 1; fi
