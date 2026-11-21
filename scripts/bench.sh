#!/bin/sh
# Time an image on a dataset with scorer flags, N runs, appending to logs/bench.log.
# Usage: scripts/bench.sh IMAGE DATASET_DIR [RUNS] [LABEL]   (env: PLATFORM, CPUS, MEM)
# Verifies each run against the first run's output (catches nondeterminism).
set -eu
ROOT=$(cd "$(dirname "$0")/.." && pwd)
export IMAGE="$1"; DATA="$2"; RUNS="${3:-3}"; LABEL="${4:-}"
export PLATFORM="${PLATFORM:-linux/arm64}" CPUS="${CPUS:-8}" MEM="${MEM:-7g}"
PY="$ROOT/.venv/bin/python"
mkdir -p "$ROOT/logs"
i=1
while [ "$i" -le "$RUNS" ]; do
  OUT="$ROOT/out/bench-$i"; rm -rf "$OUT"; mkdir -p "$OUT"
  start=$("$PY" -c 'import time;print(time.time())')
  if "$ROOT/scripts/docker_run.sh" "$DATA" "$OUT" 2> "$ROOT/logs/bench-run-$i.stderr"; then status=ok; else status="exit$?"; fi
  end=$("$PY" -c 'import time;print(time.time())')
  secs=$("$PY" -c "print(f'{$end-$start:.2f}')")
  line="$(date '+%Y-%m-%d %H:%M') image=$IMAGE data=$(basename "$DATA") platform=$PLATFORM cpus=$CPUS mem=$MEM run=$i status=$status secs=$secs $LABEL"
  echo "$line" | tee -a "$ROOT/logs/bench.log"
  if [ "$i" -gt 1 ] && [ "$status" = ok ]; then
    "$PY" "$ROOT/scripts/compare.py" "$ROOT/out/bench-1" "$OUT" > /dev/null || echo "WARNING: run $i differs from run 1" | tee -a "$ROOT/logs/bench.log"
  fi
  i=$((i + 1))
done
