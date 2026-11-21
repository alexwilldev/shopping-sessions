#!/bin/sh
# Differential test: two images on N random small datasets; any diff is a bug in one of them.
# Usage: scripts/fuzz.sh IMAGE_A IMAGE_B [N] [FIRST_SEED]   (env: PLATFORM, CPUS, MEM)
# Failing datasets are kept in data/fuzz-fail-SEED for debugging; passing ones are deleted.
set -eu
ROOT=$(cd "$(dirname "$0")/.." && pwd)
A="$1"; B="$2"; N="${3:-50}"; SEED="${4:-1000}"
export PLATFORM="${PLATFORM:-linux/arm64}" CPUS="${CPUS:-8}" MEM="${MEM:-7g}"
PY="$ROOT/.venv/bin/python"
fails=0; i=0
while [ "$i" -lt "$N" ]; do
  s=$((SEED + i))
  events=$(( (s * 7919 % 180000) + 20000 ))   # 20k..200k, varies per seed
  D="$ROOT/data/fuzz-$s"; rm -rf "$D" "$ROOT/out/fa" "$ROOT/out/fb"
  "$ROOT/bin/generate" --seed "$s" --events "$events" --clicks $((events / 4)) --output "$D" > /dev/null
  IMAGE="$A" "$ROOT/scripts/docker_run.sh" "$D" "$ROOT/out/fa" 2> /dev/null
  IMAGE="$B" "$ROOT/scripts/docker_run.sh" "$D" "$ROOT/out/fb" 2> /dev/null
  if "$PY" "$ROOT/scripts/compare.py" "$ROOT/out/fa" "$ROOT/out/fb" > "$ROOT/logs/fuzz-$s.txt" 2>&1; then
    echo "ok   seed=$s events=$events"; rm -rf "$D" "$ROOT/logs/fuzz-$s.txt"
  else
    echo "FAIL seed=$s events=$events (see logs/fuzz-$s.txt, data kept in data/fuzz-fail-$s)"
    rm -rf "$ROOT/data/fuzz-fail-$s"; mv "$D" "$ROOT/data/fuzz-fail-$s"; fails=$((fails + 1))
  fi
  i=$((i + 1))
done
echo "$((N - fails))/$N seeds matched"
[ "$fails" -eq 0 ]
