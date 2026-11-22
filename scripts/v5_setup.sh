#!/bin/sh
# Morning setup for the v5 rules (Sat 21:08 rescore). Run after the user has put the NEW
# generator.zip, dev-answer.zip and rules.md into "source files/".
#   1. builds the new generator into bin/generate (from a scratch copy, outside the repo tree)
#   2. regenerates data/dev (seed 1, the public dev set), data/gen5m and data/gen20m
#   3. unpacks the new dev answer into ref/dev-answer
#   4. builds the current binary and runs the native gate (fixtures + dev answer)
#   5. writes a 20M reference output with ORACLE (default: an older, independently written build)
# Usage: scripts/v5_setup.sh [ORACLE_BINARY]   (env: NCPU, default 2)
set -eu
ROOT=$(cd "$(dirname "$0")/.." && pwd)
SRC="$ROOT/source files"
NCPU="${NCPU:-2}"; export NCPU
WORK=$(mktemp -d)

echo "== generator"
unzip -q "$SRC/generator.zip" -d "$WORK"
(cd "$WORK/generator" && nice cargo build --release -j "$NCPU" >/dev/null 2>&1)
mkdir -p "$ROOT/bin"
cp "$WORK/generator/target/release/generate" "$ROOT/bin/generate"

echo "== datasets (old ones replaced)"
for spec in "dev 1 1000000 250000" "gen5m 5 5000000 1250000" "gen20m 20 20000000 5000000"; do
  set -- $spec
  rm -rf "$ROOT/data/$1"
  nice "$ROOT/bin/generate" --seed "$2" --events "$3" --clicks "$4" --output "$ROOT/data/$1" >/dev/null
  echo "   data/$1: $(du -sh "$ROOT/data/$1" | cut -f1)"
done

echo "== dev answer"
rm -rf "$ROOT/ref/dev-answer"; mkdir -p "$ROOT/ref/dev-answer"
unzip -q "$SRC/dev-answer.zip" -d "$ROOT/ref/dev-answer"

echo "== build + native gate (fixtures + dev answer)"
(cd "$ROOT/rust" && nice cargo build --release -j "$NCPU" 2>&1 | grep -E "^error" -A5 || true)
"$ROOT/scripts/hostcheck.sh"

if [ -n "${1:-}" ]; then
  echo "== 20M reference with oracle $1"
  REF="$ROOT/out/ref20m_v5"; rm -rf "$REF"; mkdir -p "$REF"
  DATA_DIR="$ROOT/data/gen20m" OUTPUT_DIR="$REF" nice "$1" 2>/dev/null
  "$ROOT/scripts/hostcheck.sh" "$REF"
fi
rm -rf "$WORK"
echo "== v5 setup done; also re-read the new rules.md for changed rules"
