#!/bin/sh
# Time an image on a dataset stored in a Docker volume (inside the Linux VM: no macOS file
# sharing in the way, so timings are closer to the scorer's). Usage: vbench.sh IMAGE VOLUME [RUNS] [extra docker args...]
set -eu
IMAGE="$1"; VOL="$2"; RUNS="${3:-3}"; shift 3 || true
for i in $(seq 1 "$RUNS"); do
  OUT=$(mktemp -d); chmod 777 "$OUT"
  s=$(python3 -c 'import time;print(time.time())')
  docker run --rm --network none --read-only --tmpfs /tmp:rw,size=64m --cpus "${CPUS:-8}" --memory "${MEM:-3g}" --memory-swap "${MEM:-3g}" \
    --user 1000:1000 -v "$VOL":/data:ro -v "$OUT":/output -e NCPU="${CPUS:-8}" -e MEM_GB=3 "$@" "$IMAGE" 2> "$OUT.err" || echo "run failed: $(tail -2 "$OUT.err")"
  e=$(python3 -c 'import time;print(time.time())')
  python3 -c "print(f'run $i: {$e-$s:.3f}s')"; cat "$OUT.err"
  rm -rf "$OUT" "$OUT.err"
done
