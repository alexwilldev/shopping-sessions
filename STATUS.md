# STATUS: Shopping Sessions (resume from this file)

Read first: `source files/rules.md` (the spec, wins over everything) and `PLAN.md` (strategy, phases, budget).
Cutoff: **Sun 2026-11-22 11:00 ET**. 20 scored attempts total, max 2/hour. Never submit or create/push a public repo without the user's OK.

## Phase
**Phase 2: Rust v2 READY (2026-11-20 20:55).** `rust/` (binary `shopsess`), root `Dockerfile` now builds it (`Dockerfile.polars` = v1). Gates: host 36/36 fixtures + exact dev match; Docker arm64 + amd64 `check.sh` PASSED; fuzz 30/30 seeds identical to v1; 20M output identical to v1/DuckDB. Host 20M: **4.5 s** (v1: 54 s); dev 0.29 s; peak RSS 5.4 GB. Docker 20M on the Mac: 11–14 s (dominated by OrbStack's /data file sharing, so use host timings).
v1 practice result: **37.48 s, rank 68; leader 0.93 s** → v2 must get to ~1 s.

Older: **Phase 1: v1 READY TO SUBMIT (2026-11-20 20:25).** Polars v1: `make check` PASSED on arm64 and amd64 (36/36 fixtures + exact dev match, even at MEM=3g). 20M output matches DuckDB exactly. Repo **https://github.com/alexwilldev/shopping-sessions** (public, `main`) pushed 2026-11-20. **The user submits #1 themselves** (commit `9b66a084e46543bdf4efae2ad5910b8dbd7682df`). Ask the user for the result and log it below.

## Shared machine (4 tracks on this Mac)
- Message board: `~/Developer/hackathon/DISK_LEDGER.md`. **Check it regularly (user's instruction).** Our tag is `SHOPPING`. CLAIM any job that uses >2 GB disk, >4 GB RAM, or >4 cores for more than 10 min. Edit only our own rows.
- Other sessions: MACHINA-CAD (paused), MECHROYALE (yielded the CPU to us; no local Docker), BLAST-RADIUS (Docker labeling jobs on 3–6 cores). Message them with SendMessage, using the header `[SHOPPING] <TYPE> — …`.
- Host timings are noisy (load averages of 30–80 seen). Compare engines within one run, not across runs.

## Harness (how to use)
- `source ~/.cargo/env` (rustc 1.100). Generator: `bin/generate` (macOS arm64).
- Script Python: `.venv/bin/python` (polars 2.1.0, duckdb 1.5.6, scipy, numpy, pyarrow).
- Data (gitignored): `data/dev` (seed 1, = the public dev set), `data/gen20m` (seed 20, 20M/5M, 1.8 GB), `data/gen60m` (seed 60, 60M/15M, 5.4 GB).
- `make check`: build the arm64 image, then 36 fixtures + dev vs `ref/dev-answer` with scorer flags. **Gate for every submission**, plus `make check-amd64`.
- Host-only quick loop (no Docker): `.venv/bin/python scripts/run_fixtures.py fixtures -- .venv/bin/python pipeline/pipeline_polars.py`, then `DATA_DIR=data/dev OUTPUT_DIR=out/x .venv/bin/python pipeline/pipeline_polars.py && .venv/bin/python scripts/compare.py out/x ref/dev-answer/dev-answer`.
- `make bench20` / `make bench60` → `logs/bench.log`. `make fuzz REF=<image>`: 50 seeds, image vs image.

## Code
- `pipeline/lines.py`: zero-copy line splitter (mmap + numpy, split on `\n` only, header dropped by position) → Arrow `large_string`, each line keeping its trailing `\n`. Shared by both engines. Needed because DuckDB's CSV reader splits on `\r` even with `new_line='\n'`.
- `pipeline/pipeline_polars.py`: **v1, the default in the image** (`run.sh` runs `$PIPELINE`, default `pipeline_polars.py`).
- `pipeline/pipeline.py`: the DuckDB version, kept as a second oracle (`PIPELINE=pipeline.py`).
- Shared approach: a per-line regex with one group per field (a field is `"[^",\n]*"` or `[^",\n]*`; a non-match means malformed), then unquote, with empty → null. Timestamps by a hand validator. Links by scipy `connected_components`, named by the min ID. Dedup = min ingest_ts over parsed rows, then validate. Sessions from a (person, ts) sort with a gap > 30 min. Credit by asof join to the best click per (person, ts) (min click_id). Assists: merge each (person, campaign)'s coverage `[ts, ts+7d]`, cut it by day, and count session starts with session ranks + asof.

## Engine benchmark (host, 20M events, under heavy load from other tracks)
| Impl | Wall | Notes |
|---|---|---|
| DuckDB v1 | 137–142 s | string functions slow (regex ~2.5M lines/s, ts parse slower); 2–3 cores used on average |
| Polars v1 (first) | 75 s | dedup `over()` 26 s, string sort 19 s |
| Polars v1 (native ingest parse + duplicate-only dedup) | **54 s** | events stage 22 s; credit+assists+write ~20 s (next target) |
Decision: **Polars for v1.** The custom Rust v2 is still planned (PLAN.md Phase 2). Neither engine is near the ~10 s range we want, and 60M at 3× would be about 150 s, close to the 180 s cap.

## Bugs found by differential testing (keep in mind for Rust v2)
- **Equal timestamps (rules.md Events 5):** DuckDB's `lag()` and its running `sum() ROWS` frame ordered tied rows differently, so a tied event got the previous session's number. 20M showed 4 user rows and 2 campaign rows wrong, while the fixtures and dev passed. Fixed with a RANGE frame. In Rust: sort by (person, ts) once and decide new sessions from the previous row in that one order.
- DuckDB common-subexpression elimination evaluated `CAST`s outside their guarding `CASE` (E18 crash) → use `TRY_CAST`.

## Next 3 tasks (Phase 3: optimize v2)
1. Profile v2 stages on 20M on the host: events parse is ~2 s of 4.5 s. Targets: SIMD/memchr line + comma scan, avoid the 64 B RawEv (dedup with a compact table), pre-size bucket vecs, fewer than 256 buckets × chunks Vecs, faster hash (FxHash on u128).
2. Cut memory (5.4 GB at 20M → ~16 GB at 60M is risky): smaller records, free stage outputs early.
3. Each candidate: fixtures + dev + fuzz (`MEM=3g CPUS=4 scripts/fuzz.sh shopping-sessions:v1 shopping-sessions:latest 30`), then submit if the host gain is >10%.

## Older next tasks
1. Get submission #1's result from the user (dev check + practice time) and log it below. Keep every submitted commit in the public repo.
2. Start Phase 2 (Rust v2) in `rust/`, with `make fuzz REF=<polars image>` as the gate. Design for low memory: no full copies of the string columns, interned u32 IDs.
3. Bench v2 on 20M/60M, CLAIMing on the ledger for runs over 10 min.

## Risks / notes
- **Docker memory:** the OrbStack VM (8 GB) is shared with BLAST-RADIUS (~3 GB). Running containers of ours at 7 GB likely hung the VM at 19:34 (user-approved restart at 20:06). **Keep our containers at MEM≤4g while others are running.** Polars v1 at 20M is OOM-killed at 4 GB in Docker (host peak RSS 3.9 GB; the scorer has 24 GB, so it should be fine there).
- **OrbStack hung at ~19:34** (`docker ps` hangs; every `docker run` stuck at 0% CPU; host swap 4 GB). Logged as a WARN on the ledger, and BLAST-RADIUS was told. We stopped only our own jobs.
- OrbStack VM: 8 CPU / 8 GB. Local 60M runs need `MEM=7g`.
- L3: a data row equal to the header is a real link. Skip headers by position, never by value. A tab is an ordinary byte (person `A\tB`).

## Answered by the user (2026-11-20)
- Submit via **public GitHub repo + commit SHA** (ignore the Builders-folder text on the track page).
- **rustup installed locally** (approved).
- Repo: **`shopping-sessions` under the user's default `gh` login**, public, created when v1 is ready. Still ask before creating, pushing, and every submission.
- Follow the shared message board (`DISK_LEDGER.md`) regularly.

## Submission log
| # | Time (ET) | Commit | Impl | Dev check | Practice time | Notes |
|---|---|---|---|---|---|---|
| 1 | 11-20 ~20:30 | 9b66a08 | Polars v1 | passed | **37.48 s** (rank 68; 83 ms wait) | leader 0.93 s |
| 2 | (pending) | see git log | Rust v2 | local gates passed | pending | host 20M 4.5 s |

## Local bench log (relative only)
| Date | Impl | Dataset | Where | Wall time | Notes |
|---|---|---|---|---|---|
| 11-20 19:20 | DuckDB v1 | 20M | host, loaded | 137 s | before the RANGE fix (wrong on 6 rows) |
| 11-20 19:25 | DuckDB v1 | 20M | host, loaded | 142 s | RANGE fix; matches Polars |
| 11-20 19:15 | Polars v1 | 20M | host, loaded | 75 s | |
| 11-20 19:32 | Polars v1 | 20M | host, load ~40 | 54 s | native ingest + duplicate-only dedup |
| 11-20 20:05 | Polars v1 | dev 1M | Docker arm64 | 1.92 s | whole container |
| 11-20 20:10 | Polars v1 | dev 1M | Docker amd64 (emulated) | 8.47 s | correctness only |
| 11-20 20:15 | Polars v1 batched | 20M | host, load ~57 | 60 s | peak RSS 3.9 GB |
| 11-20 20:18 | Polars v1 | 20M | Docker 4 CPU / 4 GB | OOM (exit 137) | memory limit of the shared VM, not the scorer's |
