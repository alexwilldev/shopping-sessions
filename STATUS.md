# STATUS: Shopping Sessions (resume from this file)

Read first: `source files/rules.md` (the spec, wins over everything) and `PLAN.md` (strategy, phases, budget).
Cutoff: **Sun 2026-11-22 11:00 ET**. 20 scored attempts total, max 2/hour. Never submit or create/push a public repo without the user's OK.

## Phase
**Phase 1: v1 correct, NOT yet submitted (2026-11-20 19:50).** Polars v1 passes 36/36 fixtures and matches the dev answer exactly on the host, and DuckDB and Polars agree on 20M. Blocked on: the Docker gate (OrbStack's engine hung at 19:34, see Risks) → amd64 check → create repo → submission #1.

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

## Next 3 tasks
1. Get Docker healthy (needs the user's OK to restart OrbStack; it would kill BLAST-RADIUS's containers), then run `make check` (arm64) and `make check-amd64`. Polars under amd64 emulation may need AVX; if it crashes with an illegal instruction, note it and rely on arm64 + host checks.
2. Ask the user to create `shopping-sessions` (public), push, and approve submission #1. Log it in the table below.
3. Start Phase 2 (Rust v2) in `rust/`, with `make fuzz REF=<polars image>` as the gate.

## Risks / notes
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

## Local bench log (relative only)
| Date | Impl | Dataset | Where | Wall time | Notes |
|---|---|---|---|---|---|
| 11-20 19:20 | DuckDB v1 | 20M | host, loaded | 137 s | before the RANGE fix (wrong on 6 rows) |
| 11-20 19:25 | DuckDB v1 | 20M | host, loaded | 142 s | RANGE fix; matches Polars |
| 11-20 19:15 | Polars v1 | 20M | host, loaded | 75 s | |
| 11-20 19:32 | Polars v1 | 20M | host, load ~40 | 54 s | native ingest + duplicate-only dedup |
| 11-20 19:10 | Polars v1 | dev 1M | Docker arm64 not run yet; host 1.9 s pipeline + ~1.7 s imports | | |
