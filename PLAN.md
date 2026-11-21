# Shopping Sessions: plan to win

Cutoff: **Sun 2026-11-22 11:00 ET**. Budget: **20 scored attempts total, max 2/hour**. 3 usage windows.
Score = container start→exit on hidden data. Any wrong row = nothing. Final ≈ 60M events, 3-min cap.
`ref/` holds the unzipped sources (starters, fixtures, generator, dev answer). rules.md wins over everything.

## Facts that shape the plan

| Fact | Consequence |
|---|---|
| Ties go to the earlier submission | Submit a correct v1 in window 1, even if slow |
| Wrong dev rows are shown before the timed run, but a rejected run still costs time in the hour | Never submit until `scripts/check.sh` is green locally |
| Workload is parse-bound: ~60M events ≈ 5–6 GB CSV, plus ~15M clicks | Reading CSV is most of the time. Engine startup and Parquet write are small but count |
| The CSV dialect is non-standard (`\r` is data, `""` is null, quotes only around a whole field, a bad quote makes the line malformed) | Don't trust any engine's CSV parser. Read each line as one string and split it ourselves |
| The generator is deterministic per seed and has `--threads` | Run differential tests over many seeds: v1 vs v2 on 50+ small datasets |
| Local: M1 Pro, OrbStack with 8 CPU / **8 GB RAM**, **27 GB free disk** | Local 60M runs need `MEM=7g`. Time on arm64 native images. amd64 under emulation is only for a final smoke test |
| No Rust toolchain locally | Install rustup (approved) for the generator and v2. The release image is still built by the Dockerfile |

## Architecture: two implementations, one oracle

- **v1 (oracle, first submission): Python + DuckDB or Polars.** Picked by the benchmark in Phase 1. Clear, close to the rules, correct on the 36 fixtures and on dev.
- **v2 (speed): a custom Rust binary** in a minimal image. Same outputs as v1 on every seed.

**Is a custom Rust parser worth it? Yes, after v1 is in.** Why:
- DuckDB/Polars can't parse this dialect natively. Both would read whole lines and split them with string functions. That loses their fast CSV path and makes them roughly 3–5× slower than a hand parser.
- The rest of the work is hash dedup, union-find, a sort by (person, ts), and a linear scan. That is simple in Rust and needs no query engine.
- No interpreter or engine startup.
- Expected result: tens of seconds with an engine vs a few seconds with Rust at 20M. Phase 1 measures this before we commit.
- Risk: correctness. The v1 oracle plus seed fuzzing contains that risk.

### Rules most likely to go wrong, and how each is handled
1. **CSV dialect.** Split on `\n` only. Per line: count fields by commas. A field is either unquoted with no `"`, or exactly `"…"` with no inner `"`, otherwise the line is malformed. An empty line is malformed. Wrong field count is malformed. `""` and empty both mean null. Keep spaces and `\r`. The header is skipped by position (first line of each file), never by value (L3: a data row equal to the header is data).
2. **Timestamps.** Hand-written validator for F1 `YYYY-MM-DDTHH:MM:SSZ` (exactly 20 bytes), F2 (25 bytes, `±HH:MM`, hh≤23, mm≤59, colon required), F3 (ASCII digits only, any length; i64 overflow means out of window, not malformed). Day-in-month with the leap-year rule, using `days_from_civil` as in the generator. Then the window check: events `[ws, we)`, clicks `[ws−7d, we)`. Unit tests for every bad form listed in rules.md.
3. **Linked IDs.** Intern every ID to u32. Union-find with path halving, iterative only (no recursion and no recursive SQL). Name = the byte-wise min over the component, including IDs that appear only in links. v1: `scipy.sparse.csgraph.connected_components` or a numpy union-find, never a recursive CTE.
4. **Dedup before validation.** Group by event_id over *parsed* lines (non-empty event_id) and keep the min ingest_ts. Then validate. A later valid copy never rescues an invalid kept one.
5. **Equal timestamps.** Sort by (person, ts). New session only when `ts − prev_ts > 1_800_000`. Comparing with the previous row gives the same result, since equal ts have a gap of 0.
6. **Credited click.** Per person, sort clicks by (ts, click_id). For a session start s: the latest click with ts ≤ s and s − ts ≤ 7d, and on equal ts the smallest click_id (byte-wise). Assists = distinct campaigns among clicks in `[s−7d, s]`. organic never assists.
7. Other details: `top_category` ties go to the byte-wise smallest, and `unknown` competes. Price must be ASCII digits only, value 1..=10,000,000, leading zeros allowed with any length (`000…0250`). `day` is a Parquet DATE.

## Phases (each ends in something submittable)

### Phase 0: harness (window 1, ~1 h)
- `git init`. Repo layout: `Dockerfile`, `run.sh`, `pipeline/` (v1), `rust/` (v2), `scripts/`, `fixtures/` (copied from the starter), `.dockerignore` and `.gitignore` excluding `data/`, `ref/`, `source files/`.
- Install rustup and build the generator → `bin/generate`. Generate `data/dev` (seed 1, 1M/250k) and confirm it matches `ref/dev-answer` once v1 runs.
- `scripts/check.sh IMAGE`: all 36 fixtures through `docker_run.sh` (scorer flags), then dev + `compare.py`. Non-zero exit on any diff. **This gates every submission.**
- `scripts/bench.sh IMAGE SIZE`: generates `data/gen20m` (20M events / 5M clicks) and `data/gen60m` once, in the background. Runs the image with scorer flags (`MEM=7g` locally) 3 times and logs the wall times to `logs/bench.log`.
- `scripts/fuzz.sh IMG_A IMG_B N`: N random seeds at 50k–200k events. Runs both images and diffs them. Used once v2 exists.
- Python venv (`uv`) with polars and duckdb for the scripts.

### Phase 1: correct v1 + engine choice (window 1) → **Submission #1**
- Write v1 in DuckDB: read each line as one VARCHAR column (no quote and no delimiter, header skipped per file), then split and validate in SQL. Links: numpy/scipy union-find. Get the fixtures green, then dev green.
- Port the same logic to Polars (single-column `scan_csv`, `quote_char=None`).
- Benchmark both on 20M (arm64 native, 3 runs each). Submit the faster one once `check.sh` passes. Log it in STATUS.md.

### Phase 2: Rust v2 (window 2) → **Submission #2**
- Single binary. mmap every file. Split into ~16 MB chunks aligned to `\n` and parse the chunks in parallel (rayon + `memchr`).
- IDs interned to u32 through a sharded hash map (ahash/FxHash). Strings live in an arena.
- Events: dedup by event_id (min ingest_ts) with a parallel hash or sort, then validate.
- Union-find over links → person per ID → min name.
- Radix-sort the surviving events by (person, ts). Sessionize in parallel over person ranges. Credit and assist with each person's sorted clicks and two pointers.
- Aggregate → Parquet (`arrow`/`parquet` crates), one file per table.
- Gate: fixtures + dev + **fuzz 50 seeds vs v1 with zero diffs**, then submit.

### Phase 3: optimize (window 3) → Submissions #3…
In order of expected gain, measured locally each time:
1. Parse throughput: SIMD scanning, no per-field allocation, fast paths for the common F1/F3 timestamps.
2. I/O: one pass over the data. Read large files in parallel and batch the small ones. `madvise(SEQUENTIAL)`.
3. Hash maps: pre-sized, sharded intern table, compact dedup layout.
4. Build: `-C target-cpu=cascadelake` (all scorers have AVX-512). Run the amd64 image under emulation for correctness only. LTO, codegen-units=1, mimalloc. Minimal image so startup takes milliseconds.
5. Output: one row group, cheap or no compression.
- Submit only when the local gain is >10% (scorer noise is ~20%). Keep ≥4 attempts in reserve for the end.

## Submission budget (20 total)
| # | When | What |
|---|---|---|
| 1 | Window 1 | v1 engine, correct |
| 2–3 | Window 2 | Rust v2 (+1 retry slot) |
| 4–12 | Window 3 | optimizations with >10% local gain |
| 13–20 | Reserve | resubmit the best near the cutoff if the board is noisy; emergency fixes |

## Rules of engagement
- Nothing public without your OK: repo creation, the first push, every submission.
- Long jobs (generator at 20M/60M, benches, fuzz) run in the background and log to `logs/`.
- STATUS.md is updated at the end of every phase and before every submission.
