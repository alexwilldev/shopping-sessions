# STATUS: Shopping Sessions (resume from this file)

Read first: `source files/rules.md` (the spec, wins over everything) and `PLAN.md` (strategy, phases, budget).
Cutoff: **Sun 2026-11-22 11:00 ET**. **When asking the user to submit, always give the repo URL and the full commit SHA.** 20 scored attempts total, max 3/hour (raised Sat afternoon). Never submit or create/push a public repo without the user's OK.

**Scored so far: 37.48 s (v1) → 3.39 s (v2) → 2.09 s (v2.3) → 1.51 s (v3.1 1a1dbd2, position 180; Sat 13:30) → 1.40 s (v3.3 b910dc1, best; Sat 18:19, wait 205 ms). NOTE: CPU fell ~25% v3.1->v3.3 but score only ~7%: a large fixed/serial part on the scorer. Probe branch `probe` (sleeps 9x own time) to measure in-process T. Leader 0.65 s (Sat 00:15). Quiet M1 host → scorer ratio ≈ 1.75× (1.2 s → 2.09 s), so beating 0.65 s needs ≈ 0.37 s on the host for 20M.**

## v3.3 (Sat ~18:45): SIMD ID packing (NEON/SSE4.1), lock-free intern table for links, tokenizer fast path (blocks whose quotes all sit at field edges). Single-thread 20M CPU 3.10 -> 2.81 s vs v3.2. Gates: unit tests on arm64 AND x86 (Rosetta, x86-64-v3: `RUSTFLAGS="-C target-cpu=x86-64-v3" cargo test --release --target x86_64-apple-darwin`), hostcheck with the x86 binary (BIN=rust/target/x86_64-apple-darwin/release/shopsess), 40 fuzz, long-ID test. Docker amd64 gate not run (MACHINA-CAD attempt 1 has the machine; <=2 cores).

## v3.2 (c5d7fff, pushed Sat 14:10): bucketed link table, ts day table, assists per (interval, day), SWAR digits. Native gates + 40 fuzz + 60M MATCH; amd64 Docker gate PASSED 18:05 (Docker recovered). 8-thread 20M user CPU: v2.4 5.8 s, v3.1 4.4 s, v3.2 3.85 s.
Local 8-thread 20M wall ~0.73 s vs v3.1 ~0.77-0.80 s (noisy). Per-thread parse breakdown (M1, events): tokenizer 0.59, keys 0.37, ts 0.27, validation 0.25, resolve 0.28, scatter 0.1; base (links+clicks) 0.53; dedup 0.24; sessions 0.5.

## v3 (Sat 21 Nov afternoon): rewrite from the competitor-scan list
- Done: hand-written Parquet writer + `_exit`; u64 packed keys (keys.rs); dense `p<n>`/`c_<n>` lookups; links first, parse resolves persons with a prefetched open-addressing table; person-partitioned 32 B records + 16 B (event_id, loc) dedup records; in-place dedup in ~1024 cache-sized partitions; SIMD bitmask tokenizer (scan.rs, NEON/AVX2, branch-free quotes); SWAR timestamps (ts.rs); MIMALLOC large OS pages env.
- Gates: unit tests (keys, scan, ts vs reference), host gate `scripts/hostcheck.sh REF` (36 fixtures + dev + 20M diff vs v2.4), 40 native fuzz seeds vs v2.4, 60M diff vs v2.4, Docker arm64 + amd64 check.
- Single-thread parse CPU on M1: 4.1 s -> 2.8 s; dedup phase 1.4 -> 0.27 CPU-s; sys 2.1 -> 0.7 s.
- Next: 30-min bins for sessions; link lookups (0.55 s/thread on M1, TLB-bound on 16 KB pages; should be better under THP on the scorer).

## Organizer guidance (via MECHROYALE, Sat ~18:20): leader 0.28 s. Final is ranked on NEW data: "don't overfit" to the generator; exact copies of others' code may be disqualified.
Our shortcuts are all exact with a general fallback: packed keys fall back to an interned table for any ID (long, non-[0-9A-Za-z_]); `Dense` uses an array only when EVERY product/campaign ID is prefix + canonical number (< 2^22), else an exact hash map; ts table covers 1970-2099 and falls back to the calendar formula; 30-min bins use the manifest window. Keep it that way.

## Competitor scan (Sat 21 Nov ~13:00 ET; 181 public repos, read for strategy only: COPY NO CODE)
- Board (per other teams' notes, Sat 10:13): best **0.31 s** (two teams), next 0.42–0.58 s; 0.68 s was 21st of 269. Our 2.09 s is far back.
- Scorer facts reported by teams that ran probe commits: Cascade Lake Xeon 2.8 GHz (AVX-512 F/BW/VL/VNNI), 8 CPUs via cgroup quota (4 cores + HT), **/data is tmpfs** (input always in RAM, warm), THP=madvise (huge pages work), ~0.1 s fixed overhead, score ≈ own process time. Startup ~50 ms.
- Techniques the fast teams (0.68–1.0 s on the board) report, ranked by likely gain for us:
  1. **Fast exit**: `_exit` right after the output files are written; never free the big arenas (we `drop()` evs/cks/pid_of at the end).
  2. **Hand-written Parquet writer** instead of the parquet crate (write phase 0.12→0.05 s for one team); INT32 counts, no stats, one write(2) per file.
  3. **Packed u64 keys**: IDs of ≤10 chars [0-9a-z] packed exactly into a u64 (others go to an exact fallback table); **dense array index for products/campaigns**.
  4. **Sessions without sorting**: put each person's events in 30-minute bins; consecutive occupied bins merge when next.min − prev.max ≤ 30 min. Alternatively, counting sort plus radix sort by (person rank, ts).
  5. **Size dedup and grouping so they stay in cache**: about 1024 partitions so each per-bucket table fits in L2; open addressing with **software prefetch** (batched lookups, prefetch 8–16 ahead); dedup marks the losing copies in place.
  6. **Huge pages**: MADV_HUGEPAGE / pre-fault the arenas; mmap the input (tmpfs means zero copy).
  7. Build for `target-cpu=cascadelake` (AVX-512 structural CSV scan; SWAR or SIMD timestamps with a date table).
  8. Lock-free parallel union-find with an atomic min for the person name; link table filled during the parse.
- Local A/B method used by several teams: pin to 4 cores + HT siblings, warm cache, median of 5+, CPU time per phase.

**Machine reserved for MACHINA-CAD from Sat 01:40 to ~08:00 (user-approved): no benchmarks, no Docker, no CPU claims, no big network transfers in that window.**
**To beat 0.65 s, total CPU must fall ~4×: the remaining big cost is ~4 hash lookups per event (product, dedup, resolve, pid). Ideas: array lookup for structured IDs (p<digits>, c_<digits>) with a map fallback; fuse dedup with grouping; avoid the 64 B RawEv round trip.**

## PAUSED 2026-11-20 22:25 (user prioritizing MECHROYALE, then BLAST-RADIUS). Resume heavy benchmarks only after Blast's Docker labelling finishes AND the user says so in this session (planned: Saturday morning, quiet machine). CLAIM on the ledger first.
- Submitted by the user: #3 v2.1 `528424a`, #4 v2.2 `c8b6462`, #5 v2.3 `3b796af`. **Results pending: ask the user and fill in the submission log.**
- Pushed HEAD = v2.3 `3b796af6ec40448c2ef61a1061b581ac5e4cbcea`.
- (now committed in v2.4) progressive freeing (`transpose()` [chunk][bucket] → [bucket][chunk]; each bucket's records freed after use). Fixtures 36/36 and 20M match pass; not fuzzed; peak RSS unchanged (~5.2 GB, the peak is at the end of parsing). Backup of v2.3 source: scratchpad `main.rs.v23` (may be gone after reboot; `git diff` shows the change).
- Quiet-host 20M: v2.3 ≈ 1.17–1.21 s, progressive-free ≈ 1.14 s. Stages: parse 0.55–0.66 s, dedup 0.25–0.33, sessions 0.25–0.30, write 0.06.
- **Next (in order):** (1) DONE in v2.4. (2) dedup: a smaller win table (u64 hash → index, with full-key check). (3) parse: further per-line cuts (ts fast path, smaller RawEv). Gate each: fixtures, dev, 20M vs `out/h20-db`, `MEM=3g scripts/fuzz.sh shopping-sessions:v1 shopping-sessions:latest 30 <seed>`, arm64 + amd64 `check.sh`.
- Shared board: claim released. Docker volumes `shop5m`, `shop20m` exist (VM timings at 20M are useless: 8 GB VM thrashes).

## Phase
**Phase 2: Rust v2 READY (2026-11-20 20:55).** `rust/` (binary `shopsess`), root `Dockerfile` now builds it (`Dockerfile.polars` = v1). Gates: host 36/36 fixtures + exact dev match; Docker arm64 + amd64 `check.sh` PASSED; fuzz 30/30 seeds identical to v1; 20M output identical to v1/DuckDB. Host 20M: **4.5 s** (v1: 54 s); dev 0.29 s; peak RSS 5.4 GB. Docker 20M on the Mac: 11–14 s (dominated by OrbStack's /data file sharing, so use host timings).
v1 practice result: **37.48 s, rank 68; leader 0.93 s** → v2 must get to ~1 s.

Older: **Phase 1: v1 READY TO SUBMIT (2026-11-20 20:25).** Polars v1: `make check` PASSED on arm64 and amd64 (36/36 fixtures + exact dev match, even at MEM=3g). 20M output matches DuckDB exactly. Repo **https://github.com/alexwilldev/shopping-sessions** (public, `main`) pushed 2026-11-20. **The user submits #1 themselves** (commit `9b66a084e46543bdf4efae2ad5910b8dbd7682df`). Ask the user for the result and log it below.

## Shared machine (4 tracks on this Mac)
- Message board: `~/Developer/hackathon/DISK_LEDGER.md`. **Check it regularly (user's instruction).** Our tag is `SHOPPING`. CLAIM any job that uses >2 GB disk, >4 GB RAM, or >4 cores for more than 10 min. Edit only our own rows.
- Other sessions: MACHINA-CAD (paused), MECHROYALE (yielded the CPU to us; no local Docker), BLAST-RADIUS (Docker labeling jobs on 3–6 cores). Message them with SendMessage, using the header `[SHOPPING] <TYPE> — …`.
- Host timings are noisy (load averages of 30–80 seen). Compare engines within one run, not across runs.

## Harness (how to use)
- **Linux-like timing:** `scripts/vbench.sh IMAGE shop5m 3`. Docker volume `shop5m` holds data/gen5m inside the OrbStack VM (no macOS file sharing). The binary logs user/sys CPU and minor faults per stage. Use CPU time as the metric; wall time is noisy while other tracks run.
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
Profiling notes (single-thread on cached `data/gen5m`, the reliable local metric; 20M wall time on this loaded Mac is I/O-noisy):
events parse ≈ 240 ns/line after the tokenizer (was 350). Remaining per-line costs: key packing, 2× parse_ts (~100 ns), resolve() map lookup on 1.2M IDs + product map (~90 ns), the 64 B RawEv push into 256 buckets. sys time 2.5–3.5 s = page faults from fresh allocations.
1. Profile v2 stages on 20M on the host: events parse is ~2 s of 4.5 s. Targets: SIMD/memchr line + comma scan, avoid the 64 B RawEv (dedup with a compact table), pre-size bucket vecs, fewer than 256 buckets × chunks Vecs, faster hash (FxHash on u128).
2. Cut memory (5.4 GB at 20M → ~16 GB at 60M is risky): smaller records, free stage outputs early.
3. Each candidate: fixtures + dev + fuzz (`MEM=3g CPUS=4 scripts/fuzz.sh shopping-sessions:v1 shopping-sessions:latest 30`), then submit if the host gain is >10%.

## Older next tasks
1. Get submission #1's result from the user (dev check + practice time) and log it below. Keep every submitted commit in the public repo.
2. Start Phase 2 (Rust v2) in `rust/`, with `make fuzz REF=<polars image>` as the gate. Design for low memory: no full copies of the string columns, interned u32 IDs.
3. Bench v2 on 20M/60M, CLAIMing on the ledger for runs over 10 min.

## Risks / notes
- **Memory is now the main risk/cost:** ~5 GB RSS at 20M (+1.8 GB mapped input). In the 8 GB OrbStack VM, 20M runs thrash (user CPU 20 s vs 6 s on the host), so VM timings at 20M are meaningless. 60M on the scorer would be ~15 GB + 5.4 GB input, within 24 GB but tight. Next: free records stage by stage (transpose ownership [bucket][chunk]), and smaller records.
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
| 2 | 11-20 ~21:00 | 98755db | Rust v2 | passed | **3.39 s** (rank 61; 60 ms wait) | |
| 3 | (never scored) | 528424a | Rust v2.1 | local gates + fuzz 30/30 passed | pending | SWAR tokenizer, fast key_of, contiguous dedup, parallel user_daily write; 20M user CPU 12.6 → 8.7 s |

| 4 | (never scored) | c8b6462 | Rust v2.2 | all gates + fuzz 30/30 | pending | links concurrent with parsing; users resolved after dedup; Linux 5M user CPU 2.33 → 1.77 s |

| 5 | 11-20 22:39 | 3b796af | Rust v2.3 | passed | **2.09 s** (best, entered in final; 20 ms wait) | in-place dedup (no record copy), per-thread campaign arrays, NB=64; quiet host 20M 1.36–1.95 s vs v2.2 1.70–2.81 s |

| 6 | Sat slot | v2.4 (HEAD at handover) | Rust v2.4 | all gates + fuzz 30/30 | pending | sessions stage: person grouping via counting sort (sessions CPU −45%); records freed bucket by bucket. Expect ~1.9 s (≈ −8% CPU) |

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
