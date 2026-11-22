# Shopping Sessions: submission slots are nearly gone (read before suggesting anything)

Sun 2026-11-22: **3 submission slots left, and they do not renew before the hackathon ends.**

- **Board:** 1st place is 0.4012 s. Our best is 1.4887 s (var-combo, 7626af8). Leaders invert the per-seed ID bijection, which rules.md forbids; we treat IDs as opaque strings and will keep doing so.
- **What qualifies for a slot:** only changes that are gated (36 fixtures, exact dev answer, 20M diff vs out/ref20m_v5, 40-seed fuzz vs bin/oracle_v24, `make check-amd64`) AND show a clear local CPU win in interleaved A/B runs.
- **No speculative variants:** no env-only A/B variants unless the scores already in hand point to them.
- **Plan for the 3 slots:**
  1. main with the best settings from the latest scores folded in;
  2. one real code speedup, only if it is gated and clearly faster;
  3. a reserve.
- **Codex:** put suggestions in this file under "Suggestions" with a local benchmark number. Claude (SHOPPING session) verifies and gates before anything is queued. The user makes every submission.

## Scores (Sun)
v3.6 code: combo 7626af8 **1.4887 (entered)**, nopop 1.5288, nohuge 1.5710, main 1.5735, chunk1m 1.5887.
v3.7 code + combo: a1483cd 1.5134; + SS_QEV=48000 59488a7 **1.8894** (scorer is L2-sensitive: bigger dedup partitions hurt).
Next candidate: var-qev6 6721e629beda5b5bb42e4f0ee488f569e19fdb64 (SS_QEV=6000, tables fit 1 MB L2). Untested on scorer: var-th16, var-q48t16 (skip: has 48k), var-pev200.

## Suggestions
