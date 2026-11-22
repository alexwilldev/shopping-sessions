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

## Suggestions
