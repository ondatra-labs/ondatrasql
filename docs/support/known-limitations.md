---
description: Known limitations and edge cases in OndatraSQL.
draft: false
title: Known Limitations
weight: 99
---

## Schema detection on empty lib runs

Dynamic-column libs with an existing target and 0 rows: stub cloned from target. Schema changes not detected until next non-empty run. Force backfill by editing model SQL, or use a static-column lib with declared `Columns`.

## First-run type inference

First run of a dynamic-column lib with no target: stub columns are VARCHAR. Type transition (e.g., `VARCHAR -> BIGINT`) reported on second run. If column names cannot be inferred (`SELECT *` from lib function), first run is skipped — no target created until data arrives.

## Sandbox and time travel

Sandbox fork has its own snapshot history. Time-travel may fail during CDC schema check — silently assumes no change. If CDC EXCEPT fails, falls back to full query. Correctness unaffected.

## Tracked + lib with smart-skip

Lib-driven tracked models where the lib reads the target and returns 0 rows for unchanged inputs (a "smart-skip" pattern) lose data on every skip run. The runtime treats 0 rows from the source as "all groups deleted" and runs `DELETE`. The next run sees an empty target, the lib re-fetches, the data is restored — the cycle repeats.

The cost is real even though data eventually returns: extra API calls, downstream models rebuild every other run, and any `@sink` propagates spurious delete events to external systems.

**Workaround:** lib-side fix — return the existing rows (re-read from target) for skipped inputs instead of returning 0 rows. The library author owns this until OndatraSQL adds an explicit `empty_result` contract to the lib API dict.

If you cannot modify the lib, avoid tracked for smart-skip libs. Use append + `@incremental` instead — append never deletes on missing rows.
