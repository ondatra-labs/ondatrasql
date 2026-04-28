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

## Tracked kind content hash

`md5(string_agg(...))` over non-key columns:
- Column ordering matters — reorder triggers full group replace
- NULL coerced to empty string — `NULL` to `''` change not detected
- Large groups (millions of rows per group key) slow to hash
