---
description: Known limitations and edge cases in OndatraSQL.
draft: false
title: Known Limitations
weight: 99
---

## Schema detection on empty lib runs

Dynamic-column libs that return 0 rows when the model SQL references no specific columns (e.g. `SELECT * FROM lib(...)` with an existing target): stub is cloned from the target table, so schema changes are not detected until the next non-empty run. When the SQL projects specific columns, the stub is built from the SQL projection (typed where casts exist) and additive + type changes are detected even on a 0-row run. Force a backfill by editing model SQL, or use a static-column lib with declared `Columns`.

## First-run type inference

First run of a dynamic-column lib with no target: stub columns referenced without an explicit cast in the SELECT default to VARCHAR. Cast a column (`col::BIGINT`) and that type is preserved on the first run. Type transitions for un-cast columns (e.g., `VARCHAR -> BIGINT` once data arrives) are reported on the second run. If column names cannot be inferred at all (`SELECT *` from lib function with no target), the first run is skipped — no target created until data arrives.

## Sandbox and time travel

Sandbox fork has its own snapshot history. Time-travel may fail during CDC schema check — silently assumes no change. If CDC EXCEPT fails, falls back to full query. Correctness unaffected.
