---
description: Stable API surface, dynamic behavior, and empty-run guarantees for lib functions.
draft: false
title: Blueprint Contract
date: "2026-04-28"
---

## Stable API surface

These are the parts of the API we consider stable. During v0.x, breaking changes are documented in release notes.

**Fetch kwargs:** `page`, `columns`, `target`, `is_backfill`, `last_value`, `last_run`, `cursor`, `initial_value`, plus any `args` declared in the API dict. Undeclared parameters are silently dropped.

**Push kwargs:** `rows`, `batch_number`, `kind`, `key_columns`, `columns`, plus any `args` declared in the SINK dict.

**Fetch return:** `{"rows": [...], "next": ..., "empty_result": ...}`. `empty_result` is optional and only consulted on a 0-row final page; see [Fetch Contract](/reference/lib-functions/fetch-contract/#empty-fetches-and-tracked).

**Push return:** per-row status dict (`sync`), nothing (`atomic`), or job reference dict (`async`).

**Typed columns:** column dicts with `name`, `type`, and `json_schema_type` (`"string"`, `"number"`, `"integer"`, `"array"`). Every projected column is cast (the [SQL schema contract](/reference/lib-functions/fetch-contract/#sql-schema-contract) requires it), so the column dict always carries the cast type — the runtime never falls back to `"string"`.

**Built-in functions:** see [Starlark Modules](/reference/lib-functions/starlark-modules/) for the complete list.

## Dynamic behavior

Column extraction, stub table creation, and CDC filtering may change between versions.

**Column extraction:** per-lib-call filtering via table alias. Includes JOIN ON, WHERE, GROUP BY — not just SELECT.

**Stub tables for empty runs:**

In strict-schema mode, the model SQL fully determines the stub: every projection is a cast (`col::TYPE AS col`), so the runtime knows the type of every output column without consulting data or the target. Columns used only in `WHERE` / `JOIN ON` are VARCHAR in the stub — they're not in the output schema, so their type doesn't matter for stub correctness.

The only case where SQL-shape extraction returns nothing is the degenerate one: the lib appears in `FROM` but no projection or filter ever references its columns (e.g. a cross join where the lib is unused, like `SELECT 1::INTEGER AS x FROM api() a, reg.other o` with no reference to `a`). Then the runtime falls back to cloning the target's schema if the target exists, or skips the run with a warning if it doesn't.

## Empty-run guarantees

When a lib returns 0 rows:

1. Run type preserved from backfill decision
2. Schema evolution, constraints, and audits run against stub
3. Sink processes pending backlog
4. Commit metadata written
5. Target table is created (empty, fully typed) on the first run
6. Schema evolution is detected on every empty run, not deferred to the next non-empty run

**Tracked-specific:** the runtime distinguishes "source has no new data" from "source is fully enumerated and empty". The lib declares which one applies via `empty_result` on the final fetch page (`"no_change"` is the default; `"delete_missing"` opts in to deleting groups absent from the empty result). When the run carries `no_change` and the model has no schema evolution or audits to apply in the materialize transaction, the runtime skips that transaction entirely so dependent models don't see a spurious "dep changed" signal. Models that declare `@audit` or whose temp-table schema diverges from the target still go through materialize so those checks run on every empty run. `@constraint` checks run before materialize (read-only) regardless of which path is taken. See [Fetch Contract](/reference/lib-functions/fetch-contract/#empty-fetches-and-tracked).
