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

**Typed columns:** column dicts with `name` and `type`. Columns with explicit casts also include `json_schema_type` (`"string"`, `"number"`, `"integer"`, `"array"`). Columns without casts get `type: "string"`.

**Built-in functions:** see [Starlark Modules](/reference/lib-functions/starlark-modules/) for the complete list.

## Dynamic behavior

Column extraction, stub table creation, and CDC filtering may change between versions.

**Column extraction:** per-lib-call filtering via table alias. Includes JOIN ON, WHERE, GROUP BY — not just SELECT.

**Stub tables for empty runs:**

| Situation | Stub source | Schema evolution |
|---|---|---|
| Static-column lib | Declared column names and types | Detected |
| Dynamic lib, columns referenced in model SQL | Typed from explicit casts in the SELECT projection (`col::BIGINT`, `col::DOUBLE`, etc.); VARCHAR for unqualified refs. When the target exists, established target types are preferred over the VARCHAR fallback so unchanged columns don't trigger spurious type-change warnings. | Detected (additive + type changes) |
| Dynamic lib, no columns inferrable from SQL, target exists | Cloned from target table | Not detected until next non-empty run |
| Dynamic lib, no columns inferrable, no target | Skip (no table created) | N/A |

## Empty-run guarantees

When a lib returns 0 rows:

1. Run type preserved from backfill decision
2. Schema evolution, constraints, and audits run against stub
3. Sink processes pending backlog
4. Commit metadata written
5. First run with inferrable columns creates empty target table
6. First run without inferrable columns skips with warning

**Tracked-specific:** the runtime distinguishes "source has no new data" from "source is fully enumerated and empty". The lib declares which one applies via `empty_result` on the final fetch page (`"no_change"` is the default; `"delete_missing"` opts in to deleting groups absent from the empty result). When the run carries `no_change` and the model has no schema evolution or audits to apply in the materialize transaction, the runtime skips that transaction entirely so dependent models don't see a spurious "dep changed" signal. Models that declare `@audit` or whose temp-table schema diverges from the target still go through materialize so those checks run on every empty run. `@constraint` checks run before materialize (read-only) regardless of which path is taken. See [Fetch Contract](/reference/lib-functions/fetch-contract/#empty-fetches-and-tracked).
