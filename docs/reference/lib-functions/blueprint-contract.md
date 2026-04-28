---
description: Stable API surface, dynamic behavior, and empty-run guarantees for lib functions.
draft: false
title: Blueprint Contract
date: 2026-04-28
---

## Stable API surface

These are the parts of the API we consider stable. During v0.x, breaking changes are documented in release notes.

**Fetch kwargs:** `page`, `columns`, `target`, `is_backfill`, `last_value`, `last_run`, `cursor`, `initial_value`, plus any `args` declared in the API dict. Undeclared parameters are silently dropped.

**Push kwargs:** `rows`, `batch_number`, `kind`, `key_columns`, `columns`, plus any `args` declared in the SINK dict.

**Fetch return:** `{"rows": [...], "next": ...}`

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
| Dynamic lib, target exists | Cloned from target table | Not detected until next non-empty run |
| Dynamic lib, no target, columns inferrable | VARCHAR columns from SQL references | Detected (additive) |
| Dynamic lib, no target, no column info | Skip (no table created) | N/A |

## Empty-run guarantees

When a lib returns 0 rows:

1. Run type preserved from backfill decision
2. Schema evolution, constraints, and audits run against stub
3. Sink processes pending backlog
4. Commit metadata written
5. First run with inferrable columns creates empty target table
6. First run without inferrable columns skips with warning
