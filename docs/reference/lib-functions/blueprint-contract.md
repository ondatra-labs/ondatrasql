---
date: "2026-05-01"
description: Stable API surface, dynamic behavior, and empty-run guarantees for lib functions.
draft: false
title: Blueprint Contract
weight: 16
---

## Stable API surface

These are the parts of the API we consider stable. During v0.x, breaking changes are documented in release notes.

**Fetch kwargs:** `page`, `columns`, `target`, `is_backfill`, `last_value`, `last_run`, `cursor`, `initial_value`, plus any `args` declared in the API dict. Undeclared parameters are silently dropped.

**Push kwargs:** `rows`, `batch_number`, `kind`, `key_columns`, `columns`, plus any `args` declared in the `push` section of the API dict.

**Fetch return:** `{"rows": [...], "next": ..., "empty_result": ...}`. `empty_result` is optional and only consulted on a 0-row final page; see [Fetch Contract](/reference/lib-functions/fetch-contract/#empty-fetches-and-tracked).

**Push return:** per-row status dict (`sync`), nothing (`atomic`), or job reference dict (`async`).

**Typed columns:** column dicts with `name` and `type`. The `type` value is in DuckDB-native syntax — primitives as strings (`"VARCHAR"`, `"BIGINT"`, `"DECIMAL(18,3)"`, `"TIMESTAMPTZ"`), `LIST` as `["INNER_TYPE"]`, `STRUCT` as `{"field": "TYPE", ...}`, `MAP` and `UNION` as syntax strings. The same shape DuckDB's `json_structure` produces.

For `@fetch`, `name` is the cast source COLUMN_REF (the API field name); for `@push`, `name` is the materialized column name (the SQL alias). In both cases the blueprint sees the API-facing name. See [Fetch — Typed columns](/reference/lib-functions/fetch-contract/#typed-columns) and [Push — Typed columns](/reference/lib-functions/push-contract/#typed-columns).

**Helper functions:** [`lib_helpers.to_json_schema(type)`](/reference/lib-functions/starlark-modules/) converts a DuckDB-native type to a JSON Schema dict. Stable.

**Built-in functions:** see [Starlark Modules](/reference/lib-functions/starlark-modules/) for the complete list.

## Layer responsibilities

OndatraSQL has three layers; each owns one part of the pipeline.

| Layer | Owns | Examples |
|---|---|---|
| **API dict** | Configuration | `base_url`, `auth`, `headers`, `fetch.args`, `push.batch_size` |
| **Starlark (`lib/*.star`)** | Transport — moving bytes between SQL and the API | HTTP calls, pagination, lossless format conversion (string→int, struct→ISO date), envelope unwrap |
| **SQL (`models/**/*.sql`)** | Transformation — anything that interprets meaning | Filters, joins, aggregates, enum→domain mapping, currency conversion |

Blueprints may do **wire-format-to-canonical-value conversion** that is (1) lossless, (2) deterministic, and (3) format rather than semantic — for example, parsing string-encoded numerics from APIs that send everything as strings, encoding structured `{"year": 2026, "month": 5, "day": 1}` as `"2026-05-01"`, normalizing regional decimal format, unwrapping `{"data": [...]}` envelopes.

Blueprints may **not** do conversion that interprets meaning: enum mapping (`"ACTIVE"` → `1`), currency conversion, derived expressions, joins against external data, business rules. When in doubt, mirror the API's native form and leave the conversion to a downstream model.

This boundary is enforced via review, not via the parser — Starlark is too expressive to flag automatically. The strict-schema validator catches the inverse direction (transformation in `@fetch` SQL) by rejecting JOIN, WHERE, GROUP BY, derived expressions, etc.

### Strict Starlark options

`lib/*.star` files load under tightened Starlark options:

- **No top-level `if`/`for`/`while`** — module structure is purely declarative (constants, dict literals, function defs). Conditional logic belongs inside a function body.
- **No top-level reassignment** — `API`, `fetch`, `push`, helpers can each be defined once. `API = {...}; API = {...}` is rejected.

`while`, `set()`, and recursion stay enabled inside function bodies — they are widely used for cursor parsing, retry loops, and dedup sets.

## Dynamic behavior

Column extraction, stub table creation, and CDC filtering may change between versions.

**Column extraction:** per-lib-call filtering via table alias. Includes JOIN ON, WHERE, GROUP BY — not just SELECT. (Note: `@fetch` rejects JOIN/WHERE/GROUP BY at validation time, so this only matters for the legacy non-`@fetch` path.)

**Stub tables for empty runs:**

In `@fetch` mode, the model SQL fully determines the stub: every projection is `<col>::TYPE AS alias`, so the runtime knows the type of every output column without consulting data or the target.

The only case where SQL-shape extraction returns nothing is the degenerate one: the lib appears in `FROM` but no projection ever references its columns. Then the runtime falls back to cloning the target's schema if the target exists, or skips the run with a warning if it doesn't.

## Empty-run guarantees

When a lib returns 0 rows:

1. Run type preserved from backfill decision
2. Schema evolution, constraints, and audits run against stub
3. Sink processes pending backlog
4. Commit metadata written
5. Target table is created (empty, fully typed) on the first run
6. Schema evolution is detected on every empty run, not deferred to the next non-empty run

**Tracked-specific:** the runtime distinguishes "source has no new data" from "source is fully enumerated and empty". The lib declares which one applies via `empty_result` on the final fetch page (`"no_change"` is the default; `"delete_missing"` opts in to deleting groups absent from the empty result). When the run carries `no_change` and the model has no schema evolution or audits to apply in the materialize transaction, the runtime skips that transaction entirely so dependent models don't see a spurious "dep changed" signal. Models that declare `@audit` or whose temp-table schema diverges from the target still go through materialize so those checks run on every empty run. `@constraint` checks run before materialize (read-only) regardless of which path is taken. See [Fetch Contract](/reference/lib-functions/fetch-contract/#empty-fetches-and-tracked).
