---
description: Directives turn SQL into a data pipeline in OndatraSQL. Control materialization, change detection, validation, outbound sync, and schema with SQL comment annotations.
draft: false
title: Directives
weight: 15
---
Directives are SQL comments that control materialization, change detection, and validation.

```sql
-- @kind: merge
-- @unique_key: order_id
-- @incremental: updated_at
-- @constraint: compare(total, >=, 0)
-- @audit: row_count(>, 0)
```

## Core

| Directive | What it does |
|---|---|
| `@kind` | Materialization strategy: `table`, `merge`, `append`, `scd2`, `tracked`, `events` |
| `@unique_key` | Row matching key (required for merge, scd2) |
| `@group_key` | Group key for content-hash detection (required for tracked) |
| `@incremental` | Cursor column for incremental state |
| `@incremental_initial` | Starting cursor value (default: `1970-01-01T00:00:00Z`) |

### `@unique_key` vs `@group_key`

`@unique_key` identifies individual rows for merge and scd2 kinds. It determines which rows to update, insert, or delete.

`@group_key` groups rows for the tracked kind. Tracked computes an MD5 content hash per group — if any row in the group changes, the entire group is replaced. Use `@group_key` when the identity is a grouping concept (e.g. `source_file` for a file-based pipeline) rather than a row-level primary key.

```sql
-- merge: row-level identity
-- @kind: merge
-- @unique_key: order_id

-- tracked: group-level identity
-- @kind: tracked
-- @group_key: source_file
```

## Lib-Backed Models (Inbound)

| Directive | What it does |
|---|---|
| `@fetch` | Bare marker. Declares a lib-backed fetch model — the FROM clause must contain exactly one lib call (e.g. `FROM riksbank('SEKEURPMI')`). Activates the strict-fetch validator. See [Fetch Contract](/reference/lib-functions/fetch-contract/) |

A `@fetch` model is a passthrough projection of API rows: every output column must be `<col>::TYPE AS alias`. JOIN, WHERE, GROUP BY, DISTINCT, ORDER BY, LIMIT, OFFSET, UNION are rejected at parse time — push transformation to a downstream model that reads from the @fetch table.

`@fetch` is forbidden on `@kind: events` (events have their own ingest pipeline) and cannot be combined with `@push` on the same model (split into a fetch model and a downstream push model).

## Outbound Sync

| Directive | What it does |
|---|---|
| `@push` | Push function name in `lib/` for outbound sync. See [Sync Data to External APIs](/guides/outbound-sync/) |

`@push` works with `table`, `append`, `merge`, and `tracked` kinds. Not supported with `scd2` (use `@kind: table` with `WHERE is_current = true` instead) or `events`. The runtime exposes raw DuckLake `change_type` values (`insert`, `update_preimage`, `update_postimage`, `delete`) to your push function via `__ondatra_change_type`. Your Starlark code decides how to handle each change type.

A `@push` model produces the shape sent to the API. The strict-push validator only inspects the **outermost SELECT projection** — every output column must be `<expr>::TYPE AS alias` (cast + alias on every projection, no `SELECT *`). Inside the model SQL is fully free: JOIN, WHERE, GROUP BY, aggregates, DISTINCT, ORDER BY, CTEs, derived expressions are all legitimate shape construction. The cross-cutting LIMIT/OFFSET ban still applies. Lib calls in FROM are rejected (that would be a `@fetch` model — read from the materialized fetch table instead).

## Storage and Performance

| Directive | What it does |
|---|---|
| `@partitioned_by` | File partitioning. Supports column names and transforms: `year(col)`, `month(col)`, `day(col)`, `hour(col)`, `bucket(N, col)`. Applied on new writes. |
| `@sorted_by` | Sorted table hint. Improves query performance via min/max statistics. Applied during compaction (`ondatrasql merge`). |

## Validation

| Directive | What it does |
|---|---|
| `@constraint` | Block bad data before insert. See [Constraints](/reference/validation/constraints/) |
| `@audit` | Verify results after insert, rollback on failure. See [Audits](/reference/validation/audits/) |
| `@warning` | Log issues without failing. See [Warnings](/reference/validation/warnings/) |

## Metadata

| Directive | What it does |
|---|---|
| `@description` | Table comment in DuckLake catalog |
| `@column` | Column comment + masking tags. See [Mask Sensitive Columns](/guides/mask-sensitive-columns/) |
| `@expose <column>` | Serve via OData v4.01. Key column required. See [Serve Data via OData](/guides/serve-data-via-odata/) |
| `@extension` | Load a DuckDB extension before execution |

## Example

```sql
-- @kind: merge
-- @unique_key: customer_id
-- @push: hubspot_push
-- @constraint: not_null(customer_id)
-- @audit: row_count(>, 0)

SELECT
    customer_id::BIGINT AS customer_id,
    email::VARCHAR AS email,
    plan::VARCHAR AS plan
FROM mart.customers
WHERE active = true
```

## Compatibility Matrix

| Directive | table | append | merge | scd2 | tracked | events |
|---|---|---|---|---|---|---|
| `@unique_key` | -- | -- | Required | Required | -- | -- |
| `@group_key` | -- | -- | -- | -- | Required | -- |
| `@incremental` | Yes | Yes | Yes | Yes | Yes | -- |
| `@fetch` | Yes | Yes | Yes | Yes | Yes | -- |
| `@push` | Yes | Yes | Yes | -- | Yes | -- |
| `@partitioned_by` | Hint | Hint | Hint | Hint | Hint | -- |
| `@sorted_by` | Yes | Yes | Yes | Yes | Yes | -- |
| `@column` | Yes | Yes | Yes | Yes | Yes | -- |
| `@constraint` | Yes | Yes | Yes | Yes | Yes | -- |
| `@audit` | Yes | Yes | Yes | Yes | Yes | -- |
| `@warning` | Yes | Yes | Yes | Yes | Yes | -- |
| `@extension` | Yes | Yes | Yes | Yes | Yes | -- |
| `@expose` | Yes | Yes | Yes | Yes | Yes | -- |
| `@description` | Yes | Yes | Yes | Yes | Yes | Yes |

`@fetch` and `@push` cannot be combined on the same model — split into a `@fetch` model that materializes raw API data and a downstream `@push` model that reads from it.