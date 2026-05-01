---
description: Run type values, triggers, and what executes in each.
draft: false
title: Run Types
weight: 8
---

## Values

| Run type | Trigger | Kinds |
|---|---|---|
| `skip` | Hash unchanged, no dep changes | All |
| `backfill` | Hash changed, first run, config changed | All except events |
| `incremental` | Hash unchanged, source data may have changed | append, merge, scd2, tracked |
| `full` | Upstream dep changed, dep metadata missing/invalid, destructive schema evolution | table |
| `flush` | Events buffered in Badger | events |

## Hash inputs

The run-type hash includes: SQL body, `@kind`, `@unique_key`, `@group_key`, `@partitioned_by`, `@incremental`, `@incremental_initial`, and SHA256 of all `config/*.sql` files.

## What executes per run type

| Step | skip | backfill | incremental | full | flush |
|---|---|---|---|---|---|
| SQL execution | no | full query | CDC-filtered (append/merge) or full query (other kinds) | full query | no |
| Schema evolution | no | yes | yes | yes | no |
| Constraints | no | yes | yes | yes | no |
| Audits | no | yes (transactional) | yes (transactional) | yes (transactional) | no |
| Materialize | no | yes | yes | yes | if events buffered |
| Sink | backlog only | yes | yes | yes | no |
| Commit metadata | no | yes | yes | yes | if events buffered |

## Sink on skip

When a model is skipped but has `@push`, the runner still drains pending Badger backlog from previous failed pushes. No new delta is generated.

## Incremental kwargs

On backfill: `is_backfill=True`, `last_value` reset to `@incremental_initial`.
On incremental: `is_backfill=False`, `last_value` is `MAX(cursor_column)` from target.
