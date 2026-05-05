---
description: How OndatraSQL builds and executes the dependency graph automatically from your SQL.
draft: false
title: Dependency Graph
weight: 7
---
You never declare dependencies between your models. OndatraSQL figures them out by reading your SQL.

## How it knows what depends on what

The runtime parses every model's SQL and extracts table references — everything in `FROM`, `JOIN`, CTEs, and subqueries. If your staging model reads from `raw.events`, that's a dependency. If your mart model reads from `staging.events` and `staging.customers`, those are two dependencies.

Lib function calls are resolved at execution time — the DAG only tracks table-level dependencies between models.

The result is a directed acyclic graph (DAG). Models that depend on nothing run first, models that depend on them run next, and so on. You don't have to think about ordering — it's derived from your SQL.

{{< dag-example >}}

## What happens when you run

Each model gets a run type before execution:

- **skip** — nothing changed, don't run
- **backfill** — first run or definition changed, rebuild from scratch
- **incremental** — new or changed data, process only the delta
- **full** — upstream model changed, re-evaluate

These decisions propagate through the graph. If `raw.events` runs (new data arrived), downstream models like `staging.events` are re-evaluated — they might run incrementally or do a full rebuild depending on what changed. If nothing changed anywhere in the pipeline, models skip based on their own batch-computed run types.

## What happens when something fails

If a model fails, its downstream models do **not** run. They're skipped with a warning. This prevents cascading errors — a broken staging model won't corrupt your mart layer.

On the next run, the failed model retries. If it succeeds, its downstream models run normally.

Circular dependencies are caught at build time with a clear error.