---
description: How OndatraSQL uses DuckLake snapshots to process only changed data. AST-level query rewriting, join handling, and skip detection.
draft: false
title: Change Detection
weight: 4
---
Most data pipelines re-process everything on every run. Yours doesn't have to.

OndatraSQL rewrites your SQL so only changed data flows through. You write a normal query, and the runtime figures out what's new since last time. This page explains how that works and why you should care.

## The trick

DuckLake creates a snapshot on every commit. That means it knows what your data looked like yesterday, last week, or ten minutes ago. OndatraSQL uses this: it compares each source table against the snapshot from your last successful run, and only processes the difference.

Your query `SELECT * FROM raw.events` becomes something like:

```sql
SELECT * FROM raw.events
EXCEPT
SELECT * FROM raw.events AT (VERSION => 42)
```

Where 42 is the snapshot from your previous run. The `EXCEPT` strips out everything that already existed, leaving only what's new or changed.

You don't write this yourself. The runtime rewrites your SQL at the AST level — it parses the query, finds the source tables, and reconstructs it with the `EXCEPT` subqueries inserted. Your original SQL stays exactly as you wrote it.

## Why not just filter by timestamp?

You could add `WHERE updated_at > last_run` to every query. That works, and `@incremental` gives you a cursor for exactly that — both for SQL models and for lib functions fetching from APIs. But timestamp filtering has limits: it requires every source table to have a reliable timestamp column, it misses deletes entirely, and it falls apart on schema changes.

AST rewriting avoids all of that. It works on any query, with any columns, and catches inserts, updates, and deletes — because `EXCEPT` compares full row state, not just one column. DuckLake's snapshot-aware storage means the `AT (VERSION => ...)` side of the EXCEPT can be resolved efficiently without scanning the full table.

## The two-stage gate

The runtime first determines the run type by comparing the model's SQL hash and upstream snapshot IDs against the last committed state. If nothing changed, `table` kind models are skipped entirely. For incremental kinds (`append`, `merge`), the runtime proceeds to the CDC stage.

In the CDC stage, the runtime calls `table_changes()` per source table to check if any rows were actually inserted, updated, or deleted since the last snapshot. If no source has changes, the runtime rewrites source tables with `WHERE false` so the query produces 0 rows without scanning any data. If changes exist, the EXCEPT rewrite processes only the delta — not the full table.

This means the common case — nothing changed — is cheap at both levels: the run-type decision avoids unnecessary work, and the CDC gate avoids unnecessary scans.

## What about joins?

This is where it gets interesting. When your model joins multiple tables, each source is evaluated independently. If `raw.orders` changed but `raw.customers` didn't, only `raw.orders` gets the `EXCEPT` treatment. `raw.customers` is read in full.

That matters for correctness. A new order joined against an unchanged customer must still produce a row. If both sides were filtered to changes-only, the join would find no matching customer and produce nothing. That would be a silent data loss bug — exactly the kind of thing you don't want in a pipeline.

The runtime figures out which tables need CDC and which don't by analyzing column lineage in the AST. Aggregation joins (where new rows in a dimension affect a SUM or COUNT) get CDC. Pure lookup joins don't.

## When nothing changed at all

If the model's SQL hash is unchanged and upstream snapshots haven't moved, the runtime skips the model entirely — no query runs, no I/O. This is why a pipeline with 50 models can re-run in under a second when only one source table changed. 49 models skip at the run-type stage, and the one that runs processes only the delta via CDC.

You don't have to think about it — it just happens.

## What CDC doesn't cover

Not every model kind uses this mechanism. SCD2 needs the complete source data to detect which specific columns changed, so it does full-state comparison. Tracked uses content hashing per group. Table kind either rebuilds entirely or skips — there's no incremental middle ground.

If you're curious about how each kind handles changes differently, that's covered in [Model Kinds](/concepts/kinds/).