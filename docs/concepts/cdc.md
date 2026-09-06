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

Nor does every *source*. CDC is built on DuckLake's `table_changes()`, which reads a table's snapshot history — so it only works for tables in the lake. A source in another attached catalog, for example:

```sql
-- config/sources.sql
ATTACH 'postgresql://…' AS crm (TYPE postgres, READ_ONLY);
```

has no snapshot history to compare against. A model reading `crm.decision` runs a full query on every run. This is not an error and is not reported as one. In an `append` or `merge` model — the kinds that would otherwise have used CDC — it is recorded as a `cdc.skip_foreign_catalog:<table>` step in the snapshot's `commit_extra_info`. The other kinds never consult CDC, so there is nothing to record.

The whole model falls back, not just that one source. A model joining `crm.decision` to a lake table cannot keep CDC on the lake half: the gate would then see an unchanged lake source, report no work, and the model would read an empty delta on a run where only the Postgres side moved — losing the change with no error. One source the runtime cannot observe means the model has to read everything.

Nor does an aggregate over a source that changed. CDC substitutes each source with the rows added since the last snapshot, and an aggregate computed over that delta is only correct when the whole group is new. For a group that already exists, the delta holds part of the total — merging it would replace a correct figure with a partial one. So when a source the model aggregates over has changed, the model recomputes with a full query instead, recorded as a `cdc.skip_changed_aggregate` step. A model whose aggregated sources are unchanged keeps the delta path. This applies wherever the aggregate is written, including inside a scalar subquery.

One thing CDC deliberately does not do is propagate changes from a source that is only read, never aggregated. A joined dimension table, or a table behind `EXISTS`/`IN`, is not watched: new rows on the fact side see the current state of it, but rows already in the target are not revisited when that source changes later — and a row that would now start qualifying does not appear until the model runs in full. This matches how incremental models behave elsewhere; it is a cost/correctness trade, not an oversight. There is no flag that forces a rebuild — one is triggered by the changes listed in [Run types](/reference/pipeline/run-types/), so a dimension edit that has to reach existing rows needs the model itself to change.

Writing the catalog explicitly does not change this — what matters is which catalog the table lives in, not how the name was written. A lake table read as `lake.raw.orders` gets CDC exactly as `raw.orders` does.

If you want incremental loading from such a source, use `@incremental` on a monotonic column in the foreign table rather than relying on CDC — and filter on it in the model, since the directive supplies the cursor but does not apply it: `WHERE id > getvariable('incr_last_value')`. Declaring `@incremental` without the predicate leaves the query unbounded, and an `append` model then re-reads and re-appends the whole foreign table.

If you're curious about how each kind handles changes differently, that's covered in [Model Kinds](/concepts/kinds/).