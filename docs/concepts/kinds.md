---
description: Why OndatraSQL has six model kinds and when to choose each one.
draft: false
title: Model Kinds
weight: 3
---
The `@kind` directive controls how your table is built and updated. There are six kinds because different data has different update needs. This page helps you understand when to pick which one.

## The core question: rebuild or update?

The simplest thing is to rebuild the entire table every time. That's `@kind: table` — clear the table and repopulate from your query. It's always correct because the output always matches your SQL. But it's expensive for large tables.

The alternative is to update incrementally: only process what changed. But that requires knowing *what* changed, and different data shapes need different strategies. That's why the kinds exist.

All kinds except `events` and `scd2` support `@sink` for outbound sync. The runtime exposes raw DuckLake change types to your push function, so your Starlark code decides how to handle each type.

## table — the safe default

When you don't know what to pick, use `table`. It rebuilds when your upstream data changes and skips when nothing changed. This is the right choice for aggregates, staging layers, lookup tables — anything where a full rebuild is fast enough and you want the simplest mental model.

With `@sink`, table kind pushes change events from `table_changes()` — `delete` events from the truncate and `insert` events for the new rows. For a simple full replace, your push function can filter to inserts only. For small reference tables this works well.

## append — for data that only grows

Event logs, fact tables, audit trails — data that arrives and never changes. New rows are added, old rows are never touched. OndatraSQL uses DuckLake time-travel to automatically rewrite your query so it returns only rows added since last run.

With `@sink`, push receives only `insert` change types — straightforward.

The tradeoff you should know about: if a source row gets corrected after ingestion, the correction won't propagate. Append treats history as immutable. If you need corrections to flow through, use `merge` instead.

## merge — one row per entity

This is the classic CRM sync pattern: one row per customer, one row per product, one row per order. Merge upserts by `@unique_key` using a `MERGE INTO` statement — it updates existing rows and inserts new ones.

With `@sink`, push receives `insert` for new rows and `update_postimage` for changed rows. Your Starlark code can map these to POST and PATCH calls respectively. You also get `update_preimage` if you need the row's previous state (useful for conditional updates or audit logging).

Note: merge doesn't detect rows that disappeared from your source query. If you need mirror sync (deletes included), use `tracked` instead.

## tracked — when one entity spans multiple rows

Here's the problem merge can't solve: an invoice has line items. An OCR result has extracted fields. A survey has responses. Multiple rows represent one logical object, and you need to push all of them together when *any* of them changes.

Tracked groups your rows by `@group_key`, computes a content hash per group, and only writes groups whose hash changed. It materializes via DELETE+INSERT, so `table_changes()` produces `delete` and `insert` events. Composite keys are supported: `@group_key: region, year`.

With `@sink`, your push function receives `delete` and `insert` events grouped by key. If a key has both deletes and inserts, it's an update. If only deletes, the entity was truly removed. This gives you full mirror sync — including deletes.

The tradeoff: tracked adds a `_content_hash` column and does full-state comparison per group, which is more expensive than merge's row-level CDC. But for the use cases where you need it — grouped data, document extraction, line-item APIs — there's no simpler alternative.

## scd2 — keeping history

SCD2 (Slowly Changing Dimension Type 2) keeps every version of a row. When a product's price changes, the old row gets a `valid_to_snapshot` and a new row is inserted with `is_current = true`. If you're doing dimensional modeling for BI, this is the standard technique.

`@sink` is not supported with scd2. To push current state to an external system, use `@kind: table` with `WHERE is_current = true` in a separate sync model.

## events — data from the outside

Events models are the odd one out. They don't run a query — they define a column schema and receive data via HTTP POST. Think product analytics, webhook handlers, or IoT sensor data. Events is the only kind that doesn't support `@sink` — it has its own ingest pipeline.

See [Event Collection](/guides/collect-events/) for how to set this up.

## How to choose

Ask yourself:

- **Do I need the simplest thing?** → `table`
- **Does the data only grow?** → `append`
- **Is it one row per entity?** → `merge`
- **Is it multiple rows per entity?** → `tracked`
- **Do I need historical versions?** → `scd2`
- **Is it coming from outside via HTTP?** → `events`

The per-kind directive compatibility and storage hints are in the [Directives reference](/reference/pipeline/directives/).