---
description: Why OndatraSQL uses cursor-based incrementality, how it interacts with CDC, and what the tradeoffs are.
draft: false
title: Incremental Models
weight: 6
---
The `@incremental` directive tells OndatraSQL to track a cursor column across runs. After each successful run, it records `MAX(cursor)` and makes it available next time. This page explains why that mechanism exists, how it fits with change detection, and where it falls short.

## Why you need a cursor

Most APIs paginate by time: "give me everything after this timestamp." The cursor pattern maps directly to that. After each successful run, OndatraSQL records the high-water mark, and your next run starts from there.

The cursor value isn't stored separately — it's derived from your data. On each run, the runtime queries `MAX(cursor_column)` from the target table. Because this happens after a successful DuckLake commit, the cursor advances atomically with your data. If the commit fails, the table is unchanged, MAX returns the old value, and the next run re-fetches the same range. You never end up in a state where the cursor moved but the data didn't land.

## How it works with CDC

If you're wondering "do I need `@incremental` when CDC already filters to changed rows?" — the answer is: they do different things.

- **CDC** answers: "what rows in this DuckLake table changed since the last snapshot?"
- **Incremental** answers: "what was the latest cursor value after the last run?"

For SQL models, they're complementary. CDC handles the row-level filtering automatically, and `@incremental` adds cursor tracking on top. For lib functions fetching from APIs, there is no CDC — your function reads from an external system, not from DuckLake. The cursor is how it knows where to start.

## When everything resets

The cursor resets and a full reload runs when:

- Your target table doesn't exist (first run)
- You changed the model SQL
- You changed `@kind`, `@unique_key`, `@group_key`, `@partitioned_by`, `@incremental`, or `@incremental_initial`
- You changed any file in `config/` (macros, variables, settings)

This is intentionally destructive. A changed query means the old data was produced by a different definition, so starting fresh is safer than trying to patch incrementally on top of stale data. On backfill, `is_backfill` is `True` and `last_value` falls back to `initial_value` (passed as kwargs to `fetch()`).

## The tradeoffs you should know about

Cursor-based incrementality is simple and works great for append-style data and time-ordered APIs. But it has limitations you should be aware of:

- **Late-arriving data.** If a row arrives with a timestamp older than your cursor, it's missed. The cursor only moves forward. If your source regularly backfills historical data, cursor tracking alone won't catch it.

- **Non-monotonic sources.** If the source reorders or updates old rows, the cursor doesn't help. You'd need full-state comparison — which is what `tracked` and `scd2` kinds do.

- **Cursor column choice matters.** Pick a column that is strictly monotonic and never null. `updated_at` works for most cases; `id` works if it's an auto-increment. A column that can go backward (like a user-editable timestamp) will cause missed data.

For the property reference (`is_backfill`, `last_value`, `initial_value`, `last_run`), see [Starlark Modules](/reference/lib-functions/starlark-modules/#incremental). For the directive syntax, see [Directives](/reference/pipeline/directives/).