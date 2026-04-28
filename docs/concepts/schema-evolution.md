---
description: How OndatraSQL handles schema changes automatically when your model SQL changes.
draft: false
title: Schema Evolution
weight: 5
---
When you change your model's SQL, the target table's schema needs to change too. In most pipeline tools, that means writing a migration, dropping the table, or rebuilding from scratch. OndatraSQL handles it automatically.

## What happens when you add a column

Before:

```sql
SELECT order_id, total FROM raw.orders
```

After:

```sql
SELECT order_id, total, currency FROM raw.orders
```

OndatraSQL detects the new column, runs `ALTER TABLE ADD COLUMN`, and preserves your existing data. In DuckLake this is a metadata-only operation — no files are rewritten.

## Why automatic schema evolution

Pipeline schemas change constantly. A new field appears in an API response. A column gets renamed for clarity. An integer needs to become a bigint because the values grew. Each of these would normally require you to either write a migration or drop the table and start over.

Automatic schema evolution means you just change your SQL. The runtime compares what your query produces against what the target table has, figures out the difference, and applies the change. You don't have to think about it.

## The safe changes

Some changes are straightforward and always safe:

- **New columns** — added via ALTER TABLE, existing data gets NULLs (metadata-only in DuckLake)
- **Type promotions** — widening a type (INTEGER → BIGINT, FLOAT → DOUBLE) is detected automatically. The column is dropped and re-added with the new type because DuckLake's ALTER TYPE rules are stricter than DuckDB's. Existing values are repopulated from your query on the next run.
- **Column renames** — detected via AST lineage when columns are simultaneously added and removed, applied via ALTER

## The destructive changes

Some changes can't preserve existing data:

- **Incompatible type changes** (INTEGER → VARCHAR) — the column is dropped and re-added with the new type
- **Removed columns** — dropped via ALTER

These are still handled automatically, but you should know they happen. If you change a column from INTEGER to VARCHAR, the old integer values are gone — the column is repopulated from your query on the next run.

## Why it's safe: transactions

Here's the important part. Schema evolution runs inside the same transaction as your data write and audit checks. If anything fails — the data write, an audit check — DuckDB aborts the entire transaction. The ALTER is rolled back together with the data. Added columns disappear, type changes revert. No half-evolved schema is ever committed.

This means you can't end up in a state where the schema changed but the data didn't land. It's all or nothing.

## Downstream propagation

When an upstream table's schema changes, downstream models detect it automatically. The CDC mechanism notices that the column structure differs between the current and previous snapshot, falls back to a full query instead of incremental, and applies schema evolution to the downstream table too.

This propagates through your entire DAG. If `raw.orders` gains a column, `staging.orders` picks it up on its next run (if your staging query uses `SELECT *` or explicitly includes the new column), and so on down the chain.