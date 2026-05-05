---
description: A model is a SQL file that produces one table. The file path is the table name, directives control behavior, and lib functions handle API data.
draft: false
title: Models
weight: 1
---
A model is a `.sql` file that produces one table. That's the whole idea. This page explains why that constraint exists and what it gives you.

## Why SQL files

Your models are SQL files, not YAML configs or Python scripts. That's a deliberate choice. SQL is the language your data team already knows, that your BI tools already speak, and that DuckDB already executes natively.

By keeping models as plain SQL, the runtime can do things that would be impossible with opaque code: it extracts dependencies from your `FROM` and `JOIN` clauses, detects which tables changed via DuckLake snapshots, and rewrites queries for incremental processing — all by reading the SQL AST. None of that works if your models are functions in a general-purpose language.

The tradeoff is that SQL can't make HTTP requests, handle pagination, or refresh OAuth tokens. That's why transport lives separately in `lib/` as [Starlark lib functions](/concepts/lib-functions/).

## Why your file path is your table name

```
models/staging/orders.sql    →  staging.orders
models/raw/api/users.sql     →  raw.api__users
```

The file system is your schema. First directory becomes the DuckDB schema, file name becomes the table name. There's no registry, no config file, no naming convention to memorize — your directory structure *is* the truth.

This means you can move a model by renaming a file. And `git log` shows you the full history of any table. If you've ever lost track of which config produces which table in a YAML-heavy pipeline tool, you'll appreciate this.

Deeper nesting is flattened with `__` because DuckDB only supports two-level naming (`schema.table`).

## Why directives live in SQL comments

```sql
-- @kind: merge
-- @unique_key: customer_id

SELECT customer_id, email FROM mart.customers
```

Your directives are SQL comments starting with `@`. They could have been a YAML header, a separate config file, or function arguments. Comments were chosen because they keep your model as valid SQL — you can paste it into DuckDB's CLI, into your BI tool, or into a SQL notebook, and it just runs. The `@` prefix makes them easy to grep for and clearly distinct from regular comments.

The full directive list is in the [reference](/reference/pipeline/directives/).

## One model, one table

Every model produces exactly one table. If you need two tables, you write two models. This constraint might feel limiting at first, but it's what makes everything else work: the dependency graph is unambiguous, lineage is straightforward, and the runtime knows exactly what each file produces.

Dependencies between your models are detected automatically from SQL table references and executed in topological order. You never have to declare them. See [Dependency Graph](/concepts/dag/).
