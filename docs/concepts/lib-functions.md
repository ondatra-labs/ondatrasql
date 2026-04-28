---
description: Why transport logic lives in lib/ as Starlark functions, separate from SQL models.
draft: false
title: Lib Functions
weight: 2
---
Your models are SQL. Your API transport is Starlark. They live in different directories because they solve different problems, and keeping them separate is what makes everything else work.

## Why the separation matters

SQL is great at transforming data — joins, aggregations, filters, window functions. But it can't make HTTP requests, handle pagination, refresh OAuth tokens, or retry on failure. Those are imperative concerns that need loops, conditionals, and error handling.

By keeping your models as pure SQL and putting transport in `lib/`, the runtime can analyze your models statically. It extracts dependencies from `FROM` and `JOIN` clauses, detects changes via DuckLake snapshots, rewrites queries for incremental processing, and traces column-level lineage. None of that would work if your models contained arbitrary code.

The tradeoff: you need two languages. But they're both simple — SQL you already know, and Starlark is a Python subset you'll pick up in an hour.

## How it works

A lib function is a `.star` file in `lib/` that declares an `API` dict. The file name becomes the function name in SQL. One file can handle both inbound (`fetch`) and outbound (`push`), sharing the same auth, headers, retry, and rate limit config.

```python
# lib/hubspot.star
API = {
    "base_url": "https://api.hubapi.com",
    "auth": {"provider": "hubspot"},
    "push": {"batch_size": 100, "batch_mode": "sync"},
}
```

**Inbound**: your SQL writes `FROM hubspot('contacts')`, and the runtime calls `fetch()` with managed pagination.

**Outbound**: your SQL writes `@sink: hubspot`, and the runtime detects what changed and calls `push()` per batch.

You don't register anything. Drop a `.star` file in `lib/` and it's available. The runtime scans `lib/` at startup, validates the dict, and registers DuckDB macros so the SQL parser accepts `FROM func_name(...)`.

## What the runtime handles for you

When you declare config in the API dict, the runtime injects it into every `http.*` call your function makes. You don't set auth headers manually, you don't implement retry logic, you don't write a pagination loop. The runtime does that.

This means your `fetch()` function focuses on the API-specific logic — how to parse the response, what the next page cursor looks like, how to map fields. Everything else is handled.

## Further reading

- [API Dict Reference](/reference/lib-functions/api-dict/) — full configuration reference
- [Create a Lib Function](/guides/create-a-blueprint/) — step-by-step guide
- [Blueprints](/blueprints/) — ready-made lib functions for common APIs