---
description: Configure DuckDB execution settings in OndatraSQL. Control memory limits, parallelism, and disk spill behavior for your SQL data pipeline runtime.
draft: false
title: settings.sql
weight: 1
---
**Settings: DuckDB runtime configuration**

**Phase:** Pre-catalog | **Order:** 1 | **Required:** No

Controls how DuckDB executes queries: memory usage, parallelism, and disk spill behavior.

```sql
SET memory_limit = '8GB';
SET threads = 4;
SET temp_directory = '/tmp/duckdb';
SET TimeZone = 'Europe/Stockholm';
```

## Settings

| Setting | Default | Description |
|---|---|---|
| `memory_limit` | 80% of system RAM | Maximum memory for the runtime |
| `threads` | All available cores | Number of parallel execution threads |
| `temp_directory` | `.tmp` | Disk spill location when memory is exceeded |
| `TimeZone` | System timezone | Timestamp interpretation |

For the full list, see the [DuckDB configuration reference](https://duckdb.org/docs/stable/configuration/overview).

## Minimum ondatrasql version

A project can declare the oldest ondatrasql it runs on:

```sql
SET VARIABLE ondatrasql_min_version = '0.48.0';
```

ondatrasql reads this line before it does anything else in the project. An older binary exits with an error that names both versions, such as `this project needs ondatrasql 0.48.0 or newer (ondatrasql_min_version in config/settings.sql), but this binary is 0.47.0`. The value is a quoted `MAJOR.MINOR[.PATCH]`; anything else is an error. DuckDB runs the statement like any other, so `getvariable('ondatrasql_min_version')` returns it in SQL.

Binaries older than the one that introduced the setting do not check it. Pin a version when the project starts relying on a feature, or once it no longer uses a [deprecated](/support/deprecations/) form that the next releases remove.
