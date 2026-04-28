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
