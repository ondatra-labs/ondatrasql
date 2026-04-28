---
description: Load optional DuckDB extensions in OndatraSQL. Add cloud storage access, external database connectors, geospatial functions, and more to your pipeline.
draft: false
title: extensions.sql
weight: 3
---
**Extensions: optional capabilities for the runtime**

**Phase:** Pre-catalog | **Order:** 3 | **Required:** No

Load DuckDB extensions for cloud storage, external databases, and specialized functions.

| Method | Scope |
|---|---|
| `extensions.sql` | Global (entire runtime) |
| `@extension` directive | Per-model |

## Cloud Storage

```sql
INSTALL httpfs;
LOAD httpfs;
```

## External Databases

```sql
INSTALL postgres;
LOAD postgres;
```

```sql
INSTALL mysql;
LOAD mysql;
```

## Specialized Functions

```sql
INSTALL spatial;
LOAD spatial;
```

For the full list, see the [DuckDB extensions documentation](https://duckdb.org/docs/stable/extensions/overview).
