---
description: Configure the DuckLake catalog in OndatraSQL. The only required config file. Tells the runtime where to store metadata and Parquet data files.
draft: false
title: catalog.sql
weight: 4
---
**Catalog: where your data lives**

**Phase:** Catalog | **Order:** 4 | **Required:** Yes

One ATTACH statement tells OndatraSQL where to store metadata and Parquet data.

## Backends

### SQLite (default)

```sql
ATTACH 'ducklake:sqlite:ducklake.sqlite' AS lake
    (DATA_PATH 'ducklake.sqlite.files');
```

```sql
-- With cloud data path
ATTACH 'ducklake:sqlite:ducklake.sqlite' AS lake
    (DATA_PATH 's3://my-bucket/data/');
```

### DuckDB

```sql
ATTACH 'ducklake:duckdb:ducklake_catalog.duckdb' AS lake
    (DATA_PATH 'ducklake.duckdb.files');
```

### PostgreSQL

```sql
ATTACH 'ducklake:postgres:dbname=ducklake_catalog host=db.prod.internal user=ondatra password=${PG_PASSWORD}' AS lake
    (DATA_PATH 's3://my-bucket/data/');
```

Requires `postgres` in `extensions.sql`. Environment variables (`${PG_PASSWORD}`) are expanded before parsing.

## Backend Comparison

| | SQLite | DuckDB | PostgreSQL |
|---|---|---|---|
| **Setup** | Zero config | Zero config | Requires a PostgreSQL server |
| **Read access** | OndatraSQL only | OndatraSQL only | Any PostgreSQL client |
| **Multi-machine** | No | No | Yes |
| **Cloud data** | Yes (S3/GCS) | Yes (S3/GCS) | Yes |
| **Sandbox** | File copy | File copy | `CREATE DATABASE TEMPLATE` |

{{< callout type="warning" >}}
MySQL is supported by DuckLake but not by OndatraSQL. Sandbox, CDC, and run-type detection are tested for SQLite, DuckDB, and PostgreSQL only.
{{< /callout >}}

## PostgreSQL Architecture

{{< pg-arch >}}

Sandbox creates a temporary catalog copy via `CREATE DATABASE TEMPLATE`. This requires exclusive access to the primary, so all other connections are terminated during the fork. Read replicas are unaffected.

## Options

### Data Inlining

```sql
CALL lake.set_option('data_inlining_row_limit', 20);   -- default: 10
CALL lake.set_option('data_inlining_row_limit', 0);    -- disable
```

### Deletion Vectors

```sql
CALL lake.set_option('write_deletion_vectors', true);
```

### Encryption

```sql
ATTACH 'ducklake:sqlite:ducklake.sqlite' AS lake
    (DATA_PATH 'ducklake.sqlite.files', ENCRYPTED);
```

Each file gets a unique encryption key stored in the catalog. Decryption is transparent.
