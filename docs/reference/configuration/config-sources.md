---
description: Query external databases directly in OndatraSQL via DuckDB's attach mechanism. Connect PostgreSQL, MySQL, and other sources for use in SQL models.
draft: false
title: sources.sql
weight: 7
---
**Sources: attach external databases**

**Phase:** Post-catalog | **Order:** 7 | **Required:** No

Attach external databases via DuckDB's attach mechanism. Data stays in the source system, read at query time.

## PostgreSQL

```sql
ATTACH 'postgresql://user:pass@host:5432/warehouse' AS warehouse (READ_ONLY);
```

Requires `postgres` in `extensions.sql` and credentials in `secrets.sql`.

## MySQL

```sql
ATTACH 'mysql:host=db.example.com port=3306 database=app' AS app_db (READ_ONLY);
```

Requires `mysql` in `extensions.sql` and credentials in `secrets.sql`.

## Files

No setup required. Read directly in models:

```sql
SELECT * FROM read_parquet('data/*.parquet')
SELECT * FROM read_csv('data/events.csv')
SELECT * FROM read_json('s3://bucket/api-dump.json')
```
