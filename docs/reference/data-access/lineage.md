---
description: OndatraSQL tracks column-level lineage and execution metadata in DuckLake. Every run records dependencies, schema changes, git commits, and timing.
draft: false
title: Lineage & Metadata
weight: 60
---
Every run records column-level lineage, schema changes, dependencies, and git context in DuckLake. Query it all with SQL.

## CLI

```bash
ondatrasql lineage overview              # All models with dependencies
ondatrasql lineage staging.orders        # Column lineage for one model
ondatrasql lineage staging.orders.total  # Trace one column
```

Output is rendered as ASCII art with box-drawing characters.

## Transformation Types

Each column is classified by how it was derived:

| Type | Meaning | Example |
|---|---|---|
| `IDENTITY` | Direct copy | `SELECT name` |
| `AGGREGATION` | Aggregated value | `SUM(amount)` |
| `ARITHMETIC` | Computed | `price * quantity` |
| `CONDITIONAL` | Logic applied | `CASE WHEN ...` |
| `CAST` | Type conversion | `CAST(id AS VARCHAR)` |
| `FUNCTION` | Function call | `UPPER(name)` |

Lineage is extracted from the SQL AST, across CTEs, joins, and subqueries.

## Commit Metadata

Every run stores metadata in `commit_extra_info` on the DuckLake snapshot. All fields are JSON.

| Field | Description |
|---|---|
| `model` | Target table name |
| `kind` | `table`, `append`, `merge`, `scd2`, `tracked`, `events` |
| `run_type` | `backfill`, `incremental`, `full`, `skip`, `flush` |
| `rows_affected` | Rows written |
| `start_time` | Run start (ISO 8601) |
| `end_time` | Run end (ISO 8601) |
| `duration_ms` | Execution time in milliseconds |
| `steps` | Sub-step breakdown (array of `{name, duration_ms, status}`) |
| `column_lineage` | Source columns with transformation types |
| `depends` | Upstream table dependencies |
| `columns` | Output column definitions |
| `schema_hash` | Detects schema evolution |
| `sql_hash` | Triggers backfill on change |
| `dag_run_id` | Run identifier |
| `source_file` | Model source path |
| `duckdb_version` | DuckDB version used |
| `git_commit` | Commit SHA |
| `git_branch` | Branch name |
| `git_repo_url` | Repository URL |
| `error` | Error message (on failure) |

## Querying Metadata

Metadata is stored in DuckLake snapshots. Query with SQL:

```sql
-- Recent runs
SELECT
  commit_extra_info->>'model' AS model,
  commit_extra_info->>'run_type' AS run_type,
  commit_extra_info->>'rows_affected' AS rows,
  commit_extra_info->>'duration_ms' AS ms
FROM lake.snapshots()
ORDER BY snapshot_id DESC
LIMIT 10;
```

```sql
-- Column lineage for a model
SELECT
  commit_extra_info->>'model' AS model,
  commit_extra_info->>'column_lineage' AS lineage
FROM lake.snapshots()
WHERE LOWER(commit_extra_info->>'model') = 'mart.revenue'
ORDER BY snapshot_id DESC
LIMIT 1;
```

```sql
-- Models that changed schema
SELECT
  commit_extra_info->>'model' AS model,
  commit_extra_info->>'schema_hash' AS hash
FROM lake.snapshots()
WHERE commit_extra_info->>'run_type' = 'backfill';
```
