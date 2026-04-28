---
description: OndatraSQL loads runtime variables from the config/variables/ directory. Define pipeline configuration as SQL across constant, global, and per-model scopes.
draft: false
title: variables/
weight: 6
---
**Variables: runtime configuration loaded from `config/variables/`**

**Phase:** Post-catalog | **Order:** 6 | **Required:** No

Variables define pipeline configuration as SQL. Access via `getvariable('name')` in SQL.

## Directory Structure

| File | Scope | When Loaded |
|---|---|---|
| `config/variables/constants.sql` | Constants | Once at session init |
| `config/variables/global.sql` | Global | Once at session init (can use subqueries) |
| `config/variables/local.sql` | Per-model | Before each model's warning validation step |

## Constants

```sql
SET VARIABLE default_currency = 'SEK';
SET VARIABLE vat_rate = 0.25;
SET VARIABLE alert_amount_threshold = 1000000;
```

## Global

```sql
SET VARIABLE total_models = (
    SELECT COUNT(DISTINCT commit_extra_info->>'model')
    FROM snapshots()
    WHERE commit_extra_info->>'model' IS NOT NULL
);
```

## Per-Model

```sql
-- current_model is set by the runtime before this file loads
SET VARIABLE prev_model_snapshot = COALESCE(
  (SELECT snapshot_id FROM (
    SELECT snapshot_id, ROW_NUMBER() OVER (ORDER BY snapshot_id DESC) AS rn
    FROM snapshots()
    WHERE LOWER(commit_extra_info->>'model') = LOWER(getvariable('current_model'))
  ) WHERE rn = 2), 0);
```

## Built-in Variables

| Variable | Description |
|---|---|
| `ondatra_run_time` | Run start time (UTC) |
| `ondatra_load_id` | Unique run identifier |
| `curr_snapshot` | Current DuckLake snapshot ID |
| `prev_snapshot` | Previous snapshot ID (for CDC) |
| `dag_start_snapshot` | Snapshot at DAG start |
| `current_model` | Target name of the current model (set per-model) |

For DuckDB variable syntax, see the [DuckDB variable documentation](https://duckdb.org/docs/stable/sql/statements/set_variable).
