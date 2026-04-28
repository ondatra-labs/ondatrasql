---
description: OndatraSQL warnings surface data quality issues without stopping the data pipeline. Monitor thresholds and track known issues over time.
draft: false
title: Warnings
weight: 50
---
Warnings run after data is committed. They log results without failing the model. See [Validation](/concepts/validation/) for how warnings fit into the three-stage model.

```sql
-- @warning: name(args)
```

## Available Macros

| Macro | Example | Description |
|---|---|---|
| `freshness(col, duration)` | `freshness(updated_at, 24h)` | Data staleness |
| `row_count(op, n)` | `row_count(>=, 100)` | Row count check |
| `row_count_delta(max_pct)` | `row_count_delta(20)` | Change percentage between runs |
| `sum_delta(col, max_pct)` | `sum_delta(amount, 10)` | Sum change ratio between runs |
| `mean_delta(col, max_pct)` | `mean_delta(amount, 15)` | Mean change ratio between runs |
| `approaching_limit(col)` | `approaching_limit(amount)` | Nearing configured threshold |
| `null_percent(col, max_pct)` | `null_percent(email, 5)` | NULL percentage |
| `not_null(col)` | `not_null(customer_id)` | NULL count |
| `unique(col)` | `unique(order_id)` | Duplicate detection |
| `min(col, op, val)` | `min(amount, >, 0)` | Minimum value |
| `compare(col, op, val)` | `compare(amount, >=, 0)` | Value comparison |
| `mean(col, op, val)` | `mean(amount, >=, 100)` | Average check |
| `low_entropy(col, min)` | `low_entropy(status, 1.0)` | Data diversity |
| `cardinality(col, op, n)` | `cardinality(region, >=, 3)` | Distinct value count |

Delta macros (`row_count_delta`, `sum_delta`, `mean_delta`) use DuckLake's `table_changes()` to compare against the previous run.

## Custom Macros

Defined in `config/macros/warnings.sql`. Convention: `ondatra_warning_{name}(t, ...)`.

```sql
CREATE OR REPLACE MACRO ondatra_warning_high_refund_rate(t, col, max_pct) AS TABLE
  SELECT printf('high refund rate: %.1f%%', 100.0 * SUM(CASE WHEN c < 0 THEN 1 ELSE 0 END) / COUNT(*))
  FROM query(printf('SELECT %s AS c FROM %s', col, t))
  HAVING 100.0 * SUM(CASE WHEN c < 0 THEN 1 ELSE 0 END) / COUNT(*) > max_pct;
```

```sql
-- @warning: high_refund_rate(amount, 5)
```
