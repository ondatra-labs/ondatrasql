---
description: OndatraSQL audits detect dataset-level regressions inside the materialize transaction. A failing audit aborts atomically with no data committed.
draft: false
title: Audits
weight: 40
---
Audits run inside the materialize transaction, after `INSERT`, before `COMMIT`. A failing audit aborts the entire transaction. Data, schema changes, and commit metadata roll back together. See [Validation](/concepts/validation/) for how audits fit into the three-stage model.

```sql
-- @audit: name(args)
```

Multiple audits per model are combined into a single check.

## NULL Handling

Most aggregate audits (`mean`, `max`, `min`, `sum`, `stddev`, `zscore`, `freshness`) fail when all values are NULL. The error message states `NULL (all values NULL)`. Note: `percentile` does not have this guard — it silently passes when all values are NULL.

## Available Macros

| Macro | Example | Description |
|---|---|---|
| `row_count(op, n)` | `row_count(>=, 100)` | Total row count |
| `freshness(col, duration)` | `freshness(updated_at, 24h)` | Maximum age of most recent record (`h`/`d`) |
| `mean(col, op, val)` | `mean(price, >=, 10)` | Average value |
| `mean_between(col, lo, hi)` | `mean_between(price, 10, 100)` | Average within range |
| `stddev(col, max)` | `stddev(amount, 25)` | Standard deviation threshold |
| `min(col, op, val)` | `min(price, >=, 0)` | Minimum value |
| `max(col, op, val)` | `max(price, <=, 10000)` | Maximum value |
| `sum(col, op, val)` | `sum(amount, >=, 0)` | Aggregate sum |
| `zscore(col, max)` | `zscore(amount, 3)` | Statistical outlier detection |
| `percentile(col, p, op, val)` | `percentile(latency, 0.95, <=, 500)` | Quantile validation |
| `median(col, op, val)` | `median(amount, >=, 100)` | Median value |
| `entropy(col, op, val)` | `entropy(status, >=, 1.0)` | Shannon entropy |
| `approx_distinct(col, op, val)` | `approx_distinct(customer_id, >=, 1000)` | Approximate distinct count |
| `reconcile_count(table)` | `reconcile_count(raw.orders)` | Row counts must match source |
| `reconcile_sum(col, table, col)` | `reconcile_sum(total, raw.orders, total)` | Aggregate sums must match source |
| `column_exists(col)` | `column_exists(email)` | Verify column exists |
| `column_type(col, type)` | `column_type(price, DECIMAL)` | Verify column type |
| `golden(path)` | `golden(tests/expected.csv)` | Compare output with golden dataset |

## Custom Macros

Defined in `config/macros/audits.sql`. Convention: `ondatra_audit_{name}(t, ...)`.

```sql
CREATE OR REPLACE MACRO ondatra_audit_balanced_ledger(t) AS TABLE
  SELECT 'balances do not reconcile'
  WHERE (SELECT SUM(debit) FROM query(printf('SELECT * FROM %s', t)))
     != (SELECT SUM(credit) FROM query(printf('SELECT * FROM %s', t)));
```

```sql
-- @audit: balanced_ledger
```
