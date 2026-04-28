---
description: OndatraSQL constraints validate rows before data is written. If any row fails, the model is blocked and nothing is inserted into your DuckLake tables.
draft: false
title: Constraints
weight: 30
---
Constraints run before insert, against the temp result of your query. If any row fails, nothing is written and downstream models do not run. See [Validation](/concepts/validation/) for how constraints fit into the three-stage model.

```sql
-- @constraint: name(args)
```

Multiple constraints per model are checked in a single batched query.

## Available Macros

| Macro | Example | Description |
|---|---|---|
| `primary_key(col)` | `primary_key(id)` | NOT NULL + UNIQUE |
| `not_null(col)` | `not_null(email)` | Column must not be NULL |
| `unique(col)` | `unique(code)` | No duplicate values |
| `composite_unique('cols')` | `composite_unique('year, month')` | Composite uniqueness |
| `not_empty(col)` | `not_empty(name)` | Not NULL and not empty string |
| `compare(col, op, val)` | `compare(age, >=, 0)` | Comparison operator check |
| `between(col, lo, hi)` | `between(rating, 1, 5)` | Range check |
| `in_list(col, 'vals')` | `in_list(status, 'active,inactive')` | Allowed values |
| `not_in(col, 'vals')` | `not_in(role, 'deleted')` | Forbidden values |
| `like(col, pattern)` | `like(email, %@%)` | SQL LIKE pattern |
| `not_like(col, pattern)` | `not_like(name, %test%)` | Inverse LIKE |
| `matches(col, regex)` | `matches(phone, ^\+[0-9]+)` | Regular expression |
| `email(col)` | `email(contact_email)` | Email format |
| `uuid(col)` | `uuid(transaction_id)` | UUID format |
| `length_between(col, lo, hi)` | `length_between(name, 1, 255)` | String length range |
| `length_eq(col, n)` | `length_eq(code, 3)` | Exact string length |
| `references(col, table, pk)` | `references(customer_id, customers, id)` | Foreign key check |
| `check(col, expr)` | `check(total, total >= 0)` | Custom SQL expression |
| `required_if(col, condition)` | `required_if(delivery_date, status = 'delivered')` | Conditional NOT NULL |
| `at_least_one(col)` | `at_least_one(email)` | At least one non-NULL value |
| `not_constant(col)` | `not_constant(status)` | At least 2 distinct values |
| `null_percent(col, max_pct)` | `null_percent(email, 10)` | Maximum NULL percentage |
| `distinct_count(col, op, n)` | `distinct_count(status, >=, 3)` | Cardinality check |
| `duplicate_percent(col, max_pct)` | `duplicate_percent(email, 5)` | Maximum duplicate percentage |
| `sequential(col)` | `sequential(id)` | No gaps in integer sequence |
| `sequential_step(col, n)` | `sequential_step(id, 5)` | Gaps allowed only by step N |
| `no_overlap(start, end)` | `no_overlap(start_date, end_date)` | Time intervals don't overlap |
| `valid_type(col, type)` | `valid_type(amount, DECIMAL)` | TRY_CAST succeeds for all rows |
| `no_nan(col)` | `no_nan(amount)` | No NaN values |
| `finite(col)` | `finite(amount)` | No NaN or infinity |

## Custom Macros

Defined in `config/macros/constraints.sql`. Convention: `ondatra_constraint_{name}(t, col, ...)`.

```sql
CREATE OR REPLACE MACRO ondatra_constraint_my_check(t, col) AS TABLE
  SELECT printf('my_check failed: %s has %d violations', col, COUNT(*))
  FROM query(printf('SELECT %s AS c FROM %s', col, t))
  WHERE c < 0
  HAVING COUNT(*) > 0;
```

```sql
-- @constraint: my_check(amount)
```
