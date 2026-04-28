---
description: OndatraSQL loads reusable SQL macros from the config/macros/ directory. Define DuckDB functions for validation, helpers, and masking in your pipeline.
draft: false
title: macros/
weight: 5
---
**Macros: reusable SQL functions loaded from `config/macros/`**

**Phase:** Post-catalog | **Order:** 5 | **Required:** No

All `.sql` files in `config/macros/` are loaded at startup. Macros are available to all models.

## Directory Structure

| File | Contents |
|---|---|
| `config/macros/helpers.sql` | Utility functions (`safe_divide`, `cents_to_dollars`) |
| `config/macros/masking.sql` | Data masking (`mask_email`, `mask_ssn`, `hash_pii`, `redact`) |
| `config/macros/constraints.sql` | Constraint macros for `@constraint` directives |
| `config/macros/audits.sql` | Audit macros for `@audit` directives |
| `config/macros/warnings.sql` | Warning macros for `@warning` directives |

## Scalar Macros

```sql
CREATE OR REPLACE MACRO safe_divide(a, b) AS
    CASE WHEN b = 0 THEN NULL ELSE a / b END;

CREATE OR REPLACE MACRO cents_to_dollars(cents) AS
    cents / 100.0;
```

## Table Macros

```sql
CREATE OR REPLACE MACRO recent_orders(n) AS TABLE
    SELECT * FROM mart.orders ORDER BY order_date DESC LIMIT n;
```

## Validation Macros

Convention: `ondatra_{type}_{name}(t, ...)`.

```sql
CREATE OR REPLACE MACRO ondatra_audit_balanced_ledger(t) AS TABLE
  SELECT 'balances do not reconcile'
  WHERE (SELECT SUM(debit) FROM query(printf('SELECT * FROM %s', t)))
     != (SELECT SUM(credit) FROM query(printf('SELECT * FROM %s', t)));
```

```sql
-- @audit: balanced_ledger
```

For the full list of built-in macros, see [Constraints](/reference/validation/constraints/), [Audits](/reference/validation/audits/), [Warnings](/reference/validation/warnings/).

For DuckDB macro syntax, see the [DuckDB macro documentation](https://duckdb.org/docs/stable/sql/statements/create_macro).
