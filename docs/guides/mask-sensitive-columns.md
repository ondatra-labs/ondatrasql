---
description: Apply mask, hash, or redact tags to columns and the pipeline handles the rest. Protect sensitive data during materialization.
draft: false
title: Mask Sensitive Columns
weight: 11
---
Add masking tags to `@column` directives. The pipeline applies them during materialization.

## 1. Tag Columns

```sql
-- models/mart/customers.sql
-- @kind: table
-- @column: email = Contact email | mask_email | PII
-- @column: ssn = Social security number | mask_ssn | PII
-- @column: name = Full name | hash_pii

SELECT email, ssn, name, city FROM staging.customers
```

Tags are separated by `|` after the description. Tags matching `mask`, `hash`, or `redact` (or prefixed like `mask_`, `hash_`, `redact_`) trigger masking. Other tags (`PII`) are metadata-only.

## 2. Run the Pipeline

```bash
ondatrasql run
```

Masked values are written to DuckLake. Original data is never stored.

## Built-in Macros

| Tag | Output | Example |
|---|---|---|
| `mask_email` | First character + domain | `a***@example.com` |
| `mask_ssn` | Last 4 digits | `***-**-1234` |
| `hash_pii` | SHA-256 hash | `a1b2c3d4...` |
| `redact` | `[REDACTED]` | `[REDACTED]` |

## 3. Add Custom Macros (Optional)

Create macros in `config/macros/masking.sql`:

```sql
CREATE OR REPLACE MACRO mask_phone(val) AS
    CASE WHEN val IS NULL THEN NULL
    ELSE '***-***-' || RIGHT(val, 4)
    END;
```

Use the tag:

```sql
-- @column: phone = Phone number | mask_phone
```
