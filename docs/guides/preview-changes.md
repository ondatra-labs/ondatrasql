---
description: Preview all pipeline changes before committing to production. OndatraSQL's sandbox mode runs against a temporary catalog copy.
draft: false
title: Preview Changes
weight: 8
---
Run the full pipeline against a temporary copy of the catalog. Nothing is committed to production.

## 1. Run Sandbox

```bash
ondatrasql sandbox
```

Or for a single model:

```bash
ondatrasql sandbox staging.orders
```

## 2. Review Output

The sandbox shows row changes, schema evolution, and validation results for every model:

```text
╠═══ Changes ══════════════════════════════════════════════════╣
║  staging.orders                                              ║
║    Rows: 100 → 142 (+42, +42.0%)                            ║
║                                                              ║
║  mart.revenue                                                ║
║    SCHEMA EVOLUTION: + Added: region (VARCHAR)               ║
║    Rows: 5 → 6 (+1, +20.0%)                                 ║
```

Failed models are reported without affecting production:

```text
[OK] raw.customers (table, backfill, 45 rows, 45ms)
[OK] staging.orders (table, backfill, 120 rows, 95ms)
[OK] mart.revenue (table, backfill, 3 rows, 54ms)
[FAILED] mart.kpis
  ERROR: constraint violated: total >= 0
```

## 3. Apply Changes

If the preview looks correct, run the pipeline for real:

```bash
ondatrasql run
```
