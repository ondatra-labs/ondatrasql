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

## How sandbox forks the catalog

`ondatrasql sandbox` snapshots the prod catalog by copying its files into `.sandbox/<pid>-<random>/`. For SQLite/DuckDB-backed catalogs this means the catalog file plus its WAL sibling (`-wal` for SQLite, `.wal` for DuckDB). Postgres-backed catalogs use `CREATE DATABASE … TEMPLATE`, which is consistent by construction.

**Concurrency contract.** The file-copy fork assumes no other process is actively writing to the prod catalog during the copy. In normal use this is satisfied — sandbox is invoked interactively when no `ondatrasql run`, `ondatrasql events`, or `ondatrasql odata` daemon is producing writes. If a concurrent writer is active, the catalog's WAL may be checkpointed (and deleted) mid-copy, leaving the sandbox with an inconsistent main+WAL pair. The sandbox run will then fail to open the catalog rather than silently produce wrong data; quiesce the writers and re-run.
