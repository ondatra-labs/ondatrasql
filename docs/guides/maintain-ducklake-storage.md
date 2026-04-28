---
description: Compact files, expire snapshots, and clean up storage in DuckLake.
draft: false
title: Maintain DuckLake Storage
weight: 9
---
Compact files, expire snapshots, and free storage.

## 1. Run All Maintenance

```bash
ondatrasql checkpoint
```

Runs all steps in order: flush → expire → merge → rewrite → cleanup → orphaned.

## 2. Or Run Individual Steps

```bash
ondatrasql flush                  # Materialize inlined data to Parquet
ondatrasql expire                 # Remove old snapshot metadata
ondatrasql merge                  # Combine small files
ondatrasql rewrite                # Compact files with many deletes
ondatrasql cleanup                # Delete unused files
ondatrasql orphaned               # Remove stray files from failed writes
```

## 3. Schedule Weekly

Use cron to run maintenance weekly:

```
0 3 * * 0 cd /path/to/project && ondatrasql checkpoint
```

Note: `ondatrasql schedule` installs a scheduled `ondatrasql run` (pipeline execution), not maintenance. Use your system's cron for `checkpoint`.

## Customizing

Maintenance SQL lives in `sql/`: `checkpoint.sql`, `flush.sql`, `merge.sql`, `expire.sql`, `rewrite.sql`, `cleanup.sql`, and `orphaned.sql`. Edit them directly to adjust thresholds or add custom logic.

See [DuckLake: Recommended Maintenance](https://ducklake.select/docs/stable/duckdb/maintenance/recommended_maintenance) for details on each operation.