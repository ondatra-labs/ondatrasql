---
description: "How to maintain DuckLake storage: flush inlined rows, expire old snapshots, merge small Parquet files, rewrite files with deletes and remove unused files, with one command."
draft: false
title: "DuckLake Maintenance: Compact, Expire, Clean Up"
weight: 9
---
DuckLake keeps every version of your data. It never deletes old Parquet files on its own, because time travel can still read them. Small batch writes also leave many small files behind. Over time this costs storage and slows queries.

DuckLake maintenance fixes both. It merges small files, removes snapshots you no longer need and deletes the files nothing references any more. OndatraSQL wraps each DuckLake maintenance operation in one command, and `ondatrasql checkpoint` runs all of them in the right order.

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

## What Each Step Does

| Command | DuckLake operation | When you need it |
|---|---|---|
| `ondatrasql flush` | `ducklake_flush_inlined_data` | Small inserts are inlined in the catalog database and should become Parquet files |
| `ondatrasql expire` | `ducklake_expire_snapshots`, older than 30 days | You keep more snapshot history than you need for time travel |
| `ondatrasql merge` | `ducklake_merge_adjacent_files` | Many runs wrote small batches, so there are many small files |
| `ondatrasql rewrite` | `ducklake_rewrite_data_files` | Files carry many deleted rows, for example after merge or tracked models |
| `ondatrasql cleanup` | `ducklake_cleanup_old_files`, older than 7 days | Expired snapshots left files that nothing references |
| `ondatrasql orphaned` | `ducklake_delete_orphaned_files`, older than 7 days | A failed or interrupted write left files outside the catalog |

The retention periods are the defaults that `ondatrasql init` writes into `sql/`. Change them there.

## 3. Schedule Weekly

Use your system's cron (or systemd timer, k8s CronJob, Airflow, etc.) to run maintenance weekly:

```
0 3 * * 0 cd /path/to/project && ondatrasql checkpoint
```

## What Happens If You Skip Maintenance

Nothing breaks. Storage grows with every snapshot, and queries read more small files than they need to. If you run pipelines a few times a day, weekly maintenance is usually enough. Expiring snapshots limits how far back time travel can go: with the default, you can query versions up to 30 days old. Set the retention in `sql/expire.sql` to the oldest version you still want to query.

## Customizing

Maintenance SQL lives in `sql/`: `checkpoint.sql`, `flush.sql`, `merge.sql`, `expire.sql`, `rewrite.sql`, `cleanup.sql`, and `orphaned.sql`. Edit them directly to adjust thresholds or add custom logic.

See the [DuckLake maintenance docs](https://ducklake.select/docs/stable/duckdb/maintenance/recommended_maintenance) for details on each operation.
