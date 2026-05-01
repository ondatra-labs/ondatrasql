---
description: Why OndatraSQL validates data at three stages and why each stage exists.
draft: false
title: Validation
weight: 8
---
Bad data in a pipeline is worse than no data. A wrong number in a dashboard erodes trust. A null email sent to a CRM causes an API error. A sudden row count drop means something broke upstream. OndatraSQL catches these problems at three different stages, each designed for a different class of issue.

## Why three stages

You might wonder why one validation step isn't enough. The reason is timing. Some problems are visible before data is written (a null email is never valid), some are only visible after (the total row count dropped by 80%), and some you want to know about but not block on (the average price dipped below your threshold).

Each stage runs at a different point in the pipeline and has different failure behavior:

- **Constraints** run before insert. If a row is bad, nothing is written.
- **Audits** run inside the transaction, after insert. If the result is bad, everything rolls back.
- **Warnings** run after commit. They log but don't block.

## Constraints: catching bad rows

Constraints validate individual rows before data enters your target table. If any row fails, the model stops and nothing is written. Your target table is never touched.

This is your first line of defense. It catches data quality issues at the source — before they propagate to downstream models or get pushed to external APIs via `@push`. If you're doing outbound sync, constraints are especially important: they prevent invalid data from ever reaching the external system.

See the [Constraints reference](/reference/validation/constraints/) for syntax and the full macro list.

## Audits: catching bad results

Some problems aren't visible at the row level. The row count dropped to zero. The data is three days stale. The total revenue doesn't match the source. These are dataset-level regressions that only become apparent after the data is written.

Here's the key: audits run inside the same transaction as the data write. If an audit fails, the entire transaction aborts — schema changes, data, commit metadata, everything rolls back. No DuckLake snapshot is created. Your table stays exactly as it was before the run.

This means you can't end up with committed bad data. The audit is your last chance to catch a problem before it becomes permanent.

See the [Audits reference](/reference/validation/audits/) for syntax and the full macro list.

## Warnings: monitoring without blocking

Sometimes you want to know about something without stopping the pipeline. The average order value dipped. The null rate in a column crept up. The row count changed more than 20%.

Warnings run after the data is committed. They show up in your CLI output and `--json` metadata. They're for monitoring — the pipeline equivalent of a Grafana alert.

A common workflow is to start with warnings, observe the behavior over a few runs, tune the thresholds, and then promote to constraints or audits when you're confident in the threshold.

See the [Warnings reference](/reference/validation/warnings/) for syntax and the full macro list. For custom macros, see [Configuration: Macros](/reference/configuration/config-macros/).
