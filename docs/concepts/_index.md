---
description: Core concepts behind OndatraSQL. Models, model kinds, lib functions, change detection, schema evolution, and validation.
draft: false
title: Concepts
weight: 20
---
How OndatraSQL works.

- [Models](/concepts/models/): every model is a SQL file that produces one table
- [Model Kinds](/concepts/kinds/): table, append, merge, tracked, scd2
- [Lib Functions](/concepts/lib-functions/): API dict for inbound fetch and outbound push
- [Change Detection](/concepts/cdc/): process only changed data via DuckLake snapshots
- [Incremental Models](/concepts/incremental/): cursor-based loading for SQL and lib functions
- [Schema Evolution](/concepts/schema-evolution/): additive and destructive changes without migration
- [Dependency Graph](/concepts/dag/): automatic execution order from SQL references
- [Outbound Sync](/concepts/outbound-sync/): why commit-first and at-least-once delivery
- [Validation](/concepts/validation/): constraints, audits, and warnings at three stages