---
description: How OndatraSQL stores operational state — the push queue, fetch staging buffer, and OAuth refresh tokens — separately from your DuckLake data.
draft: false
title: State
weight: 10
---
OndatraSQL has two kinds of persistent storage. **DuckLake** holds your analytical data — tables, snapshots, Parquet files. **State** holds operational runtime: the push queue, the fetch staging buffer, and OAuth refresh tokens. This page explains why they are separate and how the state catalog is configured.

## Why state is separate from DuckLake

DuckLake is optimised for append-mostly analytical data: immutable Parquet files plus a small catalog of metadata. State is the opposite — frequent small writes, heartbeat updates, claim/ack churn, per-row dedup tracking. Putting state in DuckLake would create one snapshot per heartbeat and rapidly poison the snapshot history.

State also has different durability concerns. Data in DuckLake must never be lost. State can be rebuilt: a fresh `state.duckdb` means the next pipeline run re-derives incremental cursors from DuckLake, restarts pending pushes, and re-authenticates OAuth providers. Inflight push events are lost on a state wipe, but no analytical data is.

## What state holds

| Concept | Stored as |
|---|---|
| Push queue (pending outbound events) | `state.sync_evt` |
| In-flight push claims (events being delivered) | `state.sync_inflight` + `state.sync_claim` |
| Async job references (sinks that return a job id) | `state.sync_jobref` |
| Per-row push outcomes (crash-safe ack) | `state.sync_apply_log` |
| Fetch staging buffer (`save.row()` rows pre-materialize) | `state."fetch:<target>"` (one table per fetch target) |
| OAuth refresh tokens | `state.tokens` |

Tables are created idempotently on first open by `internal/sql/state/init.sql`. You do not define them.

## How the backend is chosen

`config/state.sql` is a regular SQL file. OndatraSQL opens an in-memory DuckDB session, executes `state.sql`, then runs `USE state` so the rest of the code can use unqualified table names. The default content (created by `ondatrasql init`) attaches a local encrypted file:

```sql
ATTACH 'state.duckdb' AS state
    (ENCRYPTION_KEY '${ONDATRA_STATE_KEY}');
```

The file lives in the project root. AES-GCM-256 file-level encryption is enabled by `ONDATRA_STATE_KEY`, generated automatically during `ondatrasql init`.

Switching backend is a one-line edit in `state.sql` — see [config/state.sql](/reference/configuration/config-state/) for the Quack and Postgres alternatives.

## Why the `state` alias is reserved

Everything in OndatraSQL that talks to state uses unqualified table names (`sync_evt`, `tokens`, etc.). After ATTACH, the runner runs `USE state` so those names resolve into the attached catalog. Renaming the alias breaks state operations.

## Concurrency

DuckDB takes a process-level file lock on `state.duckdb`. Only one ondatrasql process can have it open at a time. Inside that process, push goroutines hammer state in parallel — DuckDB's MVCC handles the contention internally.

For multi-process or multi-pod deployments where several ondatrasql workers must share state, you need a backend that supports cross-process writers. Quack (a DuckDB-served state catalog) or a future Postgres backend are the supported paths. The Go code is identical for all backends — only `state.sql` changes.

## Crash recovery

On startup, `state.GC()` runs once per pipeline:

1. Replays any half-applied per-row outcomes from `sync_apply_log` (covers crashes between record-outcomes and ack).
2. Returns events older than seven days from `sync_evt` to the GC queue.
3. Returns events whose heartbeat is older than ten minutes from `sync_inflight` back to `sync_evt` (covers a worker that died without ack/nack).

The order is load-bearing — see comments in `internal/state/sync_store.go` for the details.

## Encryption tradeoff

File-level encryption with `ONDATRA_STATE_KEY` protects refresh tokens and push payloads on disk. The cost is that **losing the key makes all state unreadable**, including pending pushes. Back up the key separately from the file. For dev environments where this is overkill, you can drop `ENCRYPTION_KEY` from `state.sql`; tokens will then be plaintext on disk.

## See also

- [config/state.sql](/reference/configuration/config-state/) — backend configuration reference
- [Outbound Sync](/concepts/outbound-sync/) — how the push queue is used
- [Set Up OAuth](/guides/set-up-oauth/) — how tokens land in state
- [Environment Variables](/reference/pipeline/env/) — `ONDATRA_STATE_KEY`
