---
description: Configure the operational state backend in OndatraSQL. Holds the push queue, fetch staging buffer, and encrypted OAuth refresh tokens.
draft: false
title: state.sql
weight: 5
---
**State: where operational runtime lives**

**Phase:** State | **Order:** 5 | **Required:** Yes

One ATTACH statement tells OndatraSQL where to put the push queue, fetch staging buffer, and OAuth refresh tokens. Separate from `catalog.sql` (which defines the DuckLake catalog) because state is hot operational data, not analytical data.

## Default (created by `ondatrasql init`)

```sql
ATTACH 'state.duckdb' AS state
    (ENCRYPTION_KEY '${ONDATRA_STATE_KEY}');
```

A local DuckDB file in the project root, encrypted at rest with AES-GCM-256. `ondatrasql init` generates `ONDATRA_STATE_KEY` and writes it to `.env`.

## Why the `state` alias is reserved

After executing `state.sql`, OndatraSQL runs `USE state` so unqualified table references (`sync_evt`, `sync_inflight`, `sync_claim`, `sync_jobref`, `sync_apply_log`, `tokens`) resolve into the attached catalog. Renaming the alias breaks every state operation.

## Schema

OndatraSQL creates the tables idempotently on first run. You do not need to define them.

| Table | Purpose |
|---|---|
| `sync_evt` | Pending outbound events (push queue) |
| `sync_inflight` | Events currently being pushed by a worker |
| `sync_claim` | Per-claim heartbeat, used for orphan recovery |
| `sync_jobref` | Async-push job references (sinks that return a job id) |
| `sync_apply_log` | Per-row push outcomes for crash-safe ack |
| `tokens` | OAuth refresh tokens, plaintext column, file-level encrypted |

## Encryption

DuckDB's file-level encryption is enabled by passing `ENCRYPTION_KEY` to ATTACH. The key must be a 32-byte secret (the init template uses base64-encoded random bytes). The entire file — including WAL and any temporary spill — is encrypted.

- The token column is plaintext in SQL; the file is encrypted on disk.
- Lose the key and **all** state (push queue + tokens + jobref) becomes unreadable.
- Back up the key separately from the state file (secret manager, password manager, etc.).
- Rotate the key by exporting state with one key and re-encrypting on import — there is no in-place rotation.

To run without encryption (not recommended for tokens), omit the option:

```sql
ATTACH 'state.duckdb' AS state;
```

## Required for these commands

`state.sql` is opened by every command that touches operational state:

- `ondatrasql run` — push queue, fetch staging, OAuth refresh
- `ondatrasql sandbox` — same (in sandbox mode the state catalog is still the real one; only the DuckLake catalog is forked)
- `ondatrasql auth` — writes refresh tokens to `state.tokens`

Running these in a project without `config/state.sql` fails with `open state: config/state.sql required`.

## Other backends

The Go runtime is backend-agnostic — it only depends on `state` being an attached catalog with the schema above. Two future backends are planned:

### Quack (shared DuckDB server, multi-pod)

```sql
LOAD quack;
ATTACH 'quack:state.example.com:9494' AS state
    (TYPE quack, TOKEN '${ONDATRA_QUACK_TOKEN}');
```

Allows multiple ondatrasql processes to share a single state database without filesystem locks. Currently blocked: DuckDB 1.5.3 cannot run client-side `UPDATE`/`DELETE`/upsert statements over a Quack `ATTACH`, which the state store relies on — track the [duckdb-quack issue tracker](https://github.com/duckdb/duckdb-quack/issues). When that lands, switching backend is a one-line edit in `state.sql` with no Go code change.

### Postgres (not implemented yet)

Would require a small DDL adapter in `internal/state/` (BLOB → BYTEA, `now()` → `CURRENT_TIMESTAMP`). Useful when running ondatrasql in ephemeral containers where state must survive `cleanup` between runs.

## See also

- [Environment Variables](/reference/pipeline/env/) — `ONDATRA_STATE_KEY`
- [catalog.sql](/reference/configuration/config-catalog/) — DuckLake catalog (separate concept)
- [Push Contract](/reference/lib-functions/push-contract/) — how `sync_evt` is consumed during push
- [Set Up OAuth](/guides/set-up-oauth/) — how `tokens` is populated
