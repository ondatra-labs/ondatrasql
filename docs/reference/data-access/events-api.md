---
description: Event collection HTTP endpoints, methods, and durability.
draft: false
title: Events API
weight: 20
---

## Public endpoints

The public server binds on all interfaces on the port you specify (`ondatrasql events <port>`).

| Method | Path | Body | Response | Durability |
|---|---|---|---|---|
| `POST` | `/collect/{schema}/{table}` | JSON object | `202 Accepted` | Badger WriteBatch (flushed every 100ms) |
| `POST` | `/collect/{schema}/{table}/batch` | JSON array | `202 Accepted` | Badger transaction (immediate) |
| `GET` | `/health` | — | `200 OK` | — |

## Admin endpoints

The admin server binds to `127.0.0.1` on port + 1. Not accessible from external networks.

| Method | Path | Description |
|---|---|---|
| `POST` | `/flush/{schema}/{table}/claim` | Claim pending events for materialization |
| `POST` | `/flush/{schema}/{table}/ack` | Acknowledge successful commit |
| `POST` | `/flush/{schema}/{table}/nack` | Return events to queue |
| `GET` | `/flush/{schema}/{table}/inflight` | List inflight claims |
| `GET` | `/health` | Health check |

## Event lifecycle

| State | Meaning |
|---|---|
| Queued | In Badger store, waiting to be claimed |
| Claimed | Locked by a runner, being materialized |
| Acked | Successfully committed to DuckLake, removed from Badger |
| Nacked | Materialization failed, returned to queue for retry |

## Crash recovery

The `_ondatra_acks` table in DuckLake records successful commits. If a DuckLake commit succeeded but the Badger ack failed, the next run detects the ack record and skips re-processing.

## Validation

`NOT NULL` columns must be present in the event. `received_at` is auto-populated if declared in the model but absent from the event. Unknown fields are silently dropped during materialization.

## Binding

The port is required — there is no default. No built-in authentication. Use a reverse proxy for auth, rate limiting, and TLS.
