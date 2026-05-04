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

**Path target must match `claim_id` target.** For `ack` and `nack`, the URL path's `{schema}/{table}` MUST match the schema.table that the `claim_id` was originally claimed against. A mismatch returns:

```http
HTTP/1.1 400 Bad Request
Content-Type: application/json

{"error": "claim \"<id>\" belongs to target \"raw.events\", not \"raw.audit\" (URL path mismatch)"}
```

A `claim_id` whose inflight events have already been ack'd, reaped, or never existed returns:

```http
HTTP/1.1 400 Bad Request
Content-Type: application/json

{"error": "claim has no inflight events (already ack'd, reaped, or never claimed)"}
```

The validation and the state change are atomic in a single Badger transaction — there is no time-of-check / time-of-use window where a reaped claim can return 200 OK with no actual mutation.

### Limit param contract

`/flush/{schema}/{table}/claim` accepts an optional `?limit=N` query parameter (default 1000):

| Form | Behavior |
|---|---|
| Omitted (`/claim`) | Falls through to default limit |
| `?limit=` (explicit empty) | 400 Bad Request — distinguished from omitted via `URL.Query()["limit"]` presence |
| `?limit=N` where N > 0 | Claim up to N events |
| `?limit=0`, `?limit=-N`, `?limit=abc` | 400 Bad Request |

## Error responses

Every non-2xx response from the events server (public and admin) is `Content-Type: application/json` with a stable envelope:

```json
{"error": "<message>"}
```

The `error` value is a free-form string describing what went wrong (e.g. `"invalid limit \"abc\" (must be positive integer)"`, `"unknown event target: raw.events"`, `"invalid JSON"`). Clients can decode the envelope unconditionally regardless of HTTP status code.

The shape is intentionally minimal — not `{code, message}` or RFC 7807 `application/problem+json` — to keep the contract small while preserving room to add fields later in a backwards-compatible way (any future addition will be an additional key, never a rename of `error`).

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
