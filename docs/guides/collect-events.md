---
description: Collect events via HTTP POST with in-memory buffering. Events are materialized into DuckLake during pipeline runs. Use batch endpoint for durability.
draft: false
title: Collect Events
weight: 7
---
Accept events via HTTP POST. Single events are written to a Badger WriteBatch and flushed to disk every 100ms — a crash within that window may lose unflushed events. Batch events (`POST /collect/{schema}/{table}/batch`) are written in a Badger transaction before responding (fully durable). All events materialize into DuckLake during pipeline runs.

## 1. Define the Schema

```sql
-- models/raw/events.sql
-- @kind: events

event_name VARCHAR NOT NULL,
page_url VARCHAR,
user_id VARCHAR,
received_at TIMESTAMPTZ
```

## 2. Start the Collector

```bash
ondatrasql events 8080
```

Public endpoint on port `8080`. Admin endpoint on `8081` (localhost-only).

## 3. Send Events

```bash
curl -X POST localhost:8080/collect/raw/events \
  -d '{"event_name":"pageview","page_url":"/home","user_id":"u42"}'
```

Batch endpoint:

```bash
curl -X POST localhost:8080/collect/raw/events/batch \
  -d '[{"event_name":"pageview"},{"event_name":"click"}]'
```

## 4. Flush to DuckLake

```bash
ondatrasql run
```

## Endpoints

**Public** (port from CLI):

| Method | Path | Description |
|---|---|---|
| `POST` | `/collect/{schema}/{table}` | Single event |
| `POST` | `/collect/{schema}/{table}/batch` | Batch (atomic) |
| `GET` | `/health` | Health check |

**Admin** (`port + 1`, localhost only):

| Method | Path | Description |
|---|---|---|
| `POST` | `/flush/.../claim` | Claim events |
| `POST` | `/flush/.../ack` | Acknowledge flush |
| `POST` | `/flush/.../nack` | Return to queue |
| `GET` | `/flush/.../inflight` | List inflight claims |

## Validation

`NOT NULL` columns must be present. Unknown fields are ignored during flush. `received_at` is auto-populated if not provided.
