---
description: Why events models exist and how the buffering and claim model works.
draft: false
title: Event Collection
weight: 10
---
Events models are the one case where data comes to you instead of you going to get it. External systems POST events to an embedded HTTP endpoint, and the runner materializes them into DuckLake on the next pipeline run.

## Why a separate kind

SQL models pull data — they run a query and write the result. Events models receive data — they define a schema and accept HTTP POST. This is fundamentally different: the data arrives asynchronously, between pipeline runs, and needs to be buffered until the next run.

That's why events has its own kind, its own daemon (`ondatrasql events <port>`), and its own materialization path (flush instead of SQL execution).

## Why two durability levels

The single-event endpoint (`POST /collect/{schema}/{table}`) writes to a Badger WriteBatch that flushes every 100ms. A crash in that window loses unflushed events. The batch endpoint writes in a Badger transaction before responding — fully durable.

The tradeoff is throughput vs. safety. Analytics clicks can tolerate rare loss. Payment events cannot. Choose the endpoint that matches your data.

## Why claim/ack/nack

Events need exactly-once materialization into DuckLake, but the daemon and runner are separate processes. The claim model solves this:

1. The runner claims a batch of events (they're locked)
2. The runner inserts them into DuckLake in a transaction
3. On success, the runner acks — events are removed from Badger
4. On failure, the runner nacks — events return to the queue

If the runner crashes between commit and ack, the `_ondatra_acks` table in DuckLake records the commit. On restart, the runner finds the ack record, confirms the data is in DuckLake, and acks the daemon without re-inserting.

This gives you exactly-once into DuckLake even across crashes.

See [Events API](/reference/data-access/events-api/) for the exact endpoints and response codes.
