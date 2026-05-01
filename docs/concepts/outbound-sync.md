---
description: Why OndatraSQL commits data before pushing to external systems, and what that means for your push function.
draft: false
title: Outbound Sync
weight: 9
---
When a model has `@push`, the runner pushes change events to an external system. This page explains the design decisions behind that mechanism.

## Commit first, push second

The runner always commits data to DuckLake before pushing to the external system. If the push fails, DuckLake already has the correct data. Failed events go back to the Badger queue and retry on the next run.

This means your DuckLake state is always authoritative. The external system may lag behind but never leads.

## Why at-least-once

The same event can be pushed multiple times — if the push succeeded but the Badger ack failed (crash between push and ack), or if a batch partially succeeded. Exactly-once delivery across two independent systems (DuckLake and an external API) requires distributed transactions, which adds complexity that doesn't match the use case.

Instead, your push function should be idempotent. Use `__ondatra_rowid` and `__ondatra_change_type` to deduplicate on the receiving side.

## Why merge requires @unique_key

Merge kind uses `MERGE INTO ... WHEN MATCHED UPDATE / WHEN NOT MATCHED INSERT` to upsert rows. Without a declared key, the runner cannot construct the match clause. The key also flows to your push function as `key_columns` so it can match rows on the external system.

## Why tracked requires @group_key

Tracked kind computes content hashes per group to detect changes. Without a group key, every row is its own group — you'd get a full replace on every run. The group key defines the unit of change detection.

The group key also flows to `key_columns` in the push function. For external systems that organize data by partition (e.g., a spreadsheet tab per region), the group key tells your push function which partition changed.

## The tradeoff

At-least-once is simpler than exactly-once and works for most external APIs. The cost is that your receiving system must handle duplicates. For APIs that support idempotency keys (Stripe, HubSpot), this is trivial. For APIs that don't, you need deduplication logic in your push function.

See [Push Contract](/reference/lib-functions/push-contract/) for the exact change types, batch modes, and return format.
