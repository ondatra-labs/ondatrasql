---
title: Toggl Track
description: One workspace's time entries from the Toggl Track Reports API v3, loaded by month with an API token, with hours per day, client and project.
vendor: Toggl
tags:
  - time-tracking
  - toggl
  - reporting
auth_method: basic
source_type: fetch
model_kind: merge
args:
  - workspace_id
min_version: v0.47.1
---
Loads the time entries of one [Toggl Track](https://toggl.com/track/) workspace through the [Reports API v3](https://engineering.toggl.com/docs/reports/detailed_reports) detailed search: who tracked how long on which project, client and task, with the description, the billable flag and the tags. It signs in with your personal API token.

The raw model keeps one row per time entry. A staging model sums them to hours per day, client and project.

## What it does

- `FROM toggl_track('<workspace id>')` searches the workspace's time entries with `POST /workspace/<workspace id>/search/time_entries`. With `''` the workspace id comes from `TOGGL_WORKSPACE_ID`.
- It asks for `enrich_response: true`, so each result row carries `project_name` and `client_name` next to the ids.
- The API groups time entries that share a user, project, description, billable flag and tags into one result row. The lib returns one row per time entry, with the fields of its group copied onto it, and `date`, the day the entry starts in the time zone Toggl gives the `start` in.
- One month per request, 50 result rows per page. The next page starts at the row number the API returns in the `X-Next-Row-Number` header; the month is done when the header is absent.
- The first run fetches from `@incremental_initial` up to today. Every later run fetches last month and this month again, whatever it already has, so an entry edited, moved or added late lands in the next run.
- The raw model merges on the entry `id`, so an entry fetched again replaces itself instead of appearing twice.
- Requests are rate limited to one per second. Toggl allows 240 requests an hour per user (fewer on the free plan).

## Get an API token and the workspace id

1. Sign in to Toggl Track, click your avatar and open **Profile settings**. The **API Token** is at the bottom of the page. It acts as you: it reads every workspace you are a member of, with your rights.
2. The workspace id is the number in the address bar when you open **Reports**: `https://track.toggl.com/reports/summary/<workspace id>`. If you are in several workspaces, switch to the one you want first.
3. Put both in `.env`:

```bash
TOGGL_API_TOKEN=your-token
TOGGL_WORKSPACE_ID=1234567
```

The runtime sends `Authorization: Basic` with the token as the user and the literal `api_token` as the password, as Toggl asks.

### .env.example

```bash
TOGGL_API_TOKEN=
TOGGL_WORKSPACE_ID=
```

## Quick start

```bash
mkdir toggl && cd toggl
ondatrasql init
```

Copy the three files below into the project, keeping their paths, and fill in `.env` as above. Set `@incremental_initial` to the first day you want, then run:

```bash
ondatrasql run
ondatrasql sql "SELECT * FROM staging.toggl_hours ORDER BY date DESC, hours DESC LIMIT 10"
```

Run it again tomorrow, and it fetches last month and this month again.

### lib/toggl_track.star

{{< bp-file "lib/toggl_track.star" >}}

### models/raw/toggl_time_entries.sql

{{< bp-file "models/raw/toggl_time_entries.sql" >}}

### models/staging/toggl_hours.sql

{{< bp-file "models/staging/toggl_hours.sql" >}}

## Customisation

- **History:** `@incremental_initial` is the first day the first run fetches. A year of history is twelve requests, plus one for every 50 result rows beyond the first page of a month.
- **Workspace:** put the id in the model, `FROM toggl_track('1234567')`, instead of `TOGGL_WORKSPACE_ID`. For several workspaces, write one raw model per workspace.
- **Personal data:** the raw model leaves out who tracked the time, apart from the numeric `user_id`. The lib passes every field of the API's result row through, so to add the user's name, add a cast to the raw model:

  ```sql
      username::VARCHAR AS user_name,
  ```

  Changing the model's columns makes the next run a backfill from `@incremental_initial`. Add an email the same way if your response has one, under the field name it has there. Only add them when you have a reason to keep them, and think about who can read the table: see [Mask sensitive columns](/guides/mask-sensitive-columns/). To leave out the `user_id` too, delete its line.
- **Other fields:** any other field of the result row or the time entry is available the same way, for example `task_name`, `billable_amount_in_cents`, `hourly_rate_in_cents` and `currency` where your plan has rates.

## Notes

- Merge does not delete. An entry deleted in Toggl stays in the raw table; to drop it, rebuild the table (change the model, or drop the table and run again).
- An edit to an entry older than last month is not fetched again. Change `@incremental_initial` to fetch everything again from that day.
- A running timer has no `stop`; if the report returns it, `stop` is NULL until a later run fetches it again.
- A month and the next both ask for the first day of the next month, so no entry falls between them whichever way the API counts `end_date`. Entries the previous month already returned are skipped, so no id comes twice in a run.
- When the hourly quota is used up, Toggl answers with an error instead of rows: `retry` with `backoff` rides out a short `429`, anything else fails the run with Toggl's message. Run again when the quota has reset.
