---
title: Forgejo and Gitea pull requests
description: An organisation's or user's repositories and their pull requests from any Forgejo or Gitea instance, fetched incrementally through its REST API, with pull requests merged per week and the median time to merge.
vendor: Forgejo / Gitea
tags:
  - git
  - code-review
  - pull-requests
  - developer-productivity
  - public-api
auth_method: none
source_type: fetch
model_kind: merge
args:
  - kind
  - owner
  - base_url
min_version: v0.47.1
---
Fetches the repositories of an organisation (or a user) and their pull requests from any [Forgejo](https://forgejo.org) or [Gitea](https://about.gitea.com) instance through its REST API v1, such as [Codeberg](https://codeberg.org/api/swagger). Public repositories need no key; a token adds the private ones.

The raw models keep one row per repository and one row per pull request. A staging model counts the pull requests merged per week in each repository and the median time from opening to merge.

## What it does

- `FROM forgejo('repos', 'forgejo', 'https://codeberg.org')` fetches every repository of the owner `forgejo` on Codeberg. `FROM forgejo('pulls', …)` fetches the pull requests of those repositories, open, closed and merged.
- The owner is tried as an organisation (`/orgs/{owner}/repos`); when the instance answers 404, as a user (`/users/{owner}/repos`).
- The base URL is the third argument, with or without `/api/v1`. Leave it empty (`forgejo('pulls', 'forgejo', '')`) to use `FORGEJO_URL`.
- When `FORGEJO_TOKEN` is set, every request carries `Authorization: token <token>`. Without it, no `Authorization` header is sent and only public data is read.
- Lists are paged 50 at a time, the API's maximum, until the `X-Total-Count` header is reached.
- Pull requests are incremental on `updated_at`. The API cannot filter pull requests by time, so each repository's list is read newest update first (`sort=recentupdate`) and stops at the first pull request older than the bound. On the first run the bound is `@incremental_initial`; on later runs it is the newest `updated_at` already fetched, minus an hour. An up-to-date repository therefore costs one request per run.
- The raw pull request model merges on `id`, so a pull request updated again replaces its older version. The repository model reads the whole list on every run and merges on `id` too.

## Quick start

```bash
mkdir forgejo && cd forgejo
ondatrasql init
```

Copy the four files below into the project, keeping their paths, then run:

```bash
ondatrasql run
ondatrasql sql "SELECT * FROM staging.pull_request_merges ORDER BY week DESC, merged_prs DESC LIMIT 10"
```

Run it again later, and it fetches only the pull requests updated since.

### lib/forgejo.star

{{< bp-file "lib/forgejo.star" >}}

### models/raw/forgejo_repos.sql

{{< bp-file "models/raw/forgejo_repos.sql" >}}

### models/raw/forgejo_pulls.sql

{{< bp-file "models/raw/forgejo_pulls.sql" >}}

### models/staging/pull_request_merges.sql

{{< bp-file "models/staging/pull_request_merges.sql" >}}

## Customisation

- **Your instance and owner:** change the second and third arguments in both raw models, for example `forgejo('pulls', 'my-team', 'https://git.example.com')`, or leave the third empty and set `FORGEJO_URL`. A change to the model's SQL makes the next run a backfill.
- **Private repositories:** set `FORGEJO_TOKEN` to an access token from *Settings → Applications* on your instance, with read access to repositories and organisations (and to users, for a user's repositories).
- **History:** `@incremental_initial` in `forgejo_pulls.sql` is the oldest update the first run fetches. The blueprint sets a recent day, so a first run is small. Set it further back for more history: each 50 pull requests updated since then cost one request per repository.
- **Columns:** the lib flattens the fields the models select. To add one, such as `labels` or `milestone`, add it to `_pull_row` or `_repo_row` and a cast for it in the model.

## Notes

- The weeks in `staging.pull_request_merges` are ISO weeks in UTC, starting on Monday. Every pull request merged on or after `@incremental_initial` is counted, since its `updated_at` is at least its `merged_at`; the week that day falls in can be incomplete.
- `merged_at` is set for merged pull requests only. A pull request that was closed without merging has `closed_at` and is not counted.
- The repository list is read on every run and every repository with pull requests enabled is checked, one request each, even when nothing changed. Repositories with pull requests disabled are skipped.
- A repository or pull request that is deleted, or a repository moved to another owner, keeps its row.
- Requests are rate limited to two per second, and `retry` with `backoff` rides out a temporary `429` or `5xx`. Gitea and Forgejo instances set their own limits; Codeberg asks API users to be considerate.
- The data belongs to the repositories' authors and the instance; check the instance's terms before republishing it.
