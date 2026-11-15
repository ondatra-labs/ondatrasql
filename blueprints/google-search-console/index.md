---
title: Google Search Console
description: Search Analytics clicks, impressions, CTR and position per day, query and page for one Search Console property, read with a Google service account and loaded incrementally.
vendor: Google
tags:
  - seo
  - search
  - analytics
  - google
auth_method: service_account
source_type: fetch
model_kind: merge
args:
  - site
  - dimensions
  - days
min_version: v0.47.1
---
Loads the Search Analytics of one [Google Search Console](https://search.google.com/search-console) property through the [Search Console API](https://developers.google.com/webmaster-tools/v1/searchanalytics/query): clicks, impressions, click-through rate and average position per day, and per day and query or page. It signs in with a Google service account, so there is no browser login and no token to refresh by hand.

## What it does

- `FROM google_search_console('', 'query', 3)` takes three arguments: the property, the dimensions besides the date, and the days to fetch again.
- The property is `sc-domain:example.com` for a domain property or `https://www.example.com/` (with the trailing slash) for a URL prefix property, written as Search Console shows it. With `''` it comes from `GOOGLE_SEARCH_CONSOLE_SITE`.
- `date` is always the first dimension. The others are `query`, `page`, `country`, `device` and `searchAppearance`, comma separated; `''` gives one row per day.
- Each run sends `POST /sites/{property}/searchAnalytics/query` for web search with the dimensions and the whole date range, and reads the rows 5,000 at a time (`startRow` 0, 5000, ...) until a page has fewer.
- The first run loads the 16 months Search Console keeps, up to yesterday. A later run starts `days` days before the newest `date` it already has, and ends yesterday.
- Only final data is asked for (`dataState: final`). Search Console finishes a day about two days later, so the newest days come back empty and the next run picks them up.
- Each row carries `site`, `date`, each dimension (`searchAppearance` as `search_appearance`), `clicks`, `impressions`, `ctr` (0.05 is 5%), `position` (1 is the top), and `row_key`, a JSON list of the date and the dimension values: `["2026-11-10","ondatrasql"]`. The models merge on `row_key`, so a day fetched again replaces its rows instead of adding them twice.

### Three tables that do not add up

The three models answer three different questions, and their totals differ on purpose:

- **`raw.search_console_daily`** has the property's totals per day, aggregated by property: a search that showed two of your pages counts once.
- **`raw.search_console_pages`**: with `page` as a dimension, Google aggregates by page, so that search counts once for each page. Summing the pages of a day gives more impressions than the daily total, and the position is per page.
- **`raw.search_console_queries`**: Google leaves out anonymised queries (those made by very few people), so summing the queries of a day gives fewer clicks and impressions than the daily total. On a small site the difference can be most of the traffic.

Use the daily table for totals and the other two for breakdowns.

## Set up a service account

1. In the [Google Cloud console](https://console.cloud.google.com), pick or create a project and enable the **Google Search Console API** (APIs & Services, Library).
2. Under IAM & Admin, Service Accounts, create a service account. It needs no Cloud roles. On its **Keys** tab, add a key of type JSON and save the file somewhere outside the project's version control.
3. In [Search Console](https://search.google.com/search-console), open the property, then **Settings**, **Users and permissions**, **Add user**. Enter the service account's email (`name@project.iam.gserviceaccount.com`) with the permission **Restricted** (reading is all the blueprint does). Only an owner of the property can add users.
4. Put the key file's path and the property in `.env`, as in `.env.example`:

```bash
GOOGLE_SEARCH_CONSOLE_KEY_FILE=/path/to/service-account.json
GOOGLE_SEARCH_CONSOLE_SITE=sc-domain:example.com
```

The runtime signs a token with the key for the scope `https://www.googleapis.com/auth/webmasters.readonly` and renews it itself. A `403` means the service account is not a user of that property, or the property is written differently from Search Console (`sc-domain:` for a domain property, the exact URL with its trailing slash for a URL prefix property).

## Quick start

```bash
mkdir search-console && cd search-console
ondatrasql init
```

Copy the four files below into the project, keeping their paths, and fill in `.env` as above (the blueprint's `.env.example` lists both variables). Then run:

```bash
ondatrasql run
ondatrasql sql "SELECT date, clicks, impressions, round(ctr * 100, 1) AS ctr_pct, round(position, 1) AS position FROM raw.search_console_daily ORDER BY date DESC LIMIT 7"
ondatrasql sql "SELECT query, SUM(clicks) AS clicks FROM raw.search_console_queries WHERE date >= current_date - 28 GROUP BY query ORDER BY clicks DESC LIMIT 20"
```

The first run reads 16 months, which for a large property is many pages of 5,000 rows. Run it again tomorrow, and it fetches the last three days again plus the new ones.

### lib/google_search_console.star

{{< bp-file "lib/google_search_console.star" >}}

### models/raw/search_console_daily.sql

{{< bp-file "models/raw/search_console_daily.sql" >}}

### models/raw/search_console_queries.sql

{{< bp-file "models/raw/search_console_queries.sql" >}}

### models/raw/search_console_pages.sql

{{< bp-file "models/raw/search_console_pages.sql" >}}

## Customisation

- **Property:** put it in the first argument (`'sc-domain:example.com'`) to give each model its own, for example to load two properties into two tables. `site` is a column, but not part of `row_key`, so keep one property per table.
- **Dimensions:** combine them, for example `'query,page'` or `'country,device'`, and add one cast per dimension to the model: `country::VARCHAR AS country`. Every dimension is part of `row_key`. More dimensions mean more rows, and Search Console returns at most about 50,000 rows per day and search type, the top ones, so a fine breakdown of a large site is cut off.
- **Lookback:** the last argument is how many days before the newest loaded date each run fetches again. `0` fetches only from the newest loaded day on.
- **History:** add `-- @incremental_initial: 2026-01-01` to a model to start its first run on that day instead of 16 months back. A day older than 16 months is moved up to the oldest one Search Console has.
- **Search type:** the lib reads web search (`"type": "web"`). Change it in the request body to `image`, `video`, `news` or `discover` for those reports, in a copy of the lib with a model of its own.

## Notes

- Dates are days in Pacific Time, as Search Console reports them, whatever your time zone.
- A row that disappears from Search Console after you loaded it stays in the table: the merge replaces and adds rows, it does not delete them.
- Changing the dimensions changes `row_key`. Rows loaded with the old key stay next to the new ones, so rebuild the model after such a change (`ondatrasql run` rebuilds a model whose SQL changed).
- Data older than 16 months is deleted by Google, not by the blueprint: rows you loaded earlier stay in your table, so the history grows past 16 months from the day you start.
- The API has [usage limits](https://developers.google.com/webmaster-tools/limits) per property and per project. The three models make one request per 5,000 rows each; a run that hits a limit fails with Google's error, and the next run starts again from the newest day loaded.
