---
title: Riksbank SWEA
description: Exchange rates, policy and market rates and any other series the Swedish Riksbank publishes, fetched incrementally from its public SWEA API.
vendor: Sveriges Riksbank
tags:
  - finance
  - exchange-rates
  - interest-rates
  - public-api
auth_method: none
source_type: fetch
model_kind: merge
args:
  - series
---
Fetches daily observations from the [Riksbank SWEA API](https://api.riksbank.se/swea/v1): exchange rates such as SEK per EUR and USD, the policy rate, market rates, government bond yields, and every other series the Riksbank publishes. The API is public, so the blueprint needs no key.

The raw model keeps one row per series and banking day. A staging model pivots it to one row per day with a column per currency.

## What it does

- `FROM riksbank('SEKEURPMI,SEKUSDPMI')` fetches each listed series in turn. The API takes one series per request.
- The first run fetches from `@incremental_initial` up to yesterday. A later run asks only for the days after the newest `date` it already has, so an up-to-date project makes no request at all.
- Long ranges are split into requests of at most a year. Requests are rate limited to the API's one per second.
- A range without a fixing (a weekend or a holiday) answers `204 No Content`. That means no rows, not an error.
- The end of the range is pinned to yesterday when the run starts. A fixing published later today is fetched in full tomorrow, never half of it today.
- The raw model merges on `obs_key` (`series:date`), so a day fetched again replaces itself instead of appearing twice.

## Quick start

```bash
mkdir riksbank && cd riksbank
ondatrasql init
```

Copy the three files below into the project, keeping their paths, then run:

```bash
ondatrasql run
ondatrasql sql "SELECT * FROM staging.exchange_rates ORDER BY date DESC LIMIT 5"
```

Run it again the next banking day, and it fetches only the new day.

### lib/riksbank.star

{{< bp-file "lib/riksbank.star" >}}

### models/raw/riksbank_observations.sql

{{< bp-file "models/raw/riksbank_observations.sql" >}}

### models/staging/exchange_rates.sql

{{< bp-file "models/staging/exchange_rates.sql" >}}

## Customisation

- **Other series:** change the list in `FROM riksbank('…')`. The series IDs are listed at `https://api.riksbank.se/swea/v1/Series`, for example `SEKGBPPMI`, `SEKNOKPMI` or `SECBREPOEFF` (the policy rate). If you pivot them in staging, add a `MAX(CASE WHEN series = '…' THEN value END)` column for each one.
- **History:** `@incremental_initial` is the first day the first run fetches. Set it as far back as you need; the lib splits long ranges into one request per year and series.
- **Types:** `value` is a `DOUBLE` in raw. The staging model casts it to `DECIMAL(10, 4)`. Use a wider scale for series that need it.

## Notes

- The API allows about one request per second without a key. The `rate_limit` in the API dict keeps to that, and `retry` with `backoff` rides out a temporary `429` or `5xx`.
- A fixing the Riksbank revises after you fetched it is not fetched again: a run only asks for days after the newest one it has.
- Dates are banking days in Stockholm; weekends and Swedish holidays have no rows.
