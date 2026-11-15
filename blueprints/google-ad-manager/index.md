---
title: Google Ad Manager Reports
description: Historical Ad Manager reports by date, run through the Ad Manager REST API with a Google service account and loaded incrementally.
vendor: Google
tags:
  - advertising
  - ad-manager
  - reporting
  - google
auth_method: service_account
source_type: fetch
model_kind: merge
args:
  - networks
  - dimensions
  - metrics
  - days
min_version: v0.47.1
---
Runs a historical report in [Google Ad Manager](https://admanager.google.com) through the [Ad Manager REST API](https://developers.google.com/ad-manager/api/beta) (v1) and loads its rows, one per network, date and combination of the dimensions you choose. It signs in with a Google service account, so there is no browser login and no token to refresh by hand.

## What it does

- `FROM google_ad_manager('', 'AD_UNIT_NAME', 'AD_SERVER_IMPRESSIONS,AD_SERVER_CLICKS,AD_SERVER_REVENUE', 3)` takes four arguments: the network codes, the dimensions, the metrics and the days to fetch again.
- For each network it runs a hidden report (`visibility: HIDDEN`, so it does not show up in the Ad Manager UI) with `DATE` as the first dimension, polls the run until it is done and reads the result 10,000 rows per page.
- It keeps one such report per network and model, named `ondatrasql <model>`: `ondatrasql raw.gam_report` for the model below. The first run creates it; every later run finds it by that name and updates its dates, dimensions and metrics before running it. The Ad Manager API cannot delete a report, so creating a new one each run would pile them up.
- The network codes are comma separated. An empty string means every network the service account is an API user on.
- The first run reports from `@incremental_initial` up to yesterday. A later run starts `days` days before the newest `date` it already has, because Ad Manager keeps revising the last few days. Yesterday is the end of every run: today is still filling up.
- Dates are in the network's time zone (`timeZoneSource: PUBLISHER`), and money is in the network's currency.
- Each row carries `network_code`, `date`, every dimension and every metric under its API name in lower case (`AD_UNIT_NAME` becomes `ad_unit_name`), and `row_key`, a JSON list of the network, the date and the dimension values. The raw model merges on `row_key`, so a day fetched again replaces its rows instead of adding them twice.

## Set up a service account

1. In the [Google Cloud console](https://console.cloud.google.com), pick or create a project and enable the **Google Ad Manager API** (APIs & Services, Library).
2. Under IAM & Admin, Service Accounts, create a service account. It needs no Cloud roles. On its **Keys** tab, add a key of type JSON and save the file somewhere outside the project's version control.
3. In Ad Manager, open **Admin**, **Global settings**, and turn on **API access**. Under **Service account user**, add the service account by its email address (`name@project.iam.gserviceaccount.com`) and give it a role that may run reports. Do this in every network the blueprint should read.
4. Put the key file's path in `.env`:

```bash
GAM_KEY_FILE=/path/to/service-account.json
```

The runtime signs a token with the key for the scope `https://www.googleapis.com/auth/admanager` and renews it itself.

## Quick start

```bash
mkdir gam && cd gam
ondatrasql init
```

Copy the two files below into the project, keeping their paths, and add `GAM_KEY_FILE` to `.env` as above. Set `@incremental_initial` to the first day you want, then run:

```bash
ondatrasql run
ondatrasql sql "SELECT date, SUM(ad_server_impressions) FROM raw.gam_report GROUP BY date ORDER BY date DESC LIMIT 7"
```

Run it again tomorrow, and it fetches the last three days again plus the new one.

### lib/google_ad_manager.star

{{< bp-file "lib/google_ad_manager.star" >}}

### models/raw/gam_report.sql

{{< bp-file "models/raw/gam_report.sql" >}}

## Customisation

- **Networks:** put the codes in the first argument, comma separated (`'1234,5678'`), to read only those. The network code is in the Ad Manager URL and under Admin, Global settings.
- **Dimensions and metrics:** list their [API names](https://developers.google.com/ad-manager/api/beta/reference/rest/v1/networks.reports#dimension), for example `ORDER_NAME,LINE_ITEM_NAME` or `AD_EXCHANGE_IMPRESSIONS`. Then add one cast per name to the model, in lower case: `line_item_name::VARCHAR AS line_item_name`. The API sends text, integers, decimals and booleans; cast each column to match. Every dimension is part of `row_key`.
- **Lookback:** the last argument is how many days before the newest loaded date each run fetches again. `0` fetches only from the newest loaded day on.
- **History:** `@incremental_initial` is the first day of the first run, which asks for the whole range in one report per network.

## Notes

- A row that disappears from Ad Manager after you loaded it (a dimension combination whose numbers were revised away) stays in the table: the merge replaces and adds rows, it does not delete them.
- Changing the dimensions changes `row_key`. Rows loaded with the old key stay next to the new ones, so start over from `@incremental_initial` after such a change (`ondatrasql run` rebuilds a model whose SQL changed).
- Renaming the model (or its target) gives it a new report name, so a new report is created and the old one stays in the network, unused. Do not give a report of your own the name `ondatrasql <model>`: the blueprint would take it over and change its definition.
- A report run that fails stops the run with the error Ad Manager gave, and nothing is loaded.
