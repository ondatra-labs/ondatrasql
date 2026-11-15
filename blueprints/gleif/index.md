---
title: GLEIF LEI records
description: Legal Entity Identifier records for the countries you list, fetched incrementally from GLEIF's public API, joined to the registration authorities their local company numbers come from.
vendor: Global Legal Entity Identifier Foundation (GLEIF)
tags:
  - companies
  - lei
  - reference-data
  - public-api
auth_method: none
source_type: fetch
model_kind: merge
args:
  - dataset
  - countries
min_version: v0.47.1
---
Fetches [Legal Entity Identifier](https://www.gleif.org/en/about-lei/introducing-the-legal-entity-identifier-lei) (LEI) records from the [GLEIF API](https://api.gleif.org/api/v1) for the countries you list, plus GLEIF's list of registration authorities. GLEIF covers about 2.7 million legal entities worldwide. Each record links an entity's LEI to its local registration number (`registered_as`, such as a Swedish organisationsnummer) and to the register that number comes from (`registered_at`, an RA code such as `RA000544`). The API is public, so the blueprint needs no key.

The raw models keep one row per LEI and one row per RA code and jurisdiction. A staging model joins them, so each LEI shows the name of its register.

## What it does

- `FROM gleif('lei-records', 'SE,NO')` fetches the LEI records whose legal address is in each listed country (ISO 3166-1 alpha-2 codes), one paged query per country.
- `FROM gleif('registration-authorities')` fetches the whole registration-authority list, about 1 100 codes.
- Both are incremental on `fetched_on`, the UTC day a run fetched a row. A run on a day that already has a fetch makes no request at all.
- The first run fetches the records last updated on or after `@incremental_initial`. A later run asks only for records whose `registration.lastUpdateDate` is on or after the last fetch day minus two days. Those two days cover updates GLEIF publishes a little after their `lastUpdateDate`.
- The raw LEI model merges on `lei`, so a record updated again replaces its older version. A record seen twice in one run is returned once.
- The RA list changes rarely. It is fetched on the first run and then again when the last fetch is a week old.
- Requests are rate limited to 50 per minute, below GLEIF's 60. The client sends no identifying `User-Agent`.

## Quick start

```bash
mkdir gleif && cd gleif
ondatrasql init
```

Copy the four files below into the project, keeping their paths, then run:

```bash
ondatrasql run
ondatrasql sql "SELECT lei, name, registered_as, register_name FROM staging.legal_entities LIMIT 5"
```

Run it again tomorrow, and it fetches only the records updated since.

### lib/gleif.star

{{< bp-file "lib/gleif.star" >}}

### models/raw/gleif_lei.sql

{{< bp-file "models/raw/gleif_lei.sql" >}}

### models/raw/gleif_ra.sql

{{< bp-file "models/raw/gleif_ra.sql" >}}

### models/staging/legal_entities.sql

{{< bp-file "models/staging/legal_entities.sql" >}}

## Customisation

- **Other countries:** change the list in `FROM gleif('lei-records', '…')`, for example `'DE,FR,NL'`. A change to the model's SQL makes the next run a backfill, which fetches every listed country again from `@incremental_initial`.
- **History:** `@incremental_initial` is the first `lastUpdateDate` day the first run asks for. The blueprint sets a recent day, so a first run is quick. For every LEI of a country, including lapsed ones that have not been updated for years, set it to `1970-01-01`. Sweden alone has about 120 000 records, about 600 requests or 12 minutes at the rate limit.
- **Columns:** the lib flattens the fields the models select. To add one, such as `entity.legalName.language` or `registration.managingLou`, add it to the row dict in `gleif_lei` and a cast for it in `gleif_lei.sql`.
- **The register's name:** `registered_at` is `RA888888` when the register has no RA code. GLEIF then names it in `entity.registeredAt.other`, which the lib does not fetch.

## Notes

- GLEIF allows 60 requests per minute. The `rate_limit` keeps below that, and `retry` with `backoff` rides out a temporary `429` or `5xx`.
- Pages hold at most 200 records. The API is reported to stop paging at 10 000 results per query. After 50 pages, the lib therefore starts the query over from the `lastUpdateDate` day it reached. If more than 10 000 records of one country share a single `lastUpdateDate` day, the run fails and says so. Use GLEIF's golden-copy files for that.
- The API pages by offset. A record that is updated while a run is paging moves within the sort order and can push another record past a page boundary. The next run asks again only for the last few days, so a record pushed past a boundary is fetched again only when it is next updated.
- `fetched_on` only moves when a run fetches rows. A run that finds no updated records leaves it where it was, so the next run asks again from the same day, even on the same day. With a few large countries this does not happen, because GLEIF updates records in them every day.
- An LEI whose legal address moves to a country you do not list keeps its old row. An RA that GLEIF drops from its list keeps its row too, with an older `fetched_on`.
- GLEIF publishes the LEI data under [CC0 1.0](https://www.gleif.org/en/meta/lei-data-terms-of-use): free to use and republish, with no attribution required.
