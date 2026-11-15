---
title: Peppol Directory
description: Who can receive Peppol e-invoices in the countries you list, from the public Peppol Directory, plus the Peppol participant identifier scheme code list.
vendor: OpenPeppol
tags:
  - e-invoicing
  - peppol
  - public-sector
  - public-api
auth_method: none
source_type: fetch
model_kind: table
args:
  - dataset
  - countries
  - codelist_url
min_version: v0.47.1
---
Fetches the business entities registered as Peppol receivers from the [Peppol Directory](https://directory.peppol.eu), the public register of who can receive e-invoices and other documents over Peppol, together with the participant identifier scheme code list from the [Peppol code lists](https://docs.peppol.eu/edelivery/codelists/). Both are public, so the blueprint needs no key.

A raw model keeps one row per participant and business entity, with the document types the participant can receive. A second raw model keeps the identifier schemes (the ICD codes such as `0007`, `0088` or `0192`), and a staging model names each participant's scheme.

## What it does

- `FROM peppol('participants', 'NO,SE/0007:2021')` searches the directory country by country. An entry is a country code, which fetches every receiver in that country, or a country with a participant prefix after a slash, which fetches only the receivers whose identifier starts with it. `SE/0007:2021` is the Swedish organisation numbers 202100-xxxx: the state agencies.
- The search API only lets one query page so far into its results. A query with more hits than that is split instead of paged: a whole country into one query per identifier scheme from the code list, a prefix into one query per next character, as deep as it takes. Only the queries small enough to page through return rows, so a large country costs many requests (see Notes).
- Each split query searches its prefix and keeps only the participants whose identifier starts with it, so no participant is fetched twice. When a country (or prefix) is done, the number of rows is compared with the count the directory reported for it, and a difference is printed as a warning.
- `FROM peppol('schemes')` reads the code list page, picks the newest "Participant identifier schemes" JSON it links, and returns one row per scheme, deprecated and removed ones included, since old registrations still use them. Pass a code list URL as the third argument to pin one release: `peppol('schemes', '', 'https://docs.peppol.eu/edelivery/codelists/…json')`.
- Both raw models are `table` models: every run replaces them with the directory's and the code list's state that day.

## Quick start

```bash
mkdir peppol && cd peppol
ondatrasql init
```

Copy the four files below into the project, keeping their paths, then run:

```bash
ondatrasql run
ondatrasql sql "SELECT scheme_id, name, doc_type_count FROM staging.peppol_receivers ORDER BY name LIMIT 10"
```

### lib/peppol.star

{{< bp-file "lib/peppol.star" >}}

### models/raw/peppol_participants.sql

{{< bp-file "models/raw/peppol_participants.sql" >}}

### models/raw/peppol_schemes.sql

{{< bp-file "models/raw/peppol_schemes.sql" >}}

### models/staging/peppol_receivers.sql

{{< bp-file "models/staging/peppol_receivers.sql" >}}

## Customisation

- **Countries:** change the list in `FROM peppol('participants', '…')`, for example `'NO,DK,BE'`. Use the two-letter country codes the directory uses. A prefix after a slash narrows a country to one scheme or one range of identifiers: `'NO/0192:'` is Norwegian organisation numbers only, `'BE/0208:'` Belgian enterprise numbers.
- **Document types:** `doc_types` lists the document type identifiers each participant has registered, such as `urn:oasis:names:specification:ubl:schema:xsd:Invoice-2::Invoice##urn:cen.eu:en16931:2017#compliant#urn:fdc:peppol.eu:2017:poacc:billing:3.0::2.1`. Filter on them in staging, for example `list_contains(doc_types, '…')`, to keep only those who receive a given invoice format.
- **One code list release:** pass its JSON URL as the third argument of `peppol('schemes', '', '…')` instead of following the newest one.

## Notes

- **What the data is not.** The directory is a search index of the business cards that service providers publish for their customers. Its own `additionalInfo` field says it is no proof of SMP registration: whether a participant can be reached right now is decided by the SMP lookup in the Peppol network, which this blueprint does not do. Publishing a business card is up to the participant's service provider, so a participant can be reachable and missing here. Names, countries and registration dates are what the provider entered; `reg_date` is the date the business card was registered, not when the company joined Peppol.
- **Rows are entities, not companies.** A participant can have several business entities, in several countries; the model keeps those in the countries you list, one row each. Count distinct `participant_id` for receivers.
- **Volume.** A whole country is large: Norway had 394,863 entries on 2026-11-15. The blueprint fetches 100 per request at one request per second, so a whole large country takes an hour or more, plus the requests that only split. Start with a prefix.
- **Paging limit.** That the search stops at 1000 results per query, that `q` matches participant identifiers by substring and combines with `country`, and that the code list JSON has the keys `iso6523`, `schemeid` and `scheme-name` are taken from the directory's and code lists' documentation, not checked against every response. The warning at the end of a run is there to catch a difference: if a country reports fewer rows than the directory counts, some participants were not reached (an identifier with a character other than a letter or digit, a scheme missing from the code list, or an identifier that is itself exactly a prefix the run had to split, such as `0007:2`). A country with over a thousand matches is also read over many minutes, so a business card added or removed meanwhile shows up as a difference too.
- **Politeness.** Requests are limited to one per second. The directory has answered `503` for minutes at a time, so `retry` with `backoff` waits about five minutes in total before a run fails. No custom `User-Agent` is sent.
