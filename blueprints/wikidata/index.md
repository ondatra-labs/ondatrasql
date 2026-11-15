---
title: Wikidata SPARQL
description: Any SPARQL SELECT query against the Wikidata Query Service, loaded as a table with one row per result and a column per variable.
vendor: Wikimedia Foundation
tags:
  - open-data
  - knowledge-graph
  - sparql
  - public-api
auth_method: none
source_type: fetch
model_kind: table
args:
  - query
---
Runs a SPARQL `SELECT` query against the [Wikidata Query Service](https://query.wikidata.org) and loads the result as a table: one row per result, one column per variable. Wikidata is the open knowledge base behind Wikipedia, with over a hundred million items: companies, people, places, works, and the identifiers that link them to other datasets. The service is public, so the blueprint needs no key.

The example fetches organisations that have a Legal Entity Identifier (LEI, property P1278), with their label, country and date of inception. A staging model types it to one row per organisation and LEI.

## What it does

- `FROM wikidata('SELECT ?item ?itemLabel … WHERE { … }')` sends the query as written and reads the JSON result.
- Each result row becomes one table row. Each variable `?x` gives two columns: `x`, the value as text, and `x_qid`, the entity id (`Q42`, `P31`, `L7`) when the value is a Wikidata entity URI, and NULL otherwise. The model picks the ones it needs, so `?itemLabel` is read as `itemLabel::VARCHAR`.
- A variable that `OPTIONAL` left unbound is NULL. A literal is its lexical form, whatever its datatype or language: a date is `1998-01-01T00:00:00Z`, a number `42`. The staging model casts it.
- One query is one request. There is no paging: the query's own `LIMIT` is the size of the result, and a query without one returns everything it matches, if it finishes in time.
- A column the model reads but the query does not select fails the run, naming the query's variables, instead of loading a column of NULLs.
- The model is a `table`: every run sends the query again and replaces the rows with its answer, including an empty one.

## Quick start

```bash
mkdir wikidata && cd wikidata
ondatrasql init
```

Copy the three files below into the project, keeping their paths, then run:

```bash
ondatrasql run
ondatrasql sql "SELECT name, lei, country, inception FROM staging.lei_organisations ORDER BY lei LIMIT 5"
```

### lib/wikidata.star

{{< bp-file "lib/wikidata.star" >}}

### models/raw/organisations_with_lei.sql

{{< bp-file "models/raw/organisations_with_lei.sql" >}}

### models/staging/lei_organisations.sql

{{< bp-file "models/staging/lei_organisations.sql" >}}

## Write your own query

1. Write and run it in the [query editor](https://query.wikidata.org) first. Its examples, autocompletion (Ctrl+Space on `wdt:P…` and `wd:Q…`) and timer show what the query returns and how long it takes.
2. Paste it into `FROM wikidata('…')`. It is an SQL string, so double every single quote in it (`'` becomes `''`); SPARQL's double quotes need nothing. The usual prefixes (`wd:`, `wdt:`, `p:`, `ps:`, `pq:`, `wikibase:`, `bd:`, `rdfs:`, `schema:`) are predefined by the service.
3. List one cast per column you want, `var::VARCHAR AS name` or `var_qid::VARCHAR AS name`. A `@fetch` model only casts; parse dates and numbers, deduplicate and join in a staging model, as `lei_organisations.sql` does.

Labels come from the label service: `SERVICE wikibase:label { bd:serviceParam wikibase:language "en,mul". }` binds `?xLabel` for every `?x` you select. An entity without a label in those languages gets its QID as label.

Each value of a multi-valued property is its own row: an organisation with two countries has two rows. Aggregate it in staging, or in SPARQL with `GROUP BY` and `SAMPLE()` or `GROUP_CONCAT()`.

## Keep it inside the timeout

The service stops a query after 60 seconds and answers with an error, which fails the run. The query editor shows how long a query takes; leave a wide margin, since the same query can be slower on a busy server.

- **Always set a `LIMIT`** while you develop, and keep one in production unless you know the size of the result.
- **Limit before you label.** Put the selective pattern and its `LIMIT` in a subquery, and the `OPTIONAL`s and label service outside it, as the example does. Otherwise the service may label every match before it limits.
- **Start from the rare side.** `?item wdt:P1278 ?lei` (only entities with an LEI) is cheap; `?item wdt:P31 wd:Q5` (every human) is not.
- **Avoid `ORDER BY` and `DISTINCT` over large results**, string functions in `FILTER` over many rows, and property paths such as `wdt:P279*` over big class trees.
- A query too big for one request can often be split by a value, say one model per country (`VALUES ?country { wd:Q34 }`), each its own request.

## Notes

- Requests carry a User-Agent naming this blueprint and its repository, as [Wikimedia's policy](https://meta.wikimedia.org/wiki/User-Agent_policy) asks. Change it in the API dict if you adapt the lib into your own tool.
- The service allows each client about one minute of query time per minute and a few queries at a time. Over it, it answers `429` with `Retry-After`; `retry` waits that long and tries again, at most twice. A timed-out query is not worth many retries, since each attempt spends that quota.
- A query that is not a `SELECT` (`ASK`, `CONSTRUCT`, `DESCRIBE`) fails the run.
- The query goes in the URL of a `GET`. Very long queries (many kilobytes, say a long `VALUES` list) can exceed the URL limit; split them.
- Wikidata changes all the time, and so does a result: the same query can return other rows tomorrow. Results without `ORDER BY` come in no fixed order, so a `LIMIT` without `ORDER BY` may pick other rows each run.

## Licence

Wikidata's structured data is [CC0](https://creativecommons.org/publicdomain/zero/1.0/): free to use for any purpose, with no attribution required (see [Wikidata:Licensing](https://www.wikidata.org/wiki/Wikidata:Licensing)). This blueprint's code is MIT, like every blueprint here.
