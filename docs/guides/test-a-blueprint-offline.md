---
description: Record a blueprint's API traffic once, then replay it offline with a frozen clock so the blueprint can be tested in CI without credentials.
draft: false
title: Test a Blueprint Offline
weight: 10
---
Record the HTTP traffic of a fetch or push blueprint once, where the API keys are, and replay it anywhere else: no network, no credentials, same rows. Together with a frozen clock this makes `ondatrasql run` of a blueprint deterministic, so a test can run it on every change.

Three environment variables control it (also listed in [Environment Variables](/reference/pipeline/env/#testing-variables)):

| Variable | Value |
|---|---|
| `ONDATRA_HTTP_CASSETTE` | Directory the recordings ("cassettes") are written to and read from |
| `ONDATRA_HTTP_MODE` | `record` or `replay` |
| `ONDATRA_NOW` | RFC 3339 timestamp, e.g. `2026-03-01T08:30:00Z`; Starlark `time.now()` returns it |

With none of them set, nothing changes. Setting only one of `ONDATRA_HTTP_CASSETTE` and `ONDATRA_HTTP_MODE`, a mode other than `record` or `replay`, or an `ONDATRA_NOW` that is not RFC 3339 stops the run before any model executes.

## Record

Run the model with the real credentials in `.env`:

```bash
ONDATRA_HTTP_CASSETTE=testdata/cassettes ONDATRA_HTTP_MODE=record \
ONDATRA_NOW=2026-03-01T08:30:00Z \
ondatrasql run raw.orders
```

Every request the model's lib functions make goes to the API as usual and is also appended to a cassette. Recording a model again replaces its cassettes.

## Replay

Run the same model with `ONDATRA_HTTP_MODE=replay` and the same `ONDATRA_NOW`. The API keys need not be set:

```bash
ONDATRA_HTTP_CASSETTE=testdata/cassettes ONDATRA_HTTP_MODE=replay \
ONDATRA_NOW=2026-03-01T08:30:00Z \
ondatrasql run raw.orders
```

No request leaves the machine. Each one is answered from the cassette, and one that is not in it fails the run:

```
http replay: no recorded interaction for GET https://api.example.com/orders?page=4&since=2026-02-22 (body empty)
in testdata/cassettes/raw.orders/orders_api.json; nearest recorded: #3 GET https://api.example.com/orders?page=3&since=2026-02-22 (body empty)
```

The error names the method, the URL with credentials masked, a hash of the request body, and the closest recorded request. Re-record when the blueprint's requests change.

Replay also skips every wait that only matters against a live API: retry backoff and `Retry-After`, the push `rate_limit`, the poll interval of async fetch and async push, and `sleep()`.

## What is covered

Every HTTP request a lib function makes:

- `http.get`, `http.post`, `http.put`, `http.patch`, `http.delete` and `http.upload`, including retries and the Digest challenge;
- the token exchanges behind the API dict's `auth`: the Google `service_account` JWT exchange and the `provider` OAuth refresh.

In replay, `service_account` and `provider` auth need no key file, no stored token and no `ONDATRA_OAUTH_TOKEN_*`: the token exchange is skipped, and requests carry `Authorization: Bearer [REDACTED]`, which is never compared. A `cert`/`key`/`ca` client certificate is not read either.

Not covered: DuckDB's own HTTP (`httpfs`, `read_json('https://…')`), and anything that is not an HTTP request (`uuid()`, DuckLake snapshot times).

## Cassette files

One file per lib function per model: `<ONDATRA_HTTP_CASSETTE>/<model>/<lib>.json`, e.g. `raw.orders/orders_api.json` for a fetch and `sync.contacts/crm_push.json` for a push. A script model's own requests go to `<model>/_script.json`.

```json
{
  "version": 1,
  "model": "raw.orders",
  "lib": "orders_api",
  "now": "2026-03-01T08:30:00Z",
  "interactions": [
    {
      "request": {
        "method": "GET",
        "url": "https://api.example.com/orders?page=1&since=2026-02-22",
        "headers": {
          "Authorization": ["[REDACTED]"],
          "User-Agent": ["OndatraSQL/1.0"]
        }
      },
      "response": {
        "status": 200,
        "headers": {
          "Content-Type": ["application/json"]
        },
        "body_encoding": "json",
        "body": {
          "data": [{"id": 1, "amount": 10.5}],
          "next": 2
        }
      }
    }
  ]
}
```

Files are pretty-printed with a fixed key order (header names sorted, JSON bodies in the API's own order) so a re-recording reviews as a diff. A body is stored as JSON when it parses, else as text, else as base64 (`body_encoding`). A JSON response body is replayed compacted, so `resp.text` loses the API's whitespace; `resp.json` is the same. A request that failed without a response (connection refused, timeout) is stored as `"error"` and fails the same way in replay.

## Matching

A request matches a recorded one when these agree:

- the method;
- the URL, with query parameters in any order and credential parameters left out;
- the body: JSON compared by value (key order and whitespace ignored), form bodies in any field order, multipart bodies regardless of their random boundary, anything else byte for byte.

Headers are not compared, so it does not matter that the credentials are missing in replay. Identical requests (the same page polled twice, a retried request) are answered in the order they were recorded, each recorded response once.

## The frozen clock

`ONDATRA_NOW` makes every `time.now()` in a lib function return that instant, unchanged through the run. Record and replay with the same value. A blueprint that builds a URL or body from the clock — `since = time.now() - time.parse_duration("168h")` — then sends the same request in both, and matches. Replay with another value, or none, and that request no longer matches: the error names the URL and shows both `ONDATRA_NOW` values, since the cassette stores the one it was recorded with.

Covered: Starlark `time.now()` in fetch, push, finalize and poll functions, in modules they `load()`, and in script models. Not covered: `last_run`, which is the commit time of the model's previous DuckLake snapshot (empty on the first run of a fresh project, which is what a test should start from), and SQL's `now()` / `current_timestamp`.

## What is never written

Recording masks credentials with `[REDACTED]` before anything reaches the file:

- the `Authorization`, `Proxy-Authorization` and `Cookie` request headers, and any request header whose name contains `token`, `secret`, `password`, `key`, `auth`, `session`, `signature`, `credential` or `cookie`, including the API dict's `auth.header`;
- `Set-Cookie` in responses;
- the API dict's `auth.param` query parameter, and query parameters named `api_key`, `apikey`, `key`, `token`, `access_token`, `refresh_token`, `id_token`, `secret`, `password`, `client_secret`, `auth`, `auth_token`, `sig`, `signature` and the AWS `X-Amz-*` signature fields;
- in JSON and form bodies, the fields `client_secret`, `client_assertion`, `assertion` (the service-account JWT), `refresh_token`, `access_token`, `id_token`, `password`, `api_key`, `apikey`, `private_key`, `private_key_id` and `secret`;
- every value the run knows is a secret, wherever it appears (URL, any header, any body, raw, URL-encoded or JSON-escaped): the `auth.env` value, the basic-auth password, `http.*(auth=…)` passwords, the service account's private key and the JWT, OAuth client secrets, refresh and access tokens, the credential sent in any of the headers above, and any `env.get()` value whose variable name contains `KEY`, `TOKEN`, `SECRET`, `PASS`, `PWD`, `CREDENTIAL`, `AUTH` or `PRIVATE`. Values shorter than 4 characters (8 for credentials learned from headers) are not masked, since masking them would corrupt unrelated data.

Read a cassette before committing it all the same: a secret the run has no way to know about — one typed into the blueprint, or read with `env.get()` under a name like `SHOPIFY_STORE` — is stored as sent.

## Limits

- A push blueprint with more than one batch in flight sends them concurrently; the order of interactions in the file then varies between recordings, though each batch still matches its own request body.
- A blueprint whose requests depend on something other than the clock and the data (a random nonce, `uuid()` in a request body) cannot be replayed.
