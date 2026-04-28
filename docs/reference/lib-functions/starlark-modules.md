---
description: Runtime modules available in Starlark blueprints. Incremental state, env, url, crypto, xml, csv, json, time.
draft: false
title: Starlark Modules
weight: 22
---
Modules and builtins available in your `lib/` functions. Starlark handles I/O — these provide the tools.

## http

HTTP client. When used inside a lib function with an API dict, `base_url`, `auth`, `headers`, `timeout`, `retry`, and `backoff` are injected automatically into all methods — including `upload`. Per-call kwargs override.

### Methods

```python
resp = http.get(url, headers=?, params=?, timeout=?, retry=?, backoff=?, auth=?, cert=?, key=?, ca=?)
resp = http.post(url, json=?, data=?, body=?, headers=?, ...)
resp = http.put(url, ...)
resp = http.patch(url, ...)
resp = http.delete(url, ...)
resp = http.upload(url, file=, field=?, filename=?, headers=?, fields=?, ...)
```

Body kwargs are mutually exclusive: `json=` (dict/list), `data=` (form), `body=` (raw string).

### Response object

| Field | Type | Description |
|---|---|---|
| `status_code` | int | HTTP status code |
| `text` | string | Raw response body |
| `ok` | bool | True if 200–299 |
| `json` | any | Parsed JSON or None |
| `headers` | dict | Response headers. If the response includes a `Link` header (RFC 5988), `headers["_links"]` is a pre-parsed dict keyed by `rel` value (e.g., `headers["_links"]["next"]` for pagination). |

### Per-call auth (overrides API dict)

```python
resp = http.get(url, auth=("user", "pass"))           # Basic
resp = http.get(url, auth=("user", "pass", "digest"))  # Digest (RFC 7616)
```

### mTLS (client certificates)

```python
resp = http.get(url, cert="client.crt", key="client.key", ca="ca.crt")
```

### File upload

```python
resp = http.upload(url, file="invoice.pdf", field="document", fields={"purpose": "ocr"})
```

`http.upload` inherits `base_url` and `auth` from the API dict:

```python
resp = http.upload("/v1/files", file="doc.pdf", fields={"purpose": "ocr"})
```

### Retry behavior

Retries on 429 and 5xx. Exponential backoff with jitter. Respects `Retry-After` headers. Configure via API dict:

```python
API = {"retry": 3, "backoff": 2}  # 3 retries, 2s/4s/8s backoff
```

## env

Access environment variables from `.env` or shell.

```python
value = env.get("API_KEY")
value = env.get("API_KEY", default="fallback")
env.set("KEY", "value")  # set for current session
```

## json

JSON encoding and decoding (Starlark standard library).

```python
s = json.encode({"name": "Alice", "age": 30})
data = json.decode('{"name": "Alice", "age": 30}')
```

## time

Date and time operations (Starlark standard library).

```python
now = time.now()
t = time.parse_time("2024-03-15T10:30:00Z")
t.year, t.month, t.day
t.unix       # Unix timestamp (seconds)
t.format("2006-01-02")  # Go layout format

tomorrow = t + time.parse_duration("24h")
```

## xml

Parse and encode XML.

```python
data = xml.decode('<user><name>Alice</name></user>')
# {"user": {"name": "Alice"}}

output = xml.encode({"user": {"name": "Alice"}})
```

XML attributes are prefixed with `@`:

```python
data = xml.decode('<item id="42"><name>Widget</name></item>')
# {"item": {"@id": "42", "name": "Widget"}}
```

## csv

Parse and encode CSV.

```python
rows = csv.decode("name,age\nAlice,30\nBob,25")
# [{"name": "Alice", "age": "30"}, {"name": "Bob", "age": "25"}]

rows = csv.decode(data, delimiter="\t", header=False)  # returns list of lists

output = csv.encode([{"name": "Alice", "age": "30"}])
```

## DuckDB-backed builtins

These run DuckDB queries under the hood. Blueprints never write SQL directly — these builtins expose common operations.

### File operations

```python
files = glob("data/*.pdf")              # file paths matching pattern
hash = md5_file("data/invoice.pdf")     # MD5 hash of file contents
text = read_text("data/config.json")    # read text file
data = read_blob("data/image.png")      # read binary file
exists = file_exists("data/file.pdf")   # check if file exists
```

### Hashing

```python
h = md5("some string")                  # MD5 hash
h = sha256("some string")               # SHA-256 hash
```

### Identifiers

```python
id = uuid()                              # generate UUIDv4
```

### Key-value lookup

```python
known = lookup(
    table="raw.invoices",
    key="source_file",
    value="source_hash",
    where=["invoice-001.pdf", "invoice-002.pdf"],
)
# → {"invoice-001.pdf": "abc123", "invoice-002.pdf": "def456"}
```

Reads from the target table's last committed snapshot. Missing keys return `None` via `known.get(k)`. If the table doesn't exist (first run), returns empty dict.

## Go-native builtins

### Cryptographic operations

```python
sig = hmac_sha256("secret_key", "message")   # HMAC-SHA256 signature
encoded = base64_encode("data")               # Base64 encoding
decoded = base64_decode(encoded)              # Base64 decoding
```

## Control flow

```python
abort()                        # clean exit, 0 rows, no error
fail("something went wrong")  # stop pipeline with error
sleep(1.5)                     # pause (prefer rate_limit in API dict)
print("debug info")           # log to stderr (secrets auto-redacted)
```

## Secret redaction

Secrets from `.env` are automatically removed from `print()` output and error messages:

```
Bearer eyJhbG...  →  Bearer [REDACTED]
token=abc123      →  token=[REDACTED]
```

## Incremental state

Incremental state is passed as kwargs to `fetch()`, not as a module. Declare the kwargs you need:

```python
def fetch(page, is_backfill=True, last_value="", initial_value=""):
    if is_backfill:
        start = initial_value
    else:
        start = _next_day(last_value)
```

| Kwarg | Type | Description |
|---|---|---|
| `is_backfill` | bool | `True` on first run or when SQL changed |
| `last_value` | string | `MAX(cursor_column)` from previous run |
| `initial_value` | string | Value from `@incremental_initial` |
| `last_run` | string | Timestamp of last successful run |
| `cursor` | string | Column name from `@incremental` |

See [Fetch Contract](/reference/lib-functions/fetch-contract/) for the complete spec.