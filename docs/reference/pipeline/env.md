---
description: OndatraSQL loads secrets and configuration from a .env file at runtime. Use variables in SQL config files and Starlark scripts throughout your data pipeline.
draft: false
title: Environment Variables
weight: 20
---
Load secrets and config from `.env` in the project root. System environment variables override `.env`.

## Access

| Context | Syntax |
|---|---|
| SQL config files | `${VAR}` |
| Starlark scripts | `env.get("VAR")` |
| Starlark write | `env.set("VAR", "value")` |

## .env Syntax

```bash
KEY=VALUE
KEY="quoted value"
KEY='single quoted'
# comments ignored
```

## OndatraSQL Variables

| Variable | Description |
|---|---|
| `ONDATRA_KEY` | API key for managed OAuth2 via oauth2.ondatra.sh |
| `ONDATRA_OAUTH_HOST` | Override managed OAuth2 service URL |
| `EDITOR` | Editor for `ondatrasql edit` (falls back to nano, vi, vim) |

## OAuth2 Provider Variables

For local OAuth2 (when `ONDATRA_KEY` is not set). Provider name maps to env prefix: `google-sheets` becomes `GOOGLE_SHEETS_*`.

| Variable | Description |
|---|---|
| `<PREFIX>_CLIENT_ID` | OAuth2 client ID |
| `<PREFIX>_CLIENT_SECRET` | OAuth2 client secret |
| `<PREFIX>_AUTH_URL` | Authorization endpoint |
| `<PREFIX>_TOKEN_URL` | Token endpoint |
| `<PREFIX>_SCOPE` | OAuth2 scopes (required) |

## Cloud Storage

Picked up automatically by DuckDB's credential chain.

| Variable | Provider |
|---|---|
| `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION` | Amazon S3 |
| `GOOGLE_APPLICATION_CREDENTIALS` | Google Cloud Storage |
| `AZURE_STORAGE_ACCOUNT`, `AZURE_STORAGE_KEY` | Azure Blob Storage |
