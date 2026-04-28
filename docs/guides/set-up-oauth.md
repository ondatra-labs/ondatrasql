---
date: "2026-04-20"
description: How to authenticate with OAuth providers for API ingestion and outbound sync. One-time setup, automatic token refresh.
draft: false
title: Set Up OAuth Authentication
weight: 6
---
Authenticate once. The runtime refreshes tokens automatically on every run.

## 1. Check available providers

```bash
ondatrasql auth
```

## 2. Authenticate

```bash
ondatrasql auth hubspot
```

Your browser opens. Approve access. Done — the token is saved to your project.

You only do this once per provider per project.

## 3. Use in your lib function

```python
API = {
    "base_url": "https://api.hubapi.com",
    "auth": {"provider": "hubspot"},
    "fetch": {"args": ["object_type"]},
}

def fetch(object_type, page):
    resp = http.get("/crm/v3/objects/" + object_type)
    return {"rows": resp.json["results"], "next": None}
```

That's it. Auth headers are injected automatically into every `http.*` call.

## 4. Google service accounts

Use a JSON key file. The path is resolved from `.env` via the `{"env": "..."}` pattern:

```python
API = {
    "base_url": "https://admanager.googleapis.com",
    "auth": {
        "service_account": {"env": "GAM_KEY_FILE"},
        "scope": "https://www.googleapis.com/auth/admanager",
    },
}
```

Add to `.env`:

```bash
GAM_KEY_FILE=service-account.json
```

No `ondatrasql auth` needed. The runtime handles JWT signing and token refresh automatically.

## 5. Bring your own OAuth client

Add all required variables to `.env`:

```bash
HUBSPOT_CLIENT_ID=your-client-id
HUBSPOT_CLIENT_SECRET=your-client-secret
HUBSPOT_AUTH_URL=https://app.hubspot.com/oauth/authorize
HUBSPOT_TOKEN_URL=https://api.hubapi.com/oauth/v1/token
HUBSPOT_SCOPE=contacts
```

Then run `ondatrasql auth hubspot`. Tokens are exchanged directly with the provider.

## Token storage

Tokens are saved in `.ondatra/tokens/<provider>.json`. Add `.ondatra/tokens/` to your `.gitignore`.

## Troubleshooting

- **"Token expired"** — run `ondatrasql auth <provider>` again.
- **"missing .env variables for \<provider\>"** — add `<PROVIDER>_CLIENT_ID` and `<PROVIDER>_CLIENT_SECRET` to `.env`.
- **"not in an ondatrasql project"** — run from inside a project directory.

## Reference

- [API Dict auth patterns](/reference/lib-functions/api-dict/#auth-patterns) — all auth options
- [CLI auth command](/reference/pipeline/cli/) — command syntax