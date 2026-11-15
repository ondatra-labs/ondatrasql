---
description: How OndatraSQL retires a feature, warning first, then failing, then removing it, and the list of features on their way out.
draft: false
title: Deprecations
weight: 5
---
OndatraSQL is on 0.x and releases often. Features still change, but none disappears without warning: a directive, a lib function or module member, a CLI flag or command, or a config setting is first deprecated, and only removed six minor versions later.

## Policy

A feature deprecated in version `0.N` goes through three phases:

| Versions | Phase | What happens |
|---|---|---|
| `0.N` to `0.N+2` | Warning | The old form still works. `run` and every other command print `WARN deprecated: <file>:<line>: ...` once per run, naming the replacement; `validate` reports a `deprecation.warn` finding, which fails only with `--strict`. |
| `0.N+3` to `0.N+5` | Error | The old form fails with the same message, naming the replacement. `validate` reports `deprecation.error` (BLOCKER). |
| `0.N+6` | Removed | The code is gone. The entry below stays as the record. |

The phase is derived from the running version, so upgrading is the only thing that moves a project from warning to error. To find out what a project uses before upgrading, run `ondatrasql validate --strict` with the current version: every deprecated form shows up as a finding with its replacement.

A project can refuse binaries that are too old for it with [`ondatrasql_min_version`](/reference/configuration/config-settings/#minimum-ondatrasql-version) in `config/settings.sql`.

## Current list

| Feature | Old form | Replacement | Deprecated | Error from | Removed in |
|---|---|---|---|---|---|
| `model.hash_comment` | A `#` or `//` comment line in a model header | `--` | 0.48 | 0.51 | 0.54 |
| `lib.fetch_mode` | `API.fetch.mode` | `"async": True`, or no key for a sync fetch | 0.48 | 0.51 | 0.54 |

### Hash comments in model headers

The model parser still accepts `#` and `//` as comment markers in a model's header, so `# @kind: merge` works like `-- @kind: merge`. It is left over from Starlark model files, which were removed in v0.30. Neither is a comment in SQL, so the header is the only place they ever worked. Write `--`:

```sql
-- @kind: merge
-- @unique_key: id
```

### The fetch mode key

A blueprint can still turn on async fetch with `"mode": "async"` in `API.fetch`; any other value meant a sync fetch. The documented key is [`async`](/reference/lib-functions/api-dict/):

```python
API = {
    "fetch": {
        "async": True,
    },
}
```

For a sync fetch, leave both keys out.

## Deprecating a feature

For contributors. A feature is deprecated in one change that:

1. Adds an entry to `internal/deprecation/registry.go`: an ID, the old form, the replacement, the minor version that ships the change as `Since`, and the anchor of its heading on this page.
2. Calls `deprecation.Use(<ID>, path, line)` at the one place that still accepts the old form, and returns the error it gives. Nothing else decides the phase.
3. Adds the row and a heading to this page.
4. Lists it under **Deprecations** in the release notes.

Tests in `internal/deprecation` hold this together: every registered ID has exactly one `Use` call until its removal version and none from then on, so removing the code early fails the build, and so does keeping it past `0.N+6`. Each entry has a row and an anchor here.

## Release notes

The notes of every release carry a `## Deprecations` section. Whoever writes the notes fills it in by hand from `internal/deprecation/registry.go` at the tag commit; nothing generates it:

```markdown
## Deprecations

- Newly deprecated: `<old form>`, use `<replacement>` (warns until 0.N+2, fails from 0.N+3)
- Now an error: `<old form>`, use `<replacement>` (removed in 0.N+6)
- Removed: `<old form>`
```

A release that changes none of these says `None.` under the heading.
