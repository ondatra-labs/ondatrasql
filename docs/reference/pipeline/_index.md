---
description: Reference for OndatraSQL's pipeline surface. CLI commands, model directives, environment variables, and run-type semantics.
draft: false
title: Pipeline
weight: 20
---
The user-facing pipeline contract: what you write in models, what you call on the command line, and how the runtime decides to execute.

- [CLI Reference](/reference/pipeline/cli/) — every command with syntax and options
- [Directives](/reference/pipeline/directives/) — `@kind`, `@unique_key`, `@incremental`, `@expose`, and friends
- [Environment Variables](/reference/pipeline/env/) — `.env` loading, ONDATRA_* runtime vars
- [Run Types](/reference/pipeline/run-types/) — skip / backfill / incremental / full / flush, what triggers each, what executes
