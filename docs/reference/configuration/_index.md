---
description: Reference for OndatraSQL configuration files. Catalog, extensions, macros, secrets, settings, sources, and variables.
draft: false
title: Configuration
weight: 40
---
SQL config files loaded at startup, each owning a piece of the runtime.

- [catalog.sql](/reference/configuration/config-catalog/) — DuckLake catalog attachment (required)
- [extensions.sql](/reference/configuration/config-extensions/) — load DuckDB extensions before execution
- [secrets.sql](/reference/configuration/config-secrets/) — DuckDB secrets for cloud storage, databases, APIs
- [settings.sql](/reference/configuration/config-settings/) — memory, parallelism, spill behavior
- [sources.sql](/reference/configuration/config-sources/) — attach external databases (Postgres, MySQL, ...)
- [macros/](/reference/configuration/config-macros/) — reusable SQL macros for validation, helpers, masking
- [variables/](/reference/configuration/config-variables/) — constant/global/per-model variables
