---
description: Reference for OndatraSQL validation primitives. Constraints block bad rows before write; audits abort the materialize transaction; warnings surface issues without stopping.
draft: false
title: Validation
weight: 30
---
Three validation stages with distinct fail-modes.

- [Constraints](/reference/validation/constraints/) — row-level checks evaluated before write; failure blocks the model
- [Audits](/reference/validation/audits/) — dataset-level checks inside the materialize transaction; failure aborts atomically
- [Warnings](/reference/validation/warnings/) — non-blocking observations recorded in metadata
