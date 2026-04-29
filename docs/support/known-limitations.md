---
description: Known limitations and edge cases in OndatraSQL.
draft: false
title: Known Limitations
weight: 99
---

## Sandbox and time travel

Sandbox fork has its own snapshot history. Time-travel may fail during CDC schema check — silently assumes no change. If CDC EXCEPT fails, falls back to full query. Correctness unaffected.
