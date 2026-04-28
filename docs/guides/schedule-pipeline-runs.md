---
description: Install an OS-native scheduler for automated pipeline runs. One command sets up systemd on Linux or launchd on macOS.
draft: false
title: Schedule Pipeline Runs
weight: 10
---
Install an OS-native scheduler with one command.

## 1. Install

```bash
ondatrasql schedule "*/5 * * * *"
```

Standard 5-field cron syntax: `minute hour day month weekday`. On macOS (launchd), ranges (`1-5`) and lists (`1,3,5`) are not supported — use simple values or `*` with step (`*/5`).

| Pattern | Description |
|---|---|
| `*/5 * * * *` | Every 5 minutes |
| `0 * * * *` | Every hour |
| `0 9 * * *` | Daily at 09:00 |
| `0 0 * * 0` | Weekly on Sunday |
| `0 22 * * 1-5` | Weekdays at 22:00 (Linux only — macOS does not support ranges) |

## 2. Verify

```bash
ondatrasql schedule
```

```
Schedule for "myproject"
  Backend:  systemd
  Unit:     ondatrasql-3f2a91b4.timer
  Cron:     */5 * * * *  (every 5 minutes)
  Status:   active
  Last run: Tue 2026-04-08 14:00:01 UTC
  Next run: Tue 2026-04-08 14:05:00
```

## 3. Remove

```bash
ondatrasql schedule remove
```

## Platform Details

| OS | Backend | Files |
|---|---|---|
| Linux | systemd user timer | `~/.config/systemd/user/ondatrasql-<id>.*` |
| macOS | launchd plist | `~/Library/LaunchAgents/sh.ondatra.<id>.plist` |
| Windows | [WSL2](https://learn.microsoft.com/en-us/windows/wsl/install) | Use systemd inside WSL |

Logs: `journalctl --user -u ondatrasql-<id>` (Linux) or `~/Library/LaunchAgents/sh.ondatra.<id>.log` (macOS).

## Project Identity

Each project gets a stable ID in `.ondatra/project-id` (generated on first install). Commit this file to git.
