# Blueprints

Each directory here is a complete, runnable OndatraSQL mini project for one API: a lib function, the models that use it, and the page that documents it on [ondatra.sh/blueprints](https://ondatra.sh/blueprints/). The blueprints are MIT licensed ([LICENSE](LICENSE)), unlike the rest of this repository, so they can be copied into any project.

## Layout

```
blueprints/
  LICENSE              MIT, for everything under blueprints/
  README.md            this file
  _shared/config/      the local config ondatrasql init writes (no secrets.sql)
  <name>/
    index.md           the page: frontmatter and prose, code by reference
    .env.example       the environment variables the blueprint needs, values empty
    lib/               the lib function(s): <lib>.star
    models/            the models: raw/, staging/, sync/ ...
```

A blueprint directory holds exactly these four entries and nothing else: code and its page. No data fetched from an API goes here, not even public data or data from a test account: no cassettes, no expected rows, no samples. A guard test fails the build if any file under `blueprints/` is a `cassettes/` directory, an `expect.yaml`, or JSON with an `interactions` key.

### index.md

The page is a Hugo page. Its frontmatter:

| Field | Value |
|---|---|
| `title` | The page title, usually the vendor and API: `Riksbank SWEA` |
| `description` | One sentence |
| `vendor` | Who runs the API |
| `tags` | A list |
| `auth_method` | `none`, `api_key`, `basic`, `oauth` or `service_account`, as the API dict's `auth` says |
| `source_type` | `fetch` or `push`, the section the API dict has |
| `model_kind` | The `@kind` of every model that calls the lib, within the API dict's `supported_kinds` if it has them |
| `args` | The `fetch.args` (or `push.args`) of the API dict, in order; `[]` for none |

Then the prose: what it does, a quick start, customisation, notes. The code is never pasted into the page. Each file is shown by reference, so the page cannot drift from the code:

```
{{< bp-file "lib/riksbank.star" >}}
```

Every file under `lib/` and `models/` must be shown this way, and every reference must name an existing file.

### .env.example

One `NAME=` line per variable the blueprint reads (`auth.env`, basic-auth `user`/`pass`, `service_account`, `env.get()`), with an empty value and a comment saying where to get it. A blueprint that needs none has only a comment.

### _shared

`_shared/config/` is the `config/` that `ondatrasql init` writes, without `secrets.sql`. The harness copies it into each test project under the blueprint's `lib/` and `models/`, the same project a user gets after `ondatrasql init` and copying the blueprint in. A test keeps it identical to what `init` writes; when `init` changes, copy the files over from a fresh `ondatrasql init`.

## Add a blueprint

1. Create `blueprints/<name>/` with `lib/`, `models/`, `.env.example` and `index.md` as above. Copy `riksbank/` as a starting point.
2. Run `make test-blueprints`. It checks every blueprint offline, with no secrets:
   - `ondatrasql validate --strict` reports 0 BLOCKER and 0 WARN;
   - the frontmatter's `auth_method`, `source_type`, `args` and `model_kind` agree with `ondatrasql --json describe blueprint <lib>` and the models' `@kind`;
   - `.env.example` names every variable the API dict's `auth` reads, without values;
   - the page shows every code file with `bp-file`.
3. Ask a maintainer to record it (below). Until a recording exists, the replay of a new blueprint is skipped without `BLUEPRINT_TESTS_DIR` and fails with it.

## Recordings are kept privately

A blueprint is tested end to end by replaying a recording of its API traffic (see [Test a Blueprint Offline](../docs/guides/test-a-blueprint-offline.md)): the requests it made and the responses it got, plus the tables and rows that replay must produce. The maintainers keep these recordings and expected outputs in a private repository. They are never committed here, because:

- a recording is data fetched from an API. Even when the API is public, that data belongs to its provider, and its terms may not allow republishing it;
- a recording made with credentials can carry account data, and secrets the recorder has no way to know are secret (an ID typed into the blueprint, a webhook URL whose path is the credential);
- once data is in a public repository's history, it cannot be taken back.

So this repository holds the blueprints' code and a generic harness, and the harness reads the recordings from a directory you name.

## Replay a recording

```bash
make test-blueprints BLUEPRINT_TESTS_DIR=/path/to/recordings
```

For each blueprint `<name>`, the directory holds:

```
<dir>/<name>/cassettes/<model>/<lib>.json   the recorded requests and responses
<dir>/<name>/expect.yaml                    the runs to make and what each leaves behind
```

With `BLUEPRINT_TESTS_DIR` set, each blueprint is also run, in a fresh project, once per entry in `expect.yaml`. Each run uses that entry's `ONDATRA_NOW`, answers every request from the cassettes and has the blueprint's keys unset. A request that is not in the recording fails the run. For push blueprints, that includes a request whose body differs from the recorded one. After each run the harness asserts:

- every model ran without error, with the run type and rows affected listed under `models`;
- how many recorded interactions the run used (`requests`), e.g. 0 for an incremental run that is already up to date;
- every table in the project, and no other: its columns and types, its exact row count, and that each sample row is in it.

A blueprint with no `<dir>/<name>/` fails, naming it. Without `BLUEPRINT_TESTS_DIR` the replay is skipped, and the output says so for each blueprint.

"Offline" means the blueprint's API is never called. DuckDB still downloads its `ducklake` and `sqlite` extensions on a machine that has not cached them.

### expect.yaml

```yaml
runs:
  - now: "2026-11-14T12:00:00Z"     # ONDATRA_NOW, the one the run was recorded with
    requests: 2                      # recorded interactions this run uses
    models:
      raw.riksbank_observations: {run_type: backfill, rows: 18}
    tables:
      raw.riksbank_observations:
        columns:                     # "name TYPE", in table order; required in the first run
          - obs_key VARCHAR
          - date DATE
          - value DOUBLE
        rows: 18                     # exact row count, required
        sample:                      # rows that must be present; any subset of columns
          - {obs_key: "SEKEURPMI:2026-11-03", date: "2026-11-03", value: "10.9515"}
  - now: "2026-11-14T12:00:00Z"     # a second run in the same project
    requests: 0
    models:
      raw.riksbank_observations: {run_type: incremental, rows: 0}
    tables:
      raw.riksbank_observations: {rows: 18}
```

Sample values are written as DuckDB prints them cast to `VARCHAR` (`DECIMAL(10,4)` 11.026 is `"11.0260"`), with `null` for NULL. Quote them. Write `columns` as a block list, since `DECIMAL(10,4)` in a `[...]` flow list splits at the comma.

Every run replays from `cassettes/` unless it names a subdirectory with `cassettes: run2`. A later run that makes requests the first one did not make, such as a run on a later `now`, needs its own subdirectory, because recording it into `cassettes/` would replace the first run's files.

## Record (maintainers)

```bash
make record-blueprint BP=<name> BLUEPRINT_TESTS_DIR=/path/to/recordings
```

Recording calls the live API, so it needs network and every key in the blueprint's `.env.example` set in the environment. It writes only into `BLUEPRINT_TESTS_DIR/<name>/`, never into this repository:

- it runs the offline check first, then replaces `cassettes/` with the requests of the runs in the existing `expect.yaml` (without one, two runs at the current time);
- it then replays the new recording and prints the `expect.yaml` that replay produced. If there was no `expect.yaml`, the draft is written there, and you must review it before you trust it. If there was one, it is asserted, and a difference fails the run, with the draft beside it.

Before committing a recording to the private repository, read it: the recorder masks credentials it knows about, but not values it cannot know are secret (see [What is never written](../docs/guides/test-a-blueprint-offline.md#what-is-never-written)).
