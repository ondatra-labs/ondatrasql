# Contributing to OndatraSQL

Thanks for looking at OndatraSQL. This page explains where things go and how to send a change.

## Questions, bugs and ideas

- **Questions:** [Q&A in Discussions](https://github.com/ondatra-labs/ondatrasql/discussions/categories/q-a).
- **Blueprint requests:** [Discussions](https://github.com/ondatra-labs/ondatrasql/discussions). Say which API, whether it needs a login, and what you would load. Upvote the requests you want too.
- **Bugs:** open an [issue](https://github.com/ondatra-labs/ondatrasql/issues). Include `ondatrasql version`, the command you ran and its full output.

## Before you write code: the CLA

Every contribution is covered by the [Contributor License Agreement](CLA.md). Please read it first. Section 2 grants the project owner the right to license your contribution under any license, including a proprietary one. A bot asks you to sign it on your first pull request.

## Blueprints

A blueprint is a small, complete project that loads one public API into DuckLake. Blueprints live under [`blueprints/`](blueprints/) and are MIT licensed, so anyone can copy them. They are the easiest way to contribute.

1. Pick a request from Discussions, or propose your own there first.
2. Follow [Create a Blueprint](https://ondatra.sh/guides/create-a-blueprint/). [`blueprints/riksbank/`](blueprints/riksbank/) is the reference.
3. Test it offline with record and replay: [Test a Blueprint Offline](https://ondatra.sh/guides/test-a-blueprint-offline/).
4. Run `make test-blueprints` and open a pull request.

Do not commit recordings (cassettes). They hold data fetched from the API, and the provider's terms may not allow republishing it. The maintainers keep blueprint recordings in a private repository and record new blueprints themselves before merging. See [`blueprints/README.md`](blueprints/README.md).

## Code changes

You need Go (the version in [`go.mod`](go.mod)) and a C toolchain, because DuckDB is linked with cgo.

```bash
make build        # the binary
make test-short   # fast: every test compiles, heavy tests are skipped
make test         # unit tests
make lint         # vet, staticcheck and the project's own analyzers
```

Pull requests run the full integration suite in CI. Please:

- keep a change to one topic, with tests that fail without it;
- update `docs/` when behaviour, a directive, a CLI flag or JSON output changes;
- write commit messages that say what changed and why.

## Code of conduct

This project follows the [code of conduct](CODE_OF_CONDUCT.md).
