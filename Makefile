# Packages that use pgregory.net/rapid (accept -rapid.checks flag).
# Update this list when adding rapid to a new package:
#   grep -rl 'pgregory.net/rapid' internal/ --include='*_test.go' | sed 's|/[^/]*$||' | sort -u
RAPID_PKGS = ./internal/backfill ./internal/dag ./internal/duckdb \
             ./internal/execute ./internal/lineage ./internal/output \
             ./internal/parser ./internal/script ./internal/validation

# All other packages (do NOT accept -rapid.checks flag)
OTHER_PKGS = ./cmd/... ./internal/config ./internal/git \
             ./internal/sql ./internal/testutil

.PHONY: test test-short test-ci test-integration test-e2e test-bench test-all test-cover lint bugcheck-static build install-tools

# Unit tests only (no integration build tag)
test:
	go test -p 1 -race -timeout 20m ./...

# Fast feedback: all tests compile (incl. integration) but heavy tests skip via testing.Short()
test-short:
	go test -p 1 -race -timeout 20m -tags integration -short ./...

# CI: full integration with reduced rapid iterations (25 instead of 100)
test-ci:
	go test -p 1 -race -timeout 20m -tags integration $(RAPID_PKGS) -rapid.checks=25
	go test -p 1 -race -timeout 20m -tags integration $(OTHER_PKGS)

test-integration:
	go test -p 1 -race -timeout 20m -tags integration ./...

test-e2e:
	go test -p 1 -race -timeout 30m -tags e2e ./...

test-bench:
	go test -p 1 -timeout 30m -tags "e2e bench" ./e2e/ -v -run Bench

test-all:
	go test -p 1 -race -timeout 30m -tags "integration e2e" ./...

test-cover:
	go test -p 1 -race -timeout 30m -tags "integration e2e" -coverprofile=coverage.out ./...
	go tool cover -func=coverage.out

# Strict gate. Runs go vet (default analyzers, which is the same set as
# `-all` since Go 1.10), staticcheck, errcheck across the whole repo,
# and the bug-pattern greps. CI and the pre-commit hook should call
# this target.
lint: bugcheck-static
	go vet ./...
	staticcheck ./...
	errcheck -exclude .errcheck-excludes.txt -ignoretests ./...
	go run ./cmd/ondatrachecks ./...

# One-shot dev tool installer. CI and contributors run this once to
# get the static-analysis tools the lint target depends on.
install-tools:
	go install honnef.co/go/tools/cmd/staticcheck@latest
	go install github.com/kisielk/errcheck@latest

# Mechanical bug-pattern greps — incubator for new patterns BEFORE
# they graduate to a proper analyzer in internal/lintcheck/.
#
# Policy: a pattern lives here only as long as it's still unstable
# (false positives still being worked out) or hasn't justified the
# analyzer effort yet. Once a grep has fired in real code at least
# once and proven its signal is clean, migrate it to a go/analysis
# Analyzer via the `add-analyzer` skill and remove it from here.
#
# Currently retired patterns (now enforced by ondatrachecks):
#   - fmt-pct-v-in-sql       → internal/lintcheck/sqlfmtcheck
#   - attach-no-escape       → internal/lintcheck/escapesqlcheck
#   - search-path-no-escape  → internal/lintcheck/escapesqlcheck
#   - push-delta-kind-specific → internal/lintcheck/pushdeltacheck
#   - push-call-missing-auth → internal/lintcheck/pushauthcheck
#   - removed-lib-dicts      → internal/lintcheck/removedlibdictscheck
bugcheck-static:
	@# ast-fixture-class-is-type: a hand-written AST fixture whose `class` is
	@# set to a `type` value. DuckDB reports these as separate fields — a cast
	@# is class "CAST", type "OPERATOR_CAST" — so a fixture using the type as
	@# the class can match a buggy extractor and keep a real bug green. This
	@# hid the lineage CAST branch never firing: every col::TYPE projection
	@# returned zero column lineage while the test passed.
	@! grep -rn '"class":"OPERATOR_' --include=*.go . \
		|| (echo "bugcheck-static: AST fixture sets class to a type value (see ast-fixture-class-is-type)"; exit 1)
	@# config-sql-expandenv: config SQL expanded with os.ExpandEnv. An unset
	@# variable silently becomes "", and in a connection string that turns
	@# `host=${PG_HOST} port=5432` into a failure about resolving "port=5432"
	@# while the real cause, PG_HOST unset, never shows. configenv.Expand does
	@# the same expansion and reports the unset names for configenv.Annotate.
	@! grep -rnE 'os\.ExpandEnv\(|os\.Expand\(.*os\.Getenv' --include=*.go internal cmd \
		| grep -v '_test\.go:' | grep -v '^internal/configenv/' | grep -v '^\S*:[0-9]*:\s*//' \
		|| (echo "bugcheck-static: config SQL expanded with os.ExpandEnv, use configenv.Expand (see config-sql-expandenv)"; exit 1)
	@# sql-lambda-arrow: a lambda written with the single arrow, `x -> x + 1`.
	@# DuckDB 2.0 rejects it at bind time ("Deprecated lambda arrow (->)
	@# detected"), so an embedded query that still uses it fails only when it
	@# runs — the sandbox scd2 diff did, in front of the user. Write
	@# `lambda x: x + 1`. The JSON operator is spelled the same, so a match
	@# needs the shape of a lambda: an argument position (after `(` or `,`, or
	@# a continued line), a parameter or parameter list, and that parameter
	@# used again before the next `,` or `)`. `j -> '$$.a'`, `j -> 0` and
	@# `j -> path_col` are JSON and do not match. perl for the
	@# back-reference, which neither BSD grep -E nor ugrep has. Tests and
	@# testdata use the arrow on purpose; ondatrasql validate covers models
	@# (parser.lambda_arrow).
	@# The scan runs under pipefail, so a failure in any stage fails the
	@# gate: without that, a missing perl left the pipeline with no output,
	@# which read as clean. grep's "no match" (exit 1) is not a failure.
	@command -v perl >/dev/null 2>&1 \
		|| { echo "bugcheck-static: perl is required for sql-lambda-arrow"; exit 1; }
	@hits=$$(set -o pipefail; \
		{ grep -rlE --include=*.sql --include=*.go -e '->' internal cmd e2e || [ $$? -eq 1 ]; } \
		| { grep -vE '_test\.go$$|/testdata/' || [ $$? -eq 1 ]; } \
		| xargs perl -ne 'next if m{^\s*(//|--)}; print "$$ARGV:$$.: $$_" if m{(?:^|[(,])\s*([A-Za-z_]\w*)\s*->(?!>)(?!\s*[\x27"])(?=[^,)]*\b\1\b)} || m{(?:^|[(,])\s*\(\s*([A-Za-z_]\w*)[^)]*\)\s*->(?=[^,)]*\b\1\b)}; close ARGV if eof') \
		|| { echo "bugcheck-static: sql-lambda-arrow scan failed"; exit 1; }; \
	if [ -n "$$hits" ]; then \
		echo "$$hits"; \
		echo "bugcheck-static: lambda written as x -> ..., use lambda x: ... (see sql-lambda-arrow)"; exit 1; \
	fi
	@# ducklake-automatic-migration: an ATTACH with AUTOMATIC_MIGRATION. DuckDB
	@# 2.0's ducklake opens a 1.5-created catalog (version 1.0) as is and keeps
	@# it at 1.0, so 1.5 can still read it; AUTOMATIC_MIGRATION upgrades it in
	@# place, after which 1.5 refuses to attach it at all. The catalog has to
	@# stay readable by 1.5.
	@! grep -rniE 'automatic_migration' --include=*.go --include=*.sql --include=*.txtar internal cmd e2e \
		| grep -vE '^\S*:[0-9]*:\s*(//|--|#)' \
		|| (echo "bugcheck-static: AUTOMATIC_MIGRATION would upgrade the catalog past 1.5 (see ducklake-automatic-migration)"; exit 1)
	@echo "bugcheck-static: clean"

build:
	go build ./cmd/ondatrasql/
