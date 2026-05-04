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
	@echo "bugcheck-static: clean (all rules migrated to internal/lintcheck/* — see ondatrachecks)"

build:
	go build ./cmd/ondatrasql/
