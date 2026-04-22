# Packages that use pgregory.net/rapid (accept -rapid.checks flag).
# Update this list when adding rapid to a new package:
#   grep -rl 'pgregory.net/rapid' internal/ --include='*_test.go' | sed 's|/[^/]*$||' | sort -u
RAPID_PKGS = ./internal/backfill ./internal/dag ./internal/duckdb \
             ./internal/execute ./internal/lineage ./internal/output \
             ./internal/parser ./internal/script ./internal/validation

# All other packages (do NOT accept -rapid.checks flag)
OTHER_PKGS = ./cmd/... ./internal/config ./internal/git \
             ./internal/sql ./internal/testutil

.PHONY: test test-short test-ci test-integration test-e2e test-bench test-all test-cover lint build

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

lint:
	go vet ./...
	staticcheck ./...

build:
	go build ./cmd/ondatrasql/
