# Packages that use pgregory.net/rapid (accept -rapid.checks flag).
# Update this list when adding rapid to a new package:
#   grep -rl 'pgregory.net/rapid' internal/ --include='*_test.go' | sed 's|/[^/]*$||' | sort -u
RAPID_PKGS = ./internal/backfill ./internal/dag ./internal/duckdb \
             ./internal/execute ./internal/lineage ./internal/output \
             ./internal/parser ./internal/script ./internal/validation

# All other packages (do NOT accept -rapid.checks flag)
OTHER_PKGS = ./cmd/... ./internal/config ./internal/git \
             ./internal/sql ./internal/testutil

.PHONY: test test-short test-ci test-integration test-e2e test-bench test-all test-cover lint bugcheck-static build

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

# Mechanical bug-pattern greps. Mirrors the static layer of /bugcheck.
# Only BLOCKER-class patterns belong here (false positives expensive,
# false negatives critical). Soft / INFO patterns belong in the skill.
# Each pattern returns 0 matches when clean. Any match exits 1.
bugcheck-static:
	@fail=0; \
	check() { \
	  name="$$1"; shift; \
	  out=$$(grep -rn "$$@" 2>/dev/null); \
	  if [ -n "$$out" ]; then \
	    echo "[$$name] FAIL"; echo "$$out"; echo; fail=1; \
	  fi; \
	}; \
	check "fmt-pct-v-in-sql"       --include='*.go' -E 'fmt\.Sprintf.*%v.*(SELECT|INSERT|UPDATE|DELETE|FROM|WHERE|SET )' internal/ cmd/; \
	check "removed-lib-dicts"      --include='*.go' -E '^\s*(TABLE|SINK)\s*=' internal/parser/ internal/libregistry/; \
	check "search-path-no-escape"  --include='*.go' -E 'SET search_path.*Sprintf.*[^e]Sprintf' internal/; \
	out=$$(grep -nE 'RunPush[A-Za-z]*\(' internal/execute/push.go | grep -v httpConfigFromLib 2>/dev/null); \
	if [ -n "$$out" ]; then echo "[push-call-missing-auth] FAIL"; echo "$$out"; echo; fail=1; fi; \
	out=$$(grep -nE '\bmodel\.Kind\b|\.Kind\b' internal/execute/push_delta.go 2>/dev/null); \
	if [ -n "$$out" ]; then echo "[push-delta-kind-specific] FAIL"; echo "$$out"; echo "    createPushDelta must be kind-agnostic — all kinds use the same table_changes() query."; echo; fail=1; fi; \
	out=$$(grep -rnE "fmt\.Sprintf\([^)]*\"ATTACH '%s'" internal/ --include='*.go' --exclude='*_test.go' 2>/dev/null | grep -v EscapeSQL); \
	if [ -n "$$out" ]; then echo "[attach-no-escape] FAIL"; echo "$$out"; echo "    ATTACH 'connstr' interpolations must wrap the conn string with EscapeSQL(...)."; echo; fail=1; fi; \
	if [ $$fail -eq 1 ]; then echo "bugcheck-static: failures above"; exit 1; fi; \
	echo "bugcheck-static: clean"

build:
	go build ./cmd/ondatrasql/
