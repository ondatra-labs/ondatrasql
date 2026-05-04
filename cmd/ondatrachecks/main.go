// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

// ondatrachecks is the project's custom go/analysis multichecker.
// It bundles project-specific linters that catch contract drift the
// general-purpose tooling (go vet, staticcheck, errcheck) doesn't:
//
//   - listenandservecheck: HTTP servers must check ErrServerClosed
//   - schemaversioncheck:  public CLI output structs need schema_version
//   - sscanfcheck:         numeric fmt.Sscanf prefer strconv parsers
//   - strconvcheck:        strconv parse error discarded with `_`
//
// Run via `make lint` (which invokes `go run ./cmd/ondatrachecks ./...`)
// or directly: `go run ./cmd/ondatrachecks ./...`. Pre-commit hook
// and CI both pick it up via the same Make target.
package main

import (
	"golang.org/x/tools/go/analysis/multichecker"

	"github.com/ondatra-labs/ondatrasql/internal/lintcheck/escapesqlcheck"
	"github.com/ondatra-labs/ondatrasql/internal/lintcheck/listenandservecheck"
	"github.com/ondatra-labs/ondatrasql/internal/lintcheck/pushauthcheck"
	"github.com/ondatra-labs/ondatrasql/internal/lintcheck/pushdeltacheck"
	"github.com/ondatra-labs/ondatrasql/internal/lintcheck/removedlibdictscheck"
	"github.com/ondatra-labs/ondatrasql/internal/lintcheck/schemaversioncheck"
	"github.com/ondatra-labs/ondatrasql/internal/lintcheck/sqlfmtcheck"
	"github.com/ondatra-labs/ondatrasql/internal/lintcheck/sscanfcheck"
	"github.com/ondatra-labs/ondatrasql/internal/lintcheck/strconvcheck"
)

func main() {
	multichecker.Main(
		escapesqlcheck.Analyzer,
		listenandservecheck.Analyzer,
		pushauthcheck.Analyzer,
		pushdeltacheck.Analyzer,
		removedlibdictscheck.Analyzer,
		schemaversioncheck.Analyzer,
		sqlfmtcheck.Analyzer,
		sscanfcheck.Analyzer,
		strconvcheck.Analyzer,
	)
}
