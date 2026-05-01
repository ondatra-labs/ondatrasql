// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package execute

import (
	"fmt"

	"github.com/ondatra-labs/ondatrasql/internal/duckast"
)

// validateNoLimitOffset rejects LIMIT and OFFSET anywhere in a pipeline
// model's SQL — top-level, CTE bodies, set-op branches, and subqueries.
//
// Pipeline models materialize a deterministic table shape; LIMIT/OFFSET
// turn that into "the first N rows in undefined order", which produces
// silently non-deterministic output and a mismatch between the model name
// and what the model actually contains. Sampling for development belongs
// in `ondatrasql query --limit N`; top-N-per-group belongs in
// `ROW_NUMBER() OVER (...) ... WHERE rn <= N`.
//
// The rule is uniform across `@kind`, `@fetch`, `@push`, and other
// directives — no pipeline model has a legitimate use for LIMIT/OFFSET
// at materialize time.
//
// Returns nil when the AST is nil (parse failed upstream — DuckDB will
// surface the parse error when the model actually runs).
//
// Implementation note: DuckDB serializes LIMIT and OFFSET as a single
// LIMIT_MODIFIER node with `limit` and `offset` expression fields, both
// optional. OFFSET-without-LIMIT still emits a LIMIT_MODIFIER (with
// `limit: null`), so the walker checks the expression fields to report
// the right keyword in the error message.
func validateNoLimitOffset(ast *duckast.AST) error {
	if ast == nil {
		return nil
	}
	var found string
	ast.Walk(func(n *duckast.Node) bool {
		if found != "" {
			return false
		}
		if n.NodeType() != "LIMIT_MODIFIER" {
			return true
		}
		switch {
		case !n.LimitExpr().IsNil():
			found = "LIMIT"
		case !n.OffsetExpr().IsNil():
			found = "OFFSET"
		default:
			// Empty modifier — nothing to reject.
			return true
		}
		return false
	})
	if found != "" {
		return fmt.Errorf(
			"%s is not allowed in pipeline models — for sampling use 'ondatrasql query --limit N'; for top-N-per-group use ROW_NUMBER() OVER (...) ... WHERE rn <= N",
			found,
		)
	}
	return nil
}
