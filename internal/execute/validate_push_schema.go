// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package execute

import (
	"fmt"

	"github.com/ondatra-labs/ondatrasql/internal/duckast"
)

// validateStrictPushSchema enforces the strict-schema contract on @push
// models. Activated by the `@push` directive (independently of @fetch).
//
// Push is asymmetric to fetch:
//   - For @fetch, the API is the schema authority. The model is a
//     passthrough projection — JOIN, WHERE, GROUP BY, derived expressions
//     are forbidden because they would mask the API shape.
//   - For @push, the SQL IS the shape authority. The model produces the
//     shape that gets sent to the API. JOIN/WHERE/GROUP BY/DISTINCT/
//     ORDER BY/CTEs/subqueries/derived expressions are all legitimate
//     shape construction.
//
// What strict-push enforces is the **interface to the blueprint** — the
// outer projection's columns and their types. Hence the validator only
// inspects the outermost SELECT's projection list, not CTEs, subqueries,
// or set-op branches.
//
// Rules (outer projection only):
//   - SELECT * is rejected.
//   - Outermost node of every projection must be CAST.
//   - Every projection must carry an explicit `AS alias` — the alias is
//     the field name sent to the API, so it cannot be implicit.
//   - Lib calls in FROM are rejected (that is a @fetch model — push
//     should read from a materialized table or another model).
//
// What is NOT enforced for @push (deliberately): CAST argument can be
// any expression (CONCAT, CASE, function call, arithmetic), JOIN, WHERE,
// GROUP BY, aggregates, DISTINCT, ORDER BY, CTEs, subqueries — all
// allowed. LIMIT/OFFSET is rejected by the cross-cutting validator.
func validateStrictPushSchema(ast *duckast.AST, hasLibCalls bool) error {
	if ast == nil {
		return nil
	}
	stmts := ast.Statements()
	if len(stmts) == 0 {
		return nil
	}
	root := stmts[0]

	// Lib calls in FROM are a @fetch concern, not @push. A user who has
	// `@push: foo` and a lib call in FROM has crossed contract layers.
	// (Note: phase 5's relationship rule already requires `@fetch` for
	// any model with lib calls. The check here is the inverse: a model
	// declared as `@push` cannot also be `@fetch`. Phase 3's parser
	// rejects @fetch + @push, so a lib call being present on a @push
	// model means lib detection found a registered lib in FROM but
	// neither @fetch nor @push was set... which phase 5 already
	// rejected. This belt-and-braces check makes the error attributable
	// to @push rather than the relationship rule.)
	if hasLibCalls {
		return fmt.Errorf(
			"@push models cannot have lib calls in FROM — that would be a @fetch model. Use a downstream @push model that reads from the @fetch-materialized table",
		)
	}

	// For SET_OPERATION_NODE at the top level (UNION/INTERSECT/EXCEPT),
	// we still enforce projection rules on the leftmost branch — the
	// output schema is determined by the leftmost SELECT.
	if root.IsSetOpNode() {
		left := root.SetOpLeft()
		if !left.IsNil() {
			return validateSelectProjections(left, false /* fetchMode */)
		}
		return nil
	}

	if root.IsSelectNode() {
		return validateSelectProjections(root, false /* fetchMode */)
	}
	return nil
}
