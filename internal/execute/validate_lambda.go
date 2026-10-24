// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package execute

import (
	"fmt"
	"strings"

	"github.com/ondatra-labs/ondatrasql/internal/duckast"
)

// lambdaFunctions are the DuckDB functions that take a lambda argument.
// TestLambdaFunctions_MatchDuckDB pins the list against the functions
// duckdb_functions() reports with a LAMBDA parameter.
var lambdaFunctions = map[string]bool{
	"apply": true, "array_apply": true, "array_filter": true, "array_reduce": true,
	"array_transform": true, "filter": true, "invoke": true, "list_apply": true,
	"list_filter": true, "list_reduce": true, "list_transform": true, "reduce": true,
}

// validateNoArrowLambda rejects a lambda written with the single arrow,
// `x -> x + 1`. json_serialize_sql accepts it, so it passes every check that
// only parses, and DuckDB 2.0 fails it when it binds the query: "Deprecated
// lambda arrow (->) detected". The fix is `lambda x: x + 1`.
//
// The parser cannot tell the arrow from the JSON operator `j -> '$.a'`:
// both serialize as a LAMBDA with syntax_type SINGLE_ARROW, and the binder
// decides. A lambda is what it binds to in an argument of a lambda-taking
// function and in COLUMNS(...), so those are the places checked; anywhere
// else the arrow is JSON extraction.
func validateNoArrowLambda(ast *duckast.AST) error {
	var found error
	isArrow := func(n *duckast.Node) bool {
		return n.Class() == "LAMBDA" && n.String("syntax_type") == "SINGLE_ARROW"
	}
	ast.Walk(func(n *duckast.Node) bool {
		if found != nil {
			return false
		}
		switch {
		case n.Class() == "FUNCTION" && lambdaFunctions[strings.ToLower(n.FunctionName())]:
			for _, arg := range n.Children() {
				if isArrow(arg) {
					found = fmt.Errorf("%s(...) has a lambda written as `x -> ...`, which DuckDB 2.0 rejects when the query runs; write it as `lambda x: ...`", n.FunctionName())
				}
			}
		case n.Class() == "STAR" && n.Bool("columns") && isArrow(n.Field("expr")):
			found = fmt.Errorf("COLUMNS(...) has a lambda written as `c -> ...`, which DuckDB 2.0 rejects when the query runs; write it as `lambda c: ...`")
		}
		return found == nil
	})
	return found
}
