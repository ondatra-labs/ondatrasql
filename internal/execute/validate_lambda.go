// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package execute

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/ondatra-labs/ondatrasql/internal/duckast"
)

// lambdaArgIndex maps each DuckDB function that takes a lambda to the
// position of its lambda argument. TestLambdaFunctions_MatchDuckDB pins it
// against the LAMBDA parameters duckdb_functions() reports.
var lambdaArgIndex = map[string]int{
	"apply": 1, "array_apply": 1, "array_filter": 1, "array_reduce": 1,
	"array_transform": 1, "filter": 1, "invoke": 0, "list_apply": 1,
	"list_filter": 1, "list_reduce": 1, "list_transform": 1, "reduce": 1,
}

// ArrowLambdaError reports a lambda written `x -> ...`. validate maps it to
// the parser.lambda_arrow rule.
type ArrowLambdaError struct {
	// Function is the lambda-taking function, or "COLUMNS".
	Function string
}

func (e *ArrowLambdaError) Error() string {
	param := "x"
	if e.Function == "COLUMNS" {
		param = "c"
	}
	return fmt.Sprintf("%s(...) has a lambda written as `%s -> ...`, which DuckDB 2.0 rejects when the query runs; write it as `lambda %s: ...`",
		e.Function, param, param)
}

// validateNoArrowLambda rejects a lambda written with the single arrow,
// `x -> x + 1`. json_serialize_sql accepts it, so it passes every check that
// only parses, and DuckDB 2.0 fails it when it binds the query: "Deprecated
// lambda arrow (->) detected". The fix is `lambda x: x + 1`.
//
// The parser cannot tell the arrow from the JSON operator `j -> '$.a'`:
// both serialize as a LAMBDA with syntax_type SINGLE_ARROW, and the binder
// decides by position. The arrow is a lambda only in the lambda argument of
// a lambda-taking function and in COLUMNS(...); in any other argument —
// list_transform's list, list_reduce's initial value — it is JSON.
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
		case n.Class() == "FUNCTION":
			at, ok := lambdaArgIndex[strings.ToLower(n.FunctionName())]
			if !ok {
				break
			}
			if args := n.Children(); at < len(args) && isArrow(args[at]) {
				found = &ArrowLambdaError{Function: n.FunctionName()}
			}
		case n.Class() == "STAR" && n.Bool("columns") && isArrow(n.Field("expr")):
			found = &ArrowLambdaError{Function: "COLUMNS"}
		}
		return found == nil
	})
	return found
}

// lambdaSyntaxRe matches a SET or RESET of lambda_syntax.
var lambdaSyntaxRe = regexp.MustCompile(`(?is)\b(?:(reset)\s+(?:(?:global|session|local)\s+)?lambda_syntax\b|lambda_syntax\s*(?:=|\bto\b)\s*'([^']*)')`)

// sqlCommentRe matches SQL line and block comments.
var sqlCommentRe = regexp.MustCompile(`(?s)--[^\n]*|/\*.*?\*/`)

// SingleArrowLambdasEnabled reports whether settings SQL (config/settings.sql)
// leaves `SET lambda_syntax = 'ENABLE_SINGLE_ARROW'` in effect, under which
// DuckDB 2.0 still binds `x -> ...` lambdas. The last SET or RESET wins.
func SingleArrowLambdasEnabled(settingsSQL string) bool {
	enabled := false
	for _, m := range lambdaSyntaxRe.FindAllStringSubmatch(sqlCommentRe.ReplaceAllString(settingsSQL, " "), -1) {
		enabled = m[1] == "" && strings.EqualFold(strings.TrimSpace(m[2]), "ENABLE_SINGLE_ARROW")
	}
	return enabled
}
