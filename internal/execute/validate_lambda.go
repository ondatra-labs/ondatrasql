// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package execute

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/ondatra-labs/ondatrasql/internal/configenv"
	"github.com/ondatra-labs/ondatrasql/internal/duckast"
	"github.com/ondatra-labs/ondatrasql/internal/duckdb"
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
	// Function is the lambda-taking function, "COLUMNS", or "" for a lambda
	// in a macro body.
	Function string
}

func (e *ArrowLambdaError) Error() string {
	if e.Function == "" {
		return "a macro has a lambda written as `x -> ...`, which DuckDB 2.0 rejects when the macro is created, so every run fails; write it as `lambda x: ...`"
	}
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
// Every arrow lambda in the model is reported, not just the first.
func validateNoArrowLambda(ast *duckast.AST) []error {
	var found []error
	isArrow := func(n *duckast.Node) bool {
		return n.Class() == "LAMBDA" && n.String("syntax_type") == "SINGLE_ARROW"
	}
	var visit func(n *duckast.Node) bool
	visit = func(n *duckast.Node) bool {
		switch {
		case n.Class() == "FUNCTION":
			at, ok := lambdaArgIndex[strings.ToLower(n.FunctionName())]
			if !ok {
				break
			}
			if args := n.Children(); at < len(args) && isArrow(args[at]) {
				found = append(found, &ArrowLambdaError{Function: n.FunctionName()})
			}
		case n.Class() == "STAR" && n.Bool("columns"):
			// DuckDB 2.0 serializes COLUMNS(c -> ...) as a STAR whose expr
			// is list_filter(*, <lambda>). Report it as COLUMNS, then look
			// only inside the lambda, so the list_filter is not reported a
			// second time.
			expr := n.Field("expr")
			lambda := expr
			if expr.Class() == "FUNCTION" && strings.EqualFold(expr.FunctionName(), "list_filter") {
				if args := expr.Children(); len(args) == 2 {
					lambda = args[1]
				}
			}
			if isArrow(lambda) {
				found = append(found, &ArrowLambdaError{Function: "COLUMNS"})
			}
			if lambda != expr {
				lambda.Walk(func(inner *duckast.Node) bool { return visit(inner) })
				return false
			}
		}
		return true
	}
	ast.Walk(visit)
	return found
}

// ProjectLambdas is what a project's config says about arrow lambdas.
type ProjectLambdas struct {
	// SingleArrow is set when config/settings.sql leaves lambda_syntax at
	// ENABLE_SINGLE_ARROW, under which DuckDB 2.0 still binds `x -> ...`.
	SingleArrow bool
	// Macros maps each file in config/macros/ that DuckDB rejects for an
	// arrow lambda to the error. DuckDB 2.0 binds a macro body when the
	// macro is created, so such a file stops every run at session start.
	Macros map[string]error
}

// catalogAliasRe reads the DuckLake alias from catalog.sql's ATTACH.
var catalogAliasRe = regexp.MustCompile(`(?is)\bATTACH\b.*?\bAS\s+"?([A-Za-z_][A-Za-z0-9_]*)"?`)

// InspectProjectLambdas answers both questions the way the runtime would:
// it runs config/settings.sql, env-expanded, in a scratch DuckDB session and
// reads lambda_syntax back — so every spelling DuckDB accepts counts and a
// commented-out or quoted-away SET does not — and then loads config/macros/
// as InitWithCatalog does. The lake is not attached; an empty catalog under
// its alias stands in for {{catalog}} references.
func InspectProjectLambdas(configPath string) (ProjectLambdas, error) {
	var out ProjectLambdas
	sess, err := duckdb.NewSession(":memory:")
	if err != nil {
		return out, err
	}
	defer func() { _ = sess.Close() }() // scratch session

	if content, err := os.ReadFile(filepath.Join(configPath, "settings.sql")); err == nil && len(content) > 0 {
		settings, unset := configenv.Expand(string(content))
		if err := sess.Exec(settings); err != nil {
			return out, configenv.Annotate(fmt.Errorf("load settings.sql: %w", err), unset)
		}
	}
	syntax, err := sess.QueryValue("SELECT current_setting('lambda_syntax')")
	if err != nil {
		return out, fmt.Errorf("read lambda_syntax: %w", err)
	}
	out.SingleArrow = strings.EqualFold(syntax, "ENABLE_SINGLE_ARROW")

	alias := "lake"
	if content, err := os.ReadFile(filepath.Join(configPath, "catalog.sql")); err == nil {
		if m := catalogAliasRe.FindStringSubmatch(string(content)); m != nil {
			alias = m[1]
		}
	}
	if err := sess.Exec("ATTACH ':memory:' AS " + duckdb.QuoteIdentifier(alias)); err != nil {
		return out, fmt.Errorf("attach stand-in catalog: %w", err)
	}
	failed, err := sess.LoadMacroDir(filepath.Join(configPath, "macros"), alias)
	if err != nil {
		return out, err
	}
	for name, ferr := range failed {
		if strings.Contains(ferr.Error(), "Deprecated lambda arrow") {
			if out.Macros == nil {
				out.Macros = map[string]error{}
			}
			out.Macros[name] = &ArrowLambdaError{}
		}
	}
	return out, nil
}
