// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

//go:build integration

package execute

import (
	"testing"

	"github.com/ondatra-labs/ondatrasql/internal/duckast"
	"github.com/ondatra-labs/ondatrasql/internal/lineage"
	"github.com/ondatra-labs/ondatrasql/internal/testutil"
)

// The arrow form is rejected where DuckDB 2.0 binds it as a lambda, and the
// JSON operator, which serializes the same way, is left alone. Each case is
// also run, so the expectation is DuckDB's own and not the validator's.
func TestValidateNoArrowLambda(t *testing.T) {
	p := testutil.NewProject(t)
	for _, tt := range []struct {
		sql    string
		reject bool
	}{
		{"SELECT list_transform([1, 2], x -> x + 1)", true},
		{"SELECT list_reduce([1, 2], (a, b) -> a + b)", true},
		{"SELECT list_transform([[1]], lambda x: list_filter(x, y -> y > 0))", true},
		{"SELECT COLUMNS(c -> c LIKE 'a%') FROM (SELECT 1 AS a)", true},
		{"SELECT list_transform([1, 2], lambda x: x + 1)", false},
		{"SELECT COLUMNS(lambda c: c LIKE 'a%') FROM (SELECT 1 AS a)", false},
		{`SELECT '{"a": 1}'::JSON -> '$.a'`, false},
		{`SELECT '[1, 2]'::JSON -> 0`, false},
		{`SELECT coalesce('{"a": 1}'::JSON -> '$.a', NULL)`, false},
		{`SELECT j -> p FROM (SELECT '{"a": 1}'::JSON AS j, '$.a' AS p)`, false},
	} {
		astJSON, err := lineage.GetAST(p.Sess, tt.sql)
		if err != nil {
			t.Fatalf("%s: get AST: %v", tt.sql, err)
		}
		ast, err := duckast.Parse(astJSON)
		if err != nil {
			t.Fatalf("%s: parse AST: %v", tt.sql, err)
		}
		if got := validateNoArrowLambda(ast) != nil; got != tt.reject {
			t.Errorf("%s: rejected=%v, want %v", tt.sql, got, tt.reject)
		}
		_, runErr := p.Sess.QueryValue(tt.sql)
		if failed := runErr != nil; failed != tt.reject {
			t.Errorf("%s: DuckDB run error = %v, but the case expects rejected=%v", tt.sql, runErr, tt.reject)
		}
	}
}

// lambdaFunctions is every function DuckDB reports with a LAMBDA parameter.
func TestLambdaFunctions_MatchDuckDB(t *testing.T) {
	p := testutil.NewProject(t)
	names, err := p.Sess.QueryRows("SELECT DISTINCT function_name FROM duckdb_functions() WHERE list_contains(parameter_types, 'LAMBDA')")
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for _, n := range names {
		seen[n] = true
		if !lambdaFunctions[n] {
			t.Errorf("DuckDB function %s takes a lambda but is not in lambdaFunctions", n)
		}
	}
	for n := range lambdaFunctions {
		if !seen[n] {
			t.Errorf("lambdaFunctions lists %s, which DuckDB does not report as taking a lambda", n)
		}
	}
}
