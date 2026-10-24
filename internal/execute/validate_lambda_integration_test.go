// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

//go:build integration

package execute

import (
	"fmt"
	"strings"
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
		// The JSON arrow in a lambda-taking function's other arguments: the
		// list of list_transform, the initial value of list_reduce.
		{`SELECT list_transform(j -> '$.items', lambda x: x) FROM (SELECT '{"items": [1]}'::JSON AS j)`, false},
		{`SELECT list_reduce([1, 2], lambda a, b: a + b, j -> '$.a') FROM (SELECT '{"a": 5}'::JSON AS j)`, false},
		{"SELECT list_reduce([1, 2], (a, b) -> a + b, 0)", true},
		{"SELECT invoke(x -> x + 1, 1)", true},
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
		// DuckDB's own verdict: the run fails on the deprecated arrow exactly
		// when the validator rejects. A case may fail to bind for another
		// reason (a JSON value where a list is expected); that is not the
		// arrow.
		_, runErr := p.Sess.QueryValue(tt.sql)
		arrowErr := runErr != nil && strings.Contains(runErr.Error(), "Deprecated lambda arrow")
		if arrowErr != tt.reject {
			t.Errorf("%s: DuckDB run error = %v, but the case expects rejected=%v", tt.sql, runErr, tt.reject)
		}
	}
}

// lambdaArgIndex is every function DuckDB reports with a LAMBDA parameter,
// at the position DuckDB reports it, in every overload.
func TestLambdaFunctions_MatchDuckDB(t *testing.T) {
	p := testutil.NewProject(t)
	rows, err := p.Sess.QueryRowsMap("SELECT function_name AS name, CAST(list_position(parameter_types, 'LAMBDA') - 1 AS VARCHAR) AS at " +
		"FROM duckdb_functions() WHERE list_contains(parameter_types, 'LAMBDA')")
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for _, r := range rows {
		seen[r["name"]] = true
		at, ok := lambdaArgIndex[r["name"]]
		if !ok {
			t.Errorf("DuckDB function %s takes a lambda but is not in lambdaArgIndex", r["name"])
		} else if fmt.Sprint(at) != r["at"] {
			t.Errorf("%s takes its lambda at argument %s, lambdaArgIndex says %d", r["name"], r["at"], at)
		}
	}
	for n := range lambdaArgIndex {
		if !seen[n] {
			t.Errorf("lambdaArgIndex lists %s, which DuckDB does not report as taking a lambda", n)
		}
	}
}

func TestSingleArrowLambdasEnabled(t *testing.T) {
	for settings, want := range map[string]bool{
		"":                 false,
		"SET threads = 4;": false,
		"SET lambda_syntax = 'ENABLE_SINGLE_ARROW';":                       true,
		"set GLOBAL lambda_syntax TO 'enable_single_arrow'":                true,
		"SET lambda_syntax = 'DISABLE_SINGLE_ARROW';":                      false,
		"-- SET lambda_syntax = 'ENABLE_SINGLE_ARROW';":                    false,
		"SET lambda_syntax = 'ENABLE_SINGLE_ARROW';\nRESET lambda_syntax;": false,
	} {
		if got := SingleArrowLambdasEnabled(settings); got != want {
			t.Errorf("SingleArrowLambdasEnabled(%q) = %v, want %v", settings, got, want)
		}
	}
	// What the regex reads as enabled, DuckDB has to accept the arrow under.
	p := testutil.NewProject(t)
	if err := p.Sess.Exec("SET lambda_syntax = 'ENABLE_SINGLE_ARROW'"); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Sess.QueryValue("SELECT list_transform([1], x -> x + 1)"); err != nil {
		t.Errorf("ENABLE_SINGLE_ARROW: the arrow still fails: %v", err)
	}
}
