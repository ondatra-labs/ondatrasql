// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

//go:build integration

package execute

import (
	"errors"
	"fmt"
	"sort"
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
		if got := len(validateNoArrowLambda(ast)) > 0; got != tt.reject {
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

// Every arrow in a model is reported, each naming its function.
func TestValidateNoArrowLambda_ReportsEveryArrow(t *testing.T) {
	p := testutil.NewProject(t)
	astJSON, err := lineage.GetAST(p.Sess, "SELECT list_transform([1], x -> x), list_filter([1], lambda y: y > 0), list_reduce([1], (a, b) -> a + b), COLUMNS(c -> true) FROM (SELECT 1 AS a)")
	if err != nil {
		t.Fatal(err)
	}
	ast, err := duckast.Parse(astJSON)
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, e := range validateNoArrowLambda(ast) {
		got = append(got, e.(*ArrowLambdaError).Function)
	}
	sort.Strings(got)
	if want := []string{"COLUMNS", "list_reduce", "list_transform"}; strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("reported %v, want %v", got, want)
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

// lambda_syntax is read back from DuckDB after running settings.sql, so every
// spelling DuckDB accepts counts, and so does ${VAR} expansion.
func TestInspectProjectLambdas_Settings(t *testing.T) {
	t.Setenv("ONDATRA_TEST_LAMBDA_SYNTAX", "ENABLE_SINGLE_ARROW")
	for settings, want := range map[string]bool{
		"":                 false,
		"SET threads = 4;": false,
		"SET lambda_syntax = 'ENABLE_SINGLE_ARROW';":           true,
		"SET lambda_syntax = ENABLE_SINGLE_ARROW;":             true,
		`SET lambda_syntax = "ENABLE_SINGLE_ARROW";`:           true,
		"SET GLOBAL lambda_syntax TO 'enable_single_arrow';":   true,
		"PRAGMA lambda_syntax = 'ENABLE_SINGLE_ARROW';":        true,
		"SET lambda_syntax = '${ONDATRA_TEST_LAMBDA_SYNTAX}';": true,
		// A -- inside a string is not a comment.
		"SET VARIABLE note = 'a -- b'; SET lambda_syntax = 'ENABLE_SINGLE_ARROW';": true,
		"-- SET lambda_syntax = 'ENABLE_SINGLE_ARROW';":                            false,
		"SET lambda_syntax = 'DISABLE_SINGLE_ARROW';":                              false,
		"SET lambda_syntax = 'ENABLE_SINGLE_ARROW';\nRESET lambda_syntax;":         false,
	} {
		dir := t.TempDir()
		testutil.WriteFile(t, dir, "settings.sql", settings)
		got, err := InspectProjectLambdas(dir)
		if err != nil {
			t.Errorf("%q: %v", settings, err)
			continue
		}
		if got.SingleArrow != want {
			t.Errorf("%q: SingleArrow = %v, want %v", settings, got.SingleArrow, want)
		}
	}
}

// A macro with an arrow lambda fails when DuckDB 2.0 creates it, which stops
// every run; one with the lambda keyword, or under ENABLE_SINGLE_ARROW, loads.
func TestInspectProjectLambdas_Macros(t *testing.T) {
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "catalog.sql", "ATTACH 'ducklake:sqlite:x.sqlite' AS mylake (DATA_PATH 'x');\n")
	testutil.WriteFile(t, dir, "macros/arrow.sql", "CREATE MACRO inc_all(l) AS list_transform(l, x -> x + 1);\n")
	testutil.WriteFile(t, dir, "macros/keyword.sql", "CREATE MACRO inc_all2(l) AS list_transform(l, lambda x: x + 1);\n")
	testutil.WriteFile(t, dir, "macros/catalog_ref.sql", "CREATE MACRO n() AS TABLE SELECT 1 AS v FROM (SELECT list_transform([1], lambda x: x)) t;\n")
	got, err := InspectProjectLambdas(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Macros) != 1 || got.Macros["arrow.sql"] == nil {
		t.Errorf("Macros = %v, want only arrow.sql", got.Macros)
	}
	var arrow *ArrowLambdaError
	if !errors.As(got.Macros["arrow.sql"], &arrow) {
		t.Errorf("arrow.sql: %v is not an *ArrowLambdaError", got.Macros["arrow.sql"])
	}

	testutil.WriteFile(t, dir, "settings.sql", "SET lambda_syntax = 'ENABLE_SINGLE_ARROW';\n")
	got, err = InspectProjectLambdas(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Macros) != 0 {
		t.Errorf("under ENABLE_SINGLE_ARROW: Macros = %v, want none", got.Macros)
	}
}
