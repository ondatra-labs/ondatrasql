// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

//go:build integration

package execute

import (
	"strings"
	"testing"

	"github.com/ondatra-labs/ondatrasql/internal/dag"
	"github.com/ondatra-labs/ondatrasql/internal/duckast"
	"github.com/ondatra-labs/ondatrasql/internal/lineage"
	"github.com/ondatra-labs/ondatrasql/internal/testutil"
)

// libArgLiterals is one literal of every kind DuckDB 2.0 distinguishes, with
// the string a blueprint receives for it: what DuckDB renders for
// CAST(<literal> AS VARCHAR), and "" for NULL.
var libArgLiterals = []struct {
	sql  string
	want string
}{
	{"'k'", "k"},
	{"'it''s'", "it's"},
	{"5", "5"},
	{"-5", "-5"},
	{"1_000", "1000"},
	{"010", "10"},
	{"1.5", "1.5"},
	{"1.50", "1.50"},
	{"1e5", "100000.0"},
	{"99999999999999999999999", "99999999999999999999999"},
	{"x'ab'", `\xAB`},
	{"true", "true"},
	{"false", "false"},
	{"NULL", ""},
	{"upper('k')", "K"},
	{"2 + 3", "5"},
}

func libArgNodes(t *testing.T, p *testutil.Project) []duckast.Argument {
	t.Helper()
	var lits []string
	for _, l := range libArgLiterals {
		lits = append(lits, l.sql)
	}
	astJSON, err := lineage.GetAST(p.Sess, "SELECT * FROM f("+strings.Join(lits, ", ")+")")
	if err != nil {
		t.Fatalf("get AST: %v", err)
	}
	ast, err := duckast.Parse(astJSON)
	if err != nil {
		t.Fatalf("parse AST: %v", err)
	}
	args, err := ast.Statements()[0].FromTable().TableFunctionArgs()
	if err != nil || len(args) != len(libArgLiterals) {
		t.Fatalf("got %d args (%v), want %d", len(args), err, len(libArgLiterals))
	}
	return args
}

// Blueprints receive their args as strings, whatever the literal kind. Under
// DuckDB 2.0 an integer literal reached Starlark as an int, so a blueprint
// building "limit=" + limit failed. Every kind is pinned through both paths:
// the constant shortcut and the json_deserialize_sql evaluation it falls back
// to, which must agree with each other and with DuckDB's own CAST.
func TestLibArgs_EveryLiteralKindIsAString(t *testing.T) {
	p := testutil.NewProject(t)
	r := NewRunner(p.Sess, ModeRun, dag.GenerateRunID())
	for i, arg := range libArgNodes(t, p) {
		lit := libArgLiterals[i]
		got, err := r.evaluateSingleArg(arg.Expr)
		if err != nil || got != lit.want {
			t.Errorf("%s: evaluateSingleArg = (%q, %v), want %q", lit.sql, got, err, lit.want)
		}
		viaDuckDB, err := r.evaluateArgViaDuckDB(arg.Expr)
		if err != nil || viaDuckDB != lit.want {
			t.Errorf("%s: json_deserialize_sql path = (%q, %v), want %q", lit.sql, viaDuckDB, err, lit.want)
		}
		cast, err := p.Sess.QueryValue("SELECT CAST(" + lit.sql + " AS VARCHAR)")
		if err != nil || cast != lit.want {
			t.Errorf("%s: DuckDB renders (%q, %v), the test expects %q", lit.sql, cast, err, lit.want)
		}
	}
}
