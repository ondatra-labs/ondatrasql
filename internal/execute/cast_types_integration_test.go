// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

//go:build integration

package execute

import (
	"reflect"
	"strings"
	"testing"

	"github.com/ondatra-labs/ondatrasql/internal/duckast"
	"github.com/ondatra-labs/ondatrasql/internal/lineage"
	"github.com/ondatra-labs/ondatrasql/internal/testutil"
)

func castProjection(t *testing.T, p *testutil.Project, typ string) *duckast.AST {
	t.Helper()
	astJSON, err := lineage.GetAST(p.Sess, "SELECT c::"+typ+" AS c FROM f()")
	if err != nil {
		t.Fatalf("%s: get AST: %v", typ, err)
	}
	ast, err := duckast.Parse(astJSON)
	if err != nil {
		t.Fatalf("%s: parse AST: %v", typ, err)
	}
	return ast
}

// Every cast spelling reaches the typed @fetch columns and the stub DDL as
// the type it names. Before, a spelling missing from the canonical table or
// a qualified name silently became VARCHAR, a string modifier lost its
// quotes, and a qualifier was dropped.
func TestCastTypes_Spellings(t *testing.T) {
	p := testutil.NewProject(t)
	if err := p.Sess.Exec("CREATE SCHEMA IF NOT EXISTS memory.myschema; CREATE TYPE memory.myschema.mood AS ENUM ('ok', 'sad')"); err != nil {
		t.Fatal(err)
	}
	for _, tt := range []struct {
		typ  string
		dict any    // the column dict's type
		stub string // the stub column's DDL type
	}{
		{"double precision", "DOUBLE", "DOUBLE"},
		{"character varying", "VARCHAR", "VARCHAR"},
		{"character varying(5)", "VARCHAR", "VARCHAR"},
		{"timestamp without time zone", "TIMESTAMP", "TIMESTAMP"},
		{"timestamp with time zone", "TIMESTAMPTZ", "TIMESTAMPTZ"},
		{"time with time zone", "TIMETZ", "TIMETZ"},
		{"varint", "BIGNUM", "BIGNUM"},
		{"int4", "INTEGER", "INTEGER"},
		{"string", "VARCHAR", "VARCHAR"},
		{"geometry", "GEOMETRY", "GEOMETRY"},
		{"geometry('OGC:CRS84')", "GEOMETRY('OGC:CRS84')", "GEOMETRY('OGC:CRS84')"},
		{"decimal", "DECIMAL(18,3)", "DECIMAL(18,3)"},
		{"numeric(5)", "DECIMAL(5,0)", "DECIMAL(5,0)"},
		{"dec(10,2)", "DECIMAL(10,2)", "DECIMAL(10,2)"},
		{"int[]", []any{"INTEGER"}, "VARCHAR[]"},
		{"struct(a int, b varchar[])", map[string]any{"a": "INTEGER", "b": []any{"VARCHAR"}}, "VARCHAR"},
		{"map(varchar, bigint)", "MAP(VARCHAR, BIGINT)", "VARCHAR"},
		{"memory.myschema.mood", "memory.myschema.mood", `"memory"."myschema"."mood"`},
	} {
		ast := castProjection(t, p, tt.typ)
		cols, err := extractTypedSelectColumns(ast)
		if err != nil {
			t.Errorf("%s: typed columns: %v", tt.typ, err)
			continue
		}
		if got := cols[0].(map[string]any)["type"]; !reflect.DeepEqual(got, tt.dict) {
			t.Errorf("%s: column dict type = %#v, want %#v", tt.typ, got, tt.dict)
		}
		_, stub, _, err := projectionNameTypeAndAlias(ast.Statements()[0].SelectList()[0])
		if err != nil || stub != tt.stub {
			t.Errorf("%s: stub type = (%q, %v), want %q", tt.typ, stub, err, tt.stub)
			continue
		}
		// The stub DDL is what CREATE TEMP TABLE runs; DuckDB has to accept it.
		if err := p.Sess.Exec("CREATE OR REPLACE TEMP TABLE cast_stub (c " + stub + ")"); err != nil {
			t.Errorf("%s: stub DDL %q rejected: %v", tt.typ, stub, err)
		}
	}
}

// A name DuckDB does not know is kept as written: the stub CREATE fails on it
// naming the type, instead of the column silently becoming VARCHAR.
func TestCastTypes_UnknownTypeFailsLoudly(t *testing.T) {
	p := testutil.NewProject(t)
	ast := castProjection(t, p, "integr")
	_, stub, _, err := projectionNameTypeAndAlias(ast.Statements()[0].SelectList()[0])
	if err != nil {
		t.Fatal(err)
	}
	err = p.Sess.Exec("CREATE OR REPLACE TEMP TABLE cast_stub (c " + stub + ")")
	if err == nil || !strings.Contains(strings.ToLower(err.Error()), "integr") {
		t.Errorf("stub DDL %q: err = %v, want a failure naming integr", stub, err)
	}
}

// builtinTypes covers every type name and alias DuckDB lists, so a type DuckDB
// adds or renames fails here instead of reaching a model as an unknown name.
func TestBuiltinTypes_CoverDuckDBTypes(t *testing.T) {
	p := testutil.NewProject(t)
	rows, err := p.Sess.QueryRowsMap("SELECT DISTINCT type_name, logical_type FROM duckdb_types() " +
		"WHERE database_name = 'system' AND schema_name = 'main'")
	if err != nil {
		t.Fatal(err)
	}
	// Composite keywords and DECIMAL are handled by the callers; NULL, TYPE
	// and TUPLE are not column types a cast names.
	skip := map[string]bool{
		"ARRAY": true, "LIST": true, "MAP": true, "STRUCT": true, "UNION": true, "ENUM": true,
		"DECIMAL": true, "NULL": true, "TYPE": true, "TUPLE": true,
	}
	// Where the canonical spelling differs from DuckDB's logical type name.
	renamed := map[string]string{
		"TIME WITH TIME ZONE":      "TIMETZ",
		"TIMESTAMP WITH TIME ZONE": "TIMESTAMPTZ",
	}
	if len(rows) < 50 {
		t.Fatalf("duckdb_types() returned %d rows", len(rows))
	}
	for _, row := range rows {
		name, logical := row["type_name"], row["logical_type"]
		if skip[logical] {
			continue
		}
		want := logical
		if r, ok := renamed[logical]; ok {
			want = r
		}
		if strings.EqualFold(name, "JSON") {
			want = "JSON" // a VARCHAR alias, kept distinct for blueprints
		}
		if got, ok := canonicalDuckType(name); !ok || got != want {
			t.Errorf("canonicalDuckType(%q) = (%q, %v), want (%q, true)", name, got, ok, want)
		}
	}
}
