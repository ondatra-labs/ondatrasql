// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

//go:build integration

package execute

import (
	"testing"

	"github.com/ondatra-labs/ondatrasql/internal/libregistry"
	"github.com/ondatra-labs/ondatrasql/internal/testutil"
)

// --- extractArgsFromSQL: multiple occurrences get correct args ---

func TestExtractArgsFromSQL_SecondOccurrence(t *testing.T) {
	t.Parallel()
	// Before fix: both calls got args from the first occurrence.
	r := &Runner{sess: testutil.NewSession(t)}
	lib := &libregistry.LibFunc{Name: "api_fetch", Args: []string{"key"}}

	sql := "SELECT * FROM api_fetch('first') UNION ALL SELECT * FROM api_fetch('second')"

	args0 := mustExtractArgs(t, r, sql, lib, 0)
	args1 := mustExtractArgs(t, r, sql, lib, 1)

	if args0["key"] != "first" {
		t.Errorf("occurrence 0: key = %q, want 'first'", args0["key"])
	}
	if args1["key"] != "second" {
		t.Errorf("occurrence 1: key = %q, want 'second'", args1["key"])
	}
}

func TestExtractArgsFromSQL_SkipsStringLiterals(t *testing.T) {
	t.Parallel()
	r := &Runner{sess: testutil.NewSession(t)}
	lib := &libregistry.LibFunc{Name: "my_func", Args: []string{"val"}}

	// Function name inside string should not be matched
	sql := "SELECT * FROM t WHERE label = 'my_func(fake)' AND x = my_func('real')"

	args := mustExtractArgs(t, r, sql, lib, 0)
	if args["val"] != "real" {
		t.Errorf("should extract from real call, got %q", args["val"])
	}
}

// Regression: parentheses inside string arguments must not break arg extraction.
func TestExtractArgsFromSQL_ParensInStringArg(t *testing.T) {
	t.Parallel()
	r := &Runner{sess: testutil.NewSession(t)}
	lib := &libregistry.LibFunc{Name: "my_api", Args: []string{"query"}}

	// The argument contains parentheses inside a string literal
	sql := "SELECT * FROM my_api('SELECT count(*) FROM t WHERE x IN (1,2)')"

	args := mustExtractArgs(t, r, sql, lib, 0)
	want := "SELECT count(*) FROM t WHERE x IN (1,2)"
	if args["query"] != want {
		t.Errorf("got %q, want %q", args["query"], want)
	}
}

// Regression: closing paren inside string literal must not end the match.
func TestExtractArgsFromSQL_ClosingParenInString(t *testing.T) {
	t.Parallel()
	r := &Runner{sess: testutil.NewSession(t)}
	lib := &libregistry.LibFunc{Name: "my_api", Args: []string{"val"}}

	sql := "SELECT * FROM my_api('x)')"

	args := mustExtractArgs(t, r, sql, lib, 0)
	if args["val"] != "x)" {
		t.Errorf("got %q, want %q", args["val"], "x)")
	}
}

// The SQL-text fallback yields the same string as the AST path for every
// literal kind and for expressions (libArgLiterals in
// libcall_args_integration_test.go has the full table).
func TestExtractArgsFromSQL_SameStringsAsTheASTPath(t *testing.T) {
	t.Parallel()
	r := &Runner{sess: testutil.NewSession(t)}
	lib := &libregistry.LibFunc{Name: "my_api", Args: []string{"v"}}
	for lit, want := range map[string]string{
		"NULL": "", "TRUE": "true", "1_000": "1000", "'it''s'": "it's",
		"1.50": "1.50", "upper('k')": "K", "2 + 3": "5", "010": "10",
	} {
		args := mustExtractArgs(t, r, "SELECT * FROM my_api("+lit+")", lib, 0)
		if args["v"] != want {
			t.Errorf("%s: got %q, want %q", lit, args["v"], want)
		}
	}
}

func mustExtractArgs(t *testing.T, r *Runner, sql string, lib *libregistry.LibFunc, occurrence int) map[string]any {
	t.Helper()
	args, err := r.extractArgsFromSQL(sql, lib, occurrence)
	if err != nil {
		t.Fatalf("extractArgsFromSQL: %v", err)
	}
	return args
}

// The SQL-text fallback binds named arguments by name, as the AST path
// does, and rejects what cannot be bound instead of dropping or shifting it.
func TestExtractArgsFromSQL_NamedArguments(t *testing.T) {
	t.Parallel()
	r := &Runner{sess: testutil.NewSession(t)}
	lib := &libregistry.LibFunc{Name: "my_api", Args: []string{"resource", "page_size", "flag"}}

	args := mustExtractArgs(t, r, "SELECT * FROM my_api('items', flag => 'y', page_size := 5)", lib, 0)
	if args["resource"] != "items" || args["page_size"] != "5" || args["flag"] != "y" {
		t.Errorf("got %v, want resource=items page_size=5 flag=y", args)
	}
	// A quoted name is a name too.
	args = mustExtractArgs(t, r, `SELECT * FROM my_api('items', "page_size" => 5)`, lib, 0)
	if args["page_size"] != "5" {
		t.Errorf("quoted name: got %v, want page_size=5", args)
	}
	for _, sql := range []string{
		"SELECT * FROM my_api('items', size => 5)",
		"SELECT * FROM my_api(page_size => 5, 'items')",
		"SELECT * FROM my_api('a', 'b', 'c', 'd')",
	} {
		if args, err := r.extractArgsFromSQL(sql, lib, 0); err == nil {
			t.Errorf("%s: got %v, want an error", sql, args)
		}
	}
}
