// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

//go:build integration

package script

import (
	"context"
	"testing"
	"time"
)

func TestGetvariable(t *testing.T) {
	if err := sharedSess.Exec("SET variable base_currency = 'SEK'"); err != nil {
		t.Fatalf("set variable: %v", err)
	}
	t.Cleanup(func() { sharedSess.Exec("RESET variable base_currency") })

	rt := NewRuntime(sharedSess, nil)
	code := `
v = getvariable("base_currency")
if v != "SEK":
    fail("expected SEK, got " + v)
`
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if _, err := rt.Run(ctx, "test", code); err != nil {
		t.Fatal(err)
	}
}

func TestGetvariableUndefined(t *testing.T) {
	rt := NewRuntime(sharedSess, nil)
	code := `
v = getvariable("nonexistent_var_xyz")
if v != "":
    fail("expected empty string, got " + v)
`
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if _, err := rt.Run(ctx, "test", code); err != nil {
		t.Fatal(err)
	}
}

func TestGetvariableSQLInjection(t *testing.T) {
	// A name containing single quotes must not break the query
	rt := NewRuntime(sharedSess, nil)
	code := `
v = getvariable("'); DROP TABLE test; --")
# Should return empty string, not crash or execute injected SQL
if v != "":
    fail("expected empty string for injection attempt, got " + v)
`
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if _, err := rt.Run(ctx, "test", code); err != nil {
		t.Fatal(err)
	}
}
