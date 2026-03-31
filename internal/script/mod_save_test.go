// OndatraSQL - A data pipeline framework for DuckDB + DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package script

import (
	"context"
	"testing"
	"time"
)

func TestSaveRowViaStarlark(t *testing.T) {
	t.Parallel()
	rt := NewRuntime(nil, nil)
	code := `
save.row({"id": 1, "name": "alice"})
if save.count() != 1:
    fail("count should be 1")
save.row({"id": 2, "name": "bob"})
if save.count() != 2:
    fail("count should be 2")
`
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Run collects data; CreateTempTable will fail (no session)
	result, err := rt.Run(ctx, "test", code)
	if err != nil {
		t.Fatal(err)
	}
	if result.RowCount != 2 {
		t.Fatalf("expected 2 rows, got %d", result.RowCount)
	}
	if err := result.CreateTempTable(); err == nil {
		t.Fatal("expected error from missing session")
	}
}

func TestSaveRowsViaStarlark(t *testing.T) {
	t.Parallel()
	rt := NewRuntime(nil, nil)
	code := `
save.rows([{"id": 1}, {"id": 2}, {"id": 3}])
if save.count() != 3:
    fail("count should be 3, got: " + str(save.count()))
`
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	result, err := rt.Run(ctx, "test", code)
	if err != nil {
		t.Fatal(err)
	}
	if result.RowCount != 3 {
		t.Fatalf("expected 3 rows, got %d", result.RowCount)
	}
	if err := result.CreateTempTable(); err == nil {
		t.Fatal("expected error from missing session")
	}
}

func TestSaveCountEmpty(t *testing.T) {
	t.Parallel()
	rt := NewRuntime(nil, nil)
	code := `
if save.count() != 0:
    fail("count should be 0")
`
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// No data saved, no temp table created, should succeed
	_, err := rt.Run(ctx, "test", code)
	if err != nil {
		t.Fatal(err)
	}
}
