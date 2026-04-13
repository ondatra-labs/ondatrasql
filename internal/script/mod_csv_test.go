// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package script

import (
	"context"
	"testing"
	"time"
)

func TestCsvDecodeSimple(t *testing.T) {
	t.Parallel()
	rt := NewRuntime(nil, nil)
	code := `
result = csv.decode("name,age\nAlice,30")
if len(result) != 1:
    fail("expected 1 row, got: " + str(len(result)))
if result[0]["name"] != "Alice":
    fail("expected Alice, got: " + str(result[0]["name"]))
if result[0]["age"] != "30":
    fail("expected 30, got: " + str(result[0]["age"]))
`
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if _, err := rt.Run(ctx, "test", code); err != nil {
		t.Fatal(err)
	}
}

func TestCsvDecodeNoHeader(t *testing.T) {
	t.Parallel()
	rt := NewRuntime(nil, nil)
	code := `
result = csv.decode("Alice,30\nBob,25", header=False)
if len(result) != 2:
    fail("expected 2 rows, got: " + str(len(result)))
if result[0][0] != "Alice":
    fail("expected Alice, got: " + str(result[0][0]))
if result[0][1] != "30":
    fail("expected 30, got: " + str(result[0][1]))
`
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if _, err := rt.Run(ctx, "test", code); err != nil {
		t.Fatal(err)
	}
}

func TestCsvDecodeCustomDelimiter(t *testing.T) {
	t.Parallel()
	rt := NewRuntime(nil, nil)
	code := `
result = csv.decode("name\tage\nAlice\t30", delimiter="\t")
if len(result) != 1:
    fail("expected 1 row, got: " + str(len(result)))
if result[0]["name"] != "Alice":
    fail("expected Alice, got: " + str(result[0]["name"]))
`
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if _, err := rt.Run(ctx, "test", code); err != nil {
		t.Fatal(err)
	}
}

func TestCsvDecodeEmpty(t *testing.T) {
	t.Parallel()
	rt := NewRuntime(nil, nil)
	code := `csv.decode("")`
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if _, err := rt.Run(ctx, "test", code); err == nil {
		t.Fatal("expected error on empty string")
	}
}

func TestCsvEncode(t *testing.T) {
	t.Parallel()
	rt := NewRuntime(nil, nil)
	code := `
rows = [{"name": "Alice", "age": "30"}, {"name": "Bob", "age": "25"}]
result = csv.encode(rows)
# Decode it back to verify round-trip
parsed = csv.decode(result)
if len(parsed) != 2:
    fail("expected 2 rows, got: " + str(len(parsed)))
if parsed[0]["name"] != "Alice":
    fail("round-trip failed: " + str(parsed[0]))
if parsed[1]["age"] != "25":
    fail("round-trip failed: " + str(parsed[1]))
`
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if _, err := rt.Run(ctx, "test", code); err != nil {
		t.Fatal(err)
	}
}

func TestCsvEncodeListOfLists(t *testing.T) {
	t.Parallel()
	rt := NewRuntime(nil, nil)
	code := `
rows = [["Alice", "30"], ["Bob", "25"]]
result = csv.encode(rows, header=["name", "age"])
parsed = csv.decode(result)
if len(parsed) != 2:
    fail("expected 2 rows, got: " + str(len(parsed)))
if parsed[0]["name"] != "Alice":
    fail("round-trip failed: " + str(parsed[0]))
`
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if _, err := rt.Run(ctx, "test", code); err != nil {
		t.Fatal(err)
	}
}

func TestCsvModuleAvailable(t *testing.T) {
	t.Parallel()
	rt := NewRuntime(nil, nil)
	code := `
if not csv:
    fail("csv module not available")
if not csv.decode:
    fail("csv.decode not available")
if not csv.encode:
    fail("csv.encode not available")
`
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if _, err := rt.Run(ctx, "test", code); err != nil {
		t.Fatal(err)
	}
}
