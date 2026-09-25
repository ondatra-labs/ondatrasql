// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package duckdb

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	duckdbdriver "github.com/duckdb/duckdb-go/v2"
)

// A driver error is replaced, not wrapped: the copy keeps its Type, which
// callers branch on (isTableNotExistError reads ErrorTypeCatalog), and no
// errors.As can reach the original Msg.
func TestRedactErr_DriverErrorKeepsTypeAndHidesMsg(t *testing.T) {
	t.Parallel()
	const pw = "S3ntinel-pw_42"
	orig := &duckdbdriver.Error{Type: duckdbdriver.ErrorTypeCatalog, Msg: "attach failed: password=" + pw + " host=x"}

	got := redactErr(orig)
	var de *duckdbdriver.Error
	if !errors.As(got, &de) {
		t.Fatalf("expected a *duckdb.Error, got %T", got)
	}
	if de == orig {
		t.Fatal("the original error must not be returned")
	}
	if de.Type != duckdbdriver.ErrorTypeCatalog {
		t.Fatalf("Type = %v, want ErrorTypeCatalog", de.Type)
	}
	if strings.Contains(de.Msg, pw) {
		t.Fatalf("password survived in Msg: %s", de.Msg)
	}
}

func TestRedactErr_CleanAndWrappedErrors(t *testing.T) {
	t.Parallel()
	if redactErr(nil) != nil {
		t.Fatal("nil must stay nil")
	}
	clean := &duckdbdriver.Error{Type: duckdbdriver.ErrorTypeCatalog, Msg: "Table with name x does not exist"}
	if redactErr(clean) != clean {
		t.Fatal("an error with nothing to hide must come back unchanged")
	}
	// Wrapped by something else: the whole chain is replaced by its redacted
	// text, so the driver error inside is unreachable.
	wrapped := fmt.Errorf("init: %w", &duckdbdriver.Error{Msg: "password=S3ntinel-pw_42"})
	got := redactErr(wrapped)
	var de *duckdbdriver.Error
	if errors.As(got, &de) {
		t.Fatalf("the unredacted driver error is still reachable: %q", de.Msg)
	}
	if strings.Contains(got.Error(), "S3ntinel") {
		t.Fatalf("password survived: %s", got)
	}
}
