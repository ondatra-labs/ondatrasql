// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package state

import (
	"path/filepath"
	"testing"
)

func TestOpen_CreatesDir(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	st, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = st.Close() }()

	want := filepath.Join(dir, ".ondatra", "state.duckdb")
	if got := st.Path(); got != want {
		t.Errorf("Path() = %q, want %q", got, want)
	}

	if st.DB() == nil {
		t.Error("DB() returned nil")
	}
}

func TestGC_NoOpOnEmptyStore(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	st, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = st.Close() }()

	if err := GC(st); err != nil {
		t.Errorf("GC on empty store: %v", err)
	}
	// Idempotent — second call should also succeed.
	if err := GC(st); err != nil {
		t.Errorf("second GC: %v", err)
	}
}

func TestGC_NilStateOK(t *testing.T) {
	t.Parallel()
	if err := GC(nil); err != nil {
		t.Errorf("GC(nil) = %v, want nil", err)
	}
}
