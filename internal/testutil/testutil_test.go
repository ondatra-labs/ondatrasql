// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package testutil

import (
	"os"
	"path/filepath"
	"testing"
)

func TestWriteFile(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	WriteFile(t, dir, "sub/dir/file.txt", "hello")

	content, err := os.ReadFile(filepath.Join(dir, "sub", "dir", "file.txt"))
	if err != nil {
		t.Fatalf("read file: %v", err)
	}
	if string(content) != "hello" {
		t.Errorf("content = %q, want %q", string(content), "hello")
	}
}
