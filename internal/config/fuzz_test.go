// OndatraSQL - A data pipeline framework for DuckDB + DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func FuzzParseCatalogSQL(f *testing.F) {
	f.Add("ATTACH 'ducklake:sqlite:./ducklake.sqlite' AS lake (DATA_PATH './ducklake.sqlite.files');")
	f.Add("ATTACH 'ducklake:postgres:host=localhost' AS mycat (DATA_PATH '/data/files');")
	f.Add("attach 'ducklake:sqlite:path' as alias;") // lowercase
	f.Add("")
	f.Add("-- just a comment")
	f.Add("ATTACH 'not-ducklake' AS x;")
	f.Add("ATTACH 'ducklake:sqlite:path' AS lake;") // no DATA_PATH
	f.Add("SELECT 1;")
	f.Add("ATTACH 'ducklake:sqlite:' AS lake;") // empty path
	f.Add("'; DROP TABLE users; --")
	f.Add("ATTACH 'ducklake:sqlite:path with spaces' AS my_alias (DATA_PATH '/path/with spaces');")
	f.Add("ATTACH 'ducklake:metadata.ducklake' AS lake (DATA_PATH 'data_files');")
	f.Add("ATTACH 'ducklake:my_lakehouse.db' AS lakehouse;")
	f.Add("ATTACH 'ducklake:test.ducklake' AS lake (DATA_PATH 's3://bucket/data/');")

	f.Fuzz(func(t *testing.T, content string) {
		dir := t.TempDir()
		configDir := filepath.Join(dir, "config")
		os.MkdirAll(configDir, 0o755)
		os.WriteFile(filepath.Join(configDir, "catalog.sql"), []byte(content), 0o644)

		// Must never panic
		result := parseCatalogSQL(configDir, dir)
		// Should always return a valid struct with non-empty defaults
		if result.Alias == "" {
			t.Error("empty alias")
		}
	})
}

func FuzzLoadEnvFile(f *testing.F) {
	f.Add("KEY=value")
	f.Add("KEY=\"quoted value\"")
	f.Add("KEY='single quoted'")
	f.Add("# comment\nKEY=val")
	f.Add("")
	f.Add("NO_EQUALS_SIGN")
	f.Add("=empty_key")
	f.Add("KEY=")
	f.Add("A=1\nB=2\nC=3")
	f.Add("KEY=\"unclosed")
	f.Add("KEY=has=equals=in=value")
	f.Add("  SPACES  =  around  ")
	f.Add("KEY=\"\"\nKEY2=''")

	f.Fuzz(func(t *testing.T, content string) {
		dir := t.TempDir()
		path := filepath.Join(dir, ".env")
		os.WriteFile(path, []byte(content), 0o644)

		err := loadEnvFile(path)
		// Property: empty or comment-only content should never error
		if err != nil {
			allBlank := true
			for _, line := range strings.Split(content, "\n") {
				trimmed := strings.TrimSpace(line)
				if trimmed != "" && !strings.HasPrefix(trimmed, "#") {
					allBlank = false
					break
				}
			}
			if allBlank {
				t.Errorf("loadEnvFile should not error on blank/comment content: %v", err)
			}
		}
	})
}
