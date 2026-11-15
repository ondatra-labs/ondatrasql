// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeSettings(t *testing.T, content string) string {
	t.Helper()
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "config"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "config", "settings.sql"), []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	return dir
}

func TestMinVersion(t *testing.T) {
	cases := map[string]string{
		"SET threads = 4;": "",
		"SET VARIABLE ondatrasql_min_version = '0.48.0';":      "0.48.0",
		"set variable ONDATRASQL_MIN_VERSION to '0.48';":       "0.48",
		"SET VARIABLE\n  ondatrasql_min_version =\n  '1.2.3'":  "1.2.3",
		"-- SET VARIABLE ondatrasql_min_version = '9.0.0';":    "",
		"SET VARIABLE ondatrasql_min_version = '0.48.0'; -- x": "0.48.0",
		"SET VARIABLE other_min_version = '9.0.0';":            "",
	}
	for content, want := range cases {
		got, err := MinVersion(writeSettings(t, content))
		if err != nil || got != want {
			t.Errorf("%q: MinVersion = %q, %v; want %q", content, got, err, want)
		}
	}
	for _, content := range []string{
		"SET VARIABLE ondatrasql_min_version = 0.48;",
		"SET VARIABLE ondatrasql_min_version = 'latest';",
		"SET VARIABLE ondatrasql_min_version = '';",
	} {
		if _, err := MinVersion(writeSettings(t, content)); err == nil {
			t.Errorf("%q accepted", content)
		}
	}
	if got, err := MinVersion(t.TempDir()); err != nil || got != "" {
		t.Errorf("no settings.sql: %q, %v", got, err)
	}
}

func TestCheckMinVersion(t *testing.T) {
	dir := writeSettings(t, "SET VARIABLE ondatrasql_min_version = '0.48.0';")
	for running, refused := range map[string]bool{
		"0.47.9": true, "0.48.0": false, "0.48.1": false, "0.50.0": false,
		"1.0.0": false, "0.48.0-rc1": false, "dev": false,
	} {
		err := CheckMinVersion(dir, running)
		if (err != nil) != refused {
			t.Errorf("running %s: %v, refused want %v", running, err, refused)
		}
		if err != nil && (!strings.Contains(err.Error(), "0.48.0") || !strings.Contains(err.Error(), running)) {
			t.Errorf("running %s: %q does not name both versions", running, err)
		}
	}
}
