// OndatraSQL - You don't need a data stack anymore
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadEnvFile(t *testing.T) {
	t.Parallel()
	// Create temp dir
	tmpDir := t.TempDir()
	envFile := filepath.Join(tmpDir, ".env")

	// Write test .env file
	content := `# Comment
DEV_DUCKLAKE=md:test_lake
DEV_DATA_PATH=/data/test
PROD_DUCKLAKE="md:prod_lake"
PROD_DATA_PATH='/data/prod'
`
	if err := os.WriteFile(envFile, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}

	// Clear env vars first
	os.Unsetenv("DEV_DUCKLAKE")
	os.Unsetenv("DEV_DATA_PATH")
	os.Unsetenv("PROD_DUCKLAKE")
	os.Unsetenv("PROD_DATA_PATH")

	// Load
	if err := LoadEnvFile(envFile); err != nil {
		t.Fatalf("LoadEnvFile failed: %v", err)
	}

	tests := []struct {
		key  string
		want string
	}{
		{"DEV_DUCKLAKE", "md:test_lake"},
		{"DEV_DATA_PATH", "/data/test"},
		{"PROD_DUCKLAKE", "md:prod_lake"},
		{"PROD_DATA_PATH", "/data/prod"},
	}

	for _, tt := range tests {
		t.Run(tt.key, func(t *testing.T) {
			t.Parallel()
			got := os.Getenv(tt.key)
			if got != tt.want {
				t.Errorf("got %q, want %q", got, tt.want)
			}
		})
	}
}

func TestLoad(t *testing.T) {
	t.Parallel()
	tmpDir := t.TempDir()

	// Create config/ directory with catalog.sql
	configDir := filepath.Join(tmpDir, "config")
	if err := os.MkdirAll(configDir, 0755); err != nil {
		t.Fatal(err)
	}
	catalogSQL := `ATTACH 'ducklake:sqlite:mycat.sqlite' AS lake (DATA_PATH 'mycat.sqlite.files');`
	if err := os.WriteFile(filepath.Join(configDir, "catalog.sql"), []byte(catalogSQL), 0644); err != nil {
		t.Fatal(err)
	}

	cfg, err := Load(tmpDir)
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	if cfg.Catalog.Type != "sqlite" {
		t.Errorf("Catalog.Type = %q, want %q", cfg.Catalog.Type, "sqlite")
	}
	if cfg.Catalog.Alias != "lake" {
		t.Errorf("Catalog.Alias = %q, want %q", cfg.Catalog.Alias, "lake")
	}
	wantPath := filepath.Join(tmpDir, "mycat.sqlite")
	if cfg.Catalog.Path != wantPath {
		t.Errorf("Catalog.Path = %q, want %q", cfg.Catalog.Path, wantPath)
	}
	wantDataPath := filepath.Join(tmpDir, "mycat.sqlite.files")
	if cfg.Catalog.DataPath != wantDataPath {
		t.Errorf("Catalog.DataPath = %q, want %q", cfg.Catalog.DataPath, wantDataPath)
	}
	if cfg.ProjectDir != tmpDir {
		t.Errorf("ProjectDir = %q, want %q", cfg.ProjectDir, tmpDir)
	}
}

func TestParseCatalogSQL(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		content  string
		wantType string
		wantPath string // relative; absolute is checked separately
		wantDP   string
		wantAlias string
	}{
		{
			name:      "sqlite with data_path",
			content:   "ATTACH 'ducklake:sqlite:ducklake.sqlite' AS lake (DATA_PATH 'ducklake.sqlite.files');",
			wantType:  "sqlite",
			wantPath:  "ducklake.sqlite",
			wantDP:    "ducklake.sqlite.files",
			wantAlias: "lake",
		},
		{
			name:      "postgres",
			content:   "ATTACH 'ducklake:postgres:dbname=ducklake_catalog host=localhost' AS lake (DATA_PATH 's3://my-bucket/data/');",
			wantType:  "postgres",
			wantPath:  "dbname=ducklake_catalog host=localhost",
			wantDP:    "s3://my-bucket/data/",
			wantAlias: "lake",
		},
		{
			name:      "commented lines skipped",
			content:   "-- ATTACH 'ducklake:sqlite:old.sqlite' AS old;\nATTACH 'ducklake:sqlite:new.sqlite' AS catalog;",
			wantType:  "sqlite",
			wantPath:  "new.sqlite",
			wantDP:    "new.sqlite.files", // default for sqlite
			wantAlias: "catalog",
		},
		{
			name:      "duckdb backend",
			content:   "ATTACH 'ducklake:metadata.ducklake' AS lake (DATA_PATH 'data_files');",
			wantType:  "duckdb",
			wantPath:  "metadata.ducklake",
			wantDP:    "data_files",
			wantAlias: "lake",
		},
		{
			name:      "duckdb backend no data_path",
			content:   "ATTACH 'ducklake:my_lakehouse.db' AS lakehouse;",
			wantType:  "duckdb",
			wantPath:  "my_lakehouse.db",
			wantDP:    "my_lakehouse.db.files", // default for duckdb
			wantAlias: "lakehouse",
		},
		{
			name:      "duckdb backend with colon in path",
			content:   `ATTACH 'ducklake:/data/lake:v2.ducklake' AS lake (DATA_PATH '/data/files');`,
			wantType:  "duckdb",
			wantPath:  "/data/lake:v2.ducklake",
			wantDP:    "/data/files",
			wantAlias: "lake",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tmpDir := t.TempDir()
			configDir := filepath.Join(tmpDir, "config")
			os.MkdirAll(configDir, 0755)
			os.WriteFile(filepath.Join(configDir, "catalog.sql"), []byte(tt.content), 0644)

			info := parseCatalogSQL(configDir, tmpDir)

			if info.Type != tt.wantType {
				t.Errorf("Type = %q, want %q", info.Type, tt.wantType)
			}
			if info.Alias != tt.wantAlias {
				t.Errorf("Alias = %q, want %q", info.Alias, tt.wantAlias)
			}
			// For local backends (sqlite, duckdb), relative paths are resolved against projectDir
			if (tt.wantType == "sqlite" || tt.wantType == "duckdb") && !filepath.IsAbs(tt.wantPath) {
				wantAbsPath := filepath.Join(tmpDir, tt.wantPath)
				if info.Path != wantAbsPath {
					t.Errorf("Path = %q, want %q", info.Path, wantAbsPath)
				}
			} else {
				if info.Path != tt.wantPath {
					t.Errorf("Path = %q, want %q", info.Path, tt.wantPath)
				}
			}
		})
	}
}

// TestParseCatalogSQL_EnvVarExpansion is the regression test for Bug S18
// (see docs/SANDBOX_BUGS_FOUND.md). Pre-v0.12.2 the parser ran the regex on
// the raw file content and saw literal ${VAR} tokens, which the path-resolve
// logic then joined with projectDir into nonsense like /tmp/p/${VAR}.
// Sandbox mode would then fail because forkSqliteCatalog can't read a file
// whose name still contains a literal env-var token. Prod happened to work
// because session.go runs os.ExpandEnv() at SQL execution time on the
// original file content. The fix is to expand env vars at parse time too.
func TestParseCatalogSQL_EnvVarExpansion(t *testing.T) {
	tmpDir := t.TempDir()
	configDir := filepath.Join(tmpDir, "config")
	if err := os.MkdirAll(configDir, 0o755); err != nil {
		t.Fatal(err)
	}

	// Use platform-absolute paths so the parser doesn't join them with
	// projectDir. On Windows /var/data/... is not absolute and would be
	// turned into C:\...\tmp\...\var\data\... — masking the env-var check.
	wantPath := filepath.Join(tmpDir, "lake.sqlite")
	wantData := filepath.Join(tmpDir, "files")
	t.Setenv("ONDATRA_TEST_CATALOG", wantPath)
	t.Setenv("ONDATRA_TEST_DATA", wantData)

	// catalog.sql references env vars for both the catalog path and the
	// DATA_PATH option — both must end up resolved in the parsed config.
	content := "ATTACH 'ducklake:sqlite:${ONDATRA_TEST_CATALOG}' AS lake " +
		"(DATA_PATH '${ONDATRA_TEST_DATA}');\n"
	if err := os.WriteFile(filepath.Join(configDir, "catalog.sql"), []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}

	info := parseCatalogSQL(configDir, tmpDir)

	if info.Path != wantPath {
		t.Errorf("Path = %q, want %q (env var should be expanded)", info.Path, wantPath)
	}
	if info.DataPath != wantData {
		t.Errorf("DataPath = %q, want %q (env var should be expanded)", info.DataPath, wantData)
	}
	if info.ConnStr != "ducklake:sqlite:"+wantPath {
		t.Errorf("ConnStr = %q, want %q", info.ConnStr, "ducklake:sqlite:"+wantPath)
	}
}

func TestParseCatalogSQL_MissingFile(t *testing.T) {
	t.Parallel()
	tmpDir := t.TempDir()
	info := parseCatalogSQL(filepath.Join(tmpDir, "config"), tmpDir)

	if info.Type != "sqlite" {
		t.Errorf("Type = %q, want %q", info.Type, "sqlite")
	}
	if info.Alias != "lake" {
		t.Errorf("Alias = %q, want %q", info.Alias, "lake")
	}
}

func TestFindProjectRoot(t *testing.T) {
	t.Parallel()
	tmpDir := t.TempDir()

	// Create nested structure
	subDir := filepath.Join(tmpDir, "models", "staging")
	if err := os.MkdirAll(subDir, 0755); err != nil {
		t.Fatal(err)
	}

	// Create config/ directory as marker
	configDir := filepath.Join(tmpDir, "config")
	if err := os.MkdirAll(configDir, 0755); err != nil {
		t.Fatal(err)
	}

	// Find from subdir
	found, err := FindProjectRoot(subDir)
	if err != nil {
		t.Fatalf("FindProjectRoot failed: %v", err)
	}
	if found != tmpDir {
		t.Errorf("got %q, want %q", found, tmpDir)
	}
}

func TestFindProjectRoot_NoConfigDir(t *testing.T) {
	t.Parallel()
	tmpDir := t.TempDir()
	_, err := FindProjectRoot(tmpDir)
	if err == nil {
		t.Error("expected error when no config/ directory exists")
	}
}

func TestLoad_NoConfigDir(t *testing.T) {
	t.Parallel()
	tmpDir := t.TempDir()
	// No config/ directory exists - should still return defaults
	cfg, err := Load(tmpDir)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	// Should have default values
	if cfg.Catalog.Type != "sqlite" {
		t.Errorf("Catalog.Type = %q, want sqlite (default)", cfg.Catalog.Type)
	}
}

func TestLoad_FixedPaths(t *testing.T) {
	t.Parallel()
	tmpDir := t.TempDir()

	configDir := filepath.Join(tmpDir, "config")
	os.MkdirAll(configDir, 0755)

	catalogSQL := `ATTACH 'ducklake:sqlite:lake.sqlite' AS lake;`
	os.WriteFile(filepath.Join(configDir, "catalog.sql"), []byte(catalogSQL), 0644)

	cfg, err := Load(tmpDir)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	// Paths are fixed conventions, not configurable
	wantModels := filepath.Join(tmpDir, "models")
	if cfg.ModelsPath != wantModels {
		t.Errorf("ModelsPath = %q, want %q", cfg.ModelsPath, wantModels)
	}
	wantConfig := filepath.Join(tmpDir, "config")
	if cfg.ConfigPath != wantConfig {
		t.Errorf("ConfigPath = %q, want %q", cfg.ConfigPath, wantConfig)
	}
}

func TestParseCatalogSQL_NoDuckLakeAttach(t *testing.T) {
	t.Parallel()
	tmpDir := t.TempDir()
	configDir := filepath.Join(tmpDir, "config")
	os.MkdirAll(configDir, 0755)

	// Create catalog.sql with no ATTACH ducklake
	os.WriteFile(filepath.Join(configDir, "catalog.sql"), []byte("-- empty catalog"), 0644)

	info := parseCatalogSQL(configDir, tmpDir)
	// Should return defaults
	if info.Type != "sqlite" {
		t.Errorf("Type = %q, want sqlite (default)", info.Type)
	}
}

func TestLoad_WithEnvFile(t *testing.T) {
	t.Parallel()
	tmpDir := t.TempDir()

	// Create .env file
	envContent := "MY_TEST_VAR=hello\n"
	os.WriteFile(filepath.Join(tmpDir, ".env"), []byte(envContent), 0644)

	// Create config dir with catalog.sql
	configDir := filepath.Join(tmpDir, "config")
	os.MkdirAll(configDir, 0755)
	os.WriteFile(filepath.Join(configDir, "catalog.sql"), []byte("ATTACH 'ducklake:sqlite:lake.sqlite' AS lake;"), 0644)

	os.Unsetenv("MY_TEST_VAR")
	cfg, err := Load(tmpDir)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	_ = cfg

	// Check env var was loaded
	if os.Getenv("MY_TEST_VAR") != "hello" {
		t.Errorf("MY_TEST_VAR = %q, want hello", os.Getenv("MY_TEST_VAR"))
	}
	os.Unsetenv("MY_TEST_VAR")
}

func TestLoad_EnvFileError(t *testing.T) {
	t.Parallel()
	tmpDir := t.TempDir()

	// Create .env as a directory (will cause a read error, not IsNotExist)
	envPath := filepath.Join(tmpDir, ".env")
	os.MkdirAll(envPath, 0755)

	_, err := Load(tmpDir)
	if err == nil {
		t.Error("expected error when .env is a directory")
	}
}

func TestParseCatalogSQL_S3DataPath(t *testing.T) {
	t.Parallel()
	tmpDir := t.TempDir()
	configDir := filepath.Join(tmpDir, "config")
	os.MkdirAll(configDir, 0755)

	content := `ATTACH 'ducklake:sqlite:lake.sqlite' AS dw (DATA_PATH 's3://my-bucket/data/');`
	os.WriteFile(filepath.Join(configDir, "catalog.sql"), []byte(content), 0644)

	info := parseCatalogSQL(configDir, tmpDir)

	if info.Type != "sqlite" {
		t.Errorf("Type = %q, want sqlite", info.Type)
	}
	// S3 paths should NOT be resolved against projectDir
	if info.DataPath != "s3://my-bucket/data/" {
		t.Errorf("DataPath = %q, want s3://my-bucket/data/", info.DataPath)
	}
	if info.Alias != "dw" {
		t.Errorf("Alias = %q, want dw", info.Alias)
	}
}

func TestFindProjectRoot_NotFound(t *testing.T) {
	t.Parallel()
	tmpDir := t.TempDir()

	_, err := FindProjectRoot(tmpDir)
	if !os.IsNotExist(err) {
		t.Errorf("expected os.ErrNotExist, got %v", err)
	}
}
