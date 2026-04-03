// OndatraSQL - You don't need a data stack anymore
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

// Package config handles configuration and paths.
package config

import (
	"bufio"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

// CatalogInfo holds parsed information from catalog.sql.
type CatalogInfo struct {
	// Type is the catalog backend: "sqlite", "postgres", "mysql", or "duckdb".
	Type string

	// ConnStr is the full DuckLake connection string (e.g. "ducklake:sqlite:ducklake.sqlite").
	ConnStr string

	// Path is the resolved catalog path. For SQLite: absolute file path. For postgres/mysql: connection params.
	Path string

	// DataPath is the DATA_PATH value from the ATTACH statement, if present.
	DataPath string

	// Alias is the catalog alias (e.g. "lake").
	Alias string
}

// Config holds the runtime configuration.
type Config struct {
	// ProjectDir is the root directory containing config/.
	ProjectDir string

	// Catalog holds parsed catalog.sql information.
	Catalog CatalogInfo

	// ModelsPath is the directory containing model definitions.
	// Default: "models" (relative to ProjectDir)
	ModelsPath string

	// ConfigPath is the directory containing config SQL files (macros, variables, catalog, etc.).
	// Default: "config" (relative to ProjectDir)
	ConfigPath string

}

// Load reads configuration from .env file and environment variables.
func Load(projectDir string) (*Config, error) {
	// Load .env file first (sets environment variables)
	envFile := filepath.Join(projectDir, ".env")
	if err := LoadEnvFile(envFile); err != nil && !os.IsNotExist(err) {
		return nil, err
	}

	cfg := &Config{
		ProjectDir: projectDir,
	}

	// Fixed project structure
	cfg.ConfigPath = filepath.Join(projectDir, "config")
	cfg.ModelsPath = filepath.Join(projectDir, "models")

	// Parse catalog info from catalog.sql
	cfg.Catalog = parseCatalogSQL(cfg.ConfigPath, projectDir)

	return cfg, nil
}

// LoadEnvFile reads a .env file and sets environment variables.
func LoadEnvFile(path string) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())

		// Skip empty lines and comments
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		// Parse KEY=value
		parts := strings.SplitN(line, "=", 2)
		if len(parts) == 2 {
			key := strings.TrimSpace(parts[0])
			value := strings.TrimSpace(parts[1])

			// Remove surrounding quotes if present
			if len(value) >= 2 {
				if (value[0] == '"' && value[len(value)-1] == '"') ||
					(value[0] == '\'' && value[len(value)-1] == '\'') {
					value = value[1 : len(value)-1]
				}
			}

			os.Setenv(key, value)
		}
	}

	return scanner.Err()
}

// attachRe matches: ATTACH 'ducklake:<type>:<path>' AS <alias> (DATA_PATH '<datapath>')
// Captures: type, path, alias, and optionally DATA_PATH value.
// Supports: ducklake:sqlite:path, ducklake:postgres:conn, ducklake:mysql:conn
var attachRe = regexp.MustCompile(`(?i)ATTACH\s+'ducklake:(\w+):([^']+)'\s+AS\s+(\w+)`)

// attachDuckDBRe matches: ATTACH 'ducklake:<path>' AS <alias> (DuckDB catalog backend — no type prefix)
// Supports: ducklake:metadata.ducklake, ducklake:C:\path\file.ducklake
// Note: attachRe (with type prefix) is tried first, so ducklake:sqlite:path won't match here.
var attachDuckDBRe = regexp.MustCompile(`(?i)ATTACH\s+'ducklake:([^']+)'\s+AS\s+(\w+)`)

var dataPathRe = regexp.MustCompile(`(?i)DATA_PATH\s+'([^']+)'`)

// parseCatalogSQL reads catalog.sql and extracts catalog connection info.
// Falls back to defaults if the file is missing or unparseable.
func parseCatalogSQL(configPath, projectDir string) CatalogInfo {
	defaults := CatalogInfo{
		Type:     "sqlite",
		ConnStr:  "ducklake:sqlite:" + filepath.Join(projectDir, "ducklake.sqlite"),
		Path:     filepath.Join(projectDir, "ducklake.sqlite"),
		DataPath: filepath.Join(projectDir, "ducklake.sqlite.files"),
		Alias:    "lake",
	}

	path := filepath.Join(configPath, "catalog.sql")
	content, err := os.ReadFile(path)
	if err != nil {
		return defaults
	}

	lines := strings.Split(string(content), "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "--") {
			continue
		}

		var catType, catPath, alias string

		if matches := attachRe.FindStringSubmatch(line); matches != nil {
			// ducklake:sqlite:path, ducklake:postgres:conn, ducklake:mysql:conn
			catType = strings.ToLower(matches[1])
			catPath = matches[2]
			alias = matches[3]
		} else if matches := attachDuckDBRe.FindStringSubmatch(line); matches != nil {
			// ducklake:metadata.ducklake (DuckDB catalog backend — no type prefix)
			catType = "duckdb"
			catPath = matches[1]
			alias = matches[2]
		} else {
			continue
		}

		// Resolve relative paths against projectDir for local backends
		if (catType == "sqlite" || catType == "duckdb") && !filepath.IsAbs(catPath) {
			catPath = filepath.Join(projectDir, catPath)
		}

		var connStr string
		if catType == "duckdb" {
			connStr = "ducklake:" + catPath
		} else {
			connStr = "ducklake:" + catType + ":" + catPath
		}

		info := CatalogInfo{
			Type:    catType,
			ConnStr: connStr,
			Path:    catPath,
			Alias:   alias,
		}

		// Extract DATA_PATH if present
		dpMatches := dataPathRe.FindStringSubmatch(line)
		if dpMatches != nil {
			dp := dpMatches[1]
			if (catType == "sqlite" || catType == "duckdb") && !filepath.IsAbs(dp) && !strings.HasPrefix(dp, "s3://") {
				dp = filepath.Join(projectDir, dp)
			}
			info.DataPath = dp
		} else if catType == "sqlite" || catType == "duckdb" {
			// Default data path for local backends: <catalog>.files
			info.DataPath = catPath + ".files"
		}

		return info
	}

	return defaults
}

// FindProjectRoot returns the project root directory by walking up
// the directory tree to find a config/ directory.
func FindProjectRoot(startDir string) (string, error) {
	// Walk up directory tree looking for config/ directory
	dir := startDir
	for {
		marker := filepath.Join(dir, "config")
		if info, err := os.Stat(marker); err == nil && info.IsDir() {
			return dir, nil
		}

		parent := filepath.Dir(dir)
		if parent == dir {
			return "", os.ErrNotExist
		}
		dir = parent
	}
}
