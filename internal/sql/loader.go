// OndatraSQL - A data pipeline framework for DuckDB + DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

// Package sql provides embedded SQL files for OndatraSQL operations.
// All SQL logic is organized in subdirectories and loaded via go:embed.
package sql

import (
	"embed"
	"fmt"
	"strings"
	"sync"
)

//go:embed execute/*.sql schema/*.sql macros/*.sql queries/*.sql
var sqlFiles embed.FS

// catalogAlias holds the current DuckLake catalog alias.
// Set via SetCatalogAlias() during session initialization.
var (
	catalogAlias = "lake" // Default
	catalogMu    sync.RWMutex
)

// SetCatalogAlias sets the DuckLake catalog alias for SQL file loading.
// Call this during session initialization after detecting the alias.
func SetCatalogAlias(alias string) {
	if alias != "" {
		catalogMu.Lock()
		catalogAlias = alias
		catalogMu.Unlock()
	}
}

// Load reads a SQL file from the embedded filesystem.
// Path should be relative to internal/sql/, e.g. "metadata/snapshots.sql"
func Load(path string) (string, error) {
	content, err := sqlFiles.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("load sql %s: %w", path, err)
	}
	// Replace {{catalog}} placeholder with actual catalog alias
	catalogMu.RLock()
	alias := catalogAlias
	catalogMu.RUnlock()
	result := strings.ReplaceAll(string(content), "{{catalog}}", alias)
	return result, nil
}

// MustFormat loads a SQL file and applies fmt.Sprintf with the given arguments.
func MustFormat(path string, args ...interface{}) string {
	template, err := Load(path)
	if err != nil {
		panic(err)
	}
	return fmt.Sprintf(template, args...)
}

// LoadQuery loads a query SQL file and replaces $1, $2, etc. with arguments.
func LoadQuery(name string, args ...string) (string, error) {
	content, err := Load("queries/" + name + ".sql")
	if err != nil {
		return "", err
	}

	for i, arg := range args {
		placeholder := fmt.Sprintf("$%d", i+1)
		escaped := strings.ReplaceAll(arg, "'", "''")
		content = strings.ReplaceAll(content, placeholder, escaped)
	}

	return content, nil
}
