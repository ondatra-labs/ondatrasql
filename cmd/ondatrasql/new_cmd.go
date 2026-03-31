// OndatraSQL - A data pipeline framework for DuckDB + DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/ondatra-labs/ondatrasql/internal/config"
	"github.com/ondatra-labs/ondatrasql/internal/output"
)

// runNew creates a new model file or directory.
//
// Formats:
//
//	ondatrasql new schema.name.sql          → models/schema/name.sql
//	ondatrasql new schema.sub.name.star     → models/schema/sub/name.star
//	ondatrasql new schema                   → models/schema/  (directory only)
//	ondatrasql new schema.sub               → models/schema/sub/  (directory only)
func runNew(cfg *config.Config, target string) error {
	// Check for explicit extension (case-insensitive)
	ext := ""
	lower := strings.ToLower(target)
	for _, e := range []string{".sql", ".star"} {
		if strings.HasSuffix(lower, e) {
			ext = e
			target = target[:len(target)-len(e)]
			break
		}
	}

	// Split all parts on dots
	parts := strings.Split(target, ".")
	for _, p := range parts {
		if p == "" {
			return fmt.Errorf("empty path segment in %q", target)
		}
		// Reject path traversal attempts
		if p == ".." || strings.ContainsAny(p, `/\`) {
			return fmt.Errorf("invalid path segment in %q", target)
		}
	}

	// No extension → create directory only, but reject common file extensions (likely typos)
	if ext == "" {
		lowerTarget := strings.ToLower(target)
		for _, bad := range []string{".py", ".sh", ".ps1", ".ts", ".js", ".go", ".yaml", ".yml", ".json", ".csv"} {
			if strings.HasSuffix(lowerTarget, bad) {
				return fmt.Errorf("unsupported extension %q: use .sql or .star", bad)
			}
		}
		dirPath := filepath.Join(cfg.ModelsPath, filepath.Join(parts...))
		if err := os.MkdirAll(dirPath, 0755); err != nil {
			return fmt.Errorf("create directory: %w", err)
		}
		relPath, _ := filepath.Rel(cfg.ProjectDir, dirPath)
		output.Fprintf("Created %s/\n", relPath)
		return nil
	}

	// With extension: need at least schema.name
	if len(parts) < 2 {
		return fmt.Errorf("target must be schema.name (e.g. staging.orders.sql)")
	}

	// All parts become path segments: schema.sub.name → models/schema/sub/name
	dirPath := filepath.Join(cfg.ModelsPath, filepath.Join(parts[:len(parts)-1]...))
	name := parts[len(parts)-1]
	schema := parts[0]

	if err := os.MkdirAll(dirPath, 0755); err != nil {
		return fmt.Errorf("create directory: %w", err)
	}

	// Create file path based on extension
	var filePath, template string
	switch ext {
	case ".star":
		filePath = filepath.Join(dirPath, name+".star")
		template = generateStarlarkTemplate(schema, name)
	default:
		filePath = filepath.Join(dirPath, name+".sql")
		template = generateTemplate(schema, name)
	}

	// Check if file already exists
	if _, err := os.Stat(filePath); err == nil {
		return fmt.Errorf("model already exists: %s", filePath)
	}

	// Write file
	if err := os.WriteFile(filePath, []byte(template), 0644); err != nil {
		return fmt.Errorf("write file: %w", err)
	}

	// Show relative path
	relPath, _ := filepath.Rel(cfg.ProjectDir, filePath)
	output.Fprintf("Created %s\n", relPath)

	return nil
}

// generateTemplate creates a SQL model template.
func generateTemplate(schema, name string) string {
	return fmt.Sprintf(`-- %s.%s
-- @kind: table

SELECT
    *
FROM source_table
`, schema, name)
}

// generateStarlarkTemplate creates a Starlark script template.
func generateStarlarkTemplate(schema, name string) string {
	return fmt.Sprintf(`# Starlark script: %s.%s
# Fetches data from REST API and saves to DuckDB
# @kind: append

# API configuration
api_url = "https://api.example.com/data"
api_token = env.get("API_TOKEN")

# Fetch data
resp = http.get(api_url, headers={"Authorization": "Bearer " + api_token})

if not resp.ok:
    fail("API returned status " + str(resp.status_code))

# Save each record
for item in resp.json:
    save.row({
        "id": item["id"],
        "name": item["name"],
        "value": item["value"],
    })

print("Saved", save.count(), "records")
`, schema, name)
}

