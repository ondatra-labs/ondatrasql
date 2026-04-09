// OndatraSQL - You don't need a data stack anymore
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

// Package backfill handles SQL hash calculation and backfill detection.
package backfill

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// Hash calculates a SHA256 hash of the SQL query.
// The hash is normalized by removing comments and extra whitespace.
// Used for AST caching where only the code body matters.
func Hash(sql string) string {
	normalized := normalize(sql)
	h := sha256.Sum256([]byte(normalized))
	return hex.EncodeToString(h[:])
}

// ModelDirectives contains directive values that affect execution semantics.
// Changes to these directives should trigger a backfill, so they are included
// in the model hash alongside the SQL/script body.
type ModelDirectives struct {
	Kind               string
	UniqueKey          string
	PartitionedBy      []string
	Incremental        string
	IncrementalInitial string
	ConfigHash         string // SHA256 of config/*.sql files (macros, variables, etc.)
}

// ModelHash calculates a hash that includes both the code body and semantic
// directives. Changing @kind, @unique_key, @partitioned_by, @incremental,
// @incremental_initial, or config/*.sql file content triggers a backfill
// because the hash changes.
func ModelHash(sql string, d ModelDirectives) string {
	normalized := normalize(sql)

	// Append directive values in a deterministic format.
	// Only directives that affect how data is written are included.
	var b strings.Builder
	b.WriteString(normalized)
	b.WriteString("\x00kind=")
	b.WriteString(d.Kind)
	b.WriteString("\x00unique_key=")
	b.WriteString(d.UniqueKey)
	b.WriteString("\x00partitioned_by=")
	b.WriteString(strings.Join(d.PartitionedBy, ","))
	b.WriteString("\x00incremental=")
	b.WriteString(d.Incremental)
	b.WriteString("\x00incremental_initial=")
	b.WriteString(d.IncrementalInitial)
	if d.ConfigHash != "" {
		b.WriteString("\x00config=")
		b.WriteString(d.ConfigHash)
	}

	h := sha256.Sum256([]byte(b.String()))
	return hex.EncodeToString(h[:])
}

// normalize removes comments and normalizes whitespace for consistent hashing.
func normalize(sql string) string {
	// Replace tabs with spaces
	sql = strings.ReplaceAll(sql, "\t", " ")

	var result strings.Builder
	lines := strings.Split(sql, "\n")

	for _, line := range lines {
		// Remove single-line SQL comments (--)
		if idx := strings.Index(line, "--"); idx != -1 {
			line = line[:idx]
		}

		// Trim and add if non-empty
		line = strings.TrimSpace(line)
		if line != "" {
			if result.Len() > 0 {
				result.WriteString(" ")
			}
			result.WriteString(line)
		}
	}

	// Normalize multiple spaces to single space
	normalized := result.String()
	for strings.Contains(normalized, "  ") {
		normalized = strings.ReplaceAll(normalized, "  ", " ")
	}

	return strings.ToLower(normalized)
}

// ConfigHash computes a SHA256 hash over all .sql files in the config
// directory. Changes to macros.sql, variables.sql, secrets.sql, sources.sql,
// or any other config SQL file will change the hash and trigger re-runs for
// every model (Bug S21 fix). Files are sorted by name for determinism.
// Returns "" if the directory doesn't exist or contains no .sql files.
func ConfigHash(configDir string) string {
	entries, err := os.ReadDir(configDir)
	if err != nil {
		return ""
	}
	var names []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".sql") {
			names = append(names, e.Name())
		}
	}
	if len(names) == 0 {
		return ""
	}
	sort.Strings(names)

	h := sha256.New()
	for _, name := range names {
		content, err := os.ReadFile(filepath.Join(configDir, name))
		if err != nil {
			continue
		}
		h.Write([]byte(name))
		h.Write([]byte{0})
		h.Write(content)
		h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil))
}
