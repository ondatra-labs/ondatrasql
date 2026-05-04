// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
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
	GroupKey           string // tracked kind: group_key for content-hash dedup
	PartitionedBy      []string
	Incremental        string
	IncrementalInitial string
	Fetch              bool   // @fetch — flips strict-fetch validator + lib-call relationship rule
	Push               string // @push — flips strict-push validator + sink wiring
	ConfigHash         string // SHA256 of config/*.sql files (macros, variables, etc.)
}

// ModelHash calculates a hash that includes both the code body and semantic
// directives. Changing @kind, @unique_key, @partitioned_by, @incremental,
// @incremental_initial, @fetch, @push, or config/*.sql file content triggers
// a backfill because the hash changes — which forces validators to fire on
// the next run instead of being silently bypassed by the skip path.
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
	b.WriteString("\x00group_key=")
	b.WriteString(d.GroupKey)
	b.WriteString("\x00partitioned_by=")
	b.WriteString(strings.Join(d.PartitionedBy, ","))
	b.WriteString("\x00incremental=")
	b.WriteString(d.Incremental)
	b.WriteString("\x00incremental_initial=")
	b.WriteString(d.IncrementalInitial)
	b.WriteString("\x00fetch=")
	if d.Fetch {
		b.WriteString("1")
	}
	b.WriteString("\x00push=")
	b.WriteString(d.Push)
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
		// Respect string literals: don't strip -- inside '...'
		if idx := indexCommentOutsideString(line); idx != -1 {
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

// indexCommentOutsideString finds the first "--" that is not inside a
// single-quoted SQL string literal. Returns -1 if none found.
func indexCommentOutsideString(line string) int {
	inString := false
	for i := 0; i < len(line); i++ {
		if line[i] == '\'' {
			inString = !inString
		} else if !inString && i+1 < len(line) && line[i] == '-' && line[i+1] == '-' {
			return i
		}
	}
	return -1
}

// ConfigHash computes a SHA256 hash over all .sql files in the config
// directory and its subdirectories (config/macros/, config/variables/).
// Changes to any config SQL file will change the hash and trigger re-runs
// for every model (Bug S21 fix). Files are sorted by path for determinism.
// Returns "" if the directory doesn't exist or contains no .sql files.
func ConfigHash(configDir string) string {
	var paths []string
	// WalkDir's top-level error fires only when configDir itself cannot
	// be opened (missing dir, permission denied at the root). Both of
	// those produce an empty file list; the early return below converts
	// that to the documented `""` sentinel. Per-entry walk errors are
	// swallowed inside the callback so a single broken symlink doesn't
	// invalidate the whole hash.
	if err := filepath.WalkDir(configDir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if !d.IsDir() && strings.HasSuffix(d.Name(), ".sql") {
			rel, _ := filepath.Rel(configDir, path)
			paths = append(paths, rel)
		}
		return nil
	}); err != nil {
		return ""
	}
	if len(paths) == 0 {
		return ""
	}
	sort.Strings(paths)

	h := sha256.New()
	for _, rel := range paths {
		content, err := os.ReadFile(filepath.Join(configDir, rel))
		if err != nil {
			return ""
		}
		h.Write([]byte(rel))
		h.Write([]byte{0})
		h.Write(content)
		h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil))
}
