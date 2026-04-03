// OndatraSQL - You don't need a data stack anymore
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

// Package backfill handles SQL hash calculation and backfill detection.
package backfill

import (
	"crypto/sha256"
	"encoding/hex"
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
}

// ModelHash calculates a hash that includes both the code body and semantic
// directives. Changing @kind, @unique_key, @partitioned_by, @incremental, or
// @incremental_initial triggers a backfill because the hash changes.
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
