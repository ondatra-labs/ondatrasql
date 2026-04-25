// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package execute

import (
	"fmt"
	"sort"
	"strings"

	"github.com/ondatra-labs/ondatrasql/internal/duckdb"
	"github.com/ondatra-labs/ondatrasql/internal/parser"
)

// maskingTagPrefixes lists tag prefixes that trigger column masking.
// Tags like "PII" or "sensitive" are metadata-only (no masking).
// Tags like "mask", "mask_email", "hash_pii" map to DuckDB macros.
// The convention: if a tag starts with "mask", "hash", or "redact", it's a masking macro.
var maskingTagPrefixes = []string{"mask", "hash", "redact"}

// isMaskingTag returns true if the tag maps to a masking macro.
func isMaskingTag(tag string) bool {
	lower := strings.ToLower(tag)
	for _, prefix := range maskingTagPrefixes {
		if lower == prefix || strings.HasPrefix(lower, prefix+"_") {
			return true
		}
	}
	return false
}

// getMaskingMacro returns the first masking tag for a column, or empty string.
func getMaskingMacro(tags []string) string {
	for _, tag := range tags {
		if isMaskingTag(tag) {
			return tag
		}
	}
	return ""
}

// parseColumnList splits a comma-separated column list, trimming whitespace.
func parseColumnList(s string) []string {
	var cols []string
	for _, col := range strings.Split(s, ",") {
		col = strings.TrimSpace(col)
		if col != "" {
			cols = append(cols, col)
		}
	}
	return cols
}

// applyColumnMasking rewrites a model SQL using DuckDB SELECT * REPLACE to apply
// masking macros for tagged columns.
//
//   - Tags matching masking prefixes (mask, hash, redact) are treated as macro names.
//   - unique_key columns are never masked (required for merge/tracked matching).
//   - If no applicable masking tags are found, the input SQL is returned unchanged.
//   - Complex SQL (CTEs, subqueries) is wrapped as a derived table: FROM (<original_sql>).
//   - REPLACE overrides are sorted by column name for deterministic output.
//   - Invalid SQL or missing macros surface at execution time, not here.
func applyColumnMasking(sql string, model *parser.Model) string {
	if len(model.ColumnTags) == 0 {
		return sql
	}

	// Collect columns that need masking
	var maskedCols []struct {
		name  string
		macro string
	}
	// Build set of key columns (may be comma-separated for composite keys)
	keyCols := make(map[string]bool)
	for _, col := range parseColumnList(model.UniqueKey) {
		keyCols[col] = true
	}
	for _, col := range parseColumnList(model.GroupKey) {
		keyCols[col] = true
	}

	for colName, tags := range model.ColumnTags {
		// Never mask key columns (needed for merge/tracked matching)
		if keyCols[colName] {
			continue
		}
		macro := getMaskingMacro(tags)
		if macro != "" {
			maskedCols = append(maskedCols, struct {
				name  string
				macro string
			}{colName, macro})
		}
	}

	if len(maskedCols) == 0 {
		return sql
	}

	// Sort for deterministic output
	sort.Slice(maskedCols, func(i, j int) bool {
		return maskedCols[i].name < maskedCols[j].name
	})

	// Build column list: masked columns wrapped with macro, others passed through
	// We use SELECT * with individual column overrides via a subquery
	var overrides []string
	for _, mc := range maskedCols {
		qn := duckdb.QuoteIdentifier(mc.name)
		overrides = append(overrides, fmt.Sprintf("%s(%s) AS %s",
			mc.macro, qn, qn))
	}

	// Build REPLACE clause: SELECT * REPLACE (mask(name) AS name, mask_email(email) AS email)
	return fmt.Sprintf("SELECT * REPLACE (%s) FROM (%s)",
		strings.Join(overrides, ", "), sql)
}
