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
var maskingPrefixes = []string{"mask", "hash", "redact"}

// isMaskingTag returns true if the tag maps to a masking macro.
func isMaskingTag(tag string) bool {
	lower := strings.ToLower(tag)
	for _, prefix := range maskingPrefixes {
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

// applyColumnMasking wraps a model's SQL with masking macros based on @column tags.
// Tags that match masking prefixes (mask, hash, redact) are applied as DuckDB macro calls.
// The unique_key column is never masked (needed for merge matching).
//
// Input:  SELECT id, name, email, ssn FROM raw.customers
// Tags:  name|mask, email|mask_email, ssn|mask_ssn
// Output: SELECT id, mask(name) AS name, mask_email(email) AS email, mask_ssn(ssn) AS ssn
//         FROM (SELECT id, name, email, ssn FROM raw.customers)
func applyColumnMasking(sql string, model *parser.Model) string {
	if len(model.ColumnTags) == 0 {
		return sql
	}

	// Collect columns that need masking
	var maskedCols []struct {
		name  string
		macro string
	}
	// Build set of unique key columns (may be comma-separated for partition kind)
	uniqueKeyCols := make(map[string]bool)
	for _, col := range strings.Split(model.UniqueKey, ",") {
		col = strings.TrimSpace(col)
		if col != "" {
			uniqueKeyCols[col] = true
		}
	}

	for colName, tags := range model.ColumnTags {
		// Never mask unique key columns (needed for merge/partition matching)
		if uniqueKeyCols[colName] {
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
