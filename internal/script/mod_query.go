// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package script

import (
	"fmt"
	"strings"
)

// validateReadOnly checks that a SQL string starts with SELECT or WITH.
func validateReadOnly(sql string) error {
	// Strip leading whitespace and comments to find the first keyword.
	trimmed := strings.TrimSpace(sql)
	if trimmed == "" {
		return fmt.Errorf("empty SQL")
	}

	// Skip line comments (-- ...) and block comments (/* ... */)
	for {
		if strings.HasPrefix(trimmed, "--") {
			if idx := strings.Index(trimmed, "\n"); idx >= 0 {
				trimmed = strings.TrimSpace(trimmed[idx+1:])
			} else {
				return fmt.Errorf("SQL contains only comments")
			}
		} else if strings.HasPrefix(trimmed, "/*") {
			if idx := strings.Index(trimmed, "*/"); idx >= 0 {
				trimmed = strings.TrimSpace(trimmed[idx+2:])
			} else {
				return fmt.Errorf("unterminated block comment")
			}
		} else {
			break
		}
	}

	// Extract the first word (keyword)
	upper := strings.ToUpper(trimmed)
	if strings.HasPrefix(upper, "SELECT") {
		return nil
	}
	// WITH is allowed for CTEs, but block writable CTEs
	// (WITH ... AS (DELETE/UPDATE/INSERT ...) ...)
	// Check for DML keywords as standalone tokens, not inside string literals.
	if strings.HasPrefix(upper, "WITH") {
		if keyword := findDMLKeywordOutsideStrings(trimmed); keyword != "" {
			return fmt.Errorf("query() does not allow writable CTEs (found %s)", keyword)
		}
		return nil
	}

	// Extract keyword for error message
	keyword := trimmed
	if idx := strings.IndexAny(keyword, " \t\n\r(;"); idx >= 0 {
		keyword = keyword[:idx]
	}
	return fmt.Errorf("only SELECT/WITH allowed, got %s", strings.ToUpper(keyword))
}

// findDMLKeywordOutsideStrings scans SQL for DML keywords that appear as
// standalone tokens outside of string literals. Returns the keyword found,
// or "" if the SQL is read-only.
func findDMLKeywordOutsideStrings(sql string) string {
	dmlKeywords := []string{"DELETE", "UPDATE", "INSERT", "DROP", "ALTER", "CREATE", "TRUNCATE"}
	upper := strings.ToUpper(sql)

	inString := false
	stringChar := byte(0)

	for i := 0; i < len(upper); i++ {
		if inString {
			if upper[i] == stringChar {
				// Handle escaped quotes ('')
				if i+1 < len(upper) && upper[i+1] == stringChar {
					i++ // skip escaped quote
				} else {
					inString = false
				}
			}
			continue
		}
		if upper[i] == '\'' || upper[i] == '"' {
			inString = true
			stringChar = upper[i]
			continue
		}
		// Check if a DML keyword starts at this position as a whole word
		for _, kw := range dmlKeywords {
			if i+len(kw) > len(upper) {
				continue
			}
			if upper[i:i+len(kw)] != kw {
				continue
			}
			// Check word boundary before
			if i > 0 && isWordChar(upper[i-1]) {
				continue
			}
			// Check word boundary after
			if i+len(kw) < len(upper) && isWordChar(upper[i+len(kw)]) {
				continue
			}
			return kw
		}
	}
	return ""
}

func isWordChar(c byte) bool {
	return (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '_'
}
