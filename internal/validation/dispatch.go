// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package validation

import (
	"fmt"
	"regexp"
	"strings"
)

// validMacroName matches valid DuckDB identifier names for macro dispatch.
var validMacroName = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

// DispatchAudit converts a macro-style audit directive to a SQL query.
// Directive format: "name(arg1, arg2, ...)" or just "name"
// Dispatches to: SELECT * FROM ondatra_audit_{name}('{table}', arg1, arg2, ...)
//
// Returns 0 rows on pass, 1+ rows with error message on fail.
// Unknown macro names produce a DuckDB "Macro not found" error at runtime.
func DispatchAudit(directive, table string) (string, error) {
	directive = strings.TrimSpace(directive)
	if directive == "" {
		return "", fmt.Errorf("empty audit directive")
	}
	name, args, err := parseDirective(directive)
	if err != nil {
		return "", fmt.Errorf("audit %w", err)
	}
	if !validMacroName.MatchString(name) {
		return "", fmt.Errorf("invalid audit macro name: %q", name)
	}
	return fmt.Sprintf("SELECT * FROM memory.ondatra_audit_%s('%s'%s)",
		name, escapeSQL(table), formatArgs(args)), nil
}

// DispatchConstraint converts a macro-style constraint directive to a SQL query.
// Same convention as DispatchAudit but uses ondatra_constraint_ prefix.
func DispatchConstraint(directive, table string) (string, error) {
	directive = strings.TrimSpace(directive)
	if directive == "" {
		return "", fmt.Errorf("empty constraint directive")
	}
	name, args, err := parseDirective(directive)
	if err != nil {
		return "", fmt.Errorf("constraint %w", err)
	}
	if !validMacroName.MatchString(name) {
		return "", fmt.Errorf("invalid constraint macro name: %q", name)
	}
	return fmt.Sprintf("SELECT * FROM memory.ondatra_constraint_%s('%s'%s)",
		name, escapeSQL(table), formatArgs(args)), nil
}

// DispatchWarning converts a macro-style warning directive to a SQL query.
// Same convention as DispatchAudit but uses ondatra_warning_ prefix.
func DispatchWarning(directive, table string) (string, error) {
	directive = strings.TrimSpace(directive)
	if directive == "" {
		return "", fmt.Errorf("empty warning directive")
	}
	name, args, err := parseDirective(directive)
	if err != nil {
		return "", fmt.Errorf("warning %w", err)
	}
	if !validMacroName.MatchString(name) {
		return "", fmt.Errorf("invalid warning macro name: %q", name)
	}
	return fmt.Sprintf("SELECT * FROM memory.ondatra_warning_%s('%s'%s)",
		name, escapeSQL(table), formatArgs(args)), nil
}

// DispatchAuditsTransactional wraps dispatched audits in a CTE that calls
// error() on failure — aborting the enclosing transaction atomically.
// Same semantics as AuditsToTransactionalSQL but using macro dispatch.
func DispatchAuditsTransactional(directives []string, table string) (string, []error) {
	batchSQL, parseErrors := DispatchAuditsBatch(directives, table)
	if batchSQL == "" {
		return "", parseErrors
	}
	wrapped := fmt.Sprintf(
		"WITH audit_failures AS ("+
			"SELECT string_agg(audit_msg, '; ') AS msg FROM (%s) AS r(audit_msg) "+
			"WHERE audit_msg IS NOT NULL"+
			") "+
			"SELECT error('audit failed: ' || msg) FROM audit_failures WHERE msg IS NOT NULL",
		batchSQL,
	)
	return wrapped, parseErrors
}

// DispatchAuditsBatch dispatches multiple audit directives and combines with UNION ALL.
func DispatchAuditsBatch(directives []string, table string) (string, []error) {
	if len(directives) == 0 {
		return "", nil
	}
	var queries []string
	var parseErrors []error
	for _, directive := range directives {
		sql, err := DispatchAudit(directive, table)
		if err != nil {
			parseErrors = append(parseErrors, fmt.Errorf("audit dispatch error: %w", err))
			continue
		}
		queries = append(queries, fmt.Sprintf("(%s)", sql))
	}
	if len(queries) == 0 {
		return "", parseErrors
	}
	return strings.Join(queries, "\nUNION ALL\n"), parseErrors
}

// DispatchConstraintsBatch dispatches multiple constraint directives and combines with UNION ALL.
func DispatchConstraintsBatch(directives []string, table string) (string, []error) {
	if len(directives) == 0 {
		return "", nil
	}
	var queries []string
	var parseErrors []error
	for _, directive := range directives {
		sql, err := DispatchConstraint(directive, table)
		if err != nil {
			parseErrors = append(parseErrors, fmt.Errorf("constraint dispatch error: %w", err))
			continue
		}
		queries = append(queries, fmt.Sprintf("(%s)", sql))
	}
	if len(queries) == 0 {
		return "", parseErrors
	}
	return strings.Join(queries, "\nUNION ALL\n"), parseErrors
}

// parseDirective splits "name(arg1, arg2)" into name and args.
// "name" without parens returns name with empty args.
// Returns error for malformed directives (unbalanced parens, trailing garbage).
func parseDirective(directive string) (string, []string, error) {
	directive = strings.TrimSpace(directive)

	// Find first '('
	parenIdx := strings.Index(directive, "(")
	if parenIdx == -1 {
		// No args: "name" → name, []
		return directive, nil, nil
	}

	name := strings.TrimSpace(directive[:parenIdx])

	// Find matching closing paren by counting depth
	depth := 0
	inQuote := false
	closeIdx := -1
	for i := parenIdx; i < len(directive); i++ {
		c := directive[i]
		if c == '\'' && !inQuote {
			inQuote = true
		} else if c == '\'' && inQuote {
			if i+1 < len(directive) && directive[i+1] == '\'' {
				i++ // skip escaped quote
			} else {
				inQuote = false
			}
		} else if c == '(' && !inQuote {
			depth++
		} else if c == ')' && !inQuote {
			depth--
			if depth == 0 {
				closeIdx = i
				break
			}
		}
	}

	if closeIdx == -1 {
		return "", nil, fmt.Errorf("unbalanced parentheses in directive %q", directive)
	}

	// Check for trailing garbage after closing paren
	trailing := strings.TrimSpace(directive[closeIdx+1:])
	if trailing != "" {
		return "", nil, fmt.Errorf("unexpected trailing text %q in directive %q", trailing, directive)
	}

	inner := strings.TrimSpace(directive[parenIdx+1 : closeIdx])
	if inner == "" {
		return name, nil, nil
	}

	// Split on commas, respecting quotes and nested parens
	args := splitArgs(inner)
	return name, args, nil
}

// splitArgs splits comma-separated args respecting quotes and nested parens.
func splitArgs(s string) []string {
	var args []string
	var current strings.Builder
	depth := 0
	inQuote := false

	for i := 0; i < len(s); i++ {
		c := s[i]
		if c == '\'' && !inQuote {
			inQuote = true
			current.WriteByte(c)
		} else if c == '\'' && inQuote {
			if i+1 < len(s) && s[i+1] == '\'' {
				// Escaped quote
				current.WriteString("''")
				i++
			} else {
				inQuote = false
				current.WriteByte(c)
			}
		} else if (c == '(' || c == '{') && !inQuote {
			depth++
			current.WriteByte(c)
		} else if (c == ')' || c == '}') && !inQuote {
			depth--
			current.WriteByte(c)
		} else if c == ',' && depth == 0 && !inQuote {
			args = append(args, strings.TrimSpace(current.String()))
			current.Reset()
		} else {
			current.WriteByte(c)
		}
	}

	if current.Len() > 0 {
		args = append(args, strings.TrimSpace(current.String()))
	}
	return args
}

// formatArgs formats parsed args for SQL macro call.
// Each arg is passed as a SQL literal: strings get quoted, numbers stay raw.
func formatArgs(args []string) string {
	if len(args) == 0 {
		return ""
	}
	var parts []string
	for _, arg := range args {
		parts = append(parts, ", "+formatArg(arg))
	}
	return strings.Join(parts, "")
}

// formatArg formats a single arg as a SQL literal.
// Already-quoted strings ('...') pass through.
// Operators (>=, <=, etc.) get quoted as strings.
// Numbers pass through raw.
// Everything else gets quoted.
func formatArg(arg string) string {
	arg = strings.TrimSpace(arg)
	if arg == "" {
		return "''"
	}

	// Already quoted
	if len(arg) >= 2 && arg[0] == '\'' && arg[len(arg)-1] == '\'' {
		return arg
	}

	// Operators → quote as strings
	switch arg {
	case ">=", "<=", ">", "<", "=", "!=":
		return "'" + arg + "'"
	}

	// Numbers (integer or float) → raw
	if isNumeric(arg) {
		return arg
	}

	// Duration strings like "24h", "7d" → convert to DuckDB interval
	if len(arg) >= 2 {
		unit := arg[len(arg)-1]
		num := arg[:len(arg)-1]
		if isNumeric(num) {
			switch unit {
			case 'h':
				return "'" + num + " HOUR'"
			case 'd':
				return "'" + num + " DAY'"
			}
		}
	}

	// Everything else → quote as string
	return "'" + escapeSQL(arg) + "'"
}

// isNumeric returns true if s is a valid integer or float.
func isNumeric(s string) bool {
	if s == "" || s == "-" || s == "+" || s == "." {
		return false
	}
	hasDot := false
	for i, c := range s {
		if c == '-' || c == '+' {
			if i != 0 {
				return false
			}
			continue
		}
		if c == '.' {
			if hasDot {
				return false
			}
			hasDot = true
			continue
		}
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}

// escapeSQL doubles single quotes for SQL string literals.
func escapeSQL(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}
