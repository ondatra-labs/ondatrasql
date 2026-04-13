// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package script

import (
	"fmt"
	"strings"

	"go.starlark.net/starlark"

	"github.com/ondatra-labs/ondatrasql/internal/duckdb"
)

// queryBuiltin returns a Starlark built-in that executes read-only SQL queries.
// It takes a single positional string argument (SQL) and returns a list of dicts.
// Only SELECT and WITH statements are allowed (rejects INSERT/DELETE/DROP etc.).
func queryBuiltin(sess *duckdb.Session) *starlark.Builtin {
	return starlark.NewBuiltin("query", func(thread *starlark.Thread, fn *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
		var sql starlark.String
		if err := starlark.UnpackPositionalArgs(fn.Name(), args, kwargs, 1, &sql); err != nil {
			return nil, err
		}

		sqlStr := string(sql)
		if err := validateReadOnly(sqlStr); err != nil {
			return nil, fmt.Errorf("query: %w", err)
		}

		if sess == nil {
			return nil, fmt.Errorf("query: no database session available")
		}

		rows, err := sess.QueryRowsMap(sqlStr)
		if err != nil {
			return nil, fmt.Errorf("query: %w", err)
		}

		// Convert []map[string]string to *starlark.List of *starlark.Dict
		elems := make([]starlark.Value, 0, len(rows))
		for _, row := range rows {
			d := starlark.NewDict(len(row))
			for k, v := range row {
				if err := d.SetKey(starlark.String(k), starlark.String(v)); err != nil {
					return nil, fmt.Errorf("query: build result: %w", err)
				}
			}
			elems = append(elems, d)
		}

		return starlark.NewList(elems), nil
	})
}

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
	if strings.HasPrefix(upper, "SELECT") || strings.HasPrefix(upper, "WITH") {
		return nil
	}

	// Extract keyword for error message
	keyword := trimmed
	if idx := strings.IndexAny(keyword, " \t\n\r(;"); idx >= 0 {
		keyword = keyword[:idx]
	}
	return fmt.Errorf("only SELECT/WITH allowed, got %s", strings.ToUpper(keyword))
}
