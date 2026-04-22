// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package script

import (
	"fmt"

	"go.starlark.net/starlark"

	"github.com/ondatra-labs/ondatrasql/internal/duckdb"
)

// queryBuiltin provides read-only SQL access to DuckDB from Starlark.
// Usage: rows = query("SELECT source_hash FROM raw.invoices")
// Returns a list of dicts. Only SELECT/WITH allowed.
func queryBuiltin(sess *duckdb.Session) *starlark.Builtin {
	return starlark.NewBuiltin("query", func(thread *starlark.Thread, fn *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
		var sql string
		if err := starlark.UnpackPositionalArgs(fn.Name(), args, kwargs, 1, &sql); err != nil {
			return nil, err
		}

		if err := validateReadOnly(sql); err != nil {
			return nil, fmt.Errorf("query: %w", err)
		}

		if sess == nil {
			return starlark.NewList(nil), nil
		}

		rows, err := sess.QueryRowsAny(sql)
		if err != nil {
			return nil, fmt.Errorf("query: %w", err)
		}

		var items []starlark.Value
		for _, row := range rows {
			d := starlark.NewDict(len(row))
			for k, v := range row {
				sv, err := goToStarlark(v)
				if err != nil {
					sv = starlark.String(fmt.Sprintf("%v", v))
				}
				d.SetKey(starlark.String(k), sv)
			}
			items = append(items, d)
		}

		return starlark.NewList(items), nil
	})
}
