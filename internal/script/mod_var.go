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

// getvariableBuiltin returns a Starlark builtin that reads DuckDB session variables.
func getvariableBuiltin(sess *duckdb.Session) *starlark.Builtin {
	return starlark.NewBuiltin("getvariable", func(thread *starlark.Thread, fn *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
		var name string
		if err := starlark.UnpackPositionalArgs(fn.Name(), args, kwargs, 1, &name); err != nil {
			return nil, err
		}

		val, err := sess.QueryValue(fmt.Sprintf("SELECT getvariable('%s')", strings.ReplaceAll(name, "'", "''")))
		if err != nil {
			return nil, fmt.Errorf("getvariable(%q): %w", name, err)
		}
		return starlark.String(val), nil
	})
}
