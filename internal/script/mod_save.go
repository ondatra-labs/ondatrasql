// OndatraSQL - A data pipeline framework for DuckDB + DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package script

import (
	"fmt"

	"go.starlark.net/starlark"
	"go.starlark.net/starlarkstruct"
)

// saveModule provides data saving functions.
func saveModule(collector rowCollector) *starlarkstruct.Module {
	return &starlarkstruct.Module{
		Name: "save",
		Members: starlark.StringDict{
			// row(dict) -> None
			"row": starlark.NewBuiltin("save.row", func(thread *starlark.Thread, fn *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
				var d *starlark.Dict
				if err := starlark.UnpackPositionalArgs(fn.Name(), args, kwargs, 1, &d); err != nil {
					return nil, err
				}

				row, err := starlarkDictToGo(d)
				if err != nil {
					return nil, err
				}

				if err := collector.add(row); err != nil {
					return nil, fmt.Errorf("save.row: %w", err)
				}
				return starlark.None, nil
			}),

			// rows(list) -> None
			"rows": starlark.NewBuiltin("save.rows", func(thread *starlark.Thread, fn *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
				var list *starlark.List
				if err := starlark.UnpackPositionalArgs(fn.Name(), args, kwargs, 1, &list); err != nil {
					return nil, err
				}

				for i := 0; i < list.Len(); i++ {
					d, ok := list.Index(i).(*starlark.Dict)
					if !ok {
						return nil, fmt.Errorf("rows: element %d is %s, want dict", i, list.Index(i).Type())
					}

					row, err := starlarkDictToGo(d)
					if err != nil {
						return nil, err
					}

					if err := collector.add(row); err != nil {
						return nil, fmt.Errorf("save.rows: element %d: %w", i, err)
					}
				}

				return starlark.None, nil
			}),

			// count() -> int
			"count": starlark.NewBuiltin("save.count", func(thread *starlark.Thread, fn *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
				return starlark.MakeInt(collector.count()), nil
			}),
		},
	}
}
