// OndatraSQL - You don't need a data stack anymore
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package script

import (
	"os"

	"go.starlark.net/starlark"
	"go.starlark.net/starlarkstruct"
)

// envModule provides environment variable access.
func envModule() *starlarkstruct.Module {
	return &starlarkstruct.Module{
		Name: "env",
		Members: starlark.StringDict{
			// get(name, default?) -> string
			"get": starlark.NewBuiltin("env.get", func(thread *starlark.Thread, fn *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
				var name string
				var defaultVal starlark.String
				if err := starlark.UnpackArgs(fn.Name(), args, kwargs,
					"name", &name,
					"default?", &defaultVal,
				); err != nil {
					return nil, err
				}

				value := os.Getenv(name)
				if value == "" && string(defaultVal) != "" {
					value = string(defaultVal)
				}

				return starlark.String(value), nil
			}),

			// set(name, value) -> None
			"set": starlark.NewBuiltin("env.set", func(thread *starlark.Thread, fn *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
				var name, value string
				if err := starlark.UnpackPositionalArgs(fn.Name(), args, kwargs, 2, &name, &value); err != nil {
					return nil, err
				}

				os.Setenv(name, value)
				return starlark.None, nil
			}),
		},
	}
}
