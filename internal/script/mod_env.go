// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package script

import (
	"fmt"
	"os"
	"strings"

	"github.com/ondatra-labs/ondatrasql/internal/httpcassette"

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

				value, exists := os.LookupEnv(name)
				if !exists && string(defaultVal) != "" {
					value = string(defaultVal)
				}
				if looksSecret(name) {
					httpcassette.RegisterSecret(value)
				}

				return starlark.String(value), nil
			}),

			// set(name, value) -> None
			"set": starlark.NewBuiltin("env.set", func(thread *starlark.Thread, fn *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
				var name, value string
				if err := starlark.UnpackPositionalArgs(fn.Name(), args, kwargs, 2, &name, &value); err != nil {
					return nil, err
				}

				if err := os.Setenv(name, value); err != nil {
					return nil, fmt.Errorf("env.set(%q): %w", name, err)
				}
				return starlark.None, nil
			}),
		},
	}
}

// secretNameFragments mark an environment variable as a credential. A
// blueprint that reads one with env.get and puts it in a header or body
// itself (rather than through the API dict's auth) still gets it masked in
// a recorded cassette.
var secretNameFragments = []string{"KEY", "TOKEN", "SECRET", "PASSWORD", "PASSWD", "PASS", "PWD", "CREDENTIAL", "AUTH", "PRIVATE"}

func looksSecret(name string) bool {
	n := strings.ToUpper(name)
	for _, f := range secretNameFragments {
		if strings.Contains(n, f) {
			return true
		}
	}
	return false
}
