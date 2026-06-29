// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package script

import (
	"fmt"

	goyaml "github.com/goccy/go-yaml"
	"go.starlark.net/starlark"
	"go.starlark.net/starlarkstruct"
)

// yamlModule provides YAML parsing and encoding, mirroring the json module's
// decode/encode pair. The common use is reading YAML frontmatter from Markdown
// files (split the `---` block out of read_text(...) and pass it to
// yaml.decode) or parsing YAML config fetched over http.
func yamlModule() *starlarkstruct.Module {
	return &starlarkstruct.Module{
		Name: "yaml",
		Members: starlark.StringDict{
			// decode(string) -> dict | list | scalar | None
			//
			// Mappings become dicts, sequences become lists, scalars become the
			// matching Starlark type. An empty, whitespace-only, or comment-only
			// document decodes to None, so an empty frontmatter block is not an
			// error — note this differs from json.decode/xml.decode, which error
			// on empty input. For a multi-document stream (--- separators), only
			// the first document is returned.
			"decode": starlark.NewBuiltin("yaml.decode", func(thread *starlark.Thread, fn *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
				var data string
				if err := starlark.UnpackPositionalArgs(fn.Name(), args, kwargs, 1, &data); err != nil {
					return nil, err
				}
				var v interface{}
				if err := goyaml.Unmarshal([]byte(data), &v); err != nil {
					return nil, fmt.Errorf("yaml.decode: %w", err)
				}
				return goToStarlark(v)
			}),

			// encode(value) -> string
			"encode": starlark.NewBuiltin("yaml.encode", func(thread *starlark.Thread, fn *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
				var v starlark.Value
				if err := starlark.UnpackPositionalArgs(fn.Name(), args, kwargs, 1, &v); err != nil {
					return nil, err
				}
				goVal, err := starlarkToGo(v)
				if err != nil {
					return nil, fmt.Errorf("yaml.encode: %w", err)
				}
				out, err := goyaml.Marshal(goVal)
				if err != nil {
					return nil, fmt.Errorf("yaml.encode: %w", err)
				}
				return starlark.String(out), nil
			}),
		},
	}
}
