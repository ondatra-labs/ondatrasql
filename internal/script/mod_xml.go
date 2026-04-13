// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package script

import (
	"fmt"

	"github.com/clbanning/mxj/v2"
	"go.starlark.net/starlark"
	"go.starlark.net/starlarkstruct"
)

func init() {
	mxj.PrependAttrWithHyphen(false)
	mxj.SetAttrPrefix("@")
}

// xmlModule provides XML parsing and encoding functions.
func xmlModule() *starlarkstruct.Module {
	return &starlarkstruct.Module{
		Name: "xml",
		Members: starlark.StringDict{
			// decode(string) -> dict
			"decode": starlark.NewBuiltin("xml.decode", func(thread *starlark.Thread, fn *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
				var data string
				if err := starlark.UnpackPositionalArgs(fn.Name(), args, kwargs, 1, &data); err != nil {
					return nil, err
				}
				if data == "" {
					return nil, fmt.Errorf("xml.decode: empty string")
				}
				mv, err := mxj.NewMapXml([]byte(data))
				if err != nil {
					return nil, fmt.Errorf("xml.decode: %w", err)
				}
				return goToStarlark(map[string]interface{}(mv))
			}),

			// encode(dict) -> string
			"encode": starlark.NewBuiltin("xml.encode", func(thread *starlark.Thread, fn *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
				var d *starlark.Dict
				if err := starlark.UnpackPositionalArgs(fn.Name(), args, kwargs, 1, &d); err != nil {
					return nil, err
				}
				goMap, err := starlarkDictToGo(d)
				if err != nil {
					return nil, fmt.Errorf("xml.encode: %w", err)
				}
				mv := mxj.Map(goMap)
				xmlBytes, err := mv.Xml()
				if err != nil {
					return nil, fmt.Errorf("xml.encode: %w", err)
				}
				return starlark.String(xmlBytes), nil
			}),
		},
	}
}
