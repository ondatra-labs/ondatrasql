// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package script

import (
	"fmt"
	"net/url"
	"sort"

	"go.starlark.net/starlark"
	"go.starlark.net/starlarkstruct"
)

// urlModule provides URL building and encoding functions.
func urlModule() *starlarkstruct.Module {
	return &starlarkstruct.Module{
		Name: "url",
		Members: starlark.StringDict{
			// build(base, params?) -> "https://api.com?foo=bar&baz=1"
			"build": starlark.NewBuiltin("url.build", func(thread *starlark.Thread, fn *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
				var baseURL string
				var paramsDict *starlark.Dict
				if err := starlark.UnpackArgs(fn.Name(), args, kwargs,
					"base", &baseURL,
					"params?", &paramsDict,
				); err != nil {
					return nil, err
				}

				if paramsDict == nil {
					return starlark.String(baseURL), nil
				}

				u, err := url.Parse(baseURL)
				if err != nil {
					return nil, fmt.Errorf("parse url: %w", err)
				}

				q := u.Query()
				for _, item := range paramsDict.Items() {
					k, _ := starlark.AsString(item[0])
					val, err := starlarkToGo(item[1])
					if err == nil && val != nil {
						q.Set(k, fmt.Sprintf("%v", val))
					}
				}
				u.RawQuery = q.Encode()

				return starlark.String(u.String()), nil
			}),

			// encode(string) -> "hello%20world"
			"encode": starlark.NewBuiltin("url.encode", func(thread *starlark.Thread, fn *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
				var s string
				if err := starlark.UnpackPositionalArgs(fn.Name(), args, kwargs, 1, &s); err != nil {
					return nil, err
				}
				return starlark.String(url.QueryEscape(s)), nil
			}),

			// encode_params(dict) -> "foo=bar&baz=qux"
			"encode_params": starlark.NewBuiltin("url.encode_params", func(thread *starlark.Thread, fn *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
				var d *starlark.Dict
				if err := starlark.UnpackPositionalArgs(fn.Name(), args, kwargs, 1, &d); err != nil {
					return nil, err
				}

				// Sort keys for deterministic output
				keys := make([]string, 0, d.Len())
				for _, item := range d.Items() {
					k, _ := starlark.AsString(item[0])
					keys = append(keys, k)
				}
				sort.Strings(keys)

				values := url.Values{}
				for _, k := range keys {
					v, _, _ := d.Get(starlark.String(k))
					val, _ := starlarkToGo(v)
					if val != nil {
						values.Set(k, fmt.Sprintf("%v", val))
					}
				}

				return starlark.String(values.Encode()), nil
			}),

			// parse(url) -> struct{scheme, host, path, query: struct{key: value}}
			"parse": starlark.NewBuiltin("url.parse", func(thread *starlark.Thread, fn *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
				var urlStr string
				if err := starlark.UnpackPositionalArgs(fn.Name(), args, kwargs, 1, &urlStr); err != nil {
					return nil, err
				}

				u, err := url.Parse(urlStr)
				if err != nil {
					return nil, fmt.Errorf("parse url: %w", err)
				}

				queryMembers := starlark.StringDict{}
				for k, v := range u.Query() {
					if len(v) == 1 {
						queryMembers[k] = starlark.String(v[0])
					} else {
						elems := make([]starlark.Value, len(v))
						for i, val := range v {
							elems[i] = starlark.String(val)
						}
						queryMembers[k] = starlark.NewList(elems)
					}
				}

				return starlarkstruct.FromStringDict(starlark.String("url"), starlark.StringDict{
					"scheme": starlark.String(u.Scheme),
					"host":   starlark.String(u.Host),
					"path":   starlark.String(u.Path),
					"query":  starlarkstruct.FromStringDict(starlark.String("query"), queryMembers),
				}), nil
			}),
		},
	}
}
