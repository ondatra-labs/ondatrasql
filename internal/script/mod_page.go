// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package script

import (
	"go.starlark.net/starlark"
	"go.starlark.net/starlarkstruct"
)

// pageModule provides pagination context to fetch functions.
// Usage in Starlark:
//
//	resp = http.get(url, params={"limit": page.size, "after": page.cursor})
func pageModule(cursor starlark.Value, size int) *starlarkstruct.Struct {
	if cursor == nil {
		cursor = starlark.None
	}
	return starlarkstruct.FromStringDict(starlark.String("page"), starlark.StringDict{
		"cursor": cursor,
		"size":   starlark.MakeInt(size),
	})
}
