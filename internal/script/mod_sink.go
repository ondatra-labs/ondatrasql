// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package script

import (
	"go.starlark.net/starlark"
	"go.starlark.net/starlarkstruct"
)

// sinkModule provides batch context to push functions.
// Usage in Starlark:
//
//	if sink.batch_number == 1:
//	    clear_destination()
func sinkModule(batchNumber int) *starlarkstruct.Struct {
	return starlarkstruct.FromStringDict(starlark.String("sink"), starlark.StringDict{
		"batch_number": starlark.MakeInt(batchNumber),
	})
}
