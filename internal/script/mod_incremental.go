// OndatraSQL - You don't need a data stack anymore
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package script

import (
	"go.starlark.net/starlark"
	"go.starlark.net/starlarkstruct"

	"github.com/ondatra-labs/ondatrasql/internal/backfill"
)

// incrementalModule provides cursor-based incremental loading context.
// Usage in Starlark:
//
//	if not incremental.is_backfill:
//	    url = url + "?since=" + incremental.last_value
func incrementalModule(state *backfill.IncrementalState) *starlarkstruct.Struct {
	if state == nil {
		return starlarkstruct.FromStringDict(starlark.String("incremental"), starlark.StringDict{
			"is_backfill":   starlark.True,
			"cursor":        starlark.String(""),
			"last_value":    starlark.String(""),
			"last_run":      starlark.String(""),
			"initial_value": starlark.String(""),
		})
	}

	return starlarkstruct.FromStringDict(starlark.String("incremental"), starlark.StringDict{
		"is_backfill":   starlark.Bool(state.IsBackfill),
		"cursor":        starlark.String(state.Cursor),
		"last_value":    starlark.String(state.LastValue),
		"last_run":      starlark.String(state.LastRun),
		"initial_value": starlark.String(state.InitialValue),
	})
}
