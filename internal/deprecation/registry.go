// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package deprecation

// Registered features. Each ID has exactly one Use call outside this
// package until the feature's RemovedIn version, and none from then on
// (TestCallSites). The entry itself stays as the record of the removal.
const (
	ModelHashComment ID = "model.hash_comment"
	LibFetchMode     ID = "lib.fetch_mode"
)

// registry lists every feature ever deprecated, oldest first. Add an entry,
// its Use call and its row in docs/support/deprecations.md in one change;
// never delete one.
var registry = []Feature{
	{
		ID:          ModelHashComment,
		Kind:        KindDirective,
		Old:         "a `#` or `//` comment line in a model header",
		Replacement: "`--`, e.g. `-- @kind: merge`",
		Since:       "0.48",
		Anchor:      "hash-comments-in-model-headers",
	},
	{
		ID:          LibFetchMode,
		Kind:        KindLib,
		Old:         "`API.fetch.mode`",
		Replacement: "`\"async\": True` in `API.fetch`, or no key for a sync fetch",
		Since:       "0.48",
		Anchor:      "the-fetch-mode-key",
	},
}
