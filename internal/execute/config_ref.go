// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package execute

import (
	"sort"
	"strings"

	"github.com/ondatra-labs/ondatrasql/internal/parser"
)

// configRefText assembles every part of a model that can name a config macro
// or variable. backfill.ConfigIndex scans it for identifiers to decide which
// config fragments the model depends on, so anything left out here is a
// config change the model will not be rebuilt for.
//
// The surfaces:
//
//   - SQL — the model body (or the Starlark script, for .star models), which
//     covers macro calls and getvariable('name').
//   - @constraint / @audit / @warning — dispatched by internal/validation as
//     ondatra_{constraint,audit,warning}_{name}, so the prefix has to be
//     reattached or the fragment name never matches.
//   - @column tags — a masking tag is the macro name verbatim
//     (see getMaskingMacro).
func configRefText(model *parser.Model) string {
	var b strings.Builder
	b.WriteString(model.SQL)

	writeDispatched := func(prefix string, directives []string) {
		for _, d := range directives {
			b.WriteString("\n")
			b.WriteString(prefix)
			b.WriteString(strings.TrimSpace(d))
		}
	}
	writeDispatched("ondatra_constraint_", model.Constraints)
	writeDispatched("ondatra_audit_", model.Audits)
	writeDispatched("ondatra_warning_", model.Warnings)

	// Map iteration is unordered; sort so the reference text — and therefore
	// the identifier set — is stable across runs.
	for _, col := range sortedTagColumns(model.ColumnTags) {
		for _, tag := range model.ColumnTags[col] {
			b.WriteString("\n")
			b.WriteString(tag)
		}
	}

	return b.String()
}

func sortedTagColumns(tags map[string][]string) []string {
	cols := make([]string, 0, len(tags))
	for col := range tags {
		cols = append(cols, col)
	}
	sort.Strings(cols)
	return cols
}
