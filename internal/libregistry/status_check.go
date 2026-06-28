// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package libregistry

import (
	"fmt"
	"strings"

	"go.starlark.net/syntax"
)

// fetchFamilyFuncs are the lib entry points that perform an HTTP fetch and whose
// returned rows the runtime treats as data. The http module does NOT raise on a
// 4xx response (only 5xx/429/transport errors fail loudly) — it returns a struct
// with ok=false. So a fetch that calls http.* but never checks resp.ok /
// resp.status_code silently turns a 404/400 into 0 rows, which for a `table` or
// `scd2` model wipes/closes the target. See the fetch-status contract.
var fetchFamilyFuncs = map[string]bool{
	"fetch": true, "fetch_page": true, "submit": true, "check": true, "fetch_result": true,
}

// uncheckedStatusBypass is the file-level opt-out marker. A reason must follow.
const uncheckedStatusBypass = "ondatracheck:allow-unchecked-status"

// checkFetchStatusHandling returns a warning for each fetch-family function that
// calls http.* but never references resp.ok / resp.status_code /
// raise_for_status and never calls fail()/abort().
//
// It is a heuristic, intra-function check (same class as the project's Go
// analyzers): a status check delegated to a shared helper is a false negative,
// and a function that legitimately maps a 4xx to empty can opt out with a
// file-level `# ondatracheck:allow-unchecked-status <reason>` comment. Surfaced
// as a warning (not an error) so it flags the footgun without breaking existing
// blueprints.
func checkFetchStatusHandling(name string, f *syntax.File, code string) []string {
	if hasUncheckedStatusBypass(code) {
		return nil
	}
	var warnings []string
	for _, stmt := range f.Stmts {
		def, ok := stmt.(*syntax.DefStmt)
		if !ok || !fetchFamilyFuncs[def.Name.Name] {
			continue
		}
		// Collect `x = http` aliases so a call through the alias (`x.get(...)`)
		// is recognized as an http call too, not just direct `http.get(...)`.
		httpNames := map[string]bool{"http": true}
		syntax.Walk(def, func(n syntax.Node) bool {
			if a, ok := n.(*syntax.AssignStmt); ok {
				if rhs, ok := a.RHS.(*syntax.Ident); ok && rhs.Name == "http" {
					if lhs, ok := a.LHS.(*syntax.Ident); ok {
						httpNames[lhs.Name] = true
					}
				}
			}
			return true
		})

		var hasHTTP, hasGuard bool
		syntax.Walk(def, func(n syntax.Node) bool {
			switch e := n.(type) {
			case *syntax.CallExpr:
				// http.get / http.post / http.request / ... (incl. aliases)
				if dot, ok := e.Fn.(*syntax.DotExpr); ok {
					if id, ok := dot.X.(*syntax.Ident); ok && httpNames[id.Name] {
						hasHTTP = true
					}
				}
				// fail(...) / abort(...) halt the run on a bad status.
				if id, ok := e.Fn.(*syntax.Ident); ok && (id.Name == "fail" || id.Name == "abort") {
					hasGuard = true
				}
			case *syntax.DotExpr:
				// resp.ok / resp.status_code / resp.raise_for_status
				switch e.Name.Name {
				case "ok", "status_code", "raise_for_status":
					hasGuard = true
				}
			}
			return true
		})
		if hasHTTP && !hasGuard {
			warnings = append(warnings, fmt.Sprintf(
				"lib %q: %s() reads http.* without checking resp.ok — a 4xx becomes a silent 0-row return (wipes a table/scd2 target); add `if not resp.ok: fail(...)`, or bypass with `# %s <reason>`",
				name, def.Name.Name, uncheckedStatusBypass))
		}
	}
	return warnings
}

// hasUncheckedStatusBypass reports whether the source carries a file-level
// `# ondatracheck:allow-unchecked-status <reason>` comment. The marker must be
// the whole token (terminated by whitespace, so `…-status-anything` does NOT
// match) and a non-empty reason must follow. Heuristic: this is a raw-line
// scan, so a line inside a triple-quoted string that exactly mimics the marker
// would also count — acceptable for an opt-out (the contrived false-positive
// only ever *suppresses* a warning the author can re-enable).
func hasUncheckedStatusBypass(code string) bool {
	for _, line := range strings.Split(code, "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "#") {
			continue
		}
		rest := strings.TrimSpace(line[1:]) // text after the leading '#'
		if !strings.HasPrefix(rest, uncheckedStatusBypass) {
			continue
		}
		after := rest[len(uncheckedStatusBypass):]
		// The marker must be terminated by whitespace, then a non-empty reason.
		if after == "" || (after[0] != ' ' && after[0] != '\t') {
			continue
		}
		if strings.TrimSpace(after) != "" {
			return true
		}
	}
	return false
}
