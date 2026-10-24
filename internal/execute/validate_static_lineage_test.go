// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package execute

import (
	"strings"
	"testing"

	"github.com/ondatra-labs/ondatrasql/internal/parser"
)

// The run fails when it cannot read a model's source tables, or for append
// and merge its column lineage, so validate reports both as BLOCKERs: BLOCKER
// means "would fail at run". An AST with no statements is one of either.
func TestValidateModelStatic_LineageFailuresMatchTheRun(t *testing.T) {
	t.Parallel()
	const noStatements = `{"error":false,"statements":[]}`
	for kind, want := range map[string][]string{
		"table":  {"source tables"},
		"append": {"source tables", "column lineage"},
		"merge":  {"source tables", "column lineage"},
	} {
		results := ValidateModelStatic(&parser.Model{Kind: kind, Target: "s.t"}, noStatements, nil, StaticOptions{})
		var got []string
		for _, r := range results {
			if r.Category != "parser" {
				continue
			}
			got = append(got, r.Err.Error())
		}
		joined := strings.Join(got, "\n")
		for _, w := range want {
			if !strings.Contains(joined, w) {
				t.Errorf("%s: results %q do not report the %s", kind, got, w)
			}
		}
		if kind == "table" && strings.Contains(joined, "column lineage") {
			t.Errorf("table: column lineage is not needed by the run, got %q", got)
		}
	}
}
