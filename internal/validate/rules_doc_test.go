// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package validate

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"regexp"
	"strconv"
	"testing"
)

// Every rule ID declared in rules.go is in the catalogue at
// docs/reference/pipeline/validate-rules.md with its severity, and the
// catalogue lists nothing else.
func TestRuleIDs_AreDocumented(t *testing.T) {
	t.Parallel()
	f, err := parser.ParseFile(token.NewFileSet(), "rules.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	declared := map[RuleID]bool{}
	ast.Inspect(f, func(n ast.Node) bool {
		spec, ok := n.(*ast.ValueSpec)
		if !ok || len(spec.Values) != 1 {
			return true
		}
		if id, ok := spec.Type.(*ast.Ident); !ok || id.Name != "RuleID" {
			return true
		}
		if lit, ok := spec.Values[0].(*ast.BasicLit); ok {
			v, _ := strconv.Unquote(lit.Value)
			declared[RuleID(v)] = true
		}
		return true
	})
	doc, err := os.ReadFile("../../docs/reference/pipeline/validate-rules.md")
	if err != nil {
		t.Fatal(err)
	}
	rows := regexp.MustCompile("(?m)^\\| `([a-z_.]+)` \\| (BLOCKER|WARN|INFO) \\|").FindAllStringSubmatch(string(doc), -1)
	documented := map[RuleID]bool{}
	for _, r := range rows {
		id := RuleID(r[1])
		documented[id] = true
		if !declared[id] {
			t.Errorf("the catalogue lists %s, which rules.go does not declare", id)
			continue
		}
		if got := string(SeverityFor(id)); got != r[2] {
			t.Errorf("%s: catalogue says %s, SeverityFor says %s", id, r[2], got)
		}
	}
	for id := range declared {
		if !documented[id] {
			t.Errorf("%s is not in docs/reference/pipeline/validate-rules.md", id)
		}
	}
	if len(declared) < 50 {
		t.Fatalf("found %d rule IDs in rules.go", len(declared))
	}
}
