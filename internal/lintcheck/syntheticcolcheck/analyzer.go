// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

// Package syntheticcolcheck enforces the kind-conversion contract in
// internal/execute/materialize.go: any materialize function that issues an
// INSERT … BY NAME while also dealing with a kind's persisted *synthetic*
// columns must first ensure those columns exist on the target.
//
// Synthetic columns are produced by the materialize step itself, not by the
// model SELECT — SCD2's valid_from_snapshot / valid_to_snapshot / is_current
// and tracked's _content_hash. Schema-evolution detection compares the SELECT
// against the stored target schema, so it never proposes the synthetic
// columns. When a model's @kind changes (e.g. table → scd2/tracked) the kind
// change forces a backfill against the *existing* target, which was built
// under the prior kind and lacks the synthetic columns. Without an explicit
// ensure step the subsequent `INSERT … BY NAME` binds against a target missing
// the column and DuckDB errors ("Table X does not have a column named …").
//
// The fix is r.addMissingSyntheticColumnsSQL, folded into the backfill
// branch's schema-evolution SQL. This analyzer pins that: a function in
// materialize.go that both (a) builds an INSERT … BY NAME statement and
// (b) references a synthetic column name must call addMissingSyntheticColumnsSQL.
// A future synthetic-column kind that adds a backfill INSERT … BY NAME but
// forgets the ensure step trips the rule.
//
// When a brand-new synthetic column name is introduced, add it to
// syntheticColumnNames so the trigger keeps covering the contract.
//
// Bypass: `//syntheticcolcheck:allow <reason>` on the function doc comment,
// for the rare function that legitimately mentions a synthetic column next to
// an unrelated BY NAME insert. The reason is mandatory.
package syntheticcolcheck

import (
	"go/ast"
	"go/token"
	"strings"

	"golang.org/x/tools/go/analysis"
	"golang.org/x/tools/go/analysis/passes/inspect"
)

// Analyzer is the syntheticcolcheck go/analysis Analyzer.
var Analyzer = &analysis.Analyzer{
	Name:     "syntheticcolcheck",
	Doc:      "requires addMissingSyntheticColumnsSQL before INSERT … BY NAME on synthetic-column kinds in materialize.go (kind-conversion gap)",
	Requires: []*analysis.Analyzer{inspect.Analyzer},
	Run:      run,
}

// targetFile is the single file the rule enforces. The suffix matches both the
// local module path and the standard import-path layout so the rule fires from
// `go test`/`go run` and from analysistest's synthetic GOPATH.
const targetFile = "internal/execute/materialize.go"

// ensureHelper is the method that adds any missing synthetic columns to the
// target before the INSERT binds.
const ensureHelper = "addMissingSyntheticColumnsSQL"

// bypassMarker opts a single function out; a reason must follow.
const bypassMarker = "//syntheticcolcheck:allow"

// syntheticColumnNames are the persisted columns produced by the materialize
// step rather than the model SELECT. Keep in sync with scd2SyntheticColumns and
// the tracked _content_hash column in materialize.go.
var syntheticColumnNames = []string{
	"valid_from_snapshot",
	"valid_to_snapshot",
	"is_current",
	"_content_hash",
}

func run(pass *analysis.Pass) (any, error) {
	for _, file := range pass.Files {
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			if !strings.HasSuffix(pass.Fset.Position(fn.Pos()).Filename, targetFile) {
				continue
			}
			checkFunc(pass, fn)
		}
	}
	return nil, nil
}

func checkFunc(pass *analysis.Pass, fn *ast.FuncDecl) {
	var byNameLit *ast.BasicLit
	sawSynthetic := false
	callsEnsure := false

	ast.Inspect(fn.Body, func(n ast.Node) bool {
		switch x := n.(type) {
		case *ast.BasicLit:
			if x.Kind != token.STRING {
				return true
			}
			up := strings.ToUpper(x.Value)
			if byNameLit == nil && strings.Contains(up, "INSERT") && strings.Contains(up, "BY NAME") {
				byNameLit = x
			}
			if referencesSynthetic(x.Value) {
				sawSynthetic = true
			}
		case *ast.SelectorExpr:
			// r.addMissingSyntheticColumnsSQL(...)
			if x.Sel.Name == ensureHelper {
				callsEnsure = true
			}
		case *ast.Ident:
			// addMissingSyntheticColumnsSQL referenced bare (e.g. method value).
			if x.Name == ensureHelper {
				callsEnsure = true
			}
		}
		return true
	})

	if byNameLit == nil || !sawSynthetic || callsEnsure {
		return
	}
	if hasBypass(pass, fn) {
		return
	}
	pass.Report(analysis.Diagnostic{
		Pos:     byNameLit.Pos(),
		End:     byNameLit.End(),
		Message: "INSERT … BY NAME for a synthetic-column kind must call " + ensureHelper +
			" first, or a table→kind conversion binds the INSERT against a target missing the synthetic column",
	})
}

// referencesSynthetic reports whether a string literal mentions any synthetic
// column name. Matching is case-sensitive — these are SQL identifiers.
func referencesSynthetic(litValue string) bool {
	for _, name := range syntheticColumnNames {
		if strings.Contains(litValue, name) {
			return true
		}
	}
	return false
}

// hasBypass reports whether fn carries a //syntheticcolcheck:allow <reason>
// doc comment.
func hasBypass(pass *analysis.Pass, fn *ast.FuncDecl) bool {
	if fn.Doc == nil {
		return false
	}
	for _, c := range fn.Doc.List {
		if bypassWithReason(c.Text, bypassMarker) {
			return true
		}
	}
	return false
}

// bypassWithReason reports true only when the comment text BEGINS with the
// bypass marker, the marker is terminated by whitespace, and at least one
// non-whitespace reason character follows. A bare marker (no reason) does not
// bypass, and a marker cited mid-comment ("// see also //foo:allow x") does
// not bypass.
func bypassWithReason(text, marker string) bool {
	if !strings.HasPrefix(text, marker) {
		return false
	}
	rest := text[len(marker):]
	if rest == "" {
		return false
	}
	if rest[0] != ' ' && rest[0] != '\t' {
		return false
	}
	rest = strings.TrimSuffix(rest, "*/")
	rest = strings.TrimSpace(rest)
	return rest != ""
}
