// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

// Package pushdeltacheck enforces that internal/execute/push_delta.go
// is kind-agnostic — every supported kind goes through the same
// table_changes() query path. References to model.Kind in that file
// indicate someone is special-casing a kind, which has historically
// produced subtle CDC drift bugs (a kind getting different delta
// semantics than the others).
//
// The grep this analyzer replaces was:
//
//	grep -nE '\bmodel\.Kind\b|\.Kind\b' internal/execute/push_delta.go
//
// AST detection is more precise: the grep flagged any `.Kind` field
// access, including unrelated ones in test fixtures. The analyzer
// only flags `parser.Model.Kind` reads inside push_delta.go.
//
// The rule has no bypass marker — push_delta.go is a closed contract,
// and any legitimate need to special-case a kind belongs in the kind
// dispatch in materialize.go, not in delta computation.
package pushdeltacheck

import (
	"go/ast"
	"go/types"
	"strings"

	"golang.org/x/tools/go/analysis"
	"golang.org/x/tools/go/analysis/passes/inspect"
	"golang.org/x/tools/go/ast/inspector"
)

// Analyzer is the pushdeltacheck go/analysis Analyzer.
var Analyzer = &analysis.Analyzer{
	Name:     "pushdeltacheck",
	Doc:      "forbids parser.Model.Kind references inside internal/execute/push_delta.go (delta computation must be kind-agnostic)",
	Requires: []*analysis.Analyzer{inspect.Analyzer},
	Run:      run,
}

// targetFile is the single file we enforce the rule on. The full
// path suffix matches both the local module path and the standard
// import-path layout so the rule fires from `go test`/`go run`.
const targetFile = "internal/execute/push_delta.go"

func run(pass *analysis.Pass) (any, error) {
	insp := pass.ResultOf[inspect.Analyzer].(*inspector.Inspector)
	insp.Preorder([]ast.Node{(*ast.SelectorExpr)(nil)}, func(n ast.Node) {
		sel := n.(*ast.SelectorExpr)
		if sel.Sel.Name != "Kind" {
			return
		}
		// Confirm we're inside push_delta.go.
		filename := pass.Fset.Position(sel.Pos()).Filename
		if !strings.HasSuffix(filename, targetFile) {
			return
		}
		// Confirm the receiver type is parser.Model. Use the
		// underlying selection's recv type, which works for both
		// value and pointer receivers; fall back to TypesInfo.Types
		// if the selection isn't recorded (e.g. unresolved symbol).
		var t types.Type
		if selObj, ok := pass.TypesInfo.Selections[sel]; ok {
			t = selObj.Recv()
		} else if tv, ok := pass.TypesInfo.Types[sel.X]; ok {
			t = tv.Type
		}
		if t == nil {
			return
		}
		if ptr, ok := t.(*types.Pointer); ok {
			t = ptr.Elem()
		}
		named, ok := t.(*types.Named)
		if !ok {
			return
		}
		obj := named.Obj()
		if obj == nil || obj.Pkg() == nil {
			return
		}
		// Match either the canonical path or the bare "parser"
		// segment so the rule fires regardless of module layout.
		path := obj.Pkg().Path()
		if !strings.HasSuffix(path, "internal/parser") && !strings.HasSuffix(path, "/parser") && path != "parser" {
			return
		}
		if obj.Name() != "Model" {
			return
		}
		pass.Report(analysis.Diagnostic{
			Pos:     sel.Pos(),
			End:     sel.End(),
			Message: "model.Kind reference inside push_delta.go — push-delta must be kind-agnostic; if a kind needs different delta semantics, special-case in materialize.go's kind dispatch instead",
		})
	})
	return nil, nil
}
