// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

// Package pushauthcheck enforces that every call to a RunPush*
// method (RunPush, RunPushFinalize, RunPushPoll, ...) includes a
// call to httpConfigFromLib(...) among its arguments. The
// httpConfigFromLib argument carries the OAuth2/PAT credentials
// the push runtime needs to authenticate the outbound request;
// dropping it produces silent unauthenticated calls that fail with
// 401 at the destination.
//
// The grep this replaces was:
//
//	grep -nE 'RunPush[A-Za-z]*\(' internal/execute/push.go | grep -v httpConfigFromLib
//
// AST detection is more precise: the grep matched any line
// containing a RunPush call without httpConfigFromLib on the same
// line, which would miss multi-line calls where httpConfigFromLib
// happens to wrap to a separate line.
package pushauthcheck

import (
	"go/ast"
	"strings"

	"golang.org/x/tools/go/analysis"
	"golang.org/x/tools/go/analysis/passes/inspect"
	"golang.org/x/tools/go/ast/inspector"
)

// Analyzer is the pushauthcheck go/analysis Analyzer.
var Analyzer = &analysis.Analyzer{
	Name:     "pushauthcheck",
	Doc:      "checks that RunPush* method calls include httpConfigFromLib(...) for auth credentials",
	Requires: []*analysis.Analyzer{inspect.Analyzer},
	Run:      run,
}

func run(pass *analysis.Pass) (any, error) {
	insp := pass.ResultOf[inspect.Analyzer].(*inspector.Inspector)
	insp.Preorder([]ast.Node{(*ast.CallExpr)(nil)}, func(n ast.Node) {
		call := n.(*ast.CallExpr)
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			return
		}
		if !strings.HasPrefix(sel.Sel.Name, "RunPush") {
			return
		}
		// Test fixtures legitimately stub the http config; the rule
		// enforces the production-flow contract only.
		filename := pass.Fset.Position(call.Pos()).Filename
		if strings.HasSuffix(filename, "_test.go") {
			return
		}
		if hasHTTPConfigArg(call) {
			return
		}
		pass.Report(analysis.Diagnostic{
			Pos:     call.Pos(),
			End:     call.End(),
			Message: sel.Sel.Name + " call lacks an httpConfigFromLib(...) argument; without auth credentials the outbound push will fail with 401",
		})
	})
	return nil, nil
}

// hasHTTPConfigArg reports whether any argument is a call to
// httpConfigFromLib (regardless of receiver / package qualifier).
func hasHTTPConfigArg(call *ast.CallExpr) bool {
	for _, arg := range call.Args {
		argCall, ok := arg.(*ast.CallExpr)
		if !ok {
			continue
		}
		switch fn := argCall.Fun.(type) {
		case *ast.Ident:
			if fn.Name == "httpConfigFromLib" {
				return true
			}
		case *ast.SelectorExpr:
			if fn.Sel.Name == "httpConfigFromLib" {
				return true
			}
		}
	}
	return false
}
