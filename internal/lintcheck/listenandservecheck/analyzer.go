// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

// Package listenandservecheck flags net/http Server.Serve and
// Server.ListenAndServe call sites whose returned error isn't
// matched against http.ErrServerClosed.
//
// http.ErrServerClosed is the documented "graceful shutdown" sentinel
// returned by both functions when Server.Shutdown was the cause.
// Callers that propagate the error verbatim turn a normal SIGINT into
// an apparent failure: a server that surfaces ListenAndServe's result
// to its supervisor without filtering ErrServerClosed makes every
// clean stop look like a crash.
//
// Allowed shapes (any of these makes the call safe):
//
//	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) { ... }
//	if err := srv.Serve(l); !errors.Is(err, http.ErrServerClosed) { ... }
//	err := srv.Serve(l); if !errors.Is(err, http.ErrServerClosed) && err != nil { ... }
//
// The analyzer flags any other shape — including bare-statement and
// returned-as-is.
package listenandservecheck

import (
	"go/ast"
	"go/types"

	"golang.org/x/tools/go/analysis"
	"golang.org/x/tools/go/analysis/passes/inspect"
	"golang.org/x/tools/go/ast/inspector"
)

// Analyzer is the listenandservecheck go/analysis Analyzer.
var Analyzer = &analysis.Analyzer{
	Name:     "listenandservecheck",
	Doc:      "checks that http.Server.Serve / ListenAndServe results are matched against http.ErrServerClosed",
	Requires: []*analysis.Analyzer{inspect.Analyzer},
	Run:      run,
}

func run(pass *analysis.Pass) (any, error) {
	insp := pass.ResultOf[inspect.Analyzer].(*inspector.Inspector)

	// Walk function bodies so we can see the call's enclosing
	// statement and confirm the result is bound and checked.
	insp.Preorder([]ast.Node{(*ast.CallExpr)(nil)}, func(n ast.Node) {
		call := n.(*ast.CallExpr)
		if !isHTTPServerListenOrServe(pass, call) {
			return
		}

		// The call may appear inside an `if err := ...; cond` (good
		// shape), an assignment whose `err` is later checked (good if
		// errors.Is(..., http.ErrServerClosed) is in scope), or as a
		// bare statement / returned verbatim (bad).
		path, _ := pathTo(pass, call)
		if isInsideErrServerClosedCheck(pass, path) {
			return
		}
		pass.Report(analysis.Diagnostic{
			Pos:     call.Pos(),
			End:     call.End(),
			Message: "http.Server.Serve/ListenAndServe returns http.ErrServerClosed on graceful shutdown; the call site must check via errors.Is(err, http.ErrServerClosed) so SIGINT doesn't look like a real error",
		})
	})

	return nil, nil
}

// isHTTPServerListenOrServe reports whether call is
// (*http.Server).Serve or (*http.Server).ListenAndServe.
func isHTTPServerListenOrServe(pass *analysis.Pass, call *ast.CallExpr) bool {
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok {
		return false
	}
	name := sel.Sel.Name
	if name != "Serve" && name != "ListenAndServe" && name != "ServeTLS" && name != "ListenAndServeTLS" {
		return false
	}
	// Receiver must be *http.Server (or http.Server).
	tv, ok := pass.TypesInfo.Types[sel.X]
	if !ok {
		return false
	}
	t := tv.Type
	if ptr, ok := t.(*types.Pointer); ok {
		t = ptr.Elem()
	}
	named, ok := t.(*types.Named)
	if !ok {
		return false
	}
	obj := named.Obj()
	if obj == nil || obj.Pkg() == nil {
		return false
	}
	return obj.Pkg().Path() == "net/http" && obj.Name() == "Server"
}

// pathTo returns the AST path from the call up to the file root.
// Approximation: re-walks the file tree because go/analysis doesn't
// hand us a positional path directly. Acceptable cost — analyzers
// run once per package, not per request.
func pathTo(pass *analysis.Pass, target *ast.CallExpr) ([]ast.Node, bool) {
	for _, f := range pass.Files {
		var path []ast.Node
		var found bool
		ast.Inspect(f, func(n ast.Node) bool {
			if found {
				return false
			}
			if n == nil {
				if len(path) > 0 {
					path = path[:len(path)-1]
				}
				return false
			}
			path = append(path, n)
			if n == target {
				found = true
				return false
			}
			return true
		})
		if found {
			out := make([]ast.Node, len(path))
			copy(out, path)
			return out, true
		}
	}
	return nil, false
}

// isInsideErrServerClosedCheck reports whether the call's enclosing
// expression structure protects against http.ErrServerClosed being
// surfaced as an error.
//
// Recognised patterns:
//
//   - Direct: `if err := <call>; err != nil && !errors.Is(err, http.ErrServerClosed) { ... }`
//   - Inside an assignment + later if-stmt whose CONDITION references
//     both the assigned err variable AND http.ErrServerClosed.
//
// The "if-condition references both" rule was tightened in R6 — the
// previous check counted any mention of http.ErrServerClosed anywhere
// in subsequent statements (including nested/unreachable blocks),
// which let dead branches silently suppress the diagnostic.
//
// Conservative on purpose: false positives surface as a clear
// diagnostic that's resolvable with a one-line fix; false negatives
// would let the original bug recur.
func isInsideErrServerClosedCheck(pass *analysis.Pass, path []ast.Node) bool {
	// Walk up looking for an enclosing if-stmt or assignment whose
	// condition / sibling stmts reference http.ErrServerClosed.
	for i := len(path) - 1; i >= 0; i-- {
		node := path[i]
		switch s := node.(type) {
		case *ast.IfStmt:
			if mentionsErrServerClosed(pass, s.Cond) {
				return true
			}
		case *ast.AssignStmt:
			// `_ = srv.Serve(l)` is NOT a check; the test is whether a
			// subsequent SIBLING if-stmt's CONDITION (not its body or
			// any nested block) tests the assigned err against
			// http.ErrServerClosed.
			errName := errVarName(s)
			if errName == "" {
				continue
			}
			if i == 0 {
				continue
			}
			blk, ok := path[i-1].(*ast.BlockStmt)
			if !ok {
				continue
			}
			for j, st := range blk.List {
				if st != s {
					continue
				}
				for _, next := range blk.List[j+1:] {
					if checksErrViaSentinel(pass, next, errName) {
						return true
					}
				}
				break
			}
		}
	}
	return false
}

// errVarName returns the LHS identifier name of an AssignStmt that
// binds a single error result (`err := srv.Serve(l)` etc.). Returns
// "" if the LHS isn't a simple identifier (multi-assign, blank, etc.).
func errVarName(assign *ast.AssignStmt) string {
	if len(assign.Lhs) != 1 {
		return ""
	}
	id, ok := assign.Lhs[0].(*ast.Ident)
	if !ok || id.Name == "_" {
		return ""
	}
	return id.Name
}

// checksErrViaSentinel reports whether stmt is an IfStmt whose
// CONDITION (not body) tests `errName` against http.ErrServerClosed.
// Looking only at the condition prevents `if false { errors.Is(...) }`
// dead branches from suppressing the diagnostic.
func checksErrViaSentinel(pass *analysis.Pass, stmt ast.Stmt, errName string) bool {
	ifStmt, ok := stmt.(*ast.IfStmt)
	if !ok {
		return false
	}
	if !condReferencesIdent(ifStmt.Cond, errName) {
		return false
	}
	return mentionsErrServerClosed(pass, ifStmt.Cond)
}

// condReferencesIdent reports whether the (non-nil) expression
// references an identifier named `name`.
func condReferencesIdent(expr ast.Expr, name string) bool {
	if expr == nil {
		return false
	}
	found := false
	ast.Inspect(expr, func(n ast.Node) bool {
		if found {
			return false
		}
		id, ok := n.(*ast.Ident)
		if ok && id.Name == name {
			found = true
			return false
		}
		return true
	})
	return found
}

// mentionsErrServerClosed walks a sub-tree looking for a reference to
// net/http.ErrServerClosed.
func mentionsErrServerClosed(pass *analysis.Pass, n ast.Node) bool {
	if n == nil {
		return false
	}
	found := false
	ast.Inspect(n, func(node ast.Node) bool {
		if found {
			return false
		}
		sel, ok := node.(*ast.SelectorExpr)
		if !ok || sel.Sel.Name != "ErrServerClosed" {
			return true
		}
		if id, ok := sel.X.(*ast.Ident); ok {
			obj := pass.TypesInfo.ObjectOf(id)
			if pn, ok := obj.(*types.PkgName); ok && pn.Imported().Path() == "net/http" {
				found = true
				return false
			}
		}
		return true
	})
	return found
}
