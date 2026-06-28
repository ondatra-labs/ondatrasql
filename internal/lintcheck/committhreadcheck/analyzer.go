// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

// Package committhreadcheck enforces that every materialize write transaction
// in internal/execute/materialize.go is built through the commitTxnSQL helper
// rather than a direct sql.MustFormat("execute/commit.sql", ...) call.
//
// commitTxnSQL threads extraPreSQL — chiefly the lib-call state-store ack
// INSERTs (script.AckSQL → _ondatra_acks) — into the SAME transaction as the
// write, so a claim is acked atomically with the data it produced. A direct
// commit.sql call is how the SCD2 create/backfill and tracked create branches
// silently dropped extraPreSQL: the write committed but the ack never ran, and
// the next run re-delivered the claim (dedup drift). Routing every commit.sql
// site through the one helper makes the threading impossible to forget; this
// analyzer keeps it that way by forbidding the direct form.
//
// The sole legitimate direct call is inside commitTxnSQL itself — that function
// is exempt. A genuinely-exceptional site may opt out with a
// `//committhreadcheck:allow <reason>` comment (reason mandatory).
package committhreadcheck

import (
	"go/ast"
	"go/token"
	"path/filepath"
	"strings"

	"golang.org/x/tools/go/analysis"
	"golang.org/x/tools/go/analysis/passes/inspect"
)

// Analyzer is the committhreadcheck go/analysis Analyzer.
var Analyzer = &analysis.Analyzer{
	Name:     "committhreadcheck",
	Doc:      "forbids direct sql.MustFormat(\"execute/commit.sql\", ...) outside the commitTxnSQL helper (extraPreSQL must be threaded)",
	Requires: []*analysis.Analyzer{inspect.Analyzer},
	Run:      run,
}

// execPkgDir scopes the rule to the internal/execute package — the home of the
// commit-transaction build. Enforcing the whole package (not just
// materialize.go) means a future commit.sql site added in a sibling file is
// caught too. The path-fragment match works for both the local module layout
// and analysistest's synthetic GOPATH.
const execPkgDir = "internal/execute/"

// helperFunc is the one function allowed to call commit.sql directly — it IS
// the centralized wrapper every other site must use.
const helperFunc = "commitTxnSQL"

// commitTemplate is the embedded SQL template whose direct use is forbidden.
const commitTemplate = `"execute/commit.sql"`

// bypassMarker opts a single call out; a reason must follow.
const bypassMarker = "//committhreadcheck:allow"

func run(pass *analysis.Pass) (any, error) {
	for _, file := range pass.Files {
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			fname := filepath.ToSlash(pass.Fset.Position(fn.Pos()).Filename)
			if !strings.Contains(fname, execPkgDir) || strings.HasSuffix(fname, "_test.go") {
				continue
			}
			if fn.Name.Name == helperFunc {
				continue // the wrapper itself is the one legitimate direct call
			}
			checkFunc(pass, fn)
		}
	}
	return nil, nil
}

func checkFunc(pass *analysis.Pass, fn *ast.FuncDecl) {
	ast.Inspect(fn.Body, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok || !isCommitSQLFormat(call) {
			return true
		}
		if hasBypass(pass, call.Pos()) {
			return true
		}
		pass.Report(analysis.Diagnostic{
			Pos:     call.Pos(),
			End:     call.End(),
			Message: "direct sql.MustFormat(\"execute/commit.sql\", ...) — build the write transaction via commitTxnSQL so extraPreSQL (ack INSERTs) is threaded into it",
		})
		return true
	})
}

// isCommitSQLFormat reports whether call is `<x>.MustFormat("execute/commit.sql", ...)`.
func isCommitSQLFormat(call *ast.CallExpr) bool {
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || sel.Sel.Name != "MustFormat" || len(call.Args) == 0 {
		return false
	}
	lit, ok := call.Args[0].(*ast.BasicLit)
	return ok && lit.Kind == token.STRING && lit.Value == commitTemplate
}

// hasBypass reports whether a //committhreadcheck:allow <reason> comment sits on
// the call's line or the line above, IN THE SAME FILE. The filename guard
// matters: matching by line number alone would let a marker at the same line
// in a sibling internal/execute file silently suppress a real diagnostic.
func hasBypass(pass *analysis.Pass, pos token.Pos) bool {
	target := pass.Fset.Position(pos)
	for _, f := range pass.Files {
		for _, cg := range f.Comments {
			for _, c := range cg.List {
				if !bypassWithReason(c.Text, bypassMarker) {
					continue
				}
				cp := pass.Fset.Position(c.Pos())
				if cp.Filename == target.Filename && (cp.Line == target.Line || cp.Line == target.Line-1) {
					return true
				}
			}
		}
	}
	return false
}

// bypassWithReason reports true only when the comment text BEGINS with the
// bypass marker, the marker is terminated by whitespace, and at least one
// non-whitespace reason character follows.
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
