// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

// Package strconvcheck flags strconv parse calls (Atoi, ParseInt,
// ParseUint, ParseFloat, ParseBool) whose error return is discarded via
// the blank identifier. Discarding the error means a parse failure
// silently substitutes the zero value, which has produced multiple
// real bugs in this codebase: history --json dropped row IDs that
// failed to parse, stats --json silently zeroed kind_breakdown counts,
// and tracked-model row counts read 0 instead of erroring out.
//
// The fix at each call site is one of:
//
//   - Bind the error and surface it (return / log to stderr / append
//     to a Warnings slice on the result envelope).
//   - Tag the call with `//strconvcheck:silent <reason>` if a zero
//     fallback is genuinely intended (e.g. parsing a hint where
//     "couldn't parse → ignore" is the documented behaviour).
//
// Bare `_, _ := strconv.Atoi(...)` (both results discarded) is also
// flagged — the value parse is pointless if neither output is read.
//
// Bypass marker: `//strconvcheck:silent <reason>` on the call line or
// the line directly above. A bare marker without a reason is treated
// as if absent, matching the convention enforced by the other
// in-tree analyzers.
package strconvcheck

import (
	"go/ast"
	"go/token"
	"go/types"
	"strings"

	"golang.org/x/tools/go/analysis"
	"golang.org/x/tools/go/analysis/passes/inspect"
	"golang.org/x/tools/go/ast/inspector"
)

// Analyzer is the strconvcheck go/analysis Analyzer.
var Analyzer = &analysis.Analyzer{
	Name:     "strconvcheck",
	Doc:      "flags strconv parse calls whose error result is discarded via the blank identifier",
	Requires: []*analysis.Analyzer{inspect.Analyzer},
	Run:      run,
}

const bypassMarker = "//strconvcheck:silent"

// parseFuncs lists the strconv functions whose return signature is
// (T, error) and whose silent zero fallback has bitten this codebase.
var parseFuncs = map[string]bool{
	"Atoi":       true,
	"ParseInt":   true,
	"ParseUint":  true,
	"ParseFloat": true,
	"ParseBool":  true,
}

func run(pass *analysis.Pass) (any, error) {
	insp := pass.ResultOf[inspect.Analyzer].(*inspector.Inspector)

	// Visit BOTH assignment shapes:
	//   1. AssignStmt: `v, _ := strconv.Atoi(s)` (function-scoped)
	//   2. ValueSpec:  `var v, _ = strconv.Atoi(s)` (declaration-scoped,
	//                  including package-level — pre-R7 these were
	//                  silently missed because the analyzer only
	//                  preordered AssignStmt).
	insp.Preorder([]ast.Node{(*ast.AssignStmt)(nil), (*ast.ValueSpec)(nil)}, func(n ast.Node) {
		switch node := n.(type) {
		case *ast.AssignStmt:
			checkAssign(pass, node)
		case *ast.ValueSpec:
			checkValueSpec(pass, node)
		}
	})

	return nil, nil
}

const diagnosticMessage = "strconv parse error discarded with `_` — a parse failure silently yields the zero value. Bind the error and surface it (return, log, or append to a Warnings slice) or add `" + bypassMarker + " <reason>` if zero-on-failure is intentional"

// checkAssign flags `v, _ := strconv.X(...)` — exactly one RHS call
// producing two LHS bindings where the second is the blank identifier.
func checkAssign(pass *analysis.Pass, assign *ast.AssignStmt) {
	if len(assign.Rhs) != 1 || len(assign.Lhs) != 2 {
		return
	}
	call, ok := assign.Rhs[0].(*ast.CallExpr)
	if !ok || !isStrconvParse(pass, call) {
		return
	}
	errLHS, ok := assign.Lhs[1].(*ast.Ident)
	if !ok || errLHS.Name != "_" {
		return
	}
	if hasBypassNear(pass, assign.Pos()) {
		return
	}
	pass.Report(analysis.Diagnostic{
		Pos:     assign.Pos(),
		End:     assign.End(),
		Message: diagnosticMessage,
	})
}

// checkValueSpec flags `var v, _ = strconv.X(...)` and the package-
// level form `var v, _ = strconv.X(...)`. The Names + Values shape
// mirrors AssignStmt's Lhs + Rhs but is used for declaration scope.
func checkValueSpec(pass *analysis.Pass, spec *ast.ValueSpec) {
	if len(spec.Values) != 1 || len(spec.Names) != 2 {
		return
	}
	call, ok := spec.Values[0].(*ast.CallExpr)
	if !ok || !isStrconvParse(pass, call) {
		return
	}
	if spec.Names[1] == nil || spec.Names[1].Name != "_" {
		return
	}
	if hasBypassNear(pass, spec.Pos()) {
		return
	}
	pass.Report(analysis.Diagnostic{
		Pos:     spec.Pos(),
		End:     spec.End(),
		Message: diagnosticMessage,
	})
}

// isStrconvParse reports whether call is one of the listed
// strconv.Parse* / Atoi functions.
func isStrconvParse(pass *analysis.Pass, call *ast.CallExpr) bool {
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok {
		return false
	}
	if !parseFuncs[sel.Sel.Name] {
		return false
	}
	pkgID, ok := sel.X.(*ast.Ident)
	if !ok {
		return false
	}
	obj := pass.TypesInfo.ObjectOf(pkgID)
	if obj == nil {
		return false
	}
	pn, ok := obj.(*types.PkgName)
	if !ok {
		return false
	}
	return pn.Imported().Path() == "strconv"
}

// hasBypassNear scans for the bypass marker on the given line or the
// line directly above. A bare marker without a reason is ignored.
// Used for both AssignStmt and ValueSpec call sites.
func hasBypassNear(pass *analysis.Pass, pos token.Pos) bool {
	target := pass.Fset.Position(pos).Line
	for _, f := range pass.Files {
		for _, cg := range f.Comments {
			for _, c := range cg.List {
				if !bypassWithReason(c.Text, bypassMarker) {
					continue
				}
				cline := pass.Fset.Position(c.Pos()).Line
				if cline == target || cline == target-1 {
					return true
				}
			}
		}
	}
	return false
}

// bypassWithReason reports true only when the comment text BEGINS
// with the bypass marker, the marker is terminated by whitespace, and
// at least one non-whitespace reason character follows. A bare
// `//strconvcheck:silent` is rejected so reviewers can audit each
// opt-out.
//
// HasPrefix (not Contains) is required because a marker embedded in
// prose — e.g. `// see also //strconvcheck:silent foo` — would
// otherwise silently suppress the diagnostic. Any in-line citation,
// example, or copy-paste of marker text into prose should NOT count
// as an opt-out.
func bypassWithReason(text, marker string) bool {
	if !strings.HasPrefix(text, marker) {
		return false
	}
	rest := text[len(marker):]
	if rest == "" {
		return false
	}
	// Terminator must be whitespace so longer marker names don't match
	// (e.g. `//strconvcheck:silent2 ...` is a different identifier).
	if rest[0] != ' ' && rest[0] != '\t' {
		return false
	}
	rest = strings.TrimSuffix(rest, "*/")
	rest = strings.TrimSpace(rest)
	return rest != ""
}
