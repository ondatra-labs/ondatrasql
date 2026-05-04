// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

// Package sscanfcheck flags fmt.Sscanf calls that parse integers
// or floats. strconv.ParseInt / ParseFloat returns *NumError with
// concrete categories (ErrSyntax, ErrRange) where Sscanf's error
// is opaque, and Sscanf silently falls back to the destination's
// zero value when parsing fails — a pattern that has produced real
// bugs in this codebase (sandbox row counts, tracked-model
// change-detection fast-path).
//
// The analyzer is intentionally narrow: it only flags numeric
// formats (`%d`, `%i`, `%f`, `%g`, `%e`, `%x`, `%o`, `%b`). String
// parsing with `%s` and the Scan family that requires standard-input
// interaction are out of scope.
package sscanfcheck

import (
	"go/ast"
	"go/constant"
	"go/types"
	"strings"

	"golang.org/x/tools/go/analysis"
	"golang.org/x/tools/go/analysis/passes/inspect"
	"golang.org/x/tools/go/ast/inspector"
)

// Analyzer is the sscanfcheck go/analysis Analyzer.
var Analyzer = &analysis.Analyzer{
	Name:     "sscanfcheck",
	Doc:      "flags fmt.Sscanf calls with numeric format verbs; prefer strconv.ParseInt/ParseFloat for typed errors",
	Requires: []*analysis.Analyzer{inspect.Analyzer},
	Run:      run,
}

func run(pass *analysis.Pass) (any, error) {
	insp := pass.ResultOf[inspect.Analyzer].(*inspector.Inspector)

	insp.Preorder([]ast.Node{(*ast.CallExpr)(nil)}, func(n ast.Node) {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return
		}
		if !isFmtSscanf(pass, call) {
			return
		}
		// fmt.Sscanf(str, format, args...) — format is args[1].
		if len(call.Args) < 2 {
			return
		}
		fmtArg := call.Args[1]
		lit, ok := fmtArg.(*ast.BasicLit)
		if !ok {
			// Non-literal format string — too speculative to flag.
			tv, ok := pass.TypesInfo.Types[fmtArg]
			if !ok || tv.Value == nil || tv.Value.Kind() != constant.String {
				return
			}
			if !hasNumericVerb(constant.StringVal(tv.Value)) {
				return
			}
		} else {
			s := lit.Value
			if len(s) >= 2 && s[0] == '"' && s[len(s)-1] == '"' {
				s = s[1 : len(s)-1]
			}
			if !hasNumericVerb(s) {
				return
			}
		}
		pass.Report(analysis.Diagnostic{
			Pos:     call.Pos(),
			End:     call.End(),
			Message: "fmt.Sscanf with numeric verb silently falls back to zero on parse failure; use strconv.ParseInt/ParseFloat for typed *NumError errors",
		})
	})

	return nil, nil
}

// isFmtSscanf reports whether call is fmt.Sscanf(...).
func isFmtSscanf(pass *analysis.Pass, call *ast.CallExpr) bool {
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || sel.Sel.Name != "Sscanf" {
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
	return pn.Imported().Path() == "fmt"
}

// hasNumericVerb reports whether the format string contains a verb
// that scans into an integer or float destination.
func hasNumericVerb(format string) bool {
	for i := 0; i < len(format); i++ {
		if format[i] != '%' {
			continue
		}
		// Skip flags / width modifiers and find the verb byte.
		j := i + 1
		for j < len(format) {
			c := format[j]
			// Skip width digits, +, -, space, #, 0
			if c >= '0' && c <= '9' {
				j++
				continue
			}
			if strings.ContainsRune("+-# .", rune(c)) {
				j++
				continue
			}
			break
		}
		if j >= len(format) {
			return false
		}
		switch format[j] {
		case 'd', 'i', 'f', 'g', 'e', 'E', 'x', 'X', 'o', 'b', 'u', 'U':
			return true
		case '%':
			// %% is a literal percent — skip past.
			i = j
			continue
		}
		i = j
	}
	return false
}
