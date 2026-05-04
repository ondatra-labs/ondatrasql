// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

// Package escapesqlcheck enforces that values interpolated into
// quoted-string SQL contexts (ATTACH '<conn>', SET search_path = '<v>',
// and similar) are wrapped in EscapeSQL — never raw.
//
// The historic greps this analyzer replaces were:
//
//   - bugcheck-static "attach-no-escape": ATTACH '%s' without EscapeSQL
//   - bugcheck-static "search-path-no-escape": SET search_path = '%s'
//     without EscapeSQL (the regex was too convoluted to maintain)
//
// AST detection is more precise: the grep version had to use
// negative-lookalikes to detect the absence of EscapeSQL; the
// analyzer matches the call shape directly and consults the safe
// function table.
//
// Bypass marker: //escapesqlcheck:trusted-input on the call line or
// directly above. Reason required.
package escapesqlcheck

import (
	"go/ast"
	"go/constant"
	"go/types"
	"strings"

	"golang.org/x/tools/go/analysis"
	"golang.org/x/tools/go/analysis/passes/inspect"
	"golang.org/x/tools/go/ast/inspector"
)

// Analyzer is the escapesqlcheck go/analysis Analyzer.
var Analyzer = &analysis.Analyzer{
	Name:     "escapesqlcheck",
	Doc:      "checks ATTACH/SET search_path interpolations wrap their values in EscapeSQL",
	Requires: []*analysis.Analyzer{inspect.Analyzer},
	Run:      run,
}

const bypassMarker = "//escapesqlcheck:trusted-input"

// dangerousQuotedPatterns are substrings of the format string that
// indicate a value is being interpolated INSIDE a single-quoted SQL
// string literal under a high-risk keyword. The verb that follows
// the opening quote consumes one positional argument; that argument
// must be wrapped with EscapeSQL.
//
// We match the full quoted form ("ATTACH '%s'" / "ATTACH '%v'") so
// the rule doesn't fire on identifier interpolations like
// "ATTACH %s AS lake" (which is a different — more direct — issue
// the validator below catches once we add it).
var dangerousQuotedPatterns = []string{
	"ATTACH '%s'",
	"ATTACH '%v'",
	"SET search_path = '%s'",
	"SET search_path = '%v'",
	"search_path = '%s",
	"search_path = '%v",
}

var safeArgFunctionNames = map[string]bool{
	"EscapeSQL":    true,
	"escapeSQL":    true,
	"escapeAckSQL": true,
	"escSyncSQL":   true,
}

func run(pass *analysis.Pass) (any, error) {
	insp := pass.ResultOf[inspect.Analyzer].(*inspector.Inspector)
	insp.Preorder([]ast.Node{(*ast.CallExpr)(nil)}, func(n ast.Node) {
		call := n.(*ast.CallExpr)
		if !isFmtSprintf(pass, call) {
			return
		}
		if len(call.Args) < 2 {
			return
		}
		fmtStr, ok := constStringValue(pass, call.Args[0])
		if !ok {
			return
		}
		// For each dangerous pattern, find the verb's position in the
		// format string and identify which positional arg it consumes.
		for _, pat := range dangerousQuotedPatterns {
			idx := strings.Index(fmtStr, pat)
			if idx < 0 {
				continue
			}
			// Count verbs before idx to find the arg index.
			verbsBefore := countVerbs(fmtStr[:idx])
			argIdx := verbsBefore + 1 // +1 because call.Args[0] is the format
			if argIdx >= len(call.Args) {
				continue
			}
			if isSafeArg(call.Args[argIdx]) {
				continue
			}
			if hasBypassComment(pass, call) {
				return
			}
			pass.Report(analysis.Diagnostic{
				Pos:     call.Args[argIdx].Pos(),
				End:     call.Args[argIdx].End(),
				Message: "value interpolated into ATTACH/SET-search_path string literal must be wrapped with EscapeSQL — raw interpolation enables SQL injection. Add `" + bypassMarker + " <reason>` if the value is provably safe.",
			})
			return
		}
	})
	return nil, nil
}

func isFmtSprintf(pass *analysis.Pass, call *ast.CallExpr) bool {
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || sel.Sel.Name != "Sprintf" {
		return false
	}
	id, ok := sel.X.(*ast.Ident)
	if !ok {
		return false
	}
	pn, ok := pass.TypesInfo.ObjectOf(id).(*types.PkgName)
	if !ok {
		return false
	}
	return pn.Imported().Path() == "fmt"
}

func constStringValue(pass *analysis.Pass, expr ast.Expr) (string, bool) {
	tv, ok := pass.TypesInfo.Types[expr]
	if ok && tv.Value != nil && tv.Value.Kind() == constant.String {
		return constant.StringVal(tv.Value), true
	}
	lit, ok := expr.(*ast.BasicLit)
	if !ok || lit.Kind.String() != "STRING" {
		return "", false
	}
	s := lit.Value
	if len(s) >= 2 && s[0] == '"' && s[len(s)-1] == '"' {
		return s[1 : len(s)-1], true
	}
	if len(s) >= 2 && s[0] == '`' && s[len(s)-1] == '`' {
		return s[1 : len(s)-1], true
	}
	return "", false
}

// countVerbs counts non-literal `%X` verbs in a substring of the
// format string, ignoring `%%`.
func countVerbs(s string) int {
	count := 0
	for i := 0; i < len(s); i++ {
		if s[i] != '%' {
			continue
		}
		j := i + 1
		for j < len(s) {
			c := s[j]
			if (c >= '0' && c <= '9') || strings.ContainsRune("+-# .", rune(c)) {
				j++
				continue
			}
			break
		}
		if j >= len(s) {
			return count
		}
		if s[j] == '%' {
			i = j
			continue
		}
		count++
		i = j
	}
	return count
}

func isSafeArg(arg ast.Expr) bool {
	call, ok := arg.(*ast.CallExpr)
	if !ok {
		return false
	}
	switch fn := call.Fun.(type) {
	case *ast.Ident:
		return safeArgFunctionNames[fn.Name]
	case *ast.SelectorExpr:
		return safeArgFunctionNames[fn.Sel.Name]
	}
	return false
}

func hasBypassComment(pass *analysis.Pass, call *ast.CallExpr) bool {
	pos := call.Pos()
	for _, f := range pass.Files {
		for _, cg := range f.Comments {
			for _, c := range cg.List {
				if !bypassWithReason(c.Text, bypassMarker) {
					continue
				}
				cline := pass.Fset.Position(c.Pos()).Line
				callLine := pass.Fset.Position(pos).Line
				if cline == callLine || cline == callLine-1 {
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
// `//escapesqlcheck:trusted-input` is rejected.
//
// HasPrefix (not Contains) is required because a marker embedded in
// prose — e.g. `// see also //escapesqlcheck:trusted-input foo` —
// would otherwise silently suppress the diagnostic. Any in-line
// citation, example, or copy-paste of marker text into prose should
// NOT count as an opt-out.
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
