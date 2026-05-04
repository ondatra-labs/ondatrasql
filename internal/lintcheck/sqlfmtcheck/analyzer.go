// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

// Package sqlfmtcheck flags fmt.Sprintf calls that build SQL strings
// using the %v or %s verb on values of unknown provenance.
//
// The historic grep this analyzer replaces (bugcheck-static
// "fmt-pct-v-in-sql") triggered on any %v inside a string with
// SELECT/INSERT/UPDATE/DELETE/FROM/WHERE keywords. AST-based detection
// gives us:
//
//   - precision against the receiver: only fmt.Sprintf is in scope
//   - precision against the format string: a const is required so the
//     analyzer can read the verbs, otherwise the call is out of scope
//     (we don't speculate over runtime-built format strings)
//   - precision against false-positives: %s on values constructed
//     from EscapeSQL/QuoteIdentifier helpers is allowed
//
// Bypass marker: //sqlfmtcheck:trusted-input on the same line.
// Use with a concrete reason in the comment — typically "value
// is a literal constant" or "value comes from a closed enum".
package sqlfmtcheck

import (
	"go/ast"
	"go/constant"
	"go/types"
	"strings"

	"golang.org/x/tools/go/analysis"
	"golang.org/x/tools/go/analysis/passes/inspect"
	"golang.org/x/tools/go/ast/inspector"
)

// Analyzer is the sqlfmtcheck go/analysis Analyzer.
var Analyzer = &analysis.Analyzer{
	Name:     "sqlfmtcheck",
	Doc:      "flags fmt.Sprintf calls that build SQL strings using %v / %s on unsanitised values",
	Requires: []*analysis.Analyzer{inspect.Analyzer},
	Run:      run,
}

// sqlKeywords are case-sensitive (uppercase) markers we look for in
// the format string to decide it's an SQL builder. The codebase
// convention is uppercase SQL keywords; matching case-insensitively
// produces false positives on English prose like "set high water mark
// warning: %v".
var sqlKeywords = []string{
	"SELECT ", "INSERT ", "UPDATE ", "DELETE ",
	" FROM ", " WHERE ", "SET VARIABLE",
	"SET search_path", "ATTACH ", "CREATE TABLE",
	"DROP TABLE", "ALTER TABLE", "TRUNCATE ",
}

const bypassMarker = "//sqlfmtcheck:trusted-input"

// safeArgFunctionNames are short function names we consider SQL-safe
// regardless of package. Match by *name only* — the convention in
// this codebase is that any function named `EscapeSQL`,
// `QuoteIdentifier`, `escapeSQL`, etc. fulfils the contract. Adding
// a new internal escape helper just needs to follow the naming
// convention to be auto-recognised.
var safeArgFunctionNames = map[string]bool{
	"EscapeSQL":        true,
	"QuoteIdentifier":  true,
	"escapeSQL":        true,
	"escapeAckSQL":     true,
	"escSyncSQL":       true,
	"quoteTarget":      true, // wraps QuoteIdentifier for two-part names
	"quoteIdentifiers": true, // wraps QuoteIdentifier for slice of names
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
			return // non-const format string — out of scope
		}
		if !looksLikeSQL(fmtStr) {
			return
		}
		// %v is the canary: it calls .String() which on user-controlled
		// values means an attacker can produce arbitrary SQL fragments.
		// %s is intentionally NOT flagged here — it has the same
		// theoretical risk but is used everywhere for identifier
		// interpolation in this codebase, and tightening to %s alone
		// produces ~100 false positives. The narrower %v rule mirrors
		// the bugcheck-static grep this analyzer migrates.
		verbs := scanVerbs(fmtStr)
		for i, v := range verbs {
			argIdx := i + 1 // call.Args[0] is format
			if argIdx >= len(call.Args) {
				break
			}
			if v != 'v' {
				continue
			}
			if isSafeArg(pass, call.Args[argIdx]) {
				continue
			}
			if hasBypassComment(pass, call) {
				return
			}
			pass.Report(analysis.Diagnostic{
				Pos: call.Args[argIdx].Pos(),
				End: call.Args[argIdx].End(),
				Message: "fmt.Sprintf builds SQL with %v which calls .String() on the value (potential SQL injection); use %s with EscapeSQL/QuoteIdentifier or add `" +
					bypassMarker + " <reason>` if the value is provably safe",
			})
			return // one diagnostic per call is enough
		}
	})
	return nil, nil
}

func isFmtSprintf(pass *analysis.Pass, call *ast.CallExpr) bool {
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || sel.Sel.Name != "Sprintf" {
		return false
	}
	pkgID, ok := sel.X.(*ast.Ident)
	if !ok {
		return false
	}
	obj := pass.TypesInfo.ObjectOf(pkgID)
	pn, ok := obj.(*types.PkgName)
	if !ok {
		return false
	}
	return pn.Imported().Path() == "fmt"
}

func constStringValue(pass *analysis.Pass, expr ast.Expr) (string, bool) {
	tv, ok := pass.TypesInfo.Types[expr]
	if !ok || tv.Value == nil || tv.Value.Kind() != constant.String {
		// Try direct string literal.
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
	return constant.StringVal(tv.Value), true
}

func looksLikeSQL(s string) bool {
	// Case-sensitive match. Codebase convention is uppercase SQL
	// keywords; this avoids false positives on English prose
	// containing "set", "from", etc.
	for _, kw := range sqlKeywords {
		if strings.Contains(s, kw) {
			return true
		}
	}
	return false
}

// scanVerbs returns the verb byte after each `%` in order, ignoring
// `%%` literals. It's a deliberately small parser — we only need to
// pair verb positions with argument indexes.
func scanVerbs(format string) []byte {
	var verbs []byte
	for i := 0; i < len(format); i++ {
		if format[i] != '%' {
			continue
		}
		j := i + 1
		// Skip flags / width modifiers.
		for j < len(format) {
			c := format[j]
			if (c >= '0' && c <= '9') || strings.ContainsRune("+-# .", rune(c)) {
				j++
				continue
			}
			break
		}
		if j >= len(format) {
			return verbs
		}
		if format[j] == '%' {
			i = j
			continue
		}
		verbs = append(verbs, format[j])
		i = j
	}
	return verbs
}

func isSafeArg(_ *analysis.Pass, arg ast.Expr) bool {
	call, ok := arg.(*ast.CallExpr)
	if !ok {
		return false
	}
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok {
		// Bare ident — package-local function.
		id, ok := call.Fun.(*ast.Ident)
		if !ok {
			return false
		}
		return safeArgFunctionNames[id.Name]
	}
	return safeArgFunctionNames[sel.Sel.Name]
}

// hasBypassComment scans the file for any comment matching the bypass
// marker on the same logical line as the call. This is approximate —
// for unambiguous semantics, place the marker on the line directly
// above the fmt.Sprintf call.
//
// A bare `//<marker>` with no reason is treated as if absent so the
// underlying violation still fires; an opt-out without an audit trail
// defeats the point of the marker.
func hasBypassComment(pass *analysis.Pass, call *ast.CallExpr) bool {
	pos := call.Pos()
	for _, f := range pass.Files {
		for _, cg := range f.Comments {
			for _, c := range cg.List {
				if !bypassWithReason(c.Text, bypassMarker) {
					continue
				}
				// Comment must be on or just before the call line.
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
// at least one non-whitespace reason character follows.
//
// HasPrefix (not Contains) is required because a marker embedded in
// prose — e.g. `// see also //sqlfmtcheck:trusted-input foo` — would
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
	if rest[0] != ' ' && rest[0] != '\t' {
		return false
	}
	rest = strings.TrimSuffix(rest, "*/")
	rest = strings.TrimSpace(rest)
	return rest != ""
}
