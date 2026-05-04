// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

// Package schemaversioncheck enforces the v0.31 contract that every
// machine-readable output type emitted by the CLI carries a
// schema_version field. Round-4 caught two drifted shapes
// (output.ModelResult, validate's per-file ndjson record) that
// agents/CI consumers can't safely parse without the version tag.
//
// Rule (heuristic but tractable):
//
//   - Any struct type DECLARED in cmd/ondatrasql/* or in the
//     internal/output, internal/validate packages whose name matches
//     the public-output naming convention (Result, Info, Stats,
//     Report, Description) AND has at least one `json:"..."` field
//     tag must include a SchemaVersion-tagged field.
//
// "Public-output naming convention" is project-specific — it pins
// the existing convention from describe (ModelInfo, BlueprintDescription),
// stats (ProjectStats), validate (Report, FileResult), and history.
// Adding new types to the convention only requires using the right
// suffix; types that genuinely aren't part of the JSON surface keep
// any name they want and stay outside the rule.
package schemaversioncheck

import (
	"go/ast"
	"reflect"
	"strings"

	"golang.org/x/tools/go/analysis"
	"golang.org/x/tools/go/analysis/passes/inspect"
	"golang.org/x/tools/go/ast/inspector"
)

// Analyzer is the schemaversioncheck go/analysis Analyzer.
var Analyzer = &analysis.Analyzer{
	Name:     "schemaversioncheck",
	Doc:      "checks that public CLI output structs (*Result, *Info, *Stats, *Report, *Description) include a schema_version field for forward-compatible JSON consumption",
	Requires: []*analysis.Analyzer{inspect.Analyzer},
	Run:      run,
}

// publicOutputSuffixes are the type-name suffixes the project uses
// for machine-readable output structs. Types matching any of these
// must carry a schema_version field (or explicitly opt out — see
// the bypass below).
var publicOutputSuffixes = []string{
	"Result",
	"Info",
	"Stats",
	"Report",
	"Description",
}

// inScopePackages limits the rule to packages that the CLI emits
// JSON from. Internal-only data carriers in unrelated packages can
// share the suffix without being public output.
//
// Match by package import path suffix.
var inScopePackages = []string{
	"cmd/ondatrasql",
	"internal/output",
	"internal/validate",
	"internal/duckdb", // ScanResult etc. — narrow enough that false positives are easily bypassable
}

// bypassMarker is the comment-marker that suppresses the rule on
// types that are public-output-named but intentionally not versioned
// (e.g. transitional types or aliases). Place on the `type` keyword
// line.
const bypassMarker = "//lintcheck:nojsonversion"

func run(pass *analysis.Pass) (any, error) {
	if !packageInScope(pass) {
		return nil, nil
	}

	insp := pass.ResultOf[inspect.Analyzer].(*inspector.Inspector)

	insp.Preorder([]ast.Node{(*ast.GenDecl)(nil)}, func(n ast.Node) {
		gen := n.(*ast.GenDecl)
		if gen.Tok.String() != "type" {
			return
		}
		for _, spec := range gen.Specs {
			ts, ok := spec.(*ast.TypeSpec)
			if !ok {
				continue
			}
			st, ok := ts.Type.(*ast.StructType)
			if !ok {
				continue
			}
			if !nameMatchesConvention(ts.Name.Name) {
				continue
			}
			if !hasJSONTags(st) {
				// Convention-named struct without any JSON tags is
				// not a JSON-emitted shape — out of scope.
				continue
			}
			if hasBypass(gen, ts) {
				continue
			}
			if hasSchemaVersionField(st) {
				continue
			}
			pass.Report(analysis.Diagnostic{
				Pos:     ts.Pos(),
				End:     ts.End(),
				Message: "type " + ts.Name.Name + " is named like a public CLI output (ends in " + matchedSuffix(ts.Name.Name) + ") and has json field tags but lacks a SchemaVersion field; add `SchemaVersion int \\`json:\"schema_version\"\\`` so JSON consumers can detect schema changes — or add `" + bypassMarker + "` to the type to opt out with a reason",
			})
		}
	})

	return nil, nil
}

// packageInScope reports whether the current package is one we
// enforce the rule on.
func packageInScope(pass *analysis.Pass) bool {
	pkgPath := pass.Pkg.Path()
	for _, suffix := range inScopePackages {
		if strings.HasSuffix(pkgPath, suffix) || strings.Contains(pkgPath, suffix+"/") {
			return true
		}
	}
	return false
}

func nameMatchesConvention(name string) bool {
	for _, s := range publicOutputSuffixes {
		if strings.HasSuffix(name, s) {
			return true
		}
	}
	return false
}

func matchedSuffix(name string) string {
	for _, s := range publicOutputSuffixes {
		if strings.HasSuffix(name, s) {
			return s
		}
	}
	return ""
}

func hasJSONTags(st *ast.StructType) bool {
	if st.Fields == nil {
		return false
	}
	for _, field := range st.Fields.List {
		if field.Tag == nil {
			continue
		}
		// Tag is a raw string literal including backticks: `json:"foo"`
		raw := field.Tag.Value
		if len(raw) >= 2 {
			raw = raw[1 : len(raw)-1]
		}
		tag := reflect.StructTag(raw)
		if v, ok := tag.Lookup("json"); ok && v != "" && v != "-" {
			return true
		}
	}
	return false
}

func hasSchemaVersionField(st *ast.StructType) bool {
	if st.Fields == nil {
		return false
	}
	for _, field := range st.Fields.List {
		// Match either the conventional name or the json tag.
		for _, name := range field.Names {
			if name.Name == "SchemaVersion" {
				return true
			}
		}
		if field.Tag == nil {
			continue
		}
		raw := field.Tag.Value
		if len(raw) >= 2 {
			raw = raw[1 : len(raw)-1]
		}
		tag := reflect.StructTag(raw)
		if v, ok := tag.Lookup("json"); ok {
			// Allow either schema_version or schema-version.
			name := strings.SplitN(v, ",", 2)[0]
			if name == "schema_version" || name == "schema-version" {
				return true
			}
		}
	}
	return false
}

// hasBypass reports whether the type-spec or its parent decl has the
// bypass marker on the immediately preceding line.
//
// A bare `//<marker>` with no reason is treated as if absent so the
// underlying violation still fires; an opt-out without an audit trail
// defeats the point of the marker.
func hasBypass(gen *ast.GenDecl, ts *ast.TypeSpec) bool {
	if gen.Doc != nil {
		for _, c := range gen.Doc.List {
			if bypassWithReason(c.Text, bypassMarker) {
				return true
			}
		}
	}
	if ts.Doc != nil {
		for _, c := range ts.Doc.List {
			if bypassWithReason(c.Text, bypassMarker) {
				return true
			}
		}
	}
	if ts.Comment != nil {
		for _, c := range ts.Comment.List {
			if bypassWithReason(c.Text, bypassMarker) {
				return true
			}
		}
	}
	return false
}

// bypassWithReason reports true only when the comment text BEGINS
// with the marker, the marker is terminated by whitespace, and at
// least one non-whitespace reason character follows.
//
// HasPrefix (not Contains) is required because a marker embedded in
// prose — e.g. `// see also //lintcheck:nojsonversion foo` — would
// otherwise silently suppress the diagnostic.
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
