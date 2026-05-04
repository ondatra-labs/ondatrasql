// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

// Package removedlibdictscheck flags top-level `TABLE = ...` or
// `SINK = ...` variable declarations in `internal/parser` and
// `internal/libregistry`. These were the legacy lib-dict assignment
// shapes that v0.30 removed in favour of the `API = {...}` pattern;
// reintroducing them would resurrect the dual-shape parser bugs the
// removal closed.
//
// The grep this replaces was:
//
//	grep --include='*.go' -E '^\s*(TABLE|SINK)\s*=' internal/parser/ internal/libregistry/
//
// AST detection is more precise: the grep flagged any line starting
// with TABLE or SINK at left-margin indentation (including comments
// and string contents); the analyzer only flags top-level var/const
// declarations or package-level assignments.
package removedlibdictscheck

import (
	"go/ast"
	"strings"

	"golang.org/x/tools/go/analysis"
)

// Analyzer is the removedlibdictscheck go/analysis Analyzer.
var Analyzer = &analysis.Analyzer{
	Name: "removedlibdictscheck",
	Doc:  "forbids top-level TABLE or SINK variable declarations in internal/parser and internal/libregistry (legacy lib-dict shape removed in v0.30)",
	Run:  run,
}

// inScope returns true if the package is one of the two we enforce
// the rule on.
func inScope(pkgPath string) bool {
	return strings.HasSuffix(pkgPath, "internal/parser") ||
		strings.HasSuffix(pkgPath, "/parser") ||
		pkgPath == "parser" ||
		strings.HasSuffix(pkgPath, "internal/libregistry") ||
		strings.HasSuffix(pkgPath, "/libregistry") ||
		pkgPath == "libregistry"
}

var forbiddenNames = map[string]bool{
	"TABLE": true,
	"SINK":  true,
}

func run(pass *analysis.Pass) (any, error) {
	if !inScope(pass.Pkg.Path()) {
		return nil, nil
	}
	for _, file := range pass.Files {
		for _, decl := range file.Decls {
			gen, ok := decl.(*ast.GenDecl)
			if !ok {
				continue
			}
			if gen.Tok.String() != "var" && gen.Tok.String() != "const" {
				continue
			}
			for _, spec := range gen.Specs {
				vs, ok := spec.(*ast.ValueSpec)
				if !ok {
					continue
				}
				for _, name := range vs.Names {
					if forbiddenNames[name.Name] {
						pass.Report(analysis.Diagnostic{
							Pos:     name.Pos(),
							End:     name.End(),
							Message: "top-level " + name.Name + " declaration in " + pass.Pkg.Path() + " — this is the legacy lib-dict shape removed in v0.30; use the API = {...} pattern instead",
						})
					}
				}
			}
		}
	}
	return nil, nil
}
