// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package dag

import (
	"os"
	"path/filepath"
	"strings"

	"go.starlark.net/syntax"

	"github.com/ondatra-labs/ondatrasql/internal/duckdb"
	"github.com/ondatra-labs/ondatrasql/internal/lineage"
)

// fileOptionsForParse returns the shared Starlark syntax options.
// Must match internal/script/runtime.go fileOptions().
func fileOptionsForParse() *syntax.FileOptions {
	return &syntax.FileOptions{
		Set:             true,
		While:           true,
		TopLevelControl: true,
		GlobalReassign:  true,
		Recursion:       true,
	}
}

// starlarkCalls holds extracted call information from a Starlark file.
type starlarkCalls struct {
	querySQLs []string // SQL strings from query("...") calls
	loads     []string // Module paths from load("...") calls
}

// extractCalls parses Starlark source code and extracts all string literal
// arguments from query() and load() calls. Dynamic arguments are ignored.
func extractCalls(code string) starlarkCalls {
	f, err := fileOptionsForParse().Parse("model.star", code, 0)
	if err != nil {
		return starlarkCalls{}
	}

	var result starlarkCalls
	syntax.Walk(f, func(n syntax.Node) bool {
		call, ok := n.(*syntax.CallExpr)
		if !ok {
			return true
		}

		ident, ok := call.Fn.(*syntax.Ident)
		if !ok {
			return true
		}

		if len(call.Args) == 0 {
			return true
		}

		switch ident.Name {
		case "query":
			lit, ok := call.Args[0].(*syntax.Literal)
			if !ok || lit.Token != syntax.STRING {
				return true
			}
			if s, ok := lit.Value.(string); ok {
				result.querySQLs = append(result.querySQLs, s)
			}
		case "load":
			lit, ok := call.Args[0].(*syntax.Literal)
			if !ok || lit.Token != syntax.STRING {
				return true
			}
			if s, ok := lit.Value.(string); ok {
				result.loads = append(result.loads, s)
			}
		}

		return true
	})

	// Also extract load() statements (top-level load directives use different AST node)
	for _, stmt := range f.Stmts {
		loadStmt, ok := stmt.(*syntax.LoadStmt)
		if !ok {
			continue
		}
		mod := loadStmt.Module.Value.(string)
		result.loads = append(result.loads, mod)
	}

	return result
}

// extractQueryCalls parses Starlark source code and extracts all string literal
// arguments from query() calls. Dynamic SQL (variables, concatenation) is ignored.
// This is the simple single-file version used by unit tests.
func extractQueryCalls(code string) []string {
	return extractCalls(code).querySQLs
}

// extractAllQueryCalls recursively extracts query() SQL strings from a Starlark
// file and all its load() dependencies. The visited set prevents infinite loops.
func extractAllQueryCalls(projectDir, code string, visited map[string]bool) []string {
	calls := extractCalls(code)
	allSQLs := append([]string{}, calls.querySQLs...)

	for _, mod := range calls.loads {
		if projectDir == "" {
			continue
		}
		absPath := resolveModulePath(projectDir, mod)
		if absPath == "" || visited[absPath] {
			continue
		}
		visited[absPath] = true

		data, err := os.ReadFile(absPath)
		if err != nil {
			continue
		}
		allSQLs = append(allSQLs, extractAllQueryCalls(projectDir, string(data), visited)...)
	}

	return allSQLs
}

// resolveModulePath resolves a Starlark load() module path to an absolute file path.
// Returns empty string if the path cannot be resolved or escapes projectDir.
func resolveModulePath(projectDir, module string) string {
	// Prevent path traversal
	if strings.Contains(module, "..") {
		return ""
	}
	joined := filepath.Join(projectDir, module)
	abs, err := filepath.Abs(joined)
	if err != nil {
		return ""
	}
	absProject, err := filepath.Abs(projectDir)
	if err != nil {
		return ""
	}
	if !strings.HasPrefix(abs, absProject+string(filepath.Separator)) {
		return ""
	}
	return abs
}

// ExtractStarlarkDeps extracts table dependencies from Starlark code
// by parsing query() calls and extracting table references from the SQL.
// When projectDir is provided, load() dependencies are followed recursively.
func ExtractStarlarkDeps(sess *duckdb.Session, code string, projectDir ...string) []string {
	var sqls []string
	if len(projectDir) > 0 && projectDir[0] != "" {
		visited := make(map[string]bool)
		sqls = extractAllQueryCalls(projectDir[0], code, visited)
	} else {
		sqls = extractQueryCalls(code)
	}

	if len(sqls) == 0 {
		return nil
	}

	seen := make(map[string]bool)
	var deps []string

	for _, sql := range sqls {
		if sess == nil {
			continue
		}
		tables, err := lineage.ExtractTables(sess, sql)
		if err != nil {
			continue // Skip unparseable SQL
		}
		for _, t := range tables {
			name := t.Table
			if !seen[name] && !isSystemTable(name) {
				seen[name] = true
				deps = append(deps, name)
			}
		}
	}

	return deps
}

// extractStarlarkDeps extracts dependencies for a script model.
// For YAML models, reads lib/<source>.star. For .star models, uses model.SQL.
// Recursively follows load() calls to find query() dependencies in imported modules.
func (g *Graph) extractStarlarkDeps(model *modelInfo) []string {
	var code string
	if model.source != "" && g.projectDir != "" {
		// YAML model: read lib/<source>.star
		path := filepath.Join(g.projectDir, "lib", model.source+".star")
		data, err := os.ReadFile(path)
		if err != nil {
			return nil
		}
		code = string(data)
	} else {
		code = model.sql
	}

	if code == "" {
		return nil
	}

	return ExtractStarlarkDeps(g.sess, code, g.projectDir)
}
