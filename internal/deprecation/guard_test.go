// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package deprecation

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
	"unicode"
)

const (
	modulePath = "github.com/ondatra-labs/ondatrasql"
	importPath = modulePath + "/internal/deprecation"
	moduleRoot = "../.."
)

// binaryVersion is the version cmd/ondatrasql/main.go declares, the one a
// release is tagged with.
func binaryVersion(t *testing.T) string {
	t.Helper()
	f, err := parser.ParseFile(token.NewFileSet(), filepath.Join(moduleRoot, "cmd", "ondatrasql", "main.go"), nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, d := range f.Decls {
		gd, ok := d.(*ast.GenDecl)
		if !ok || gd.Tok != token.VAR {
			continue
		}
		for _, spec := range gd.Specs {
			vs := spec.(*ast.ValueSpec)
			if len(vs.Names) == 1 && vs.Names[0].Name == "version" && len(vs.Values) == 1 {
				if lit, ok := vs.Values[0].(*ast.BasicLit); ok {
					v, _ := strconv.Unquote(lit.Value)
					return v
				}
			}
		}
	}
	t.Fatal("cmd/ondatrasql/main.go declares no `var version = \"...\"`")
	return ""
}

// idConstants maps the names of this package's ID constants to their values.
func idConstants(t *testing.T) map[string]ID {
	t.Helper()
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]ID{}
	for _, name := range files {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(token.NewFileSet(), name, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		ast.Inspect(f, func(n ast.Node) bool {
			vs, ok := n.(*ast.ValueSpec)
			if !ok || len(vs.Names) != len(vs.Values) {
				return true
			}
			if typ, ok := vs.Type.(*ast.Ident); !ok || typ.Name != "ID" {
				return true
			}
			for i, name := range vs.Names {
				if lit, ok := vs.Values[i].(*ast.BasicLit); ok {
					v, _ := strconv.Unquote(lit.Value)
					out[name.Name] = ID(v)
				}
			}
			return true
		})
	}
	return out
}

// callSites finds every deprecation.Use call in the module's non-test Go
// code outside this package, keyed by the ID it passes. A call whose first
// argument is not one of this package's ID constants is a problem: the
// guard cannot see which feature it belongs to.
func callSites(t *testing.T) (map[ID][]string, []string) {
	t.Helper()
	consts := idConstants(t)
	sites := map[ID][]string{}
	var problems []string
	fset := token.NewFileSet()
	err := filepath.WalkDir(moduleRoot, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			switch d.Name() {
			case ".git", "testdata", "target", "node_modules":
				return filepath.SkipDir
			}
			if abs, _ := filepath.Abs(path); abs == mustAbs(t, ".") {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		f, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			return err
		}
		name := ""
		for _, imp := range f.Imports {
			if p, _ := strconv.Unquote(imp.Path.Value); p == importPath {
				name = "deprecation"
				if imp.Name != nil {
					name = imp.Name.Name
				}
			}
		}
		if name == "" || name == "_" {
			return nil
		}
		ast.Inspect(f, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok || !isPkgSel(call.Fun, name, "Use") || len(call.Args) == 0 {
				return true
			}
			rel, _ := filepath.Rel(moduleRoot, fset.Position(call.Pos()).Filename)
			where := fmt.Sprintf("%s:%d", filepath.ToSlash(rel), fset.Position(call.Pos()).Line)
			sel, ok := call.Args[0].(*ast.SelectorExpr)
			id, known := ID(""), false
			if ok && isPkgSel(sel, name, sel.Sel.Name) {
				id, known = consts[sel.Sel.Name]
			}
			if !known {
				problems = append(problems, where+": deprecation.Use must be passed one of the ID constants in internal/deprecation/registry.go")
				return true
			}
			sites[id] = append(sites[id], where)
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return sites, problems
}

func isPkgSel(e ast.Expr, pkg, sel string) bool {
	s, ok := e.(*ast.SelectorExpr)
	if !ok || s.Sel.Name != sel {
		return false
	}
	x, ok := s.X.(*ast.Ident)
	return ok && x.Name == pkg
}

func mustAbs(t *testing.T, p string) string {
	abs, err := filepath.Abs(p)
	if err != nil {
		t.Fatal(err)
	}
	return abs
}

// audit is the guard: at the given version, every registered feature short
// of its removal version has exactly one call site, and a removed one has
// none. Call sites for unregistered IDs are reported too.
func audit(features []Feature, sites map[ID][]string, version string) []string {
	var problems []string
	registered := map[ID]bool{}
	for _, f := range features {
		registered[f.ID] = true
		got := sites[f.ID]
		if f.LevelAt(version) == LevelRemoved {
			if len(got) > 0 {
				problems = append(problems, fmt.Sprintf("%s reached its removal version %s (this is %s): remove the old form and its deprecation.Use call at %s; keep the registry entry",
					f.ID, f.RemovedIn(), version, strings.Join(got, ", ")))
			}
			continue
		}
		switch len(got) {
		case 1:
		case 0:
			problems = append(problems, fmt.Sprintf("%s has no deprecation.Use call, but it may only be removed in %s (this is %s): restore the old form and its call",
				f.ID, f.RemovedIn(), version))
		default:
			problems = append(problems, fmt.Sprintf("%s has %d deprecation.Use calls (%s); a feature has one",
				f.ID, len(got), strings.Join(got, ", ")))
		}
	}
	var stray []string
	for id, got := range sites {
		if !registered[id] {
			stray = append(stray, fmt.Sprintf("%s is used at %s but not registered", id, strings.Join(got, ", ")))
		}
	}
	sort.Strings(stray)
	return append(problems, stray...)
}

// TestCallSites is the build guard: removing a registered feature's code
// before its removal version fails here, and so does keeping it after.
func TestCallSites(t *testing.T) {
	t.Parallel()
	sites, problems := callSites(t)
	problems = append(problems, audit(Features(), sites, binaryVersion(t))...)
	for _, p := range problems {
		t.Error(p)
	}
	if len(registry) > 0 && len(sites) == 0 {
		t.Fatal("found no deprecation.Use calls at all; the scan is broken")
	}
}

func TestRegistry_Valid(t *testing.T) {
	t.Parallel()
	consts := idConstants(t)
	declared := map[ID]bool{}
	for _, id := range consts {
		declared[id] = true
	}
	seen := map[ID]bool{}
	anchors := map[string]bool{}
	for _, f := range registry {
		if !declared[f.ID] {
			t.Errorf("%q is not an ID constant", f.ID)
		}
		if seen[f.ID] {
			t.Errorf("%s is registered twice", f.ID)
		}
		seen[f.ID] = true
		if anchors[f.Anchor] {
			t.Errorf("%s: anchor %q is used twice", f.ID, f.Anchor)
		}
		anchors[f.Anchor] = true
		if v, err := ParseVersion(f.Since); err != nil || v.Patch != 0 || strings.Count(f.Since, ".") != 1 {
			t.Errorf("%s: Since %q is not a MAJOR.MINOR version", f.ID, f.Since)
		}
		switch f.Kind {
		case KindDirective, KindLib, KindCLI, KindConfig:
		default:
			t.Errorf("%s: unknown kind %q", f.ID, f.Kind)
		}
		if f.Old == "" || f.Replacement == "" || f.Anchor == "" {
			t.Errorf("%s: Old, Replacement and Anchor are required", f.ID)
		}
	}
	for _, id := range consts {
		if !seen[id] {
			t.Errorf("ID constant %q has no registry entry", id)
		}
	}
}

// slug is the heading anchor Hugo derives from a heading's text (its
// default, GitHub-style).
func slug(heading string) string {
	var b strings.Builder
	for _, r := range strings.ToLower(heading) {
		switch {
		case unicode.IsLetter(r) || unicode.IsDigit(r) || r == '-' || r == '_':
			b.WriteRune(r)
		case r == ' ':
			b.WriteByte('-')
		}
	}
	return b.String()
}

// Every registered feature has a row in the list on
// docs/support/deprecations.md with its versions, and a heading its anchor
// points at; the list holds nothing else.
func TestRegistry_Documented(t *testing.T) {
	t.Parallel()
	doc, err := os.ReadFile(filepath.Join(moduleRoot, "docs", "support", "deprecations.md"))
	if err != nil {
		t.Fatal(err)
	}
	headings := map[string]bool{}
	for _, m := range regexp.MustCompile(`(?m)^#{2,4} (.+)$`).FindAllStringSubmatch(string(doc), -1) {
		headings[slug(strings.TrimSpace(m[1]))] = true
	}
	rows := regexp.MustCompile("(?m)^\\| `([a-z_.]+)` \\|.*\\| ([0-9.]+) \\| ([0-9.]+) \\| ([0-9.]+) \\|$").FindAllStringSubmatch(string(doc), -1)
	byID := map[ID]Feature{}
	for _, f := range registry {
		byID[f.ID] = f
	}
	listed := map[ID]bool{}
	for _, r := range rows {
		f, ok := byID[ID(r[1])]
		if !ok {
			t.Errorf("the list has %s, which is not registered", r[1])
			continue
		}
		listed[f.ID] = true
		if got, want := r[2:], []string{f.since().minorString(), f.ErrorIn(), f.RemovedIn()}; strings.Join(got, " ") != strings.Join(want, " ") {
			t.Errorf("%s: the list says %v, the registry %v", f.ID, got, want)
		}
	}
	for _, f := range registry {
		if !listed[f.ID] {
			t.Errorf("%s has no row in docs/support/deprecations.md", f.ID)
		}
		if !headings[f.Anchor] {
			t.Errorf("%s: docs/support/deprecations.md has no heading with anchor #%s", f.ID, f.Anchor)
		}
	}
	for _, a := range []string{"policy", "current-list", "release-notes"} {
		if !headings[a] {
			t.Errorf("docs/support/deprecations.md lost its #%s heading, which other pages link to", a)
		}
	}
}
