// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

//go:build integration || e2e

package testutil

import (
	"path/filepath"
	"testing"

	"github.com/ondatra-labs/ondatrasql/internal/duckdb"
	sqlfiles "github.com/ondatra-labs/ondatrasql/internal/sql"
)

// Project is a self-contained test project with a DuckDB/DuckLake session.
type Project struct {
	Dir  string
	Sess *duckdb.Session
	t    testing.TB
}

// NewProject creates a complete project structure in t.TempDir() with an in-memory DuckLake catalog.
func NewProject(t *testing.T) *Project {
	t.Helper()
	dir := t.TempDir()

	// Create directory structure
	WriteFile(t, dir, "config/.keep", "")
	WriteFile(t, dir, "models/.keep", "")

	// Create DuckLake catalog backed by a temp SQLite file
	catalogPath := filepath.Join(dir, "ducklake.sqlite")
	WriteFile(t, dir, "config/catalog.sql",
		"ATTACH 'ducklake:sqlite:"+catalogPath+"' AS lake (DATA_PATH '"+filepath.Join(dir, "data")+"');\n")

	// Load validation macros into config/macros/ directory
	for _, name := range []string{"macros_helpers.sql", "macros_masking.sql", "macros_constraint.sql", "macros_audit.sql", "macros_warning.sql"} {
		content, _ := sqlfiles.Load("init/" + name)
		WriteFile(t, dir, "config/macros/"+name, content)
	}
	// Load variables (constants, global, local)
	varFiles := map[string]string{
		"variables_constants.sql": "constants.sql",
		"variables_global.sql":    "global.sql",
		"variables_models.sql":    "local.sql",
	}
	for src, dst := range varFiles {
		content, _ := sqlfiles.Load("init/" + src)
		WriteFile(t, dir, "config/variables/"+dst, content)
	}

	// Create DuckDB session and initialize (loads all macros, attaches catalog, etc.)
	sess, err := duckdb.NewSession(":memory:")
	if err != nil {
		t.Fatalf("create session: %v", err)
	}

	configPath := filepath.Join(dir, "config")
	if err := sess.InitWithCatalog(configPath); err != nil {
		sess.Close()
		t.Fatalf("init catalog: %v", err)
	}

	t.Cleanup(func() { sess.Close() })

	return &Project{Dir: dir, Sess: sess, t: t}
}

// AddModel creates a SQL model file at the given relative path under models/.
func (p *Project) AddModel(relPath, content string) {
	p.t.Helper()
	WriteFile(p.t, p.Dir, filepath.Join("models", relPath), content)
}

// NewSandboxProject creates a sandbox project against an existing prod project.
// The prod session is closed and replaced with a sandbox session that has
// dual DuckLake attach (prod read-only + sandbox writable).
func NewSandboxProject(t *testing.T, prod *Project) *Project {
	t.Helper()

	// Extract prod catalog path from the existing project
	prodCatalogPath := filepath.Join(prod.Dir, "ducklake.sqlite")
	prodDataPath := filepath.Join(prod.Dir, "data")

	// Close the prod session so we can attach it read-only
	prod.Sess.Close()

	// Create sandbox directory
	sandboxDir := t.TempDir()
	sandboxCatalog := filepath.Join(sandboxDir, "sandbox.sqlite")
	configPath := filepath.Join(prod.Dir, "config")

	// Create sandbox session
	sess, err := duckdb.NewSession(":memory:")
	if err != nil {
		t.Fatalf("create sandbox session: %v", err)
	}

	if err := sess.InitSandbox(configPath, "ducklake:sqlite:"+prodCatalogPath, prodDataPath, sandboxCatalog, "lake"); err != nil {
		sess.Close()
		t.Fatalf("init sandbox: %v", err)
	}

	t.Cleanup(func() { sess.Close() })

	return &Project{Dir: prod.Dir, Sess: sess, t: t}
}
