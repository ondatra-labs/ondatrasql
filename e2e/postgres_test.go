// OndatraSQL - A data pipeline framework for DuckDB + DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/ondatra-labs/ondatrasql/internal/dag"
	"github.com/ondatra-labs/ondatrasql/internal/duckdb"
	"github.com/ondatra-labs/ondatrasql/internal/execute"
	"github.com/ondatra-labs/ondatrasql/internal/parser"
)

// setupPostgres starts a PostgreSQL container and returns a project directory
// configured to use it as the DuckLake catalog backend.
func setupPostgres(t *testing.T) (projectDir string, connStr string) {
	t.Helper()
	ctx := context.Background()

	pgContainer, err := postgres.Run(ctx,
		"postgres:16-alpine",
		postgres.WithDatabase("ducklake_test"),
		postgres.WithUsername("test"),
		postgres.WithPassword("test"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(30*time.Second),
		),
	)
	if err != nil {
		t.Fatalf("start postgres container: %v", err)
	}
	t.Cleanup(func() { pgContainer.Terminate(ctx) })

	host, err := pgContainer.Host(ctx)
	if err != nil {
		t.Fatalf("get host: %v", err)
	}
	port, err := pgContainer.MappedPort(ctx, "5432")
	if err != nil {
		t.Fatalf("get port: %v", err)
	}

	// DuckLake postgres connection string
	connStr = fmt.Sprintf("ducklake:postgres:dbname=ducklake_test host=%s port=%s user=test password=test", host, port.Port())

	// Create project directory
	dir := t.TempDir()
	configDir := filepath.Join(dir, "config")
	os.MkdirAll(configDir, 0o755)
	os.MkdirAll(filepath.Join(dir, "models", "staging"), 0o755)

	dataPath := filepath.Join(dir, "data")

	// Write catalog.sql pointing to postgres
	catalogSQL := fmt.Sprintf(
		"ATTACH '%s' AS lake (DATA_PATH '%s');\n",
		connStr, dataPath,
	)
	os.WriteFile(filepath.Join(configDir, "catalog.sql"), []byte(catalogSQL), 0o644)

	return dir, connStr
}

// newPostgresSession creates a DuckDB session with postgres extension loaded
// and initializes it with the project's catalog config.
func newPostgresSession(t *testing.T, configDir string) *duckdb.Session {
	t.Helper()
	sess, err := duckdb.NewSession(":memory:")
	if err != nil {
		t.Fatalf("create session: %v", err)
	}

	// DuckLake needs the postgres extension to use postgres as catalog backend
	if err := sess.Exec("INSTALL postgres"); err != nil {
		t.Fatalf("install postgres ext: %v", err)
	}
	if err := sess.Exec("LOAD postgres"); err != nil {
		t.Fatalf("load postgres ext: %v", err)
	}

	if err := sess.InitWithCatalog(configDir); err != nil {
		sess.Close()
		t.Fatalf("init catalog: %v", err)
	}

	t.Cleanup(func() { sess.Close() })
	return sess
}

func TestPostgres_BasicModel(t *testing.T) {
	dir, _ := setupPostgres(t)
	configDir := filepath.Join(dir, "config")

	sess := newPostgresSession(t, configDir)

	// Create and run a model
	modelSQL := "-- @kind: table\nSELECT 1 AS id, 'Alice' AS name\nUNION ALL\nSELECT 2, 'Bob'\n"
	modelPath := filepath.Join(dir, "models", "staging", "users.sql")
	os.WriteFile(modelPath, []byte(modelSQL), 0o644)

	model, err := parser.ParseModel(modelPath, dir)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	runner := execute.NewRunner(sess, execute.ModeRun, dag.GenerateRunID())
	result, err := runner.Run(context.Background(), model)
	if err != nil {
		t.Fatalf("run: %v", err)
	}

	if result.RowsAffected != 2 {
		t.Errorf("rows = %d, want 2", result.RowsAffected)
	}

	val, err := sess.QueryValue("SELECT COUNT(*) FROM staging.users")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if val != "2" {
		t.Errorf("count = %s, want 2", val)
	}
}

func TestPostgres_Sandbox(t *testing.T) {
	dir, connStr := setupPostgres(t)
	configDir := filepath.Join(dir, "config")

	// Phase 1: Run model in prod
	sess1 := newPostgresSession(t, configDir)

	modelSQL := "-- @kind: table\nSELECT 1 AS id, 'Alice' AS name\nUNION ALL\nSELECT 2, 'Bob'\n"
	modelPath := filepath.Join(dir, "models", "staging", "users.sql")
	os.WriteFile(modelPath, []byte(modelSQL), 0o644)

	model, err := parser.ParseModel(modelPath, dir)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	runner := execute.NewRunner(sess1, execute.ModeRun, dag.GenerateRunID())
	_, err = runner.Run(context.Background(), model)
	if err != nil {
		t.Fatalf("prod run: %v", err)
	}
	sess1.Close()

	// Phase 2: Run sandbox with modified data
	modelSQL2 := "-- @kind: table\nSELECT 1 AS id, 'Alice' AS name\nUNION ALL\nSELECT 2, 'Bob'\nUNION ALL\nSELECT 3, 'Charlie'\n"
	os.WriteFile(modelPath, []byte(modelSQL2), 0o644)

	model2, err := parser.ParseModel(modelPath, dir)
	if err != nil {
		t.Fatalf("parse v2: %v", err)
	}

	sess2, err := duckdb.NewSession(":memory:")
	if err != nil {
		t.Fatalf("create sandbox session: %v", err)
	}
	t.Cleanup(func() { sess2.Close() })

	// Load postgres extension for sandbox session too
	if err := sess2.Exec("INSTALL postgres"); err != nil {
		t.Fatalf("install postgres: %v", err)
	}
	if err := sess2.Exec("LOAD postgres"); err != nil {
		t.Fatalf("load postgres: %v", err)
	}

	dataPath := filepath.Join(dir, "data")
	sandboxCatalog := filepath.Join(dir, ".sandbox", "sandbox.sqlite")
	os.MkdirAll(filepath.Join(dir, ".sandbox"), 0o755)

	err = sess2.InitSandbox(configDir, connStr, dataPath, sandboxCatalog, "lake")
	if err != nil {
		t.Fatalf("init sandbox: %v", err)
	}

	// Run model in sandbox
	runner2 := execute.NewRunner(sess2, execute.ModeRun, dag.GenerateRunID())
	result2, err := runner2.Run(context.Background(), model2)
	if err != nil {
		t.Fatalf("sandbox run: %v", err)
	}

	if result2.RowsAffected != 3 {
		t.Errorf("sandbox rows = %d, want 3", result2.RowsAffected)
	}

	// Verify sandbox has 3 rows
	sandboxCount, err := sess2.QueryValue("SELECT COUNT(*) FROM sandbox.staging.users")
	if err != nil {
		t.Fatalf("query sandbox: %v", err)
	}
	if sandboxCount != "3" {
		t.Errorf("sandbox count = %s, want 3", sandboxCount)
	}

	// Verify prod still has 2 rows (unchanged)
	prodCount, err := sess2.QueryValue("SELECT COUNT(*) FROM lake.staging.users")
	if err != nil {
		t.Fatalf("query prod: %v", err)
	}
	if prodCount != "2" {
		t.Errorf("prod count = %s, want 2 (should be unchanged)", prodCount)
	}

	// Verify diff: sandbox EXCEPT prod should yield 1 row (Charlie)
	diffCount, err := sess2.QueryValue(
		"SELECT COUNT(*) FROM (SELECT * FROM sandbox.staging.users EXCEPT SELECT * FROM lake.staging.users)")
	if err != nil {
		t.Fatalf("query diff: %v", err)
	}
	if diffCount != "1" {
		t.Errorf("diff count = %s, want 1", diffCount)
	}

	// Verify the added row is Charlie
	addedName, err := sess2.QueryValue(
		"SELECT name FROM (SELECT * FROM sandbox.staging.users EXCEPT SELECT * FROM lake.staging.users)")
	if err != nil {
		t.Fatalf("query added: %v", err)
	}
	if addedName != "Charlie" {
		t.Errorf("added name = %s, want Charlie", addedName)
	}
}

func TestPostgres_SandboxSchemaEvolution(t *testing.T) {
	dir, connStr := setupPostgres(t)
	configDir := filepath.Join(dir, "config")

	// Phase 1: Run model in prod with 2 columns
	sess1 := newPostgresSession(t, configDir)

	modelSQL := "-- @kind: table\nSELECT 1 AS id, 'Alice' AS name\n"
	modelPath := filepath.Join(dir, "models", "staging", "evolving.sql")
	os.WriteFile(modelPath, []byte(modelSQL), 0o644)

	model, err := parser.ParseModel(modelPath, dir)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	runner := execute.NewRunner(sess1, execute.ModeRun, dag.GenerateRunID())
	_, err = runner.Run(context.Background(), model)
	if err != nil {
		t.Fatalf("prod run: %v", err)
	}
	sess1.Close()

	// Phase 2: Run sandbox with added column
	modelSQL2 := "-- @kind: table\nSELECT 1 AS id, 'Alice' AS name, 30 AS age\n"
	os.WriteFile(modelPath, []byte(modelSQL2), 0o644)

	model2, err := parser.ParseModel(modelPath, dir)
	if err != nil {
		t.Fatalf("parse v2: %v", err)
	}

	sess2, err := duckdb.NewSession(":memory:")
	if err != nil {
		t.Fatalf("create session: %v", err)
	}
	t.Cleanup(func() { sess2.Close() })

	if err := sess2.Exec("INSTALL postgres"); err != nil {
		t.Fatalf("install postgres: %v", err)
	}
	if err := sess2.Exec("LOAD postgres"); err != nil {
		t.Fatalf("load postgres: %v", err)
	}

	dataPath := filepath.Join(dir, "data")
	sandboxCatalog := filepath.Join(dir, ".sandbox", "sandbox.sqlite")
	os.MkdirAll(filepath.Join(dir, ".sandbox"), 0o755)

	err = sess2.InitSandbox(configDir, connStr, dataPath, sandboxCatalog, "lake")
	if err != nil {
		t.Fatalf("init sandbox: %v", err)
	}

	runner2 := execute.NewRunner(sess2, execute.ModeRun, dag.GenerateRunID())
	result2, err := runner2.Run(context.Background(), model2)
	if err != nil {
		t.Fatalf("sandbox run: %v", err)
	}

	if result2.RowsAffected != 1 {
		t.Errorf("rows = %d, want 1", result2.RowsAffected)
	}

	// Sandbox should have 3 columns (id, name, age)
	sandboxCols, err := sess2.QueryValue(
		"SELECT COUNT(*) FROM information_schema.columns WHERE table_catalog = 'sandbox' AND table_name = 'evolving'")
	if err != nil {
		t.Fatalf("query sandbox cols: %v", err)
	}
	if sandboxCols != "3" {
		t.Errorf("sandbox columns = %s, want 3", sandboxCols)
	}

	// Prod should still have 2 columns (id, name)
	prodCols, err := sess2.QueryValue(
		"SELECT COUNT(*) FROM information_schema.columns WHERE table_catalog = 'lake' AND table_name = 'evolving'")
	if err != nil {
		t.Fatalf("query prod cols: %v", err)
	}
	if prodCols != "2" {
		t.Errorf("prod columns = %s, want 2 (should be unchanged)", prodCols)
	}
}
