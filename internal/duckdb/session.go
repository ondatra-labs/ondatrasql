// OndatraSQL - You don't need a data stack anymore
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

// Package duckdb provides an embedded DuckDB session using go-duckdb.
package duckdb

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	_ "github.com/duckdb/duckdb-go/v2" // DuckDB driver
	sqlfiles "github.com/ondatra-labs/ondatrasql/internal/sql"
)

// Session represents an embedded DuckDB connection.
type Session struct {
	db           *sql.DB
	conn         *sql.Conn // Single connection for session state (variables, macros)
	mu           sync.Mutex
	closed       bool
	version      string
	catalogAlias string // DuckLake catalog alias (e.g., "lake" or "sandbox" in sandbox mode)
	prodAlias    string // Production catalog alias in sandbox mode (e.g., "lake")
}

// NewSession creates a new embedded DuckDB session.
func NewSession(dbFile string) (*Session, error) {
	if dbFile == "" {
		dbFile = ":memory:"
	}

	db, err := sql.Open("duckdb", dbFile)
	if err != nil {
		return nil, fmt.Errorf("open duckdb: %w", err)
	}

	// Single connection to preserve session state
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	conn, err := db.Conn(context.Background())
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("get connection: %w", err)
	}

	s := &Session{
		db:   db,
		conn: conn,
	}

	// Disable progress bar
	if err := s.Exec("SET enable_progress_bar = false"); err != nil {
		s.Close()
		return nil, fmt.Errorf("disable progress bar: %w", err)
	}

	// Load DuckLake extension
	if err := s.loadExtensions(); err != nil {
		s.Close()
		return nil, fmt.Errorf("load extensions: %w", err)
	}

	return s, nil
}

// installOnce serializes INSTALL ducklake across sessions in the same process.
// On Windows, concurrent installs race when moving the downloaded extension
// file into place ("Could not move file: Access is denied").
var (
	installOnce sync.Once
	installErr  error
)

func (s *Session) loadExtensions() error {
	// Set an explicit extension directory. DuckDB's default uses platform-specific
	// resolution that can produce malformed paths on Windows when HOME/USERPROFILE
	// are non-standard (e.g. CI runners). Use a known-good path under the user's
	// home or temp dir.
	if extDir := defaultExtensionDir(); extDir != "" {
		_ = s.Exec(fmt.Sprintf("SET extension_directory = '%s'", strings.ReplaceAll(extDir, "'", "''")))
	}

	// Install DuckLake exactly once per process to avoid Windows file-move races
	// when multiple sessions install in parallel.
	installOnce.Do(func() {
		installErr = s.Exec("INSTALL ducklake")
	})
	if installErr != nil {
		return installErr
	}
	if err := s.Exec("LOAD ducklake"); err != nil {
		return err
	}

	// SQLite is a core extension (autoloaded), but explicit load ensures availability
	if err := s.Exec("LOAD sqlite"); err != nil {
		return err
	}
	return nil
}

// defaultExtensionDir returns a writable directory for DuckDB extensions.
// Falls back to the temp dir if home cannot be resolved.
func defaultExtensionDir() string {
	if home, err := os.UserHomeDir(); err == nil && home != "" {
		return filepath.Join(home, ".duckdb", "extensions")
	}
	return filepath.Join(os.TempDir(), "duckdb-extensions")
}

// GetVersion returns the DuckDB version (cached after first call).
func (s *Session) GetVersion() string {
	if s.version != "" {
		return s.version
	}
	result, err := s.QueryValue("SELECT version()")
	if err != nil {
		return "unknown"
	}
	s.version = result
	return s.version
}

// RawConn provides access to the underlying driver.Conn for low-level
// operations like the Appender API. The callback runs under the session mutex.
func (s *Session) RawConn(fn func(any) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return errors.New("session closed")
	}
	return s.conn.Raw(fn)
}

// ExecContext executes SQL with context support.
func (s *Session) ExecContext(ctx context.Context, sqlStr string) error {
	sqlStr = strings.TrimSpace(sqlStr)
	if sqlStr == "" || isOnlyComments(sqlStr) {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return errors.New("session closed")
	}
	_, err := s.conn.ExecContext(ctx, sqlStr)
	return err
}

// Exec executes SQL that doesn't return results.
func (s *Session) Exec(sqlStr string) error {
	return s.ExecContext(context.Background(), sqlStr)
}

// QueryContext executes SQL and returns CSV-formatted output for compatibility.
func (s *Session) QueryContext(ctx context.Context, sqlStr string) (string, error) {
	sqlStr = strings.TrimSpace(sqlStr)
	if sqlStr == "" || isOnlyComments(sqlStr) {
		return "", nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return "", errors.New("session closed")
	}

	rows, err := s.conn.QueryContext(ctx, sqlStr)
	if err != nil {
		return "", err
	}
	defer rows.Close()

	return rowsToCSV(rows)
}

// Query executes SQL and returns CSV output.
func (s *Session) Query(sqlStr string) (string, error) {
	return s.QueryContext(context.Background(), sqlStr)
}

// QueryValueContext returns the first value of the first row.
func (s *Session) QueryValueContext(ctx context.Context, sqlStr string) (string, error) {
	sqlStr = strings.TrimSpace(sqlStr)
	if sqlStr == "" || isOnlyComments(sqlStr) {
		return "", nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return "", errors.New("session closed")
	}

	var result any
	err := s.conn.QueryRowContext(ctx, sqlStr).Scan(&result)
	if err == sql.ErrNoRows {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	return anyToString(result), nil
}

// QueryValue returns the first value of the first row.
func (s *Session) QueryValue(sqlStr string) (string, error) {
	return s.QueryValueContext(context.Background(), sqlStr)
}

// QueryRowsContext returns first column of each row.
func (s *Session) QueryRowsContext(ctx context.Context, sqlStr string) ([]string, error) {
	sqlStr = strings.TrimSpace(sqlStr)
	if sqlStr == "" || isOnlyComments(sqlStr) {
		return nil, nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil, errors.New("session closed")
	}

	rows, err := s.conn.QueryContext(ctx, sqlStr)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []string
	for rows.Next() {
		var val any
		if err := rows.Scan(&val); err != nil {
			return nil, err
		}
		result = append(result, anyToString(val))
	}
	return result, rows.Err()
}

// QueryRows returns first column of each row.
func (s *Session) QueryRows(sqlStr string) ([]string, error) {
	return s.QueryRowsContext(context.Background(), sqlStr)
}

// QueryRowsMapContext returns rows as maps.
func (s *Session) QueryRowsMapContext(ctx context.Context, sqlStr string) ([]map[string]string, error) {
	sqlStr = strings.TrimSpace(sqlStr)
	if sqlStr == "" || isOnlyComments(sqlStr) {
		return nil, nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil, errors.New("session closed")
	}

	rows, err := s.conn.QueryContext(ctx, sqlStr)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	var results []map[string]string
	for rows.Next() {
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}

		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}

		row := make(map[string]string)
		for i, col := range cols {
			row[col] = anyToString(vals[i])
		}
		results = append(results, row)
	}

	return results, rows.Err()
}

// QueryRowsMap returns rows as maps.
func (s *Session) QueryRowsMap(sqlStr string) ([]map[string]string, error) {
	return s.QueryRowsMapContext(context.Background(), sqlStr)
}

// QueryRowsAny returns rows as maps with native Go types preserved.
// NULL values are nil, DuckDB types map to their Go equivalents.
func (s *Session) QueryRowsAny(sqlStr string) ([]map[string]any, error) {
	return s.QueryRowsAnyContext(context.Background(), sqlStr)
}

// QueryRowsAnyContext returns rows as maps with native Go types preserved.
func (s *Session) QueryRowsAnyContext(ctx context.Context, sqlStr string) ([]map[string]any, error) {
	sqlStr = strings.TrimSpace(sqlStr)
	if sqlStr == "" || isOnlyComments(sqlStr) {
		return nil, nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil, errors.New("session closed")
	}

	rows, err := s.conn.QueryContext(ctx, sqlStr)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	var results []map[string]any
	for rows.Next() {
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}

		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}

		row := make(map[string]any, len(cols))
		for i, col := range cols {
			row[col] = vals[i]
		}
		results = append(results, row)
	}

	return results, rows.Err()
}

// QueryPrint executes SQL and prints results in the specified format.
// Supported formats: "markdown", "box", "table", "json", "csv"
func (s *Session) QueryPrint(sqlQuery, format string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return errors.New("session closed")
	}

	rows, err := s.conn.QueryContext(context.Background(), sqlQuery)
	if err != nil {
		return err
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return err
	}

	// Collect all rows
	var data [][]string
	for rows.Next() {
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return err
		}
		row := make([]string, len(cols))
		for i, v := range vals {
			row[i] = anyToString(v)
		}
		data = append(data, row)
	}

	if err := rows.Err(); err != nil {
		return err
	}

	// Print based on format
	switch format {
	case "json":
		return printJSON(cols, data)
	case "csv":
		return printCSV(cols, data)
	default:
		return printMarkdown(cols, data)
	}
}

// Close terminates the session.
func (s *Session) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil
	}
	s.closed = true

	if s.conn != nil {
		s.conn.Close()
	}
	if s.db != nil {
		return s.db.Close()
	}
	return nil
}

// InitWithCatalog initializes session using SQL files from configPath.
// SQL files are loaded in order: settings, secrets, extensions, macros, catalog,
// then after attach: schemas, variables, sources.
func (s *Session) InitWithCatalog(configPath string) error {
	// Helper to load and execute SQL file (ignores missing/empty files).
	// Environment variables (${VAR} syntax) are expanded before execution.
	loadSQL := func(name string) error {
		path := filepath.Join(configPath, name)
		content, err := os.ReadFile(path)
		if err != nil || len(content) == 0 {
			return nil // File missing or empty is OK
		}
		sql := os.ExpandEnv(string(content))
		if err := s.Exec(sql); err != nil {
			return fmt.Errorf("load %s: %w", name, err)
		}
		return nil
	}

	// PHASE 1: Pre-catalog setup
	if err := loadSQL("settings.sql"); err != nil {
		return err
	}
	if err := loadSQL("secrets.sql"); err != nil {
		return err
	}
	if err := loadSQL("extensions.sql"); err != nil {
		return err
	}

	// PHASE 2: Built-in runtime variables and macros
	runTime := time.Now().UTC().Format("2006-01-02 15:04:05")
	if err := s.Exec(fmt.Sprintf("SET VARIABLE ondatra_run_time = TIMESTAMP '%s';", runTime)); err != nil {
		return fmt.Errorf("set run_time: %w", err)
	}

	loadID := fmt.Sprintf("%d_%d", time.Now().Unix(), rand.Int())
	if err := s.Exec(fmt.Sprintf("SET VARIABLE ondatra_load_id = '%s';", loadID)); err != nil {
		return fmt.Errorf("set load_id: %w", err)
	}

	// Runtime macros (embedded)
	if runtimeMacros, err := sqlfiles.Load("macros/runtime.sql"); err == nil {
		if err := s.Exec(runtimeMacros); err != nil {
			return fmt.Errorf("load runtime macros: %w", err)
		}
	}

	// Load schema introspection macros (embedded)
	if schemaMacros, err := sqlfiles.Load("macros/schema.sql"); err == nil {
		if err := s.Exec(schemaMacros); err != nil {
			return fmt.Errorf("load schema macros: %w", err)
		}
	}

	// PHASE 3: Attach DuckLake catalog (user-defined in catalog.sql)
	catalogFilePath := filepath.Join(configPath, "catalog.sql")
	catalogContent, err := os.ReadFile(catalogFilePath)
	if err != nil {
		return fmt.Errorf("catalog.sql required: %w", err)
	}
	if err := s.Exec(os.ExpandEnv(string(catalogContent))); err != nil {
		return fmt.Errorf("load catalog.sql: %w", err)
	}

	// Get the DuckLake catalog alias from system tables
	catalogAlias, err := s.QueryValue("SELECT database_name FROM duckdb_databases() WHERE type = 'ducklake' LIMIT 1")
	if err != nil || catalogAlias == "" {
		return fmt.Errorf("no ducklake catalog found in catalog.sql")
	}
	s.catalogAlias = catalogAlias
	sqlfiles.SetCatalogAlias(catalogAlias) // Set for SQL file loading

	// Disable data inlining to work around DuckLake bug where ALTER ADD COLUMN
	// + INSERT produces NULL values for the new column with inlined data.
	// See: ducklake-inlined-data-alter-bug.md
	s.Exec(fmt.Sprintf("CALL %s.set_option('data_inlining_row_limit', 0)", catalogAlias))

	// CDC macros (still in memory context, before USE)
	if cdcMacros, err := sqlfiles.Load("macros/cdc.sql"); err == nil {
		if err := s.Exec(cdcMacros); err != nil {
			return fmt.Errorf("load cdc macros: %w", err)
		}
	}

	// Metadata macros (still in memory context, lake exists for validation)
	if metadataMacros, err := sqlfiles.Load("macros/metadata.sql"); err == nil {
		if err := s.Exec(metadataMacros); err != nil {
			return fmt.Errorf("load metadata macros: %w", err)
		}
	}

	// Switch to lake catalog (before user macros so table refs resolve against lake)
	if err := s.Exec(fmt.Sprintf("USE %s;", catalogAlias)); err != nil {
		return fmt.Errorf("use %s: %w", catalogAlias, err)
	}

	// User macros: stored in memory catalog (DuckLake doesn't support macros).
	// CREATE MACRO is auto-prefixed with memory. so macros go to the in-memory
	// catalog. Since USE lake is active, table refs like mart.orders resolve
	// against the catalog without needing a catalog prefix.
	if err := s.loadUserMacros(configPath, catalogAlias); err != nil {
		return err
	}

	// PHASE 4: CDC variables
	cdcVars := []string{
		fmt.Sprintf("SET VARIABLE curr_snapshot = COALESCE((SELECT id FROM %s.current_snapshot()), 0);", catalogAlias),
		"SET VARIABLE prev_snapshot = COALESCE(getvariable('curr_snapshot') - 1, 0);",
		"SET VARIABLE dag_start_snapshot = getvariable('curr_snapshot');",
	}
	for _, sqlStmt := range cdcVars {
		if err := s.Exec(sqlStmt); err != nil {
			return fmt.Errorf("cdc vars: %w", err)
		}
	}

	// Create metadata registry table in DuckLake catalog (anchor for view metadata snapshots)
	if err := s.Exec(fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s._ondatra_registry (target VARCHAR, kind VARCHAR, updated_at TIMESTAMP)", catalogAlias)); err != nil {
		return fmt.Errorf("create registry: %w", err)
	}

	// Include memory in search path so macros are found
	if err := s.Exec(fmt.Sprintf("SET search_path = '%s,memory';", catalogAlias)); err != nil {
		return fmt.Errorf("set search_path: %w", err)
	}

	// PHASE 5: Post-catalog setup (catalog is attached and active)
	if err := loadSQL("schemas.sql"); err != nil {
		return err
	}
	if err := loadSQL("variables.sql"); err != nil {
		return err
	}
	if err := loadSQL("sources.sql"); err != nil {
		return err
	}

	return nil
}

// macroPrefix matches CREATE [OR REPLACE] MACRO at start of line (ignoring leading whitespace)
// and inserts memory. before the macro name. Skips commented-out lines.
var macroPrefix = regexp.MustCompile(`(?im)(^\s*CREATE\s+(?:OR\s+REPLACE\s+)?MACRO\s+)`)

// loadUserMacros reads macros.sql, prefixes each CREATE MACRO with memory.
// so macros are stored in DuckDB's in-memory catalog instead of DuckLake
// (which doesn't support macros). Also replaces {{catalog}} with the catalog
// alias as an escape hatch for fully qualified references.
func (s *Session) loadUserMacros(configPath, catalogAlias string) error {
	path := filepath.Join(configPath, "macros.sql")
	content, err := os.ReadFile(path)
	if err != nil || len(content) == 0 {
		return nil // File missing or empty is OK
	}

	sql := os.ExpandEnv(string(content))
	sql = strings.ReplaceAll(sql, "{{catalog}}", catalogAlias)
	sql = macroPrefix.ReplaceAllString(sql, "${1}memory.")

	if err := s.Exec(sql); err != nil {
		return fmt.Errorf("load macros.sql: %w", err)
	}
	return nil
}

// CatalogAlias returns the DuckLake catalog alias (e.g., "lake").
// In sandbox mode, this returns "sandbox" (the writable catalog).
func (s *Session) CatalogAlias() string {
	if s.catalogAlias == "" {
		return "lake" // Default fallback
	}
	return s.catalogAlias
}

// ProdAlias returns the production catalog alias in sandbox mode.
// Returns empty string in normal mode.
func (s *Session) ProdAlias() string {
	return s.prodAlias
}

// RefreshSnapshot updates curr_snapshot.
// In sandbox mode, uses prod catalog since source data lives there.
func (s *Session) RefreshSnapshot() error {
	catalog := s.CatalogAlias()
	if s.prodAlias != "" {
		catalog = s.prodAlias
	}
	sqlStr := fmt.Sprintf("SET VARIABLE curr_snapshot = COALESCE((SELECT id FROM %s.current_snapshot()), 0);", catalog)
	return s.Exec(sqlStr)
}

// SetHighWaterMark sets dag_start_snapshot for CDC.
// In sandbox mode, uses prod catalog since model commits live there.
func (s *Session) SetHighWaterMark(target string) error {
	catalog := s.CatalogAlias()
	if s.prodAlias != "" {
		catalog = s.prodAlias
	}
	sqlStr := fmt.Sprintf(`SET VARIABLE dag_start_snapshot = COALESCE(
		(SELECT snapshot_id FROM %s.snapshots()
		 WHERE commit_extra_info->>'model' = '%s'
		 ORDER BY snapshot_id DESC LIMIT 1), 0);`, catalog, strings.ReplaceAll(target, "'", "''"))
	return s.Exec(sqlStr)
}

// HasCDCChanges checks if there are changes since dag_start_snapshot.
func (s *Session) HasCDCChanges() (bool, error) {
	result, err := s.QueryValue("SELECT (getvariable('dag_start_snapshot')::BIGINT + 1) <= getvariable('curr_snapshot')::BIGINT;")
	if err != nil {
		return false, err
	}
	return result == "true", nil
}

// GetDagStartSnapshot returns dag_start_snapshot value.
func (s *Session) GetDagStartSnapshot() (int64, error) {
	result, err := s.QueryValue("SELECT getvariable('dag_start_snapshot')::BIGINT;")
	if err != nil {
		return 0, err
	}
	if result == "" {
		return 0, nil // Variable not set (no DuckLake attached)
	}
	var id int64
	if _, err := fmt.Sscanf(result, "%d", &id); err != nil {
		return 0, fmt.Errorf("parse dag_start_snapshot %q: %w", result, err)
	}
	return id, nil
}

// InitSandbox initializes a sandbox session with dual DuckLake attach.
// Sandbox mode attaches prod (read-only) and sandbox (writable) catalogs.
// prodConnStr is the full DuckLake connection string (e.g. "ducklake:sqlite:/path/to/catalog.sqlite").
// prodDataPath is the DATA_PATH for the prod catalog.
// prodAlias is the user's catalog alias from catalog.sql (e.g., "lake").
// Note: catalog.sql is NOT used in sandbox mode - catalogs are attached programmatically.
func (s *Session) InitSandbox(configPath, prodConnStr, prodDataPath, sandboxCatalog, prodAlias string) error {
	// Helper to load and execute SQL file (ignores missing/empty files)
	loadSQL := func(name string) error {
		path := filepath.Join(configPath, name)
		content, err := os.ReadFile(path)
		if err != nil || len(content) == 0 {
			return nil
		}
		if err := s.Exec(string(content)); err != nil {
			return fmt.Errorf("load %s: %w", name, err)
		}
		return nil
	}

	// PHASE 1: Pre-catalog setup
	if err := loadSQL("settings.sql"); err != nil {
		return err
	}
	if err := loadSQL("secrets.sql"); err != nil {
		return err
	}
	if err := loadSQL("extensions.sql"); err != nil {
		return err
	}

	// PHASE 2: Built-in runtime variables and macros
	runTime := time.Now().UTC().Format("2006-01-02 15:04:05")
	if err := s.Exec(fmt.Sprintf("SET VARIABLE ondatra_run_time = TIMESTAMP '%s';", runTime)); err != nil {
		return fmt.Errorf("set run_time: %w", err)
	}

	loadID := fmt.Sprintf("%d_%d", time.Now().Unix(), rand.Int())
	if err := s.Exec(fmt.Sprintf("SET VARIABLE ondatra_load_id = '%s';", loadID)); err != nil {
		return fmt.Errorf("set load_id: %w", err)
	}

	// Runtime macros (embedded)
	if runtimeMacros, err := sqlfiles.Load("macros/runtime.sql"); err == nil {
		if err := s.Exec(runtimeMacros); err != nil {
			return fmt.Errorf("load runtime macros: %w", err)
		}
	}

	if schemaMacros, err := sqlfiles.Load("macros/schema.sql"); err == nil {
		if err := s.Exec(schemaMacros); err != nil {
			return fmt.Errorf("load schema macros: %w", err)
		}
	}

	// PHASE 3: Attach PROD as READ_ONLY (with user's alias), SANDBOX as writable
	// Prod uses the user's configured alias (e.g., "lake") so existing model SQL works
	// Sandbox uses "sandbox" alias for the writable copy
	prodAttach := fmt.Sprintf("ATTACH '%s' AS %s (READ_ONLY", prodConnStr, prodAlias)
	if prodDataPath != "" {
		prodAttach += fmt.Sprintf(", DATA_PATH '%s', OVERRIDE_DATA_PATH true", prodDataPath)
	}
	prodAttach += ");"
	if err := s.Exec(prodAttach); err != nil {
		return fmt.Errorf("attach prod: %w", err)
	}

	sandboxDataPath := sandboxCatalog + ".files"
	if err := s.Exec(fmt.Sprintf("ATTACH 'ducklake:sqlite:%s' AS sandbox (DATA_PATH '%s', OVERRIDE_DATA_PATH true);", sandboxCatalog, sandboxDataPath)); err != nil {
		return fmt.Errorf("attach sandbox: %w", err)
	}

	// Store aliases for later use
	s.catalogAlias = "sandbox"
	s.prodAlias = prodAlias
	sqlfiles.SetCatalogAlias("sandbox")

	// Disable data inlining on sandbox catalog (workaround for DuckLake ALTER + inlined data bug)
	s.Exec("CALL sandbox.set_option('data_inlining_row_limit', 0)")

	// CDC macros (in memory context, DuckLake doesn't support CREATE MACRO)
	if cdcMacros, err := sqlfiles.Load("macros/cdc.sql"); err == nil {
		if err := s.Exec(cdcMacros); err != nil {
			return fmt.Errorf("load cdc macros: %w", err)
		}
	}

	// Metadata macros (reference lake.snapshots())
	if metadataMacros, err := sqlfiles.Load("macros/metadata.sql"); err == nil {
		if err := s.Exec(metadataMacros); err != nil {
			return fmt.Errorf("load metadata macros: %w", err)
		}
	}

	// PHASE 4: CDC variables (requires prod catalog)
	cdcVars := []string{
		fmt.Sprintf("SET VARIABLE curr_snapshot = COALESCE((SELECT id FROM %s.current_snapshot()), 0);", prodAlias),
		"SET VARIABLE prev_snapshot = COALESCE(getvariable('curr_snapshot') - 1, 0);",
		"SET VARIABLE dag_start_snapshot = getvariable('curr_snapshot');",
	}
	for _, sqlStmt := range cdcVars {
		if err := s.Exec(sqlStmt); err != nil {
			return fmt.Errorf("cdc vars: %w", err)
		}
	}

	// USE prod catalog so macro table refs (e.g. mart.orders) resolve against
	// existing production tables, then switch to sandbox for the rest of init.
	if err := s.Exec(fmt.Sprintf("USE %s;", prodAlias)); err != nil {
		return fmt.Errorf("use %s: %w", prodAlias, err)
	}

	// User macros: stored in memory catalog (DuckLake doesn't support macros).
	if err := s.loadUserMacros(configPath, prodAlias); err != nil {
		return err
	}

	if err := s.Exec("USE sandbox;"); err != nil {
		return fmt.Errorf("use sandbox: %w", err)
	}

	// Create metadata registry table in sandbox catalog (anchor for view metadata snapshots)
	if err := s.Exec("CREATE TABLE IF NOT EXISTS sandbox._ondatra_registry (target VARCHAR, kind VARCHAR, updated_at TIMESTAMP)"); err != nil {
		return fmt.Errorf("create registry: %w", err)
	}

	// Search path: sandbox first (for writes), then prod (for reads from existing tables)
	if err := s.Exec(fmt.Sprintf("SET search_path = 'sandbox,%s,memory';", prodAlias)); err != nil {
		return fmt.Errorf("set search_path: %w", err)
	}

	// PHASE 5: Post-catalog setup (catalog is attached and active)
	if err := loadSQL("schemas.sql"); err != nil {
		return err
	}
	if err := loadSQL("variables.sql"); err != nil {
		return err
	}
	if err := loadSQL("sources.sql"); err != nil {
		return err
	}

	return nil
}

// QuoteIdentifier quotes a SQL identifier for safe use in queries.
func QuoteIdentifier(s string) string {
	escaped := strings.ReplaceAll(s, `"`, `""`)
	return `"` + escaped + `"`
}

// EscapeSQL escapes a string value for safe use in SQL string literals.
// It doubles single quotes to prevent SQL injection.
func EscapeSQL(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}

// Helper functions

func isOnlyComments(sqlStr string) bool {
	for _, line := range strings.Split(sqlStr, "\n") {
		line = strings.TrimSpace(line)
		if line != "" && !strings.HasPrefix(line, "--") {
			return false
		}
	}
	return true
}

func anyToString(v any) string {
	if v == nil {
		return ""
	}
	switch val := v.(type) {
	case string:
		return val
	case []byte:
		return string(val)
	case int64:
		return fmt.Sprintf("%d", val)
	case float64:
		return fmt.Sprintf("%v", val)
	case bool:
		if val {
			return "true"
		}
		return "false"
	case time.Time:
		return val.Format("2006-01-02 15:04:05")
	default:
		return fmt.Sprintf("%v", val)
	}
}

func rowsToCSV(rows *sql.Rows) (string, error) {
	cols, err := rows.Columns()
	if err != nil {
		return "", err
	}

	var lines []string
	lines = append(lines, strings.Join(cols, ","))

	for rows.Next() {
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return "", err
		}

		fields := make([]string, len(cols))
		for i, v := range vals {
			fields[i] = escapeCSV(anyToString(v))
		}
		lines = append(lines, strings.Join(fields, ","))
	}

	return strings.Join(lines, "\n"), rows.Err()
}

func escapeCSV(s string) string {
	if strings.ContainsAny(s, ",\"\n\r") {
		return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
	}
	return s
}

func printMarkdown(cols []string, data [][]string) error {
	if len(cols) == 0 {
		return nil
	}

	// Calculate column widths
	widths := make([]int, len(cols))
	for i, col := range cols {
		widths[i] = len(col)
	}
	for _, row := range data {
		for i, cell := range row {
			if len(cell) > widths[i] {
				widths[i] = len(cell)
			}
		}
	}

	// Print header
	var header []string
	for i, col := range cols {
		header = append(header, fmt.Sprintf("%-*s", widths[i], col))
	}
	fmt.Println("| " + strings.Join(header, " | ") + " |")

	// Print separator
	var sep []string
	for _, w := range widths {
		sep = append(sep, strings.Repeat("-", w))
	}
	fmt.Println("| " + strings.Join(sep, " | ") + " |")

	// Print rows
	for _, row := range data {
		var cells []string
		for i, cell := range row {
			cells = append(cells, fmt.Sprintf("%-*s", widths[i], cell))
		}
		fmt.Println("| " + strings.Join(cells, " | ") + " |")
	}

	return nil
}

func printJSON(cols []string, data [][]string) error {
	var rows []map[string]string
	for _, row := range data {
		m := make(map[string]string)
		for i, col := range cols {
			if i < len(row) {
				m[col] = row[i]
			}
		}
		rows = append(rows, m)
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(rows)
}

func printCSV(cols []string, data [][]string) error {
	fmt.Println(strings.Join(cols, ","))
	for _, row := range data {
		escaped := make([]string, len(row))
		for i, cell := range row {
			escaped[i] = escapeCSV(cell)
		}
		fmt.Println(strings.Join(escaped, ","))
	}
	return nil
}
