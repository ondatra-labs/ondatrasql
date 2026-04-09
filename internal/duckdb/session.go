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
	"math/big"
	"math/rand"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	duckdbdriver "github.com/duckdb/duckdb-go/v2" // DuckDB driver (aliased; internal package is also named duckdb)
	_ "github.com/lib/pq"                          // postgres driver for sandbox-fork side connection
	sqlfiles "github.com/ondatra-labs/ondatrasql/internal/sql"
)

// sandboxDataInliningOptionName is the DuckLake catalog option that disables
// row-level data inlining on the sandbox catalog. Held as a var (not a const)
// so a regression test can swap it for a known-bad value to verify that the
// error from CALL sandbox.set_option(...) is propagated rather than swallowed.
// Production code paths must NOT mutate this.
var sandboxDataInliningOptionName = "data_inlining_row_limit"

// Session represents an embedded DuckDB connection.
type Session struct {
	db           *sql.DB
	conn         *sql.Conn // Single connection for session state (variables, macros)
	mu           sync.Mutex
	closed       bool
	version      string
	catalogAlias string // DuckLake catalog alias (e.g., "lake" or "sandbox" in sandbox mode)
	prodAlias    string // Production catalog alias in sandbox mode (e.g., "lake")

	// Sandbox v2 cleanup state. When InitSandbox forks a postgres prod catalog
	// via CREATE DATABASE TEMPLATE, the resulting sandbox postgres database has
	// to be dropped at session close. Empty for sqlite-fork sandboxes (the
	// caller's `os.RemoveAll(.sandbox/)` handles those).
	sandboxPostgresDropDB     string // sandbox database name to DROP
	sandboxPostgresAdminConnStr string // connection string to the admin database

	// Bug S16: sandbox shares the prod data path so it can read inherited
	// parquet files via DuckLake's per-catalog file references. The cost is
	// that sandbox writes new parquet files into that shared directory which
	// neither prod nor sandbox knows to clean up afterwards. We capture both
	// catalogs' data-file manifests at Close and delete the diff (sandbox-only
	// files) before tearing down. This requires knowing the absolute data
	// path so we can resolve the relative paths the catalog stores.
	sandboxDataPath string // absolute prod/sandbox shared data path
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

// loadExtensionsMu serializes extension install/load across sessions in the
// same process. On Windows, concurrent installs race on the file move
// ("Could not move file: Access is denied"), and concurrent LOAD calls can
// also collide on file opens. Holding a process-wide mutex around the entire
// install+load sequence makes this race-free without depending on DuckDB's
// own locking, and lets us retry the install on transient Windows failures
// (Defender scans, antivirus locks, etc.) without caching a one-time error.
var (
	loadExtensionsMu sync.Mutex
	ducklakeInstalled bool
)

func (s *Session) loadExtensions() error {
	loadExtensionsMu.Lock()
	defer loadExtensionsMu.Unlock()

	// Set an explicit extension directory. DuckDB's default uses platform-specific
	// resolution that can produce malformed paths on Windows when HOME/USERPROFILE
	// are non-standard (e.g. CI runners). Use a known-good path under the user's
	// home or temp dir.
	if extDir := defaultExtensionDir(); extDir != "" {
		_ = s.Exec(fmt.Sprintf("SET extension_directory = '%s'", strings.ReplaceAll(extDir, "'", "''")))
	}

	// Install DuckLake at most once per process. Retry transient failures (e.g.
	// Windows file-move races with antivirus/Defender) instead of caching the
	// first error and breaking every subsequent session.
	if !ducklakeInstalled {
		var installErr error
		for attempt := 0; attempt < 5; attempt++ {
			installErr = s.Exec("INSTALL ducklake")
			if installErr == nil {
				ducklakeInstalled = true
				break
			}
			time.Sleep(time.Duration(attempt+1) * 200 * time.Millisecond)
		}
		if !ducklakeInstalled {
			return installErr
		}
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
// Test binaries get a per-process dir to avoid the cross-process install race
// (duckdb/duckdb#12589): "go test ./..." launches multiple test binaries in
// parallel, all of which would otherwise INSTALL ducklake into the same shared
// ~/.duckdb/extensions directory and collide on the file move on Windows.
// Production binaries keep using the stable shared cache so the extension is
// only downloaded once per machine.
func defaultExtensionDir() string {
	if isTestBinary() {
		return filepath.Join(os.TempDir(), fmt.Sprintf("duckdb-ext-%d", os.Getpid()))
	}
	if home, err := os.UserHomeDir(); err == nil && home != "" {
		return filepath.Join(home, ".duckdb", "extensions")
	}
	return filepath.Join(os.TempDir(), "duckdb-extensions")
}

// isTestBinary reports whether the current process is a Go test binary.
// Go's test runner names compiled test binaries with a ".test" suffix
// (".test.exe" on Windows).
func isTestBinary() bool {
	exe := os.Args[0]
	return strings.HasSuffix(exe, ".test") || strings.HasSuffix(exe, ".test.exe")
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

// allowedSQLStmtTypes is the set of DuckDB statement types accepted by
// `ondatrasql sql`. The command is read-only by design — anything that
// modifies data, schema, session state, or attaches/loads is rejected.
//
// STATEMENT_TYPE_CALL is intentionally NOT in this set: although the name
// suggests "function call", DuckDB uses it for top-level procedure calls
// (`CALL ducklake_merge_adjacent_files(...)`, `CALL ducklake_expire_snapshots(...)`)
// which mutate catalog state. Table functions like `read_csv()`, `glob()`,
// and `lake.snapshots()` are invoked via FROM clauses and parse as SELECTs,
// so they remain accessible.
//
// STATEMENT_TYPE_PRAGMA is also NOT in this set: DuckDB has read pragmas
// (`PRAGMA show_tables`, `PRAGMA table_info('foo')`) AND mutating ones
// (`PRAGMA threads=1`, `PRAGMA memory_limit='8GB'`, `PRAGMA enable_profiling`)
// — both share the same statement type. Letting the type through would
// allow session-state mutations. The read-pragma equivalents are already
// available as RELATION statements (`SHOW TABLES`, `DESCRIBE foo`,
// `SUMMARIZE foo`), so users lose nothing by going through those.
var allowedSQLStmtTypes = map[duckdbdriver.StmtType]bool{
	duckdbdriver.STATEMENT_TYPE_SELECT:   true, // SELECT, WITH ... SELECT, FROM-first, VALUES
	duckdbdriver.STATEMENT_TYPE_EXPLAIN:  true, // EXPLAIN, EXPLAIN ANALYZE
	duckdbdriver.STATEMENT_TYPE_RELATION: true, // DESCRIBE, SHOW, SUMMARIZE
}

// EnsureReadOnly verifies the query is a read-only statement.
// It prepares the statement against DuckDB to determine its type, then
// rejects anything not in allowedSQLStmtTypes.
//
// Used by `ondatrasql sql` to prevent accidental DDL/DML — users should
// write models in models/ for data mutations.
func (s *Session) EnsureReadOnly(query string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return errors.New("session closed")
	}

	var stmtType duckdbdriver.StmtType
	rawErr := s.conn.Raw(func(driverConn any) error {
		innerConn, ok := driverConn.(*duckdbdriver.Conn)
		if !ok {
			return fmt.Errorf("driver connection is not *duckdb.Conn: %T", driverConn)
		}
		// Use Prepare (not PrepareContext) — Prepare rejects multi-statement
		// queries automatically, which is what we want for `ondatrasql sql`.
		stmt, err := innerConn.Prepare(query)
		if err != nil {
			return err
		}
		defer stmt.Close()

		ddbStmt, ok := stmt.(*duckdbdriver.Stmt)
		if !ok {
			return fmt.Errorf("prepared statement is not *duckdb.Stmt: %T", stmt)
		}
		stmtType, err = ddbStmt.StatementType()
		return err
	})
	if rawErr != nil {
		// Translate driver-internal multi-statement error into a user-friendly message.
		if strings.Contains(rawErr.Error(), "multi-statement") {
			return errors.New("ondatrasql sql accepts only one statement at a time (got multi-statement query — split into separate calls)")
		}
		return fmt.Errorf("validate query: %w", rawErr)
	}

	if !allowedSQLStmtTypes[stmtType] {
		return fmt.Errorf("ondatrasql sql is read-only: %s statements are not allowed (use models in models/ for data mutations)", stmtTypeName(stmtType))
	}
	return nil
}

// stmtTypeName returns a human-readable name for a DuckDB statement type.
// Used in error messages from EnsureReadOnly.
func stmtTypeName(t duckdbdriver.StmtType) string {
	switch t {
	case duckdbdriver.STATEMENT_TYPE_INSERT:
		return "INSERT"
	case duckdbdriver.STATEMENT_TYPE_UPDATE:
		return "UPDATE"
	case duckdbdriver.STATEMENT_TYPE_DELETE:
		return "DELETE"
	case duckdbdriver.STATEMENT_TYPE_CREATE, duckdbdriver.STATEMENT_TYPE_CREATE_FUNC:
		return "CREATE"
	case duckdbdriver.STATEMENT_TYPE_DROP:
		return "DROP"
	case duckdbdriver.STATEMENT_TYPE_ALTER:
		return "ALTER"
	case duckdbdriver.STATEMENT_TYPE_COPY:
		return "COPY"
	case duckdbdriver.STATEMENT_TYPE_TRANSACTION:
		return "TRANSACTION (BEGIN/COMMIT/ROLLBACK)"
	case duckdbdriver.STATEMENT_TYPE_VACUUM:
		return "VACUUM"
	case duckdbdriver.STATEMENT_TYPE_ATTACH:
		return "ATTACH"
	case duckdbdriver.STATEMENT_TYPE_DETACH:
		return "DETACH"
	case duckdbdriver.STATEMENT_TYPE_LOAD:
		return "LOAD"
	case duckdbdriver.STATEMENT_TYPE_EXPORT:
		return "EXPORT"
	case duckdbdriver.STATEMENT_TYPE_MULTI:
		return "multi-statement (use one statement per call)"
	case duckdbdriver.STATEMENT_TYPE_SET, duckdbdriver.STATEMENT_TYPE_VARIABLE_SET:
		return "SET"
	case duckdbdriver.STATEMENT_TYPE_PREPARE:
		return "PREPARE"
	case duckdbdriver.STATEMENT_TYPE_EXECUTE:
		return "EXECUTE"
	case duckdbdriver.STATEMENT_TYPE_CALL:
		return "CALL (procedure)"
	case duckdbdriver.STATEMENT_TYPE_PRAGMA:
		return "PRAGMA (mutates session — use SHOW/DESCRIBE/SUMMARIZE for read-only introspection)"
	case duckdbdriver.STATEMENT_TYPE_INVALID:
		return "invalid"
	default:
		return fmt.Sprintf("statement type %d", int(t))
	}
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

	// Collect all rows. We keep both the raw any values (for JSON, which needs
	// to preserve types) and the stringified form (for markdown/csv which only
	// produce text). The stringification happens once per row up front.
	var rawData [][]any
	var strData [][]string
	for rows.Next() {
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return err
		}
		// Copy vals so subsequent Scan() calls don't overwrite our row data.
		rawRow := make([]any, len(cols))
		copy(rawRow, vals)
		rawData = append(rawData, rawRow)

		strRow := make([]string, len(cols))
		for i, v := range vals {
			strRow[i] = anyToString(v)
		}
		strData = append(strData, strRow)
	}

	if err := rows.Err(); err != nil {
		return err
	}

	// Print based on format
	switch format {
	case "json":
		return printJSON(cols, rawData)
	case "csv":
		return printCSV(cols, strData)
	default:
		return printMarkdown(cols, strData)
	}
}

// Close terminates the session. If a postgres sandbox fork is active, the
// sandbox database is detached and dropped (via a direct lib/pq admin
// connection) before the duckdb session itself is released. Drop failures
// are returned but the duckdb close still runs.
//
// Bug S16 cleanup: in sandbox mode, before tearing down the connection,
// captureSandboxOrphans queries both catalogs' data-file manifests to find
// parquet files that exist in the sandbox catalog but not in prod's. Those
// files are deleted from disk (best-effort) after the duckdb session closes.
// This avoids the linear disk-leak that would otherwise accumulate at every
// sandbox session.
func (s *Session) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil
	}
	s.closed = true

	// Capture sandbox-only parquet files BEFORE detaching/dropping anything,
	// while the duckdb session and both catalogs are still attached.
	var orphanFiles []string
	if s.prodAlias != "" && s.conn != nil {
		orphanFiles = s.captureSandboxOrphansLocked()
	}

	// Detach the sandbox catalog from the duckdb session BEFORE closing s.conn,
	// using s.conn directly because s.db.Exec would block waiting for the conn
	// pool slot that s.conn currently holds.
	if s.sandboxPostgresDropDB != "" && s.conn != nil {
		_, _ = s.conn.ExecContext(context.Background(), "DETACH sandbox;")
	}

	if s.conn != nil {
		s.conn.Close()
	}

	// Now that s.conn is released we can drop the sandbox postgres database
	// via a fresh lib/pq admin connection.
	var dropErr error
	if s.sandboxPostgresDropDB != "" && s.sandboxPostgresAdminConnStr != "" {
		dropErr = s.dropPostgresSandboxDatabaseAfterCloseLocked()
		s.sandboxPostgresDropDB = ""
		s.sandboxPostgresAdminConnStr = ""
	}

	if s.db != nil {
		if err := s.db.Close(); err != nil {
			if dropErr != nil {
				return fmt.Errorf("close duckdb: %w (also: drop sandbox postgres db: %v)", err, dropErr)
			}
			return err
		}
	}

	// Delete orphan parquet files from disk after the duckdb connection is
	// fully released. Best-effort; individual delete failures are silent.
	for _, p := range orphanFiles {
		_ = os.Remove(p)
	}

	return dropErr
}

// captureSandboxOrphansLocked queries the sandbox and prod data-file
// manifests via the DuckLake metadata shadow databases and returns absolute
// paths of parquet files that the sandbox catalog references but prod does
// not. Caller holds s.mu and must call this BEFORE detaching/closing.
//
// On any error the function returns nil — orphan cleanup is best-effort and
// must never block the main close path.
func (s *Session) captureSandboxOrphansLocked() []string {
	if s.sandboxDataPath == "" {
		return nil
	}
	prodPaths, err := s.queryDataFilePathsLocked(s.prodAlias)
	if err != nil {
		return nil
	}
	sandboxPaths, err := s.queryDataFilePathsLocked("sandbox")
	if err != nil {
		return nil
	}
	prodSet := make(map[string]bool, len(prodPaths))
	for _, p := range prodPaths {
		prodSet[p] = true
	}
	var orphans []string
	for _, p := range sandboxPaths {
		if !prodSet[p] {
			orphans = append(orphans, filepath.Join(s.sandboxDataPath, p))
		}
	}
	return orphans
}

// queryDataFilePathsLocked returns the *relative* file paths under the
// shared data directory for parquet files registered in a DuckLake catalog.
// DuckLake stores `ducklake_data_file.path` as just a filename, so we join
// with `ducklake_table.table_name` and `ducklake_schema.schema_name` to
// reconstruct the full relative path "<schema>/<table>/<filename>".
//
// Caller holds s.mu.
func (s *Session) queryDataFilePathsLocked(alias string) ([]string, error) {
	q := fmt.Sprintf(`
		SELECT sch.schema_name || '/' || tbl.table_name || '/' || df.path AS rel_path
		FROM __ducklake_metadata_%s.ducklake_data_file df
		JOIN __ducklake_metadata_%s.ducklake_table tbl USING (table_id)
		JOIN __ducklake_metadata_%s.ducklake_schema sch USING (schema_id)`,
		alias, alias, alias)
	rows, err := s.conn.QueryContext(context.Background(), q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var p string
		if err := rows.Scan(&p); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// dropPostgresSandboxDatabaseAfterCloseLocked drops the sandbox postgres
// database via a direct lib/pq admin connection. Called from Close after
// s.conn has been released. Uses DROP DATABASE WITH (FORCE) (postgres 13+)
// to terminate any lingering connections that DuckDB hasn't yet cleaned up.
func (s *Session) dropPostgresSandboxDatabaseAfterCloseLocked() error {
	adminDB, err := sql.Open("postgres", s.sandboxPostgresAdminConnStr)
	if err != nil {
		return fmt.Errorf("open postgres admin for sandbox cleanup: %w", err)
	}
	defer adminDB.Close()

	dropSQL := fmt.Sprintf("DROP DATABASE %s WITH (FORCE)", quoteIdent(s.sandboxPostgresDropDB))
	if _, err := adminDB.Exec(dropSQL); err != nil {
		return fmt.Errorf("drop sandbox postgres database %s: %w", s.sandboxPostgresDropDB, err)
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

	// Validate the catalog backend. OndatraSQL supports DuckLake catalogs over
	// sqlite or postgres only. Reject:
	//   - Raw sqlite/duckdb/mysql attaches that bypass DuckLake (no time travel,
	//     no snapshots, no schema evolution — the runner needs all of that).
	//   - DuckLake-over-mysql or DuckLake-over-duckdb backends, because the
	//     sandbox-fork strategy in v0.12.0+ requires per-backend native fork
	//     primitives that we only ship for sqlite (cp) and postgres
	//     (CREATE DATABASE TEMPLATE).
	if err := s.validateCatalogBackend(); err != nil {
		return err
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

// validateCatalogBackend is called after catalog.sql has been executed.
// It enforces OndatraSQL's supported-backend policy:
//
//   - The user must attach a DuckLake catalog (not a raw sqlite or duckdb file
//     attached via the sqlite/duckdb extensions). Raw attaches lack snapshots,
//     time travel, and the commit_extra_info metadata that the runner relies on.
//
//   - The DuckLake catalog must be backed by sqlite or postgres. Other DuckLake
//     backends (mysql, duckdb-as-catalog) are rejected because the sandbox-fork
//     architecture in v0.12.0+ uses backend-native fork primitives that we only
//     ship for those two: `cp` for sqlite and `CREATE DATABASE TEMPLATE` for
//     postgres.
//
// The error messages are written to be actionable for users coming from
// catalog.sql templates that include commented-out non-supported examples.
func (s *Session) validateCatalogBackend() error {
	// Find every database the user attached, excluding the auto-created
	// __ducklake_metadata_* shadow databases (those reflect the user's
	// chosen DuckLake backend, which we check via the ducklake row's path).
	type dbRow struct {
		name string
		typ  string
		path string
	}
	rows, err := s.QueryRowsMap(`
		SELECT database_name, type, path
		FROM duckdb_databases()
		WHERE NOT internal
		  AND database_name NOT IN ('memory', 'system', 'temp')
		  AND database_name NOT LIKE '__ducklake_metadata_%'
		ORDER BY database_oid`)
	if err != nil {
		return fmt.Errorf("inspect attached catalogs: %w", err)
	}
	if len(rows) == 0 {
		return fmt.Errorf("catalog.sql did not attach a DuckLake catalog. Add an ATTACH like:\n  ATTACH 'ducklake:sqlite:ducklake.sqlite' AS lake (DATA_PATH 'ducklake.sqlite.files/');")
	}

	var attached []dbRow
	for _, r := range rows {
		attached = append(attached, dbRow{name: r["database_name"], typ: r["type"], path: r["path"]})
	}

	// Reject if any attached non-DuckLake catalog is present.
	for _, db := range attached {
		if db.typ != "ducklake" {
			return fmt.Errorf(
				"catalog.sql attached %q as type %q (path %q), but OndatraSQL requires a DuckLake catalog. "+
					"Replace your ATTACH with one of:\n"+
					"  ATTACH 'ducklake:sqlite:ducklake.sqlite' AS lake (DATA_PATH 'ducklake.sqlite.files/');\n"+
					"  ATTACH 'ducklake:postgres:host=localhost dbname=lake' AS lake (DATA_PATH '/path/to/data/');\n"+
					"Raw sqlite, duckdb, or other database attaches are not supported because the runner needs DuckLake's snapshots, time travel, and commit metadata.",
				db.name, db.typ, db.path)
		}
	}

	// At least one DuckLake catalog is attached. Validate the backend(s).
	// DuckLake stores the underlying connection as `path` in the form
	// `<backend>:<connection-string>`, e.g. `sqlite:/var/data/lake.sqlite`
	// or `postgres:host=db.example.com dbname=lake`.
	for _, db := range attached {
		backend := backendFromDuckLakePath(db.path)
		switch backend {
		case "sqlite", "postgres":
			// supported
		case "mysql":
			return fmt.Errorf(
				"catalog %q is a DuckLake-on-mysql catalog (path %q), but OndatraSQL only supports sqlite and postgres backends. "+
					"The sandbox feature uses backend-native fork primitives (cp for sqlite, CREATE DATABASE TEMPLATE for postgres) and there is no equivalent for mysql. "+
					"Migrate your catalog to postgres or sqlite, or open an issue if mysql support is critical for your use case.",
				db.name, db.path)
		case "duckdb":
			return fmt.Errorf(
				"catalog %q is a DuckLake-on-duckdb catalog (path %q), but OndatraSQL only supports sqlite and postgres backends. "+
					"DuckDB-as-catalog stores metadata in a duckdb file, which collides with our sqlite-fork sandbox strategy. "+
					"Use sqlite instead: ATTACH 'ducklake:sqlite:lake.sqlite' AS lake (DATA_PATH '...');",
				db.name, db.path)
		default:
			return fmt.Errorf(
				"catalog %q has an unrecognised DuckLake backend (path %q). OndatraSQL supports sqlite and postgres only.",
				db.name, db.path)
		}
	}

	return nil
}

// backendFromDuckLakePath extracts the backend identifier from a DuckLake path
// returned by duckdb_databases().path. Examples:
//
//	"sqlite:/var/data/lake.sqlite"          -> "sqlite"
//	"postgres:host=localhost dbname=lake"   -> "postgres"
//	"mysql:db=lake host=db.example.com"     -> "mysql"
//	"duckdb:catalog.duckdb"                 -> "duckdb"
func backendFromDuckLakePath(path string) string {
	idx := strings.Index(path, ":")
	if idx <= 0 {
		return ""
	}
	return path[:idx]
}

// forkSqliteCatalog implements sandbox v2 fork for sqlite-backed DuckLake.
// It copies the prod sqlite catalog file to sandboxCatalog and returns the
// DuckLake connection string for the new sandbox catalog. Cleanup is the
// caller's responsibility (usually os.RemoveAll on .sandbox/<sub>).
//
// Bug S15 fix: when prod catalog doesn't exist (typical for a freshly-init'd
// project before the user has run any model), produce a friendly actionable
// error rather than the low-level "Failed to load DuckLake table data" error
// chain that DuckDB would produce on ATTACH of a missing file.
func (s *Session) forkSqliteCatalog(prodConnStr, sandboxCatalog string) (string, error) {
	prodCatalogPath := strings.TrimPrefix(prodConnStr, "ducklake:sqlite:")

	if _, err := os.Stat(prodCatalogPath); os.IsNotExist(err) {
		return "", fmt.Errorf(
			"sandbox needs an existing prod catalog to fork from, but %s does not exist yet. "+
				"Run `ondatrasql run` to materialize at least one model first, then `ondatrasql sandbox` "+
				"can validate changes against it.", prodCatalogPath)
	}

	if err := os.MkdirAll(filepath.Dir(sandboxCatalog), 0o755); err != nil {
		return "", fmt.Errorf("fork prod catalog: ensure dir: %w", err)
	}
	src, err := os.ReadFile(prodCatalogPath)
	if err != nil {
		return "", fmt.Errorf("fork prod catalog: read %s: %w", prodCatalogPath, err)
	}
	if err := os.WriteFile(sandboxCatalog, src, 0o644); err != nil {
		return "", fmt.Errorf("fork prod catalog: write %s: %w", sandboxCatalog, err)
	}
	return "ducklake:sqlite:" + sandboxCatalog, nil
}

// forkPostgresCatalog implements sandbox v2 fork for postgres-backed DuckLake
// using `CREATE DATABASE <sandbox> TEMPLATE <prod>` — postgres' own zero-cost
// metadata-fork primitive. The sandbox database name uses a process+random
// suffix so concurrent sandbox runs don't collide.
//
// We use a direct lib/pq connection (not duckdb's postgres extension) because
// the duckdb extension wraps every statement in a transaction and CREATE
// DATABASE / DROP DATABASE cannot run inside transactions. The admin
// connection is opened, used for one statement, and closed immediately.
//
// Cleanup state is stored on the Session so Close() can DROP DATABASE.
func (s *Session) forkPostgresCatalog(prodConnStr string) (string, error) {
	rawConn := strings.TrimPrefix(prodConnStr, "ducklake:postgres:")
	params, err := parsePostgresConnString(rawConn)
	if err != nil {
		return "", fmt.Errorf("fork prod catalog: parse postgres conn: %w", err)
	}
	prodDB := params["dbname"]
	if prodDB == "" {
		return "", fmt.Errorf("fork prod catalog: postgres conn string is missing dbname (got %q)", rawConn)
	}

	// Generate a unique sandbox database name. Process id + random keeps
	// concurrent sandbox sessions from colliding on the same postgres server.
	sandboxDB := strings.ToLower(fmt.Sprintf("ondatrasql_sandbox_%d_%d", os.Getpid(), rand.Int63()))

	// Build admin connection string by swapping dbname for the postgres
	// admin database. "postgres" exists on every standard install.
	// lib/pq defaults to requiring SSL; we inherit whatever sslmode the user
	// configured for prod, defaulting to "disable" so local docker postgres
	// (which has no SSL by default) just works.
	adminParams := make(map[string]string, len(params))
	for k, v := range params {
		adminParams[k] = v
	}
	adminParams["dbname"] = "postgres"
	if _, ok := adminParams["sslmode"]; !ok {
		adminParams["sslmode"] = "disable"
	}
	adminConnStr := buildPostgresConnString(adminParams)

	// Open a short-lived admin connection via lib/pq, run CREATE DATABASE
	// TEMPLATE, close. Direct database/sql avoids the duckdb postgres
	// extension's transaction wrapping.
	adminDB, err := sql.Open("postgres", adminConnStr)
	if err != nil {
		return "", fmt.Errorf("fork prod catalog: open postgres admin: %w", err)
	}
	defer adminDB.Close()

	// Terminate any other connections to the source database. CREATE DATABASE
	// TEMPLATE refuses to run if anyone else has the source open, and the
	// duckdb postgres extension caches connections that may linger past the
	// previous session's Close. We're in a sandbox flow — by design the
	// caller is preparing to fork prod for local validation, so disconnecting
	// stragglers is acceptable.
	terminateSQL := `
		SELECT pg_terminate_backend(pid)
		FROM pg_stat_activity
		WHERE datname = $1 AND pid <> pg_backend_pid()`
	if _, err := adminDB.Exec(terminateSQL, prodDB); err != nil {
		return "", fmt.Errorf("fork prod catalog: terminate stale prod connections: %w", err)
	}

	createSQL := fmt.Sprintf("CREATE DATABASE %s TEMPLATE %s",
		quoteIdent(sandboxDB), quoteIdent(prodDB))
	if _, err := adminDB.Exec(createSQL); err != nil {
		return "", fmt.Errorf("fork prod catalog: CREATE DATABASE TEMPLATE: %w", err)
	}

	// Stash cleanup state so Close() can drop the sandbox database.
	s.sandboxPostgresDropDB = sandboxDB
	s.sandboxPostgresAdminConnStr = adminConnStr

	// Build the sandbox DuckLake connection string by swapping dbname.
	sandboxParams := make(map[string]string, len(params))
	for k, v := range params {
		sandboxParams[k] = v
	}
	sandboxParams["dbname"] = sandboxDB
	return "ducklake:postgres:" + buildPostgresConnString(sandboxParams), nil
}

// parsePostgresConnString parses a libpq-style key=value space-separated
// connection string into a map. Whitespace inside quoted values is preserved.
func parsePostgresConnString(s string) (map[string]string, error) {
	result := make(map[string]string)
	for _, field := range strings.Fields(s) {
		eq := strings.IndexByte(field, '=')
		if eq <= 0 {
			return nil, fmt.Errorf("invalid postgres conn token %q", field)
		}
		result[field[:eq]] = field[eq+1:]
	}
	return result, nil
}

// buildPostgresConnString reverses parsePostgresConnString.
func buildPostgresConnString(params map[string]string) string {
	// Sort keys for stable output (helps tests and logging).
	keys := make([]string, 0, len(params))
	for k := range params {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, k+"="+params[k])
	}
	return strings.Join(parts, " ")
}

// quoteIdent wraps a postgres identifier in double quotes, escaping any
// embedded quotes. We use it for the database names in CREATE/DROP DATABASE
// since they can contain underscores and digits.
func quoteIdent(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
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

// RefreshSnapshot updates curr_snapshot. v0.12.0+: in sandbox mode the
// sandbox catalog is a fork of prod, so it has the full snapshot history
// (inherited at fork plus new sandbox commits). Always read from the active
// catalog regardless of mode — no more prod-alias special-case.
func (s *Session) RefreshSnapshot() error {
	catalog := s.CatalogAlias()
	sqlStr := fmt.Sprintf("SET VARIABLE curr_snapshot = COALESCE((SELECT id FROM %s.current_snapshot()), 0);", catalog)
	return s.Exec(sqlStr)
}

// SetHighWaterMark sets dag_start_snapshot for CDC. Same v0.12.0 reasoning
// as RefreshSnapshot — sandbox has the inherited commit history, so we read
// from the active catalog instead of forcing prod.
func (s *Session) SetHighWaterMark(target string) error {
	catalog := s.CatalogAlias()
	sqlStr := fmt.Sprintf(`SET VARIABLE dag_start_snapshot = COALESCE(
		(SELECT snapshot_id FROM %s.snapshots()
		 WHERE LOWER(commit_extra_info->>'model') = LOWER('%s')
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

// InitSandbox initializes a sandbox session as a fork of prod.
//
// Sandbox v2 architecture (v0.12.0+): the sandbox catalog is created as a copy
// of the prod catalog at session start, so all stateful kinds (SCD2, append+
// incremental, tracked) see prod's prior state and produce correct deltas.
// Both catalogs share the same data path; sandbox writes new parquet files
// that prod's catalog cannot see (file references live per-catalog).
//
//   - prodConnStr: full DuckLake connection string (e.g. "ducklake:sqlite:/path/to/catalog.sqlite").
//     Only sqlite and postgres backends are supported; mysql/duckdb are rejected.
//   - prodDataPath: DATA_PATH for the prod catalog. Sandbox shares this path.
//   - sandboxCatalog: filesystem path where the forked sqlite catalog will live.
//     For postgres prod (TODO), this is used as a marker file inside .sandbox/.
//   - prodAlias: the user's catalog alias from catalog.sql (e.g., "lake").
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

	// PHASE 3: Fork the prod catalog FIRST (via a side connection that doesn't
	// touch the duckdb session), THEN attach prod and sandbox to the duckdb
	// session as fresh connections. This order matters for postgres because
	// pg_terminate_backend would otherwise kill duckdb's cached connection
	// to the previously-attached prod, leaving stale state in the cache.
	var sandboxConnStr string
	switch {
	case strings.HasPrefix(prodConnStr, "ducklake:sqlite:"):
		var err error
		sandboxConnStr, err = s.forkSqliteCatalog(prodConnStr, sandboxCatalog)
		if err != nil {
			return err
		}
	case strings.HasPrefix(prodConnStr, "ducklake:postgres:"):
		var err error
		sandboxConnStr, err = s.forkPostgresCatalog(prodConnStr)
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("sandbox v2 supports sqlite and postgres catalog backends (got %q)", prodConnStr)
	}

	prodAttach := fmt.Sprintf("ATTACH '%s' AS %s (READ_ONLY", prodConnStr, prodAlias)
	if prodDataPath != "" {
		prodAttach += fmt.Sprintf(", DATA_PATH '%s', OVERRIDE_DATA_PATH true", prodDataPath)
	}
	prodAttach += ");"
	if err := s.Exec(prodAttach); err != nil {
		return fmt.Errorf("attach prod: %w", err)
	}

	// Sandbox shares the prod data path so it can read inherited parquet files.
	// New writes from sandbox become new parquet files in the same directory;
	// they are isolated from prod by virtue of being in sandbox's catalog only.
	// OVERRIDE_DATA_PATH is required because the catalog stores its data path
	// as a relative string and we want the absolute prod data path to win.
	if err := s.Exec(fmt.Sprintf("ATTACH '%s' AS sandbox (DATA_PATH '%s', OVERRIDE_DATA_PATH true);", sandboxConnStr, prodDataPath)); err != nil {
		return fmt.Errorf("attach sandbox: %w", err)
	}

	// Store aliases for later use
	s.catalogAlias = "sandbox"
	s.prodAlias = prodAlias
	s.sandboxDataPath = prodDataPath
	sqlfiles.SetCatalogAlias("sandbox")

	// Disable data inlining on sandbox catalog (workaround for DuckLake ALTER + inlined data bug).
	// Failing here is not silently recoverable: with inlining still on, schema-evolution
	// scenarios on sandbox tables can corrupt state. See ducklake-inlined-data-alter-bug.md.
	//
	// The option name is held in a package var (not a literal) so a regression
	// test can swap it for a known-bad value and verify the error path is wired
	// up. Production code paths never mutate it.
	if err := s.Exec(fmt.Sprintf("CALL sandbox.set_option('%s', 0)", sandboxDataInliningOptionName)); err != nil {
		return fmt.Errorf("disable sandbox data inlining: %w", err)
	}

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

// printJSON emits rows as a JSON array of objects, preserving native types.
// Numbers stay numbers, booleans stay booleans, NULL becomes null.
// Bug 4 + 6 fix: previously took [][]string which discarded type information.
func printJSON(cols []string, data [][]any) error {
	var rows []map[string]any
	for _, row := range data {
		m := make(map[string]any, len(cols))
		for i, col := range cols {
			if i < len(row) {
				m[col] = jsonValue(row[i])
			}
		}
		rows = append(rows, m)
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(rows)
}

// jsonValue converts a database value to a JSON-friendly Go type.
// Times become RFC3339 strings; DuckDB Decimal and HUGEINT (*big.Int) are
// emitted as json.Number to preserve full precision; everything else passes
// through unchanged (numbers stay numbers, bools stay bools, nil becomes
// JSON null, []byte becomes a string).
func jsonValue(v any) any {
	if v == nil {
		return nil
	}
	switch val := v.(type) {
	case time.Time:
		return val.Format(time.RFC3339Nano)
	case []byte:
		return string(val)
	case duckdbdriver.Decimal:
		// Use the exact decimal string via json.Number — converting to
		// float64 silently rounds large/precise decimals (e.g. 18-digit
		// monetary amounts) and is a regression we already avoid in the
		// OData path. (Review finding 2)
		return json.Number(val.String())
	case *big.Int:
		// HUGEINT (int128) doesn't fit in any native JSON number type;
		// emit the exact integer string via json.Number.
		return json.Number(val.String())
	default:
		return val
	}
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
