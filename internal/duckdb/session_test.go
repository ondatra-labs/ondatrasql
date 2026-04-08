// OndatraSQL - You don't need a data stack anymore
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

//go:build integration

package duckdb

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// captureStdout captures stdout output from f and returns it as a string.
func captureStdout(t *testing.T, f func()) string {
	t.Helper()
	old := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe: %v", err)
	}
	os.Stdout = w
	defer func() { os.Stdout = old }()

	f()

	w.Close()
	out, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("read pipe: %v", err)
	}
	return string(out)
}

var shared *Session

func TestMain(m *testing.M) {
	s, err := NewSession(":memory:?threads=4&memory_limit=2GB")
	if err != nil {
		panic("create shared session: " + err.Error())
	}
	shared = s
	code := m.Run()
	shared.Close()
	os.Exit(code)
}

func TestNewSession(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	result, err := shared.QueryValue("SELECT 1")
	if err != nil {
		t.Errorf("session not working: %v", err)
	}
	if result != "1" {
		t.Errorf("got %q, want %q", result, "1")
	}
}

func TestSession_Query(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	result, err := shared.Query("SELECT 1 AS num, 'hello' AS msg")
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	// Query returns CSV: header + data row
	lines := strings.Split(strings.TrimSpace(result), "\n")
	if len(lines) != 2 {
		t.Fatalf("expected 2 lines (header+data), got %d: %q", len(lines), result)
	}
	if lines[0] != "num,msg" {
		t.Errorf("header = %q, want %q", lines[0], "num,msg")
	}
	if lines[1] != "1,hello" {
		t.Errorf("data = %q, want %q", lines[1], "1,hello")
	}
}

func TestSession_QueryValue(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	result, err := shared.QueryValue("SELECT 42")
	if err != nil {
		t.Fatalf("QueryValue failed: %v", err)
	}

	if result != "42" {
		t.Errorf("got %q, want %q", result, "42")
	}
}

func TestSession_Exec(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	err := shared.Exec("CREATE TABLE test_exec (id INT, name VARCHAR)")
	if err != nil {
		t.Fatalf("Exec CREATE failed: %v", err)
	}
	defer shared.Exec("DROP TABLE IF EXISTS test_exec")

	err = shared.Exec("INSERT INTO test_exec VALUES (1, 'foo'), (2, 'bar')")
	if err != nil {
		t.Fatalf("Exec INSERT failed: %v", err)
	}

	count, err := shared.QueryValue("SELECT COUNT(*) FROM test_exec")
	if err != nil {
		t.Fatalf("QueryValue failed: %v", err)
	}

	if count != "2" {
		t.Errorf("got count=%q, want %q", count, "2")
	}
}

func TestSession_QueryError(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	_, err := shared.Query("SELECT * FROM nonexistent_table_xyz")
	if err == nil {
		t.Error("expected error for nonexistent table")
	}
}

func TestSession_MultipleQueries(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	for i := 1; i <= 5; i++ {
		sql := "SELECT " + string(rune('0'+i))
		result, err := shared.QueryValue(sql)
		if err != nil {
			t.Fatalf("Query %d failed: %v", i, err)
		}
		want := string(rune('0' + i))
		if result != want {
			t.Errorf("query %d: got %q, want %q", i, result, want)
		}
	}
}

func TestSession_TempTable(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	err := shared.Exec("CREATE TEMP TABLE tmp_session_test AS SELECT 1 AS id, 'test' AS name")
	if err != nil {
		t.Fatalf("Create temp table failed: %v", err)
	}
	defer shared.Exec("DROP TABLE IF EXISTS tmp_session_test")

	result, err := shared.QueryValue("SELECT COUNT(*) FROM tmp_session_test")
	if err != nil {
		t.Fatalf("Query temp table failed: %v", err)
	}

	if result != "1" {
		t.Errorf("got count=%q, want %q", result, "1")
	}
}

func TestSession_Close(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	t.Run("first close succeeds", func(t *testing.T) {
		s, err := NewSession(":memory:?threads=1&memory_limit=256MB")
		if err != nil {
			t.Fatalf("NewSession failed: %v", err)
		}
		if err := s.Close(); err != nil {
			t.Fatalf("first Close: %v", err)
		}
	})

	t.Run("double close is idempotent", func(t *testing.T) {
		s, err := NewSession(":memory:?threads=1&memory_limit=256MB")
		if err != nil {
			t.Fatalf("NewSession failed: %v", err)
		}
		s.Close()
		if err := s.Close(); err != nil {
			t.Fatalf("second Close: %v", err)
		}
	})

	t.Run("query after close errors", func(t *testing.T) {
		s, err := NewSession(":memory:?threads=1&memory_limit=256MB")
		if err != nil {
			t.Fatalf("NewSession failed: %v", err)
		}
		s.Close()
		if _, err := s.Query("SELECT 1"); err == nil {
			t.Error("expected error after close")
		}
	})
}

func TestSession_ClosedSessionErrors(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	tests := []struct {
		name string
		fn   func(s *Session) error
	}{
		{"Exec", func(s *Session) error { return s.Exec("SELECT 1") }},
		{"Query", func(s *Session) error { _, err := s.Query("SELECT 1"); return err }},
		{"QueryValue", func(s *Session) error { _, err := s.QueryValue("SELECT 1"); return err }},
		{"QueryRows", func(s *Session) error { _, err := s.QueryRows("SELECT 1"); return err }},
		{"QueryRowsMap", func(s *Session) error { _, err := s.QueryRowsMap("SELECT 1 AS id"); return err }},
		{"QueryPrint", func(s *Session) error { return s.QueryPrint("SELECT 1", "csv") }},
		{"RawConn", func(s *Session) error { return s.RawConn(func(any) error { return nil }) }},
		{"ExecContext", func(s *Session) error { return s.ExecContext(context.Background(), "SELECT 1") }},
		{"QueryContext", func(s *Session) error { _, err := s.QueryContext(context.Background(), "SELECT 1"); return err }},
		{"QueryValueContext", func(s *Session) error { _, err := s.QueryValueContext(context.Background(), "SELECT 1"); return err }},
		{"QueryRowsContext", func(s *Session) error { _, err := s.QueryRowsContext(context.Background(), "SELECT 1"); return err }},
		{"QueryRowsMapContext", func(s *Session) error { _, err := s.QueryRowsMapContext(context.Background(), "SELECT 1 AS id"); return err }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, err := NewSession(":memory:")
			if err != nil {
				t.Fatalf("NewSession: %v", err)
			}
			s.Close()

			if err := tt.fn(s); err == nil {
				t.Errorf("expected error from closed session %s", tt.name)
			}
		})
	}
}

func TestSession_EmptySQL(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	t.Run("Exec empty", func(t *testing.T) {
		if err := shared.Exec(""); err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	})
	t.Run("Exec comment-only", func(t *testing.T) {
		if err := shared.Exec("-- just a comment"); err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	})
	t.Run("Query empty", func(t *testing.T) {
		if r, err := shared.Query(""); err != nil || r != "" {
			t.Errorf("result=%q, err=%v", r, err)
		}
	})
	t.Run("QueryValue empty", func(t *testing.T) {
		if r, err := shared.QueryValue(""); err != nil || r != "" {
			t.Errorf("result=%q, err=%v", r, err)
		}
	})
	t.Run("QueryRows empty", func(t *testing.T) {
		if r, err := shared.QueryRows(""); err != nil || r != nil {
			t.Errorf("result=%v, err=%v", r, err)
		}
	})
	t.Run("QueryRowsMap empty", func(t *testing.T) {
		if r, err := shared.QueryRowsMap(""); err != nil || r != nil {
			t.Errorf("result=%v, err=%v", r, err)
		}
	})
}

func TestSession_ExecContext(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	ctx := context.Background()
	err := shared.ExecContext(ctx, "SELECT 1")
	if err != nil {
		t.Errorf("ExecContext: %v", err)
	}
}

func TestSession_ExecContext_CancelledContext(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // immediately cancel
	// DuckDB may or may not error on cancelled context for simple queries.
	// We just verify the code path doesn't panic.
	_ = shared.ExecContext(ctx, "SELECT 1")
}

func TestSession_QueryRows(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	rows, err := shared.QueryRows("SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3")
	if err != nil {
		t.Fatalf("QueryRows: %v", err)
	}
	if len(rows) != 3 {
		t.Errorf("got %d rows, want 3", len(rows))
	}
}

func TestSession_QueryRowsMap(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	rows, err := shared.QueryRowsMap("SELECT 1 AS id, 'hello' AS msg, 3.14 AS val UNION ALL SELECT 2, 'world', 2.72")
	if err != nil {
		t.Fatalf("QueryRowsMap: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("got %d rows, want 2", len(rows))
	}
	if rows[0]["id"] != "1" || rows[0]["msg"] != "hello" || rows[0]["val"] != "3.14" {
		t.Errorf("row 0 = %v", rows[0])
	}
	if rows[1]["id"] != "2" || rows[1]["msg"] != "world" {
		t.Errorf("row 1 = %v", rows[1])
	}
}

func TestSession_GetVersion(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	v := shared.GetVersion()
	if v == "" || v == "unknown" {
		t.Errorf("GetVersion = %q, expected valid version string", v)
	}
	// Call again to verify caching returns same value
	if v2 := shared.GetVersion(); v2 != v {
		t.Errorf("GetVersion not cached: %q vs %q", v, v2)
	}
}

func TestSession_QuoteIdentifier(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	tests := []struct {
		input string
		want  string
	}{
		{"simple", `"simple"`},
		{`has"quote`, `"has""quote"`},
		{"", `""`},
	}
	for _, tt := range tests {
		got := QuoteIdentifier(tt.input)
		if got != tt.want {
			t.Errorf("QuoteIdentifier(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestSession_QueryPrint_Markdown(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	out := captureStdout(t, func() {
		if err := shared.QueryPrint("SELECT 1 AS id, 'hello' AS msg", "markdown"); err != nil {
			t.Errorf("QueryPrint markdown: %v", err)
		}
	})
	// Markdown format: | col | col | header, | --- | --- | separator, | val | val | rows
	if !strings.Contains(out, "| id") {
		t.Errorf("markdown output missing '| id' header, got:\n%s", out)
	}
	if !strings.Contains(out, "| 1") {
		t.Errorf("markdown output missing '| 1' data row, got:\n%s", out)
	}
	if !strings.Contains(out, "hello") {
		t.Errorf("markdown output missing 'hello', got:\n%s", out)
	}
	if !strings.Contains(out, "---") {
		t.Errorf("markdown output missing separator '---', got:\n%s", out)
	}
}

func TestSession_QueryPrint_CSV(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	out := captureStdout(t, func() {
		if err := shared.QueryPrint("SELECT 1 AS id, 'hello' AS msg", "csv"); err != nil {
			t.Errorf("QueryPrint csv: %v", err)
		}
	})
	lines := strings.Split(strings.TrimSpace(out), "\n")
	if len(lines) != 2 {
		t.Fatalf("expected 2 CSV lines (header + data), got %d:\n%s", len(lines), out)
	}
	if lines[0] != "id,msg" {
		t.Errorf("CSV header = %q, want %q", lines[0], "id,msg")
	}
	if lines[1] != "1,hello" {
		t.Errorf("CSV data = %q, want %q", lines[1], "1,hello")
	}
}

func TestSession_QueryPrint_JSON(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	out := captureStdout(t, func() {
		if err := shared.QueryPrint("SELECT 1 AS id, 'hello' AS msg", "json"); err != nil {
			t.Errorf("QueryPrint json: %v", err)
		}
	})
	// Bug 4/6 fix: JSON output preserves native types — id is a number,
	// msg is a string. Decode into []map[string]any so both can land in
	// their natural Go type.
	var rows []map[string]any
	if err := json.Unmarshal([]byte(out), &rows); err != nil {
		t.Fatalf("invalid JSON: %v\noutput:\n%s", err, out)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 JSON row, got %d", len(rows))
	}
	if got, ok := rows[0]["id"].(float64); !ok || got != 1 {
		t.Errorf("id = %v (%T), want JSON number 1", rows[0]["id"], rows[0]["id"])
	}
	if rows[0]["msg"] != "hello" {
		t.Errorf("msg = %v, want \"hello\"", rows[0]["msg"])
	}
}

func TestSession_QueryPrint_Default(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	out := captureStdout(t, func() {
		if err := shared.QueryPrint("SELECT 1 AS id", "box"); err != nil {
			t.Errorf("QueryPrint box (default): %v", err)
		}
	})
	// "box" falls through to markdown in the current implementation
	if !strings.Contains(out, "id") {
		t.Errorf("box output missing 'id' header, got:\n%s", out)
	}
	if !strings.Contains(out, "1") {
		t.Errorf("box output missing '1' value, got:\n%s", out)
	}
}

func TestSession_QueryPrint_Error(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	err := shared.QueryPrint("SELECT * FROM nonexistent_table_xyz", "csv")
	if err == nil {
		t.Error("expected error for nonexistent table")
	}
}

func TestSession_RawConn(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	err := shared.RawConn(func(raw any) error {
		if raw == nil {
			return fmt.Errorf("raw conn is nil")
		}
		return nil
	})
	if err != nil {
		t.Errorf("RawConn: %v", err)
	}
}

func TestSession_QueryContext(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	ctx := context.Background()
	result, err := shared.QueryContext(ctx, "SELECT 1 AS num")
	if err != nil {
		t.Fatalf("QueryContext: %v", err)
	}
	lines := strings.Split(strings.TrimSpace(result), "\n")
	if len(lines) != 2 {
		t.Fatalf("expected 2 lines, got %d: %q", len(lines), result)
	}
	if lines[0] != "num" {
		t.Errorf("header = %q, want %q", lines[0], "num")
	}
	if lines[1] != "1" {
		t.Errorf("data = %q, want %q", lines[1], "1")
	}
}

func TestSession_QueryValueContext(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	ctx := context.Background()
	result, err := shared.QueryValueContext(ctx, "SELECT 42")
	if err != nil {
		t.Fatalf("QueryValueContext: %v", err)
	}
	if result != "42" {
		t.Errorf("got %q, want 42", result)
	}
}

func TestSession_QueryRowsContext(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	ctx := context.Background()
	rows, err := shared.QueryRowsContext(ctx, "SELECT 1 UNION ALL SELECT 2")
	if err != nil {
		t.Fatalf("QueryRowsContext: %v", err)
	}
	if len(rows) != 2 {
		t.Errorf("got %d rows, want 2", len(rows))
	}
}

func TestSession_QueryRowsMapContext(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	ctx := context.Background()
	rows, err := shared.QueryRowsMapContext(ctx, "SELECT 1 AS id, 'hello' AS msg")
	if err != nil {
		t.Fatalf("QueryRowsMapContext: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("got %d rows, want 1", len(rows))
	}
	if rows[0]["id"] != "1" || rows[0]["msg"] != "hello" {
		t.Errorf("row = %v", rows[0])
	}
}

func TestSession_CatalogAlias(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	// Fresh session should return default "lake"
	s, err := NewSession(":memory:?threads=1&memory_limit=256MB")
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	defer s.Close()

	if alias := s.CatalogAlias(); alias != "lake" {
		t.Errorf("CatalogAlias() = %q, want lake", alias)
	}

	if prod := s.ProdAlias(); prod != "" {
		t.Errorf("ProdAlias() = %q, want empty", prod)
	}
}

func TestSession_RefreshSnapshot_NoLake(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	s, err := NewSession(":memory:?threads=1&memory_limit=256MB")
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	defer s.Close()
	// Without DuckLake attached, RefreshSnapshot should error
	if err := s.RefreshSnapshot(); err == nil {
		t.Error("expected error from RefreshSnapshot without DuckLake catalog")
	}
}

func TestSession_CSVEscape(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	// Test with values that need CSV escaping (commas, quotes, newlines)
	err := shared.Exec("CREATE TEMP TABLE csv_test (val VARCHAR)")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	defer shared.Exec("DROP TABLE IF EXISTS csv_test")

	err = shared.Exec(`INSERT INTO csv_test VALUES ('hello,world'), ('"quoted"'), ('line1
line2')`)
	if err != nil {
		t.Fatalf("insert: %v", err)
	}

	result, err := shared.Query("SELECT val FROM csv_test ORDER BY val")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	// Verify all three values appear in output
	if !strings.Contains(result, "hello,world") && !strings.Contains(result, "hello") {
		t.Errorf("output missing 'hello,world': %q", result)
	}
	if !strings.Contains(result, "quoted") {
		t.Errorf("output missing 'quoted': %q", result)
	}
	if !strings.Contains(result, "line1") {
		t.Errorf("output missing 'line1': %q", result)
	}
}

func TestSession_AnyToString_Types(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	// Test various DuckDB types through QueryRowsMap for exact value checking
	err := shared.Exec("CREATE TEMP TABLE type_test (i INT, f DOUBLE, b BOOLEAN, s VARCHAR, ts TIMESTAMP)")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	defer shared.Exec("DROP TABLE IF EXISTS type_test")

	err = shared.Exec("INSERT INTO type_test VALUES (42, 3.14, true, 'hello', '2024-01-15 10:30:00')")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}

	rows, err := shared.QueryRowsMap("SELECT * FROM type_test")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
	row := rows[0]
	if row["i"] != "42" {
		t.Errorf("INT: got %q, want 42", row["i"])
	}
	if row["f"] != "3.14" {
		t.Errorf("DOUBLE: got %q, want 3.14", row["f"])
	}
	if row["b"] != "true" {
		t.Errorf("BOOLEAN: got %q, want true", row["b"])
	}
	if row["s"] != "hello" {
		t.Errorf("VARCHAR: got %q, want hello", row["s"])
	}
	if !strings.Contains(row["ts"], "2024-01-15") {
		t.Errorf("TIMESTAMP: got %q, want to contain 2024-01-15", row["ts"])
	}
}

func TestSession_QueryPrint_MultipleRows(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	out := captureStdout(t, func() {
		if err := shared.QueryPrint("SELECT 'short' AS col1, 'a much longer value here' AS col2 UNION ALL SELECT 'x', 'y'", "markdown"); err != nil {
			t.Errorf("QueryPrint multiple rows: %v", err)
		}
	})
	lines := strings.Split(strings.TrimSpace(out), "\n")
	if len(lines) != 4 { // header + separator + 2 data rows
		t.Fatalf("expected 4 markdown lines, got %d:\n%s", len(lines), out)
	}
	if !strings.Contains(lines[0], "col1") || !strings.Contains(lines[0], "col2") {
		t.Errorf("header missing column names: %q", lines[0])
	}
	if !strings.Contains(lines[2], "short") {
		t.Errorf("first data row missing 'short': %q", lines[2])
	}
	if !strings.Contains(lines[3], "x") {
		t.Errorf("second data row missing 'x': %q", lines[3])
	}
}

func TestNewSession_EmptyDBFile(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	// Empty string should default to :memory:
	s, err := NewSession("")
	if err != nil {
		t.Fatalf("NewSession(''): %v", err)
	}
	defer s.Close()

	val, err := s.QueryValue("SELECT 42")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if val != "42" {
		t.Errorf("got %q, want 42", val)
	}
}

// TestSession_EmptySQLContext removed — Exec/Query/etc. delegate to ExecContext/QueryContext/etc.
// with context.Background(), so TestSession_EmptySQL already covers these code paths.

func TestSession_QueryValueNoRows(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	// Query that returns no rows should return empty string
	err := shared.Exec("CREATE TEMP TABLE empty_test (id INT)")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	defer shared.Exec("DROP TABLE IF EXISTS empty_test")

	val, err := shared.QueryValue("SELECT id FROM empty_test")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if val != "" {
		t.Errorf("got %q, want empty string", val)
	}
}

func TestSession_QueryPrint_Table(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	out := captureStdout(t, func() {
		if err := shared.QueryPrint("SELECT 1 AS id", "table"); err != nil {
			t.Errorf("QueryPrint table: %v", err)
		}
	})
	// "table" falls through to markdown
	if !strings.Contains(out, "| id") {
		t.Errorf("table output missing '| id': got:\n%s", out)
	}
	if !strings.Contains(out, "| 1") {
		t.Errorf("table output missing '| 1': got:\n%s", out)
	}
}

func TestSession_GetVersion_ClosedSession(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	s, err := NewSession(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	s.Close()
	v := s.GetVersion()
	if v != "unknown" {
		t.Errorf("GetVersion on closed session = %q, want unknown", v)
	}
}

func TestSession_QueryPrint_NullValues(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	out := captureStdout(t, func() {
		if err := shared.QueryPrint("SELECT NULL AS col1, 1 AS col2", "csv"); err != nil {
			t.Errorf("QueryPrint with NULL: %v", err)
		}
	})
	lines := strings.Split(strings.TrimSpace(out), "\n")
	if len(lines) != 2 {
		t.Fatalf("expected 2 CSV lines, got %d:\n%s", len(lines), out)
	}
	if lines[0] != "col1,col2" {
		t.Errorf("header = %q, want %q", lines[0], "col1,col2")
	}
	// NULL should render as empty string in CSV
	if !strings.HasPrefix(lines[1], ",") && !strings.Contains(lines[1], "NULL") {
		t.Errorf("NULL rendering unexpected: %q", lines[1])
	}
}

func TestSession_QueryPrint_BooleanValues(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	out := captureStdout(t, func() {
		if err := shared.QueryPrint("SELECT true AS flag, false AS other", "json"); err != nil {
			t.Errorf("QueryPrint boolean: %v", err)
		}
	})
	// Bug 4/6 fix: booleans render as JSON true/false, not strings.
	var rows []map[string]any
	if err := json.Unmarshal([]byte(out), &rows); err != nil {
		t.Fatalf("invalid JSON: %v\noutput:\n%s", err, out)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
	if rows[0]["flag"] != true {
		t.Errorf("flag = %v, want JSON true", rows[0]["flag"])
	}
	if rows[0]["other"] != false {
		t.Errorf("other = %v, want JSON false", rows[0]["other"])
	}
}

func TestSession_QueryPrint_TimestampValues(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	out := captureStdout(t, func() {
		if err := shared.QueryPrint("SELECT TIMESTAMP '2024-01-15 12:30:45' AS ts", "csv"); err != nil {
			t.Errorf("QueryPrint timestamp: %v", err)
		}
	})
	lines := strings.Split(strings.TrimSpace(out), "\n")
	if len(lines) != 2 {
		t.Fatalf("expected 2 CSV lines, got %d:\n%s", len(lines), out)
	}
	if lines[0] != "ts" {
		t.Errorf("header = %q, want %q", lines[0], "ts")
	}
	if !strings.Contains(lines[1], "2024-01-15") {
		t.Errorf("timestamp not found in output: %q", lines[1])
	}
}

// --- Error path tests ---

func TestNewSession_InvalidDSN(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	// An invalid path that DuckDB can't write to should fail
	_, err := NewSession("/nonexistent/deeply/nested/path/db.duckdb")
	if err == nil {
		t.Error("expected error for invalid database path")
	}
}

func TestSession_InitWithCatalog_InvalidConfigFiles(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	// Pre-catalog files: settings, secrets, extensions fail before catalog.sql runs
	preCatalogFiles := []string{"settings.sql", "secrets.sql", "extensions.sql"}
	for _, file := range preCatalogFiles {
		t.Run(file, func(t *testing.T) {
			s, err := NewSession(":memory:")
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()

			dir := t.TempDir()
			configPath := filepath.Join(dir, "config")
			os.MkdirAll(configPath, 0o755)
			os.WriteFile(filepath.Join(configPath, file), []byte("INVALID SQL;"), 0o644)
			os.WriteFile(filepath.Join(configPath, "catalog.sql"), []byte("SELECT 1;"), 0o644)

			err = s.InitWithCatalog(configPath)
			if err == nil {
				t.Fatalf("expected error from invalid %s", file)
			}
			if !strings.Contains(err.Error(), file) {
				t.Errorf("error should mention %s: %v", file, err)
			}
		})
	}

	// Post-catalog files: macros, schemas, variables, sources need a valid catalog first
	postCatalogFiles := []string{"macros.sql", "schemas.sql", "variables.sql", "sources.sql"}
	for _, file := range postCatalogFiles {
		t.Run(file, func(t *testing.T) {
			dir := t.TempDir()
			configPath := filepath.Join(dir, "config")
			os.MkdirAll(configPath, 0o755)

			catalogPath := filepath.Join(dir, "ducklake.sqlite")
			dataPath := filepath.Join(dir, "data")
			os.WriteFile(filepath.Join(configPath, "catalog.sql"),
				[]byte("ATTACH 'ducklake:sqlite:"+catalogPath+"' AS lake (DATA_PATH '"+dataPath+"');\n"), 0o644)
			os.WriteFile(filepath.Join(configPath, file), []byte("INVALID SQL;"), 0o644)

			s, err := NewSession(":memory:")
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()

			err = s.InitWithCatalog(configPath)
			if err == nil {
				t.Fatalf("expected error from invalid %s", file)
			}
			if !strings.Contains(err.Error(), file) {
				t.Errorf("error should mention %s: %v", file, err)
			}
		})
	}
}

func TestSession_InitWithCatalog_InvalidCatalogSQL(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	s, err := NewSession(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	dir := t.TempDir()
	configPath := filepath.Join(dir, "config")
	os.MkdirAll(configPath, 0o755)
	os.WriteFile(filepath.Join(configPath, "catalog.sql"), []byte("THIS IS NOT VALID SQL;"), 0o644)

	err = s.InitWithCatalog(configPath)
	if err == nil {
		t.Fatal("expected error from invalid catalog.sql")
	}
	if !strings.Contains(err.Error(), "catalog.sql") {
		t.Errorf("error should mention catalog.sql: %v", err)
	}
}

func TestSession_InitWithCatalog_NoCatalogAttached(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	s, err := NewSession(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	dir := t.TempDir()
	configPath := filepath.Join(dir, "config")
	os.MkdirAll(configPath, 0o755)
	os.WriteFile(filepath.Join(configPath, "catalog.sql"), []byte("SELECT 1;"), 0o644)

	err = s.InitWithCatalog(configPath)
	if err == nil {
		t.Fatal("expected error when no ducklake catalog found")
	}
	if !strings.Contains(err.Error(), "no ducklake catalog found") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestSession_InitWithCatalog_EmptyConfigFiles(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config")
	os.MkdirAll(configPath, 0o755)

	catalogPath := filepath.Join(dir, "ducklake.sqlite")
	dataPath := filepath.Join(dir, "data")
	os.WriteFile(filepath.Join(configPath, "settings.sql"), []byte(""), 0o644)
	os.WriteFile(filepath.Join(configPath, "secrets.sql"), []byte(""), 0o644)
	os.WriteFile(filepath.Join(configPath, "extensions.sql"), []byte(""), 0o644)
	os.WriteFile(filepath.Join(configPath, "catalog.sql"),
		[]byte("ATTACH 'ducklake:sqlite:"+catalogPath+"' AS lake (DATA_PATH '"+dataPath+"');\n"), 0o644)

	s, err := NewSession(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	err = s.InitWithCatalog(configPath)
	if err != nil {
		t.Fatalf("InitWithCatalog with empty config files: %v", err)
	}
	if alias := s.CatalogAlias(); alias != "lake" {
		t.Errorf("CatalogAlias = %q, want lake", alias)
	}
}

func TestSession_HasCDCChanges_NoLake(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	// Without CDC variables set, HasCDCChanges should return false
	// (getvariable returns NULL for undefined variables, comparison yields false)
	s, err := NewSession(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	has, err := s.HasCDCChanges()
	if err != nil {
		t.Fatalf("HasCDCChanges: %v", err)
	}
	if has {
		t.Error("expected no changes without CDC variables set")
	}
}

func TestSession_GetDagStartSnapshot_NoLake(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	// Without CDC variables set, GetDagStartSnapshot should return 0
	// (getvariable returns NULL, cast to BIGINT yields NULL, Sscanf parses 0)
	s, err := NewSession(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	id, err := s.GetDagStartSnapshot()
	if err != nil {
		t.Fatalf("GetDagStartSnapshot: %v", err)
	}
	if id != 0 {
		t.Errorf("GetDagStartSnapshot = %d, want 0 without CDC variables", id)
	}
}

func TestSession_SetHighWaterMark_NoLake(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	s, err := NewSession(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	// Without DuckLake, SetHighWaterMark should error (no snapshots function)
	err = s.SetHighWaterMark("some.model")
	if err == nil {
		t.Error("expected error from SetHighWaterMark without catalog")
	}
}

func TestSession_InitSandbox_MissingProdCatalog(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	s, err := NewSession(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	dir := t.TempDir()
	configPath := filepath.Join(dir, "config")
	os.MkdirAll(configPath, 0o755)

	err = s.InitSandbox(configPath, "ducklake:sqlite:/nonexistent/path/catalog.sqlite", "", filepath.Join(dir, "sandbox.sqlite"), "lake")
	if err == nil {
		t.Fatal("expected error when prod catalog path doesn't exist")
	}
	if !strings.Contains(err.Error(), "attach prod") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestSession_InitSandbox_InvalidPreCatalogFiles(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	// settings, secrets, extensions fail before prod catalog is attached
	for _, file := range []string{"settings.sql", "secrets.sql", "extensions.sql"} {
		t.Run(file, func(t *testing.T) {
			s, err := NewSession(":memory:")
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()

			dir := t.TempDir()
			configPath := filepath.Join(dir, "config")
			os.MkdirAll(configPath, 0o755)
			os.WriteFile(filepath.Join(configPath, file), []byte("INVALID SQL;"), 0o644)

			err = s.InitSandbox(configPath, "ducklake:sqlite:"+filepath.Join(dir, "prod.sqlite"), "", filepath.Join(dir, "sandbox.sqlite"), "lake")
			if err == nil {
				t.Fatalf("expected error from invalid %s", file)
			}
			if !strings.Contains(err.Error(), file) {
				t.Errorf("error should mention %s: %v", file, err)
			}
		})
	}
}

// createProdCatalogForSession creates a prod catalog and returns its path and data path.
func createProdCatalogForSession(t *testing.T) (prodCatalogPath, prodDataPath string) {
	t.Helper()
	prodDir := t.TempDir()
	prodCatalogPath = filepath.Join(prodDir, "ducklake.sqlite")
	prodDataPath = filepath.Join(prodDir, "data")

	prodSess, err := NewSession(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	prodConfigPath := filepath.Join(prodDir, "config")
	os.MkdirAll(prodConfigPath, 0o755)
	os.WriteFile(filepath.Join(prodConfigPath, "catalog.sql"),
		[]byte("ATTACH 'ducklake:sqlite:"+prodCatalogPath+"' AS lake (DATA_PATH '"+prodDataPath+"');\n"), 0o644)
	if err := prodSess.InitWithCatalog(prodConfigPath); err != nil {
		prodSess.Close()
		t.Fatalf("init prod catalog: %v", err)
	}
	prodSess.Close()
	return prodCatalogPath, prodDataPath
}

func TestSession_HasCDCChanges_WithVariables(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	s, err := NewSession(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	// Set variables so CDC comparison works
	s.Exec("SET VARIABLE dag_start_snapshot = 1")
	s.Exec("SET VARIABLE curr_snapshot = 5")

	has, err := s.HasCDCChanges()
	if err != nil {
		t.Fatalf("HasCDCChanges: %v", err)
	}
	if !has {
		t.Error("expected changes when curr > dag_start + 1")
	}

	// No changes when curr == dag_start
	s.Exec("SET VARIABLE curr_snapshot = 1")
	has, err = s.HasCDCChanges()
	if err != nil {
		t.Fatalf("HasCDCChanges: %v", err)
	}
	if has {
		t.Error("expected no changes when curr == dag_start")
	}
}

func TestSession_GetDagStartSnapshot_WithVariable(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	s, err := NewSession(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	s.Exec("SET VARIABLE dag_start_snapshot = 42")

	id, err := s.GetDagStartSnapshot()
	if err != nil {
		t.Fatalf("GetDagStartSnapshot: %v", err)
	}
	if id != 42 {
		t.Errorf("GetDagStartSnapshot = %d, want 42", id)
	}
}

func TestSession_InitSandbox_Success(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	prodCatalogPath, prodDataPath := createProdCatalogForSession(t)

	sandboxDir := t.TempDir()
	sandboxCatalog := filepath.Join(sandboxDir, "sandbox.sqlite")
	sandboxConfigPath := filepath.Join(sandboxDir, "config")
	os.MkdirAll(sandboxConfigPath, 0o755)

	s, err := NewSession(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	err = s.InitSandbox(sandboxConfigPath, "ducklake:sqlite:"+prodCatalogPath, prodDataPath, sandboxCatalog, "lake")
	if err != nil {
		t.Fatalf("InitSandbox: %v", err)
	}

	// Verify catalog aliases
	if s.CatalogAlias() != "sandbox" {
		t.Errorf("CatalogAlias = %q, want sandbox", s.CatalogAlias())
	}
	if s.ProdAlias() != "lake" {
		t.Errorf("ProdAlias = %q, want lake", s.ProdAlias())
	}

	// Verify we can query in sandbox mode
	if err := s.Exec("CREATE TABLE sandbox.test_tbl (id INTEGER)"); err != nil {
		t.Fatalf("create table in sandbox: %v", err)
	}
	val, err := s.QueryValue("SELECT COUNT(*) FROM sandbox.test_tbl")
	if err != nil {
		t.Fatalf("query sandbox: %v", err)
	}
	if val != "0" {
		t.Errorf("count = %s, want 0", val)
	}

	// Verify CDC variables are set
	id, err := s.GetDagStartSnapshot()
	if err != nil {
		t.Fatalf("GetDagStartSnapshot: %v", err)
	}
	// Should be >= 0 (prod catalog has snapshots from init)
	if id < 0 {
		t.Errorf("dag_start_snapshot = %d, want >= 0", id)
	}
}

func TestSession_InitSandbox_InvalidPostCatalogFiles(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	// macros, schemas, variables, sources need a valid prod catalog first
	for _, file := range []string{"macros.sql", "schemas.sql", "variables.sql", "sources.sql"} {
		t.Run(file, func(t *testing.T) {
			prodCatalogPath, prodDataPath := createProdCatalogForSession(t)

			sandboxDir := t.TempDir()
			sandboxCatalog := filepath.Join(sandboxDir, "sandbox.sqlite")
			sandboxConfigPath := filepath.Join(sandboxDir, "config")
			os.MkdirAll(sandboxConfigPath, 0o755)
			os.WriteFile(filepath.Join(sandboxConfigPath, file), []byte("INVALID SQL;"), 0o644)

			s, err := NewSession(":memory:")
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()

			err = s.InitSandbox(sandboxConfigPath, "ducklake:sqlite:"+prodCatalogPath, prodDataPath, sandboxCatalog, "lake")
			if err == nil {
				t.Fatalf("expected error from invalid %s in sandbox", file)
			}
			if !strings.Contains(err.Error(), file) {
				t.Errorf("error should mention %s: %v", file, err)
			}
		})
	}
}

func TestSession_AnyToString_AllBranches(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	type customType struct{ X int }

	ts := time.Date(2025, 6, 15, 10, 30, 45, 0, time.UTC)

	tests := []struct {
		name  string
		input any
		want  string
	}{
		{"nil", nil, ""},
		{"string", "hello", "hello"},
		{"bytes", []byte("world"), "world"},
		{"int64", int64(42), "42"},
		{"float64", float64(3.14), "3.14"},
		{"bool_true", true, "true"},
		{"bool_false", false, "false"},
		{"time", ts, "2025-06-15 10:30:45"},
		{"custom_type", customType{X: 7}, "{7}"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := anyToString(tt.input)
			if got != tt.want {
				t.Errorf("anyToString(%v) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestSession_EscapeCSV_AllBranches(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{"plain", "hello", "hello"},
		{"comma", "a,b", `"a,b"`},
		{"quote", `say "hi"`, `"say ""hi"""`},
		{"newline", "line1\nline2", "\"line1\nline2\""},
		{"carriage_return", "a\rb", "\"a\rb\""},
		{"empty", "", ""},
		{"all_special", "a,\"b\"\nc\r", "\"a,\"\"b\"\"\nc\r\""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := escapeCSV(tt.input)
			if got != tt.want {
				t.Errorf("escapeCSV(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestSession_QueryPrint_CSVEscaping(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	err := shared.Exec(`CREATE OR REPLACE TABLE csv_escape_test (val VARCHAR)`)
	if err != nil {
		t.Fatal(err)
	}
	err = shared.Exec(`INSERT INTO csv_escape_test VALUES ('hello, "world"')`)
	if err != nil {
		t.Fatal(err)
	}

	out := captureStdout(t, func() {
		if err := shared.QueryPrint(`SELECT val FROM csv_escape_test`, "csv"); err != nil {
			t.Fatal(err)
		}
	})

	lines := strings.Split(strings.TrimSpace(out), "\n")
	if len(lines) != 2 {
		t.Fatalf("expected 2 lines (header + row), got %d: %q", len(lines), out)
	}
	// Header should be plain column name
	if lines[0] != "val" {
		t.Errorf("header = %q, want %q", lines[0], "val")
	}
	// Data row should have escaped CSV: comma and doubled quotes
	want := `"hello, ""world"""`
	if lines[1] != want {
		t.Errorf("data = %q, want %q", lines[1], want)
	}
}

func TestSession_InitWithCatalog_ExpandsEnvVars(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}

	t.Run("loadSQL expands env vars", func(t *testing.T) {
		dir := t.TempDir()
		configPath := filepath.Join(dir, "config")
		os.MkdirAll(configPath, 0o755)

		catalogPath := filepath.Join(dir, "ducklake.sqlite")
		dataPath := filepath.Join(dir, "data")
		os.WriteFile(filepath.Join(configPath, "catalog.sql"),
			[]byte("ATTACH 'ducklake:sqlite:"+catalogPath+"' AS lake (DATA_PATH '"+dataPath+"');\n"), 0o644)

		// settings.sql uses ${ONDATRA_TEST_THREADS} env var
		t.Setenv("ONDATRA_TEST_THREADS", "2")
		os.WriteFile(filepath.Join(configPath, "settings.sql"),
			[]byte("SET threads = ${ONDATRA_TEST_THREADS};\n"), 0o644)

		s, err := NewSession(":memory:")
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()

		if err := s.InitWithCatalog(configPath); err != nil {
			t.Fatalf("InitWithCatalog: %v", err)
		}

		val, err := s.QueryValue("SELECT current_setting('threads')")
		if err != nil {
			t.Fatalf("query threads: %v", err)
		}
		if val != "2" {
			t.Errorf("threads = %q, want %q", val, "2")
		}
	})

	t.Run("catalog.sql expands env vars", func(t *testing.T) {
		dir := t.TempDir()
		configPath := filepath.Join(dir, "config")
		os.MkdirAll(configPath, 0o755)

		catalogPath := filepath.Join(dir, "ducklake.sqlite")
		dataPath := filepath.Join(dir, "data")

		// catalog.sql references env vars for paths
		t.Setenv("ONDATRA_TEST_CATALOG", catalogPath)
		t.Setenv("ONDATRA_TEST_DATA", dataPath)
		os.WriteFile(filepath.Join(configPath, "catalog.sql"),
			[]byte("ATTACH 'ducklake:sqlite:${ONDATRA_TEST_CATALOG}' AS lake (DATA_PATH '${ONDATRA_TEST_DATA}');\n"), 0o644)

		s, err := NewSession(":memory:")
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()

		if err := s.InitWithCatalog(configPath); err != nil {
			t.Fatalf("InitWithCatalog with env vars in catalog.sql: %v", err)
		}

		if alias := s.CatalogAlias(); alias != "lake" {
			t.Errorf("CatalogAlias = %q, want lake", alias)
		}
	})

	t.Run("variables.sql expands env vars", func(t *testing.T) {
		dir := t.TempDir()
		configPath := filepath.Join(dir, "config")
		os.MkdirAll(configPath, 0o755)

		catalogPath := filepath.Join(dir, "ducklake.sqlite")
		dataPath := filepath.Join(dir, "data")
		os.WriteFile(filepath.Join(configPath, "catalog.sql"),
			[]byte("ATTACH 'ducklake:sqlite:"+catalogPath+"' AS lake (DATA_PATH '"+dataPath+"');\n"), 0o644)

		t.Setenv("ONDATRA_TEST_VAR", "hello_from_env")
		os.WriteFile(filepath.Join(configPath, "variables.sql"),
			[]byte("SET VARIABLE test_env_var = '${ONDATRA_TEST_VAR}';\n"), 0o644)

		s, err := NewSession(":memory:")
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()

		if err := s.InitWithCatalog(configPath); err != nil {
			t.Fatalf("InitWithCatalog: %v", err)
		}

		val, err := s.QueryValue("SELECT getvariable('test_env_var')")
		if err != nil {
			t.Fatalf("query variable: %v", err)
		}
		if val != "hello_from_env" {
			t.Errorf("test_env_var = %q, want %q", val, "hello_from_env")
		}
	})

	t.Run("macros.sql expands env vars", func(t *testing.T) {
		dir := t.TempDir()
		configPath := filepath.Join(dir, "config")
		os.MkdirAll(configPath, 0o755)

		catalogPath := filepath.Join(dir, "ducklake.sqlite")
		dataPath := filepath.Join(dir, "data")
		os.WriteFile(filepath.Join(configPath, "catalog.sql"),
			[]byte("ATTACH 'ducklake:sqlite:"+catalogPath+"' AS lake (DATA_PATH '"+dataPath+"');\n"), 0o644)

		t.Setenv("ONDATRA_TEST_DEFAULT", "42")
		os.WriteFile(filepath.Join(configPath, "macros.sql"),
			[]byte("CREATE MACRO test_env_macro() AS ${ONDATRA_TEST_DEFAULT};\n"), 0o644)

		s, err := NewSession(":memory:")
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()

		if err := s.InitWithCatalog(configPath); err != nil {
			t.Fatalf("InitWithCatalog: %v", err)
		}

		val, err := s.QueryValue("SELECT test_env_macro()")
		if err != nil {
			t.Fatalf("query macro: %v", err)
		}
		if val != "42" {
			t.Errorf("test_env_macro() = %q, want %q", val, "42")
		}
	})

	t.Run("undefined env var expands to empty", func(t *testing.T) {
		dir := t.TempDir()
		configPath := filepath.Join(dir, "config")
		os.MkdirAll(configPath, 0o755)

		catalogPath := filepath.Join(dir, "ducklake.sqlite")
		dataPath := filepath.Join(dir, "data")
		os.WriteFile(filepath.Join(configPath, "catalog.sql"),
			[]byte("ATTACH 'ducklake:sqlite:"+catalogPath+"' AS lake (DATA_PATH '"+dataPath+"');\n"), 0o644)

		// Unset variable should expand to empty string
		os.Unsetenv("ONDATRA_TEST_UNDEFINED_VAR_XYZ")
		os.WriteFile(filepath.Join(configPath, "variables.sql"),
			[]byte("SET VARIABLE test_undef = '${ONDATRA_TEST_UNDEFINED_VAR_XYZ}';\n"), 0o644)

		s, err := NewSession(":memory:")
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()

		if err := s.InitWithCatalog(configPath); err != nil {
			t.Fatalf("InitWithCatalog: %v", err)
		}

		val, err := s.QueryValue("SELECT getvariable('test_undef')")
		if err != nil {
			t.Fatalf("query variable: %v", err)
		}
		if val != "" {
			t.Errorf("undefined var = %q, want empty string", val)
		}
	})
}

func TestSession_ProdAlias_Default(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	got := shared.ProdAlias()
	if got != "" {
		t.Errorf("ProdAlias() = %q, want empty string for non-sandbox session", got)
	}
}

func TestSession_CatalogAlias_Default(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	// A session that went through InitWithCatalog should return "lake"
	// The shared session did not go through InitWithCatalog, so test the
	// default fallback (catalogAlias == "" → "lake").
	got := shared.CatalogAlias()
	if got != "lake" {
		t.Errorf("CatalogAlias() = %q, want %q", got, "lake")
	}
}

func TestSession_RawConn_ClosedSession(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	s, err := NewSession(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	s.Close()

	err = s.RawConn(func(_ any) error { return nil })
	if err == nil {
		t.Fatal("expected error from RawConn on closed session")
	}
	if !strings.Contains(err.Error(), "closed") {
		t.Errorf("error = %q, want it to mention 'closed'", err.Error())
	}
}

func TestSession_RefreshSnapshot_ClosedSession(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	s, err := NewSession(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	s.Close()

	err = s.RefreshSnapshot()
	if err == nil {
		t.Fatal("expected error from RefreshSnapshot on closed session")
	}
	if !strings.Contains(err.Error(), "closed") {
		t.Errorf("error = %q, want it to mention 'closed'", err.Error())
	}
}

func TestSession_SetHighWaterMark_ClosedSession(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	s, err := NewSession(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	s.Close()

	err = s.SetHighWaterMark("some_target")
	if err == nil {
		t.Fatal("expected error from SetHighWaterMark on closed session")
	}
	if !strings.Contains(err.Error(), "closed") {
		t.Errorf("error = %q, want it to mention 'closed'", err.Error())
	}
}

// --- Regression: EnsureReadOnly statement-type filtering ---
//
// `ondatrasql sql` is documented as read-only. Earlier allowlists let
// PRAGMA and CALL through even though both can mutate session/catalog state.

func TestEnsureReadOnly_AllowsSelect(t *testing.T) {
	s, err := NewSession(":memory:?threads=1&memory_limit=256MB")
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	defer s.Close()
	if err := s.EnsureReadOnly("SELECT 1"); err != nil {
		t.Errorf("SELECT 1 should pass read-only check: %v", err)
	}
	if err := s.EnsureReadOnly("WITH x AS (SELECT 1) SELECT * FROM x"); err != nil {
		t.Errorf("WITH ... SELECT should pass read-only check: %v", err)
	}
}

func TestEnsureReadOnly_AllowsRelation(t *testing.T) {
	s, err := NewSession(":memory:?threads=1&memory_limit=256MB")
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	defer s.Close()
	if err := s.EnsureReadOnly("DESCRIBE SELECT 1 AS a"); err != nil {
		t.Errorf("DESCRIBE should pass: %v", err)
	}
	if err := s.EnsureReadOnly("SHOW TABLES"); err != nil {
		t.Errorf("SHOW TABLES should pass: %v", err)
	}
}

func TestEnsureReadOnly_RejectsCallProcedure(t *testing.T) {
	// CALL is the syntax used for DuckLake maintenance procedures like
	// ducklake_merge_adjacent_files which mutate catalog state. The
	// allowlist used to include STATEMENT_TYPE_CALL, letting them through.
	s, err := NewSession(":memory:?threads=1&memory_limit=256MB")
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	defer s.Close()
	// pg_catalog.pg_table_is_visible is a benign callable; we just need
	// the parser to recognise the CALL form.
	err = s.EnsureReadOnly("CALL pragma_database_size()")
	if err == nil {
		t.Fatal("CALL should be rejected by read-only check")
	}
	if !strings.Contains(err.Error(), "read-only") {
		t.Errorf("expected error to mention 'read-only', got %q", err.Error())
	}
}

func TestEnsureReadOnly_RejectsMutatingPragma(t *testing.T) {
	// PRAGMA threads / memory_limit / enable_profiling all mutate session
	// state. Toggle-style pragmas (no `=value`) parse as STATEMENT_TYPE_PRAGMA;
	// assignment-style pragmas parse as STATEMENT_TYPE_SET. Both must reject.
	s, err := NewSession(":memory:?threads=1&memory_limit=256MB")
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	defer s.Close()
	cases := []string{
		"PRAGMA enable_profiling",
		"PRAGMA disable_optimizer",
		"PRAGMA threads=4",
		"PRAGMA memory_limit='1GB'",
	}
	for _, q := range cases {
		err := s.EnsureReadOnly(q)
		if err == nil {
			t.Errorf("%q should be rejected by read-only check", q)
			continue
		}
		if !strings.Contains(err.Error(), "read-only") {
			t.Errorf("%q: expected error to mention 'read-only', got %q", q, err.Error())
		}
	}
}

func TestEnsureReadOnly_RejectsDDL(t *testing.T) {
	s, err := NewSession(":memory:?threads=1&memory_limit=256MB")
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	defer s.Close()
	// Pre-create the table so DuckDB's prepare doesn't reject DML/ALTER
	// with a Catalog Error before the read-only check has a chance to run.
	if err := s.Exec("CREATE TABLE rdo_test (a INT)"); err != nil {
		t.Fatalf("setup: %v", err)
	}
	cases := map[string]string{
		"CREATE TABLE rdo_other (a INT)":      "CREATE",
		"DROP TABLE IF EXISTS rdo_test":       "DROP",
		"ALTER TABLE rdo_test RENAME TO rdo2": "ALTER",
		"INSERT INTO rdo_test VALUES (1)":     "INSERT",
		"DELETE FROM rdo_test":                "DELETE",
		"UPDATE rdo_test SET a=1":             "UPDATE",
	}
	for q, kind := range cases {
		err := s.EnsureReadOnly(q)
		if err == nil {
			t.Errorf("%s: %q should be rejected", kind, q)
			continue
		}
		if !strings.Contains(err.Error(), "read-only") {
			t.Errorf("%s: expected error to mention 'read-only', got %q", kind, err.Error())
		}
	}
}

// --- Regression: jsonValue precision for DECIMAL and HUGEINT ---
//
// Earlier `jsonValue` rendered duckdb.Decimal via Float64() which silently
// rounds large/precise decimals (e.g. 18-digit monetary amounts). HUGEINT
// (*big.Int) had no case at all and fell through to fmt.Sprintf "%v".

func TestQueryPrint_DecimalExactPrecision(t *testing.T) {
	s, err := NewSession(":memory:?threads=1&memory_limit=256MB")
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	defer s.Close()
	out := captureStdout(t, func() {
		err := s.QueryPrint("SELECT CAST('12345678901234567.89' AS DECIMAL(20,2)) AS d", "json")
		if err != nil {
			t.Fatalf("QueryPrint: %v", err)
		}
	})
	if !strings.Contains(out, "12345678901234567.89") {
		t.Errorf("decimal precision lost in JSON output:\n%s", out)
	}
}

func TestQueryPrint_HugeIntExactPrecision(t *testing.T) {
	s, err := NewSession(":memory:?threads=1&memory_limit=256MB")
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	defer s.Close()
	out := captureStdout(t, func() {
		// 21-digit number doesn't fit in int64; HUGEINT can hold it.
		err := s.QueryPrint("SELECT CAST('123456789012345678901' AS HUGEINT) AS h", "json")
		if err != nil {
			t.Fatalf("QueryPrint: %v", err)
		}
	})
	if !strings.Contains(out, "123456789012345678901") {
		t.Errorf("HUGEINT precision lost in JSON output:\n%s", out)
	}
}

func TestEnsureReadOnly_RejectsMultiStatement(t *testing.T) {
	// Multi-statement queries used to slip through PrepareContext silently;
	// the fix uses Prepare() which auto-rejects.
	s, err := NewSession(":memory:?threads=1&memory_limit=256MB")
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	defer s.Close()
	err = s.EnsureReadOnly("SELECT 1; SELECT 2")
	if err == nil {
		t.Fatal("multi-statement query should be rejected")
	}
	if !strings.Contains(err.Error(), "one statement at a time") {
		t.Errorf("expected error to mention multi-statement guidance, got %q", err.Error())
	}
}
