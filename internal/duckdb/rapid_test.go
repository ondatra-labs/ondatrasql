// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package duckdb

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	duckdbdriver "github.com/duckdb/duckdb-go/v2"
	"pgregory.net/rapid"
)

// --- Session State Machine ---

// Property: Session operations never panic, regardless of call order.
// After Close, all operations must error (or return "unknown" for GetVersion).
func TestRapid_Session_StateMachine(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		s, err := NewSession(":memory:")
		if err != nil {
			t.Fatalf("NewSession: %v", err)
		}
		defer s.Close()

		closed := false
		versionQueried := false

		t.Repeat(map[string]func(*rapid.T){
			"Exec": func(t *rapid.T) {
				err := s.Exec("SELECT 1")
				if closed && err == nil {
					t.Fatal("Exec should error after Close")
				}
			},
			"Query": func(t *rapid.T) {
				_, err := s.Query("SELECT 1 AS a")
				if closed && err == nil {
					t.Fatal("Query should error after Close")
				}
			},
			"QueryValue": func(t *rapid.T) {
				val, err := s.QueryValue("SELECT 42")
				if closed && err == nil {
					t.Fatal("QueryValue should error after Close")
				}
				if !closed && err == nil && val != "42" {
					t.Fatalf("QueryValue = %q, want 42", val)
				}
			},
			"QueryRows": func(t *rapid.T) {
				_, err := s.QueryRows("SELECT 1 AS a UNION ALL SELECT 2")
				if closed && err == nil {
					t.Fatal("QueryRows should error after Close")
				}
			},
			"QueryRowsMap": func(t *rapid.T) {
				_, err := s.QueryRowsMap("SELECT 1 AS a, 2 AS b")
				if closed && err == nil {
					t.Fatal("QueryRowsMap should error after Close")
				}
			},
			"QueryPrint": func(t *rapid.T) {
				format := rapid.SampledFrom([]string{"markdown", "csv", "json"}).Draw(t, "format")
				err := s.QueryPrint("SELECT 1 AS a, 'hello' AS b", format)
				if closed && err == nil {
					t.Fatal("QueryPrint should error after Close")
				}
			},
			"HasCDCChanges": func(t *rapid.T) {
				_, err := s.HasCDCChanges()
				if closed && err == nil {
					t.Fatal("HasCDCChanges should error after Close")
				}
			},
			"GetDagStartSnapshot": func(t *rapid.T) {
				_, err := s.GetDagStartSnapshot()
				if closed && err == nil {
					t.Fatal("GetDagStartSnapshot should error after Close")
				}
			},
			"GetVersion": func(t *rapid.T) {
				v := s.GetVersion()
				if !closed {
					// Before close, version should always succeed
					if v == "" || v == "unknown" {
						t.Fatalf("GetVersion before Close = %q", v)
					}
					versionQueried = true
				} else if !versionQueried {
					// After close without prior query, version query fails → "unknown"
					if v != "unknown" {
						t.Fatalf("GetVersion after Close (never queried) = %q, want unknown", v)
					}
				}
				// After close with prior query: cached value is returned (valid behavior)
			},
			"Close": func(t *rapid.T) {
				_ = s.Close()
				closed = true
			},
		})
	})
}

// --- InitWithCatalog State Machine ---

// Pre-catalog config files are loaded before ATTACH — invalid SQL here must fail init.
var preCatalogFiles = []string{"settings.sql", "secrets.sql", "extensions.sql"}

// Post-catalog config files are loaded after ATTACH — invalid SQL here must also fail.
var postCatalogFiles = []string{"schemas.sql", "sources.sql"}

// Property: InitWithCatalog never panics regardless of config file state.
// Invariants:
//   - Missing catalog.sql → error (contains "catalog.sql required")
//   - Invalid SQL in any config file → *duckdb.Error with ErrorTypeParser
//   - Valid config → success, session is usable
func TestRapid_InitWithCatalog_StateMachine(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		dir := t.TempDir()
		configDir := filepath.Join(dir, "config")
		os.MkdirAll(configDir, 0o755)

		hasCatalog := rapid.Bool().Draw(rt, "hasCatalog")
		catalogPath := filepath.Join(dir, "ducklake.sqlite")
		dataPath := filepath.Join(dir, "data")

		if hasCatalog {
			catalogSQL := "ATTACH 'ducklake:sqlite:" + catalogPath + "' AS lake (DATA_PATH '" + dataPath + "');\n"
			os.WriteFile(filepath.Join(configDir, "catalog.sql"), []byte(catalogSQL), 0o644)
		}

		// Track which files have invalid SQL and their order relative to catalog
		hasInvalidPreCatalog := false
		hasInvalidPostCatalog := false

		writeConfigFile := func(name string, isPre bool) {
			choice := rapid.IntRange(0, 3).Draw(rt, "choice_"+name)
			switch choice {
			case 0: // missing — skip
			case 1: // empty
				os.WriteFile(filepath.Join(configDir, name), []byte(""), 0o644)
			case 2: // valid SQL
				os.WriteFile(filepath.Join(configDir, name), []byte("SELECT 1;\n"), 0o644)
			case 3: // invalid SQL
				os.WriteFile(filepath.Join(configDir, name), []byte("THIS IS NOT VALID SQL;\n"), 0o644)
				if isPre {
					hasInvalidPreCatalog = true
				} else {
					hasInvalidPostCatalog = true
				}
			}
		}

		for _, name := range preCatalogFiles {
			writeConfigFile(name, true)
		}
		for _, name := range postCatalogFiles {
			writeConfigFile(name, false)
		}

		s, err := NewSession(":memory:")
		if err != nil {
			rt.Fatalf("NewSession: %v", err)
		}
		defer s.Close()

		err = s.InitWithCatalog(configDir)

		switch {
		case !hasCatalog && !hasInvalidPreCatalog:
			// Missing catalog.sql → must error
			if err == nil {
				rt.Fatal("expected error without catalog.sql")
			}
			if !strings.Contains(err.Error(), "catalog.sql") {
				rt.Fatalf("expected catalog.sql error, got: %v", err)
			}

		case hasInvalidPreCatalog:
			// Invalid SQL before catalog → must error with parser error
			if err == nil {
				rt.Fatal("expected error with invalid pre-catalog SQL")
			}
			var de *duckdbdriver.Error
			if !errors.As(err, &de) {
				rt.Fatalf("expected *duckdb.Error, got %T: %v", err, err)
			}
			if de.Type != duckdbdriver.ErrorTypeParser {
				rt.Fatalf("expected ErrorTypeParser, got %v: %v", de.Type, err)
			}

		case hasCatalog && hasInvalidPostCatalog:
			// Invalid SQL after catalog → must error with parser error
			if err == nil {
				rt.Fatal("expected error with invalid post-catalog SQL")
			}
			var de *duckdbdriver.Error
			if !errors.As(err, &de) {
				rt.Fatalf("expected *duckdb.Error, got %T: %v", err, err)
			}
			if de.Type != duckdbdriver.ErrorTypeParser {
				rt.Fatalf("expected ErrorTypeParser, got %v: %v", de.Type, err)
			}

		default:
			// Valid catalog + all valid/missing/empty files → must succeed
			if err != nil {
				rt.Fatalf("InitWithCatalog with valid config: %v", err)
			}
			// Session should be usable after successful init
			val, qErr := s.QueryValue("SELECT 42")
			if qErr != nil || val != "42" {
				rt.Fatalf("session not usable after init: val=%q err=%v", val, qErr)
			}
		}
	})
}

// Property: InitWithCatalog on a closed session fails at each phase.
// This covers error paths in PHASE 2-5 that are unreachable with a healthy DuckDB.
func TestRapid_InitWithCatalog_ClosedSession(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		dir := t.TempDir()
		configDir := filepath.Join(dir, "config")
		os.MkdirAll(configDir, 0o755)

		catalogPath := filepath.Join(dir, "ducklake.sqlite")
		dataPath := filepath.Join(dir, "data")
		catalogSQL := "ATTACH 'ducklake:sqlite:" + catalogPath + "' AS lake (DATA_PATH '" + dataPath + "');\n"
		os.WriteFile(filepath.Join(configDir, "catalog.sql"), []byte(catalogSQL), 0o644)

		// Randomly include valid optional files so different code paths are reached
		// before the closed-session error triggers
		for _, name := range append(preCatalogFiles, postCatalogFiles...) {
			if rapid.Bool().Draw(rt, "include_"+name) {
				os.WriteFile(filepath.Join(configDir, name), []byte("SELECT 1;\n"), 0o644)
			}
		}

		s, err := NewSession(":memory:")
		if err != nil {
			rt.Fatalf("NewSession: %v", err)
		}

		// Close session before init — forces errors in whichever phase first does Exec
		s.Close()

		err = s.InitWithCatalog(configDir)
		if err == nil {
			rt.Fatal("expected error on closed session")
		}
	})
}

// Property: InitWithCatalog with invalid catalog.sql returns proper error type.
func TestRapid_InitWithCatalog_InvalidCatalog(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		dir := t.TempDir()
		configDir := filepath.Join(dir, "config")
		os.MkdirAll(configDir, 0o755)

		// Write invalid catalog.sql
		invalidSQL := rapid.SampledFrom([]string{
			"NOT VALID SQL;",
			"ATTACH 'nonexistent_driver:foo' AS lake;",
			"CREATE TABLE oops;",
		}).Draw(rt, "invalidCatalog")
		os.WriteFile(filepath.Join(configDir, "catalog.sql"), []byte(invalidSQL), 0o644)

		s, err := NewSession(":memory:")
		if err != nil {
			rt.Fatalf("NewSession: %v", err)
		}
		defer s.Close()

		err = s.InitWithCatalog(configDir)
		if err == nil {
			rt.Fatal("expected error with invalid catalog.sql")
		}
		// Error should mention catalog.sql or be a DuckDB error
		var de *duckdbdriver.Error
		isCatalogFileErr := strings.Contains(err.Error(), "catalog.sql")
		isDuckDBErr := errors.As(err, &de)
		if !isCatalogFileErr && !isDuckDBErr {
			rt.Fatalf("unexpected error type %T: %v", err, err)
		}
	})
}

// Property: InitWithCatalog with invalid macros.sql returns error.
func TestRapid_InitWithCatalog_InvalidMacros(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	t.Parallel()
	rapid.Check(t, func(rt *rapid.T) {
		dir := t.TempDir()
		configDir := filepath.Join(dir, "config")
		os.MkdirAll(configDir, 0o755)

		// Valid catalog
		catalogPath := filepath.Join(dir, "ducklake.sqlite")
		dataPath := filepath.Join(dir, "data")
		os.WriteFile(filepath.Join(configDir, "catalog.sql"),
			[]byte("ATTACH 'ducklake:sqlite:"+catalogPath+"' AS lake (DATA_PATH '"+dataPath+"');\n"), 0o644)

		// Invalid macro in config/macros/ directory
		macroDir := filepath.Join(configDir, "macros")
		os.MkdirAll(macroDir, 0o755)
		os.WriteFile(filepath.Join(macroDir, "bad.sql"),
			[]byte("CREATE MACRO bad_macro AS (INVALID SYNTAX HERE);\n"), 0o644)

		s, err := NewSession(":memory:")
		if err != nil {
			rt.Fatalf("NewSession: %v", err)
		}
		defer s.Close()

		err = s.InitWithCatalog(configDir)
		if err == nil {
			rt.Fatal("expected error with invalid macros.sql")
		}
	})
}

// --- QuoteIdentifier Properties ---

// Property: QuoteIdentifier output is always wrapped in double quotes.
func TestRapid_QuoteIdentifier_Wrapped(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		s := rapid.String().Draw(t, "s")
		result := QuoteIdentifier(s)
		if len(result) < 2 || result[0] != '"' || result[len(result)-1] != '"' {
			t.Fatalf("QuoteIdentifier(%q) = %q, not properly quoted", s, result)
		}
	})
}

// Property: QuoteIdentifier is recoverable — unquoting recovers the original.
func TestRapid_QuoteIdentifier_Recoverable(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		s := rapid.String().Draw(t, "s")
		result := QuoteIdentifier(s)
		inner := result[1 : len(result)-1]
		recovered := strings.ReplaceAll(inner, `""`, `"`)
		if recovered != s {
			t.Fatalf("not recoverable: input %q, quoted %q, recovered %q", s, result, recovered)
		}
	})
}

// Property: QuoteIdentifier interior has no unescaped double quotes.
func TestRapid_QuoteIdentifier_NoUnescapedQuotes(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		s := rapid.String().Draw(t, "s")
		result := QuoteIdentifier(s)
		inner := result[1 : len(result)-1]
		noEscaped := strings.ReplaceAll(inner, `""`, ``)
		if strings.Contains(noEscaped, `"`) {
			t.Fatalf("unescaped quote in QuoteIdentifier(%q): %q", s, result)
		}
	})
}

// Property: double-quoting is recoverable — unquoting both layers recovers the original.
func TestRapid_QuoteIdentifier_DoubleQuoteRecoverable(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		s := rapid.String().Draw(t, "s")
		q1 := QuoteIdentifier(s)
		q2 := QuoteIdentifier(q1)

		// Unquote outer layer
		inner2 := q2[1 : len(q2)-1]
		recovered1 := strings.ReplaceAll(inner2, `""`, `"`)

		// Unquote inner layer
		inner1 := recovered1[1 : len(recovered1)-1]
		recovered0 := strings.ReplaceAll(inner1, `""`, `"`)

		if recovered0 != s {
			t.Fatalf("double-quote not recoverable:\n  input: %q\n  q1: %q\n  q2: %q\n  recovered: %q", s, q1, q2, recovered0)
		}
	})
}
