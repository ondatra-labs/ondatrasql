// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

//go:build integration

package duckdb_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ondatra-labs/ondatrasql/internal/duckdb"
	"github.com/ondatra-labs/ondatrasql/internal/state"
	"github.com/ondatra-labs/ondatrasql/internal/testutil"
)

// The shape that overflows in DuckDB 2.0.0 (internal/duckdb/latemat.go): a
// native table with deleted rows and a small ORDER BY ... LIMIT. With
// late_materialization on, the plan is a SEMI join on rowid = rowid; off, a
// plain Top N.
const (
	lateMatSetup = "CREATE OR REPLACE TABLE memory.main.latemat AS SELECT range AS a, repeat('x', 100) AS c FROM range(100000); " +
		"DELETE FROM memory.main.latemat WHERE a % 3 = 0"
	lateMatQuery = "EXPLAIN SELECT * FROM memory.main.latemat ORDER BY a LIMIT 5"
)

func assertNoLateMat(t *testing.T, what string, disabled string, plan string) {
	t.Helper()
	if !strings.Contains(disabled, "late_materialization") {
		t.Errorf("%s: disabled_optimizers = %q, want it to include late_materialization", what, disabled)
	}
	if strings.Contains(plan, "rowid = rowid") {
		t.Errorf("%s: the plan still uses late materialization:\n%s", what, plan)
	}
}

func sessionCheck(t *testing.T, what string, sess *duckdb.Session) {
	t.Helper()
	disabled, err := sess.QueryValue("SELECT current_setting('disabled_optimizers')")
	if err != nil {
		t.Fatal(err)
	}
	if err := sess.Exec(lateMatSetup); err != nil {
		t.Fatal(err)
	}
	rows, err := sess.QueryRowsMap(lateMatQuery)
	if err != nil {
		t.Fatal(err)
	}
	var plan strings.Builder
	for _, r := range rows {
		plan.WriteString(r["explain_value"])
	}
	assertNoLateMat(t, what, disabled, plan.String())
}

func TestLateMaterialization_DisabledInEverySession(t *testing.T) {
	sess := testutil.NewSession(t)
	sessionCheck(t, "new session", sess)

	// A user SET replaces the list; the session adds late_materialization back
	// and keeps the user's entry.
	if err := sess.Exec("SET disabled_optimizers = 'join_order'"); err != nil {
		t.Fatal(err)
	}
	sessionCheck(t, "after SET disabled_optimizers", sess)
	if got, _ := sess.QueryValue("SELECT current_setting('disabled_optimizers')"); !strings.Contains(got, "join_order") {
		t.Errorf("the user's join_order was dropped: %q", got)
	}
	if err := sess.Exec("RESET disabled_optimizers"); err != nil {
		t.Fatal(err)
	}
	sessionCheck(t, "after RESET", sess)
}

// Every way SQL reaches the session re-applies it: the query paths that
// ondatrasql sql and query use, RawConn, and an Exec script that resets the
// setting and then fails on a later statement.
func TestLateMaterialization_EverySessionPath(t *testing.T) {
	sess := testutil.NewSession(t)
	for name, run := range map[string]func() error{
		"Query":        func() error { _, err := sess.Query("SET disabled_optimizers = 'join_order'"); return err },
		"QueryValue":   func() error { _, err := sess.QueryValue("RESET disabled_optimizers"); return err },
		"QueryRows":    func() error { _, err := sess.QueryRows("SET disabled_optimizers = ''"); return err },
		"QueryRowsMap": func() error { _, err := sess.QueryRowsMap("RESET ALL"); return err },
		"QueryRowsAny": func() error { _, err := sess.QueryRowsAny("SET disabled_optimizers = 'join_order'"); return err },
		"QueryPrint":   func() error { return sess.QueryPrint("SET disabled_optimizers = 'join_order'", "csv") },
		"RawConn": func() error {
			return sess.RawConn(func(raw any) error {
				_, err := raw.(driver.ExecerContext).ExecContext(context.Background(), "SET disabled_optimizers = ''", nil)
				return err
			})
		},
		"failing Exec script": func() error {
			if err := sess.Exec("RESET disabled_optimizers; SELECT * FROM no_such_table"); err == nil {
				t.Errorf("the script should fail on its second statement")
			}
			return nil
		},
	} {
		_ = run() // a statement that returns no rows may report an error; the setting is what counts
		sessionCheck(t, "after "+name, sess)
	}
}

// DuckDB's error for an optimizer name it does not know is what
// unknownOptimizer tolerates, so a rename cannot stop sessions opening.
func TestLateMaterialization_UnknownOptimizerError(t *testing.T) {
	sess := testutil.NewSession(t)
	err := sess.Exec("SET disabled_optimizers = 'no_such_optimizer'")
	if err == nil || !strings.Contains(err.Error(), "not recognized") {
		t.Fatalf("err = %v, want DuckDB's \"not recognized\" error", err)
	}
}

// settings.sql runs before anything else and may set disabled_optimizers.
func TestLateMaterialization_ProjectAndSandboxSessions(t *testing.T) {
	prod := testutil.NewProject(t)
	testutil.WriteFile(t, prod.Dir, "config/settings.sql", "SET disabled_optimizers = 'join_order';\n")
	sess, err := duckdb.NewSession(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer sess.Close()
	if err := sess.InitWithCatalog(filepath.Join(prod.Dir, "config")); err != nil {
		t.Fatal(err)
	}
	sessionCheck(t, "project session with settings.sql", sess)
	if got, _ := sess.QueryValue("SELECT current_setting('disabled_optimizers')"); !strings.Contains(got, "join_order") {
		t.Errorf("settings.sql's join_order was dropped: %q", got)
	}
	sess.Close()

	sandbox := testutil.NewSandboxProject(t, prod)
	sessionCheck(t, "sandbox session", sandbox.Sess)
}

// The state store runs in its own DuckDB instance on native tables with
// DELETEs, through a connector init hook that also runs on every connection
// the pool re-creates.
func TestLateMaterialization_StateStore(t *testing.T) {
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "state.sql",
		"ATTACH '"+filepath.Join(dir, "state.duckdb")+"' AS state;\nSET disabled_optimizers = 'join_order';\n")
	st, err := state.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	check := func(what string, db *sql.DB) {
		t.Helper()
		var disabled string
		if err := db.QueryRow("SELECT current_setting('disabled_optimizers')").Scan(&disabled); err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(disabled, "join_order") {
			t.Errorf("%s: state.sql's join_order was dropped: %q", what, disabled)
		}
		if _, err := db.Exec(strings.ReplaceAll(lateMatSetup, "memory.main.", "state.")); err != nil {
			t.Fatal(err)
		}
		var plan strings.Builder
		rows, err := db.Query(strings.ReplaceAll(lateMatQuery, "memory.main.", "state."))
		if err != nil {
			t.Fatal(err)
		}
		for rows.Next() {
			var k, v string
			if err := rows.Scan(&k, &v); err != nil {
				t.Fatal(err)
			}
			plan.WriteString(v)
		}
		_ = rows.Close()
		assertNoLateMat(t, what, disabled, plan.String())
	}
	check("state store", st.DB())

	// Make the pool drop its connection and open a new one.
	conn, err := st.DB().Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	_ = conn.Raw(func(any) error { return driver.ErrBadConn })
	_ = conn.Close()
	check("state store after reconnect", st.DB())
}
