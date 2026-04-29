// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package execute

import "testing"

// Pin the transition matrix for runSQLState. Every method has a row;
// every observable has a column. If a future edit changes which form is
// returned by Current() / Rewritten() after a transition, this test
// will catch it before the bug reaches the runner.

func TestRunSQLState_Initial(t *testing.T) {
	t.Parallel()
	s := newRunSQLState("SELECT 1")
	if s.Current() != "SELECT 1" {
		t.Errorf("Current = %q, want %q", s.Current(), "SELECT 1")
	}
	if s.Rewritten() != "SELECT 1" {
		t.Errorf("Rewritten = %q, want %q", s.Rewritten(), "SELECT 1")
	}
	if !s.CurrentMatchesRewritten() {
		t.Error("CurrentMatchesRewritten should be true on init")
	}
	if s.TmpTable() != "" {
		t.Errorf("TmpTable = %q, want empty before SetTmpTable", s.TmpTable())
	}
}

func TestRunSQLState_Promote(t *testing.T) {
	t.Parallel()
	s := newRunSQLState("RAW")
	s.Promote("REWRITTEN")
	if s.Current() != "REWRITTEN" {
		t.Errorf("Current after Promote = %q, want REWRITTEN", s.Current())
	}
	if s.Rewritten() != "REWRITTEN" {
		t.Errorf("Rewritten after Promote = %q, want REWRITTEN", s.Rewritten())
	}
	if !s.CurrentMatchesRewritten() {
		t.Error("Promote must keep Current and Rewritten in lock-step")
	}
}

func TestRunSQLState_SetCurrent(t *testing.T) {
	t.Parallel()
	s := newRunSQLState("RAW")
	s.Promote("LIB_REWRITE")
	s.SetCurrent("CDC_FILTERED")

	if s.Current() != "CDC_FILTERED" {
		t.Errorf("Current after SetCurrent = %q, want CDC_FILTERED", s.Current())
	}
	// Critical: Rewritten stays as the pre-CDC retry target
	if s.Rewritten() != "LIB_REWRITE" {
		t.Errorf("Rewritten after SetCurrent = %q, want LIB_REWRITE (retry target lost)", s.Rewritten())
	}
	if s.CurrentMatchesRewritten() {
		t.Error("CurrentMatchesRewritten should be false after CDC filter applied")
	}
}

func TestRunSQLState_RevertToRewritten(t *testing.T) {
	t.Parallel()
	s := newRunSQLState("RAW")
	s.Promote("LIB_REWRITE")
	s.SetCurrent("CDC_FILTERED")
	s.RevertToRewritten()

	if s.Current() != "LIB_REWRITE" {
		t.Errorf("Current after revert = %q, want LIB_REWRITE", s.Current())
	}
	if s.Rewritten() != "LIB_REWRITE" {
		t.Errorf("Rewritten after revert = %q, want LIB_REWRITE (must remain stable)", s.Rewritten())
	}
	if !s.CurrentMatchesRewritten() {
		t.Error("CurrentMatchesRewritten must be true after revert")
	}
}

func TestRunSQLState_SetCurrentTwice(t *testing.T) {
	t.Parallel()
	// CDC filter then masking — both go through SetCurrent; Rewritten preserved.
	s := newRunSQLState("RAW")
	s.Promote("LIB_REWRITE")
	s.SetCurrent("CDC_FILTERED")
	s.SetCurrent("MASKED")

	if s.Current() != "MASKED" {
		t.Errorf("Current = %q, want MASKED", s.Current())
	}
	if s.Rewritten() != "LIB_REWRITE" {
		t.Errorf("Rewritten = %q, want LIB_REWRITE — multiple SetCurrent must not advance the retry target", s.Rewritten())
	}
}

func TestRunSQLState_TmpTable(t *testing.T) {
	t.Parallel()
	s := newRunSQLState("RAW")
	s.Promote("LIB_REWRITE")
	s.SetCurrent("FILTERED")
	s.SetTmpTable("tmp_mart_orders")

	if s.TmpTable() != "tmp_mart_orders" {
		t.Errorf("TmpTable = %q, want tmp_mart_orders", s.TmpTable())
	}
	// SQL forms must be unaffected by SetTmpTable
	if s.Current() != "FILTERED" {
		t.Errorf("Current changed after SetTmpTable = %q, want FILTERED", s.Current())
	}
	if s.Rewritten() != "LIB_REWRITE" {
		t.Errorf("Rewritten changed after SetTmpTable = %q, want LIB_REWRITE", s.Rewritten())
	}
}

func TestRunSQLState_NoLibRewriteFlow(t *testing.T) {
	t.Parallel()
	// When no libs are detected, Promote is never called — Rewritten stays
	// at the initial (raw) form. CDC + masking apply on top of raw.
	s := newRunSQLState("SELECT * FROM raw.foo")
	s.SetCurrent("SELECT * FROM raw.foo WHERE id IN (table_changes(...))")

	if s.Rewritten() != "SELECT * FROM raw.foo" {
		t.Errorf("Rewritten = %q, want raw form when no Promote was called", s.Rewritten())
	}
	// On CDC revert in this flow, we go back to the raw form
	s.RevertToRewritten()
	if s.Current() != "SELECT * FROM raw.foo" {
		t.Errorf("Current after revert = %q, want raw form", s.Current())
	}
}
