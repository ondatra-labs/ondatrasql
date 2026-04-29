// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package execute

// runSQLState tracks the SQL transformation pipeline for a single Run().
// Each runner invocation moves through phases that progressively rewrite
// the model's SQL:
//
//	model.SQL  →  (lib-rewrite)  →  Rewritten  →  (CDC filter)  →  Current
//	                                Rewritten  →  (revert)      →  Current
//	                                Current    →  (masking)     →  Current
//
// The Rewritten form is preserved as a fallback when CDC must be reverted —
// e.g. if table_changes() returns no rows but the model needs full data, we
// reset Current to Rewritten and re-run without the CDC predicate.
//
// model.SQL itself is treated as immutable input owned by the parser; this
// struct only owns the mutable working copies. Direct field access is not
// exposed — call sites must go through the typed methods, which centralises
// the "which SQL for which use-case" policy and prevents the variable
// confusion that has historically been a Cat 9 bug class.
type runSQLState struct {
	rewritten string
	current   string
	tmpTable  string
}

// newRunSQLState seeds Rewritten and Current to the given SQL (typically
// model.SQL). The temp table name is set later via SetTmpTable, since the
// runner derives it after schema-evolution decisions.
func newRunSQLState(initialSQL string) *runSQLState {
	return &runSQLState{
		rewritten: initialSQL,
		current:   initialSQL,
	}
}

// Promote saves the supplied SQL as the new Rewritten baseline AND updates
// Current. Use after lib-rewrite: both forms move forward together because
// downstream phases (CDC, masking) work against the lib-rewritten SQL, not
// the raw model.SQL.
func (s *runSQLState) Promote(newSQL string) {
	s.rewritten = newSQL
	s.current = newSQL
}

// SetCurrent updates only Current, leaving Rewritten unchanged. Use for
// transformations that should not survive a CDC revert — CDC filter
// application, column masking, and similar.
func (s *runSQLState) SetCurrent(newSQL string) {
	s.current = newSQL
}

// RevertToRewritten resets Current back to the Rewritten baseline. Used
// when a CDC pass produced an unusable query (e.g. table_changes() has
// nothing to return after CDC filter) and we need to fall back to the
// pre-CDC form.
func (s *runSQLState) RevertToRewritten() {
	s.current = s.rewritten
}

// SetTmpTable records the temp table name that will hold Current's output.
// Independent of SQL transforms — kept here so the materialise call site
// has a single state object to thread through.
func (s *runSQLState) SetTmpTable(name string) {
	s.tmpTable = name
}

// Current returns the SQL to execute right now (lib-rewrite + CDC + masking
// already applied). This is what feeds CREATE TEMP TABLE AS, runtime
// checks, and other end-of-pipeline operations.
func (s *runSQLState) Current() string { return s.current }

// Rewritten returns the post-lib-rewrite SQL preserved as a retry target.
// Used by the CDC retry path and by sink-delta logic that needs the
// pre-filter form.
func (s *runSQLState) Rewritten() string { return s.rewritten }

// TmpTable returns the temp table name set via SetTmpTable.
func (s *runSQLState) TmpTable() string { return s.tmpTable }

// CurrentMatchesRewritten reports whether Current and Rewritten are the
// same string. Useful for branches that behave differently when no CDC
// filter has been applied (or when CDC was reverted).
func (s *runSQLState) CurrentMatchesRewritten() bool {
	return s.current == s.rewritten
}
