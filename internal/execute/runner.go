// OndatraSQL - You don't need a data stack anymore
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

// Package execute runs SQL models through the DuckDB session.
package execute

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	duckdbdriver "github.com/duckdb/duckdb-go/v2"
	"github.com/ondatra-labs/ondatrasql/internal/backfill"
	"github.com/ondatra-labs/ondatrasql/internal/duckdb"
	"github.com/ondatra-labs/ondatrasql/internal/lineage"
	"github.com/ondatra-labs/ondatrasql/internal/parser"
	"github.com/ondatra-labs/ondatrasql/internal/validation"
)

// Mode represents the execution mode.
type Mode int

const (
	// ModeRun executes the model and writes to DuckLake.
	ModeRun Mode = iota
)

// Result contains the outcome of running a model.
type Result struct {
	Target        string
	Kind          string
	RunType       string // "incremental", "backfill", "full", or "skip"
	RunReason     string // Human-readable reason for the run_type decision
	RowsAffected  int64
	Duration      time.Duration
	Errors        []string
	Warnings      []string
	Trace         []TraceStep `json:"trace,omitempty"`
	lastTraceEnd  time.Time   // internal: when last trace was recorded
}

// Runner executes SQL models.
type Runner struct {
	sess             *duckdb.Session
	mode             Mode
	dagRunID         string
	projectDir       string               // Project root directory (for Starlark load())
	gitInfo          gitInfo              // Cached Git metadata
	runTypeDecisions RunTypeDecisions     // Pre-computed run_type decisions (batch optimization)
	astCache         map[string]string    // Cached AST JSON by SQL hash (reduces duplicate lineage queries)
	adminPort        string               // Admin port for event daemon (flush operations)
	claimLimit       int                  // Max events per claim batch (0 = default 1000)
}

// gitInfo holds Git repository metadata for the current run.
type gitInfo struct {
	Commit  string
	Branch  string
	RepoURL string
}

// NewRunner creates a new model runner.
func NewRunner(sess *duckdb.Session, mode Mode, dagRunID string) *Runner {
	return &Runner{
		sess:     sess,
		mode:     mode,
		dagRunID: dagRunID,
		astCache: make(map[string]string),
	}
}

// SetGitInfo sets the Git metadata for this run.
func (r *Runner) SetGitInfo(commit, branch, repoURL string) {
	r.gitInfo = gitInfo{Commit: commit, Branch: branch, RepoURL: repoURL}
}

// SetRunTypeDecisions sets pre-computed run_type decisions for batch optimization.
// When set, the runner skips individual backfill detection queries.
func (r *Runner) SetRunTypeDecisions(decisions RunTypeDecisions) {
	r.runTypeDecisions = decisions
}

// SetAdminPort sets the event daemon admin port for flush operations.
func (r *Runner) SetAdminPort(port string) {
	r.adminPort = port
}

// SetClaimLimit sets the max events per claim batch for event flush.
// Default is 1000 if not set.
func (r *Runner) SetClaimLimit(limit int) {
	r.claimLimit = limit
}

// SetProjectDir sets the project root directory for Starlark load() support.
func (r *Runner) SetProjectDir(dir string) {
	r.projectDir = dir
}

// getAST returns the AST JSON for a SQL query, using cache if available.
// This reduces duplicate lineage queries when the same SQL appears multiple times.
func (r *Runner) getAST(sql string) (string, error) {
	// Use SQL hash as cache key
	sqlHash := backfill.Hash(sql)
	if cached, ok := r.astCache[sqlHash]; ok {
		return cached, nil
	}

	// Fetch from database and cache
	astJSON, err := lineage.GetAST(r.sess, sql)
	if err != nil {
		return "", err
	}

	r.astCache[sqlHash] = astJSON
	return astJSON, nil
}

// extractLineage extracts both column lineage and table dependencies using cached AST.
func (r *Runner) extractLineage(sql string) ([]lineage.ColumnLineage, []string, error) {
	astJSON, err := r.getAST(sql)
	if err != nil {
		return nil, nil, err
	}

	colLineage, err := lineage.ExtractFromAST(astJSON)
	if err != nil {
		return nil, nil, err
	}

	tableDeps, err := lineage.GetAllTablesFromAST(astJSON)
	if err != nil {
		return colLineage, nil, err
	}
	if tableDeps == nil {
		tableDeps = []string{}
	}

	return colLineage, tableDeps, nil
}

// Run executes a parsed model with tracing context.
func (r *Runner) Run(ctx context.Context, model *parser.Model) (*Result, error) {
	start := time.Now()
	result := &Result{
		Target:       model.Target,
		Kind:         model.Kind,
		lastTraceEnd: start, // Initialize for gap tracking
	}

	// Load required DuckDB extensions
	if len(model.Extensions) > 0 {
		stepStart := time.Now()
		for _, ext := range model.Extensions {
			if err := r.loadExtension(ext); err != nil {
				r.trace(result, "load_extensions", stepStart, "error")
				return nil, fmt.Errorf("load extension %s: %w", ext, err)
			}
		}
		r.trace(result, "load_extensions", stepStart, "ok")
	}

	// Script models (Starlark) are executed differently
	if model.IsScript {
		return r.runScript(ctx, model)
	}

	// View models have a separate code path — no materialization
	if model.Kind == "view" {
		return r.runView(model, result, start)
	}

	// Events models: flush from daemon's Badger store into DuckLake.
	// Early dispatch — bypasses batch run_type decisions entirely.
	if model.Kind == "events" {
		return r.runEvents(ctx, model, result, start)
	}

	// Trace: Calculate SQL hash (in-memory, should be fast)
	var stepStart time.Time
	stepStart = time.Now()
	sqlHash := backfill.ModelHash(model.SQL, backfill.ModelDirectives{
		Kind:               model.Kind,
		UniqueKey:          model.UniqueKey,
		PartitionedBy:      model.PartitionedBy,
		Incremental:        model.Incremental,
		IncrementalInitial: model.IncrementalInitial,
	})
	r.trace(result, "hash_sql", stepStart, "ok")

	// Determine run_type using SQL-based logic (same for batch and single)
	var decision *RunTypeDecision

	if d := r.runTypeDecisions.GetDecision(model.Target); d != nil {
		// BATCH: Use pre-computed decision from run_all
		stepStart = time.Now()
		decision = d
		r.trace(result, "run_type.batch_lookup", stepStart, "ok")
	} else {
		// SINGLE: Compute using same SQL logic as batch
		stepStart = time.Now()
		var err error
		decision, err = ComputeSingleRunType(r.sess, model)
		r.trace(result, "run_type.compute", stepStart, "ok")
		if err != nil {
			result.Errors = append(result.Errors, fmt.Sprintf("run_type check: %v", err))
			decision = &RunTypeDecision{RunType: "backfill"} // Safe fallback
		}
	}

	result.RunType = decision.RunType
	result.RunReason = decision.Reason

	// Skip: nothing changed, no work to do
	if result.RunType == "skip" {
		result.Duration = time.Since(start)
		return result, nil
	}

	needsBackfill := decision.RunType == "backfill"

	// Track if we need to apply schema evolution (additive changes)
	var schemaChange *backfill.SchemaChange

	// Set incremental variables if @incremental is specified (for SQL models)
	if model.Incremental != "" {
		stepStart = time.Now()
		incrState, _ := backfill.GetIncrementalState(
			r.sess, model.Target, model.Incremental, model.IncrementalInitial)
		if incrState != nil {
			// Force is_backfill when the runner decided on backfill (e.g. hash changed).
			// GetIncrementalState only checks table existence, but the runner may trigger
			// backfill for other reasons (directive change, schema change, etc.).
			if needsBackfill {
				incrState.IsBackfill = true
				incrState.LastValue = incrState.InitialValue
			}
			// Set DuckDB variables that SQL can reference via getvariable()
			// Escape values to prevent SQL injection from data-driven cursors
			r.sess.Exec(fmt.Sprintf("SET VARIABLE incr_last_value = '%s'", escapeSQL(incrState.LastValue)))
			r.sess.Exec(fmt.Sprintf("SET VARIABLE incr_last_run = '%s'", escapeSQL(incrState.LastRun)))
			r.sess.Exec(fmt.Sprintf("SET VARIABLE incr_is_backfill = %t", incrState.IsBackfill))
			r.sess.Exec(fmt.Sprintf("SET VARIABLE incr_cursor = '%s'", escapeSQL(incrState.Cursor)))
		}
		r.trace(result, "incremental.set_vars", stepStart, "ok")
	}

	// Get the SQL to execute - apply CDC transformation for incremental append/merge
	execSQL := model.SQL

	// Auto-detect tables and column lineage from SQL (single AST query)
	stepStart = time.Now()
	astJSON, _ := r.getAST(model.SQL)
	tables, _ := lineage.ExtractTablesFromAST(astJSON)
	colLineage, _ := lineage.ExtractFromAST(astJSON)

	// Determine which tables need CDC:
	// 1. Primary table (first FROM) always gets CDC
	// 2. JOIN tables with AGGREGATION get CDC (new rows affect aggregates)
	// 3. JOIN tables with only IDENTITY are dimension lookups (full scan)
	var cdcTables []string
	var allTableNames []string
	aggregationTables := lineage.GetCDCTables(colLineage)

	for _, t := range tables {
		allTableNames = append(allTableNames, t.Table)
		if t.IsFirstFrom {
			// Primary table always gets CDC
			cdcTables = append(cdcTables, t.Table)
		} else if aggregationTables[t.Table] {
			// JOIN table with aggregations gets CDC
			cdcTables = append(cdcTables, t.Table)
		}
		// JOIN tables without aggregations get full scan (dimension lookups)
	}
	tableExtractTime := time.Since(stepStart)

	// Smart CDC: auto-detect tables, apply CDC to fact tables and aggregated joins
	// Note: SCD2 is excluded because it needs full source data for proper change detection
	// tracked excluded: it does its own hash-based change detection and needs full source data
	isIncremental := model.Kind == "append" || model.Kind == "merge" || model.Kind == "partition"
	if !needsBackfill && isIncremental && len(cdcTables) > 0 {
		// NOTE: Smart rebuild detection (skipping when source hasn't changed) is disabled
		// because DuckLake's tables_inserted_into contains table IDs that can't be reliably
		// resolved to table names. CDC transformation still works correctly without it.

		// Log table extraction time
		if tableExtractTime > time.Millisecond {
			r.trace(result, "cdc.parse_tables", time.Now().Add(-tableExtractTime), "ok")
		}

		// Sub-step: Set high water mark for CDC (queries lake.snapshots)
		stepStart = time.Now()
		if err := r.sess.SetHighWaterMark(model.Target); err != nil {
			result.Warnings = append(result.Warnings, fmt.Sprintf("set high water mark warning: %v", err))
		}
		r.trace(result, "cdc.high_water_mark", stepStart, "ok")

		// Sub-step: Refresh snapshot (queries lake.current_snapshot)
		stepStart = time.Now()
		if err := r.sess.RefreshSnapshot(); err != nil {
			result.Warnings = append(result.Warnings, fmt.Sprintf("refresh snapshot warning: %v", err))
		}
		r.trace(result, "cdc.refresh_snapshot", stepStart, "ok")

		// Sub-step: Check for CDC changes
		stepStart = time.Now()
		hasChanges, err := r.sess.HasCDCChanges()
		if err != nil {
			result.Warnings = append(result.Warnings, fmt.Sprintf("check CDC changes warning: %v", err))
		}
		r.trace(result, "cdc.has_changes", stepStart, "ok")

		// Sandbox: CDC checks prod catalog snapshots, but source tables may
		// have been re-created in sandbox during this DAG run. If any source
		// table exists in sandbox, the upstream changed and we must backfill
		// (not CDC — the sandbox table has the full new data, not versions).
		// This check runs regardless of hasChanges because CDC may report
		// false positives (other models committed to prod) while the actual
		// source tables only changed in sandbox.
		sandboxUpstreamChanged := false
		if r.sess.ProdAlias() != "" {
			for _, t := range allTableNames {
				if r.tableExistsInCatalog(t, r.sess.CatalogAlias()) {
					sandboxUpstreamChanged = true
					break
				}
			}
		}

		if sandboxUpstreamChanged {
			// Source tables changed in sandbox — run full query (no CDC).
			// Table refs that are in sandbox will resolve correctly via
			// search_path; table refs in prod get qualified in the block below.
			needsBackfill = true
			execSQL = model.SQL
		} else if hasChanges {
			// Sub-step: Get the snapshot ID for time travel
			stepStart = time.Now()
			snapshotID, err := r.sess.GetDagStartSnapshot()
			if err != nil {
				result.Warnings = append(result.Warnings, fmt.Sprintf("get snapshot warning: %v", err))
			}
			r.trace(result, "cdc.get_snapshot", stepStart, "ok")

			// Check if upstream schema changed — CDC EXCEPT requires matching column counts.
			// Compare current vs snapshot schema for each CDC table. If any differ,
			// skip CDC and run full query (the downstream will detect schema evolution).
			stepStart = time.Now()
			schemaChanged := r.cdcSchemaChanged(cdcTables, snapshotID)
			r.trace(result, "cdc.schema_check", stepStart, "ok")

			if schemaChanged {
				result.Warnings = append(result.Warnings, "upstream schema changed, skipping CDC")
				execSQL = model.SQL
				needsBackfill = true
			} else {
				// Apply CDC to fact tables and aggregated joins via AST rewriting
				var cdcErr error
				execSQL, cdcErr = r.applySmartCDC(astJSON, model.Kind, cdcTables, snapshotID)
				if cdcErr != nil {
					result.Warnings = append(result.Warnings, fmt.Sprintf("AST CDC failed, using full query: %v", cdcErr))
					execSQL = model.SQL // safe fallback
				}
			}
		} else {
			// No upstream changes. In sandbox mode, if target doesn't exist in
			// sandbox, skip entirely — the prod table is visible via search_path.
			// Creating an empty table would shadow it and produce a false diff.
			if r.sess.ProdAlias() != "" && !r.tableExistsInCatalog(model.Target, r.sess.CatalogAlias()) {
				result.RunType = "skip"
				result.RunReason = "no upstream changes"
				result.Duration = time.Since(start)
				return result, nil
			}
			var emptyErr error
			execSQL, emptyErr = r.applyEmptySmartCDC(astJSON, cdcTables)
			if emptyErr != nil {
				result.Warnings = append(result.Warnings, fmt.Sprintf("AST empty CDC failed, using full query: %v", emptyErr))
				execSQL = model.SQL
			}
		}
	} else if tableExtractTime > time.Millisecond {
		// Log table extraction time if significant and no CDC
		r.trace(result, "parse_tables", time.Now().Add(-tableExtractTime), "ok")
	}

	// Sandbox: qualify table refs with prod catalog via AST node manipulation.
	// DuckDB resolves schema-qualified names in the current catalog only, ignoring
	// search_path. In sandbox (USE sandbox), raw.source → sandbox.raw.source which
	// doesn't exist. We set catalog_name on BASE_TABLE nodes so they resolve to
	// the prod catalog (e.g. lake.raw.source) unless the table already exists in
	// sandbox (DAG mode where upstream ran first).
	// CDC tables are skipped — CDC already qualifies them via cdc.go.
	if r.sess.ProdAlias() != "" && len(allTableNames) > 0 {
		cdcHandled := make(map[string]bool)
		if !needsBackfill && isIncremental && len(cdcTables) > 0 {
			for _, t := range cdcTables {
				cdcHandled[t] = true
			}
		}
		tablesToQualify := make(map[string]bool)
		for _, t := range allTableNames {
			if cdcHandled[t] {
				continue
			}
			// Unqualified names (no schema prefix) resolve via search_path — skip
			if !strings.Contains(t, ".") {
				continue
			}
			if !r.tableExistsInCatalog(t, r.sess.CatalogAlias()) {
				tablesToQualify[strings.ToLower(t)] = true
			}
		}
		if len(tablesToQualify) > 0 {
			// Re-parse the current execSQL AST for qualification
			qualified := false
			if execAST, qualErr := r.getAST(execSQL); qualErr == nil {
				if root, parseErr := parseASTJSON(execAST); parseErr == nil {
					qualifyTablesInAST(root, tablesToQualify, r.sess.ProdAlias())
					if modified, marshalErr := json.Marshal(root); marshalErr == nil {
						if deserialized, deserErr := r.deserializeAST(string(modified)); deserErr == nil {
							execSQL = deserialized
							qualified = true
						}
					}
				}
			}
			if !qualified {
				return nil, fmt.Errorf("sandbox qualification failed for %s: table refs may resolve to wrong catalog", model.Target)
			}
		}
	}

	// Apply column masking if @column tags reference masking macros
	execSQL = applyColumnMasking(execSQL, model)

	// Create temp model
	tmpTable := "tmp_" + sanitizeTableName(model.Target)
	createSQL := fmt.Sprintf("CREATE TEMP TABLE %s AS %s", tmpTable, execSQL)
	stepStart = time.Now()
	if err := r.sess.Exec(createSQL); err != nil {
		// CDC EXCEPT can fail on edge cases (e.g. DuckDB unicode stats after TRUNCATE+INSERT).
		// Fall back to full query without CDC.
		if isIncremental && execSQL != model.SQL {
			r.trace(result, "create_temp", stepStart, "retry")
			result.Warnings = append(result.Warnings, "CDC query failed, using full query")
			execSQL = model.SQL
			needsBackfill = true
			createSQL = fmt.Sprintf("CREATE TEMP TABLE %s AS %s", tmpTable, execSQL)
			stepStart = time.Now()
			if retryErr := r.sess.Exec(createSQL); retryErr != nil {
				r.trace(result, "create_temp", stepStart, "error")
				return nil, fmt.Errorf("create temp table: %w", retryErr)
			}
		} else {
			r.trace(result, "create_temp", stepStart, "error")
			return nil, fmt.Errorf("create temp table: %w", err)
		}
	}
	r.trace(result, "create_temp", stepStart, "ok")

	// Sandbox skip: if incremental model produced 0 rows and target doesn't
	// exist in sandbox, skip — the prod table is visible via search_path.
	// This handles the case where CDC runs (hasChanges=true due to other
	// catalog commits) but the source tables didn't actually change.
	if isIncremental && !needsBackfill && r.sess.ProdAlias() != "" {
		if !r.tableExistsInCatalog(model.Target, r.sess.CatalogAlias()) {
			countVal, countErr := r.sess.QueryValue(fmt.Sprintf("SELECT COUNT(*) FROM %s", tmpTable))
			if countErr == nil && countVal == "0" {
				r.cleanup(tmpTable)
				result.RunType = "skip"
				result.RunReason = "no upstream changes"
				result.Duration = time.Since(start)
				return result, nil
			}
		}
	}

	// Schema evolution check — shared with script.go's runScript so the
	// two execution paths can't drift on this critical correctness logic.
	schemaChange, needsBackfill = r.detectSchemaEvolution(model, tmpTable, needsBackfill, result)

	// Run constraints (batched - single query for all constraints)
	stepStart = time.Now()
	if len(model.Constraints) > 0 {
		batchSQL, parseErrors := validation.ConstraintsToBatchSQL(model.Constraints, tmpTable)

		// Add any parse errors
		for _, err := range parseErrors {
			result.Errors = append(result.Errors, err.Error())
		}

		// Execute batched constraint check if we have valid constraints
		if batchSQL != "" {
			rows, err := r.sess.QueryRows(batchSQL)
			if err != nil {
				result.Errors = append(result.Errors, fmt.Sprintf("constraint check error: %v", err))
			} else {
				// Each row is an error message from a failed constraint
				for _, row := range rows {
					if row != "" {
						result.Errors = append(result.Errors, row)
					}
				}
			}
		}

		r.trace(result, "constraints", stepStart, "ok")
	}

	// If constraints failed, abort
	if len(result.Errors) > 0 {
		r.cleanup(tmpTable)
		result.Duration = time.Since(start)
		return result, fmt.Errorf("constraint validation failed")
	}

	// Capture snapshot BEFORE materialize. Historical audits time-travel
	// against this snapshot to compare current vs. previous values.
	stepStart = time.Now()
	prevSnapshot, _ := backfill.GetPreviousSnapshot(r.sess, model.Target)
	r.trace(result, "prev_snapshot", stepStart, "ok")

	// Render audits as a transactional pre-commit check. The result is
	// a SELECT error(...) wrapper that aborts the materialize transaction
	// if any audit fails — so failing audits roll back the schema ALTER,
	// the data write, and the commit metadata together (no zombie state
	// where metadata says one thing and the physical table says another).
	//
	// Parse errors abort BEFORE materialize: there's no point trying to
	// materialize a model whose audit directives are syntactically broken.
	stepStart = time.Now()
	auditSQL, auditParseErrors := r.buildAuditSQL(model, prevSnapshot)
	r.trace(result, "audits.render", stepStart, "ok")
	if len(auditParseErrors) > 0 {
		for _, e := range auditParseErrors {
			result.Errors = append(result.Errors, e.Error())
		}
		r.cleanup(tmpTable)
		result.Duration = time.Since(start)
		return result, fmt.Errorf("audit parse errors")
	}

	// Execute based on kind (includes audits + commit metadata in same transaction)
	stepStart = time.Now()
	rowsAffected, err := r.materialize(model, tmpTable, needsBackfill, schemaChange, auditSQL, sqlHash, result.RunType, result, start)
	if err != nil {
		r.trace(result, "materialize", stepStart, "error")
		// A failed audit raises error() inside the BEGIN/COMMIT, which
		// aborts the transaction — but DuckDB leaves the session in an
		// "aborted transaction" state. Without an explicit ROLLBACK the
		// next model in the same batch will fail with "cannot start a
		// transaction within a transaction". Best-effort: ignore any
		// error from the ROLLBACK itself (the session might already be
		// clean if the error came from a non-transactional path).
		r.sess.Exec("ROLLBACK")
		// No need to call rollback()/reverseSchemaEvolution — the
		// transaction abort already restored the physical state.
		result.Errors = append(result.Errors, err.Error())
		r.cleanup(tmpTable)
		result.Duration = time.Since(start)
		return result, fmt.Errorf("materialize: %w", err)
	}
	r.trace(result, "materialize", stepStart, "ok")
	result.RowsAffected = rowsAffected

	// Run warnings (soft validations, log only)
	stepStart = time.Now()
	r.runWarnings(model, model.Target, prevSnapshot, result)
	if len(model.Warnings) > 0 {
		r.trace(result, "warnings", stepStart, "ok")
	}

	stepStart = time.Now()
	r.cleanup(tmpTable)
	r.trace(result, "cleanup", stepStart, "ok")

	// Calculate total traced time and add overhead as explicit step
	result.Duration = time.Since(start)
	var tracedTotal time.Duration
	for _, step := range result.Trace {
		tracedTotal += step.Duration
	}
	overhead := result.Duration - tracedTotal
	if overhead > time.Microsecond {
		result.Trace = append(result.Trace, TraceStep{
			Name:     "_overhead",
			Duration: overhead,
			Status:   "ok",
		})
	}

	return result, nil
}

// rollback reverts to a previous snapshot using TRUNCATE + INSERT with time travel.
// buildAuditSQL renders the model's audits as a transactional pre-commit
// check. The returned SQL, when executed inside a BEGIN/COMMIT, raises a
// DuckDB error() (and aborts the surrounding transaction) the moment any
// audit query produces an error row. Returns "" when the model has no
// audits, in which case callers should pass an empty string into the
// commit.sql template's pre-commit-checks slot.
//
// In sandbox mode, historical (time-travel) audit clauses must reference
// the prod catalog explicitly because the sandbox catalog does not have
// the model's snapshot history; we surface that here so audits keep
// working in both modes without each materialize call site duplicating
// the branch.
//
// Parse errors are returned to the caller so they can abort BEFORE
// materialize runs — there's no value in trying to materialize a model
// whose audit directives are syntactically broken.
func (r *Runner) buildAuditSQL(model *parser.Model, prevSnapshot int64) (string, []error) {
	if len(model.Audits) == 0 {
		return "", nil
	}
	historicalTable := model.Target
	if r.sess.ProdAlias() != "" {
		historicalTable = r.sess.ProdAlias() + "." + model.Target
	}
	return validation.AuditsToTransactionalSQL(
		model.Audits, model.Target, historicalTable, prevSnapshot,
	)
}

// This preserves the DuckLake table_id and snapshot chain (unlike CREATE OR REPLACE
// which creates a new table_id and breaks row lineage).
// If schema evolution was applied, it is reversed first so the table schema matches
// the previous snapshot before restoring data.
// If the table did not exist at prevSnapshot (first run of a new model),
// falls back to DROP TABLE.
//
// In sandbox mode, prevSnapshot is a prod snapshot ID that doesn't exist in
// the sandbox catalog, so time-travel rollback is impossible. Sandbox is also
// throwaway by design — drop the sandbox table and let the next sandbox run
// rebuild it from scratch.
func (r *Runner) rollback(target string, prevSnapshot int64, sc *backfill.SchemaChange) error {
	if r.sess.ProdAlias() != "" {
		// Sandbox mode — no meaningful snapshot to roll back to.
		return r.sess.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", target))
	}
	if prevSnapshot == 0 {
		// No previous snapshot, drop the table
		if err := r.sess.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", target)); err != nil {
			return err
		}
	} else {
		// Reverse schema evolution before restoring data
		if sc != nil {
			r.reverseSchemaEvolution(target, *sc)
		}
		sql := fmt.Sprintf("TRUNCATE %s;\nINSERT INTO %s BY NAME SELECT * FROM %s AT (VERSION => %d)",
			target, target, target, prevSnapshot)
		if err := r.sess.Exec(sql); err != nil {
			if isTableNotExistError(err) {
				if err := r.sess.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", target)); err != nil {
					return err
				}
			} else {
				return fmt.Errorf("rollback to snapshot %d: %w", prevSnapshot, err)
			}
		}
	}

	// Clean up ack records for this target. Without this, a crash between
	// rollback and AckClaims would leave stale ack records — on next startup
	// IsAcked() would return true and discard inflight events permanently.
	// Best-effort: _ondatra_acks may not exist if no scripts have run.
	r.sess.Exec(fmt.Sprintf(
		"DELETE FROM _ondatra_acks WHERE target = '%s'", escapeSQL(target)))

	return nil
}

// cdcSchemaChanged checks if any CDC table's schema differs between current and snapshot.
// CDC uses EXCEPT which requires matching column counts on both sides.
// Returns true if any table has a different number of columns at the snapshot version.
func (r *Runner) cdcSchemaChanged(cdcTables []string, snapshotID int64) bool {
	for _, table := range cdcTables {
		qt := quoteTableName(table)
		// Compare column count: current vs snapshot version
		query := fmt.Sprintf(
			"SELECT (SELECT COUNT(*) FROM (DESCRIBE SELECT * FROM %s)) != "+
				"(SELECT COUNT(*) FROM (DESCRIBE SELECT * FROM %s AT (VERSION => %d)))",
			qt, qt, snapshotID)
		val, err := r.sess.QueryValue(query)
		if err != nil {
			// If we can't check (e.g. table didn't exist at snapshot, or sandbox mode
			// where time travel may not work), assume unchanged — the CDC EXCEPT will
			// fail gracefully via the retry fallback if the schema truly differs.
			return false
		}
		if val == "true" {
			return true
		}
	}
	return false
}

// isTableNotExistError returns true only for DuckDB catalog errors indicating
// the table didn't exist at the requested version. Uses the typed error from
// duckdb-go rather than string matching on error prefixes.
func isTableNotExistError(err error) bool {
	var de *duckdbdriver.Error
	if errors.As(err, &de) {
		return de.Type == duckdbdriver.ErrorTypeCatalog &&
			strings.Contains(strings.ToLower(de.Msg), "does not exist")
	}
	return false
}

// tableExistsInCatalog checks if a schema-qualified table (e.g. "raw.source")
// exists in a specific catalog using information_schema.
func (r *Runner) tableExistsInCatalog(table, catalog string) bool {
	parts := strings.SplitN(table, ".", 2)
	if len(parts) != 2 {
		return false // Unqualified names resolve via search path, no fix needed
	}
	schema, tbl := parts[0], parts[1]
	q := fmt.Sprintf(
		"SELECT COUNT(*) FROM information_schema.tables WHERE table_catalog = '%s' AND table_schema = '%s' AND table_name = '%s'",
		escapeSQL(catalog), escapeSQL(schema), escapeSQL(tbl))
	val, err := r.sess.QueryValue(q)
	return err == nil && val != "0"
}

// cleanup removes the temp table.
func (r *Runner) cleanup(tmpTable string) {
	r.sess.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", tmpTable))
}

// runWarnings runs warning validations (log only, no rollback).
// Warnings support both audit patterns (post-INSERT, history-aware) and
// constraint patterns (row-level checks). Both are tried for each directive.
func (r *Runner) runWarnings(model *parser.Model, table string, prevSnapshot int64, result *Result) {
	historicalTable := table
	if r.sess.ProdAlias() != "" {
		historicalTable = r.sess.ProdAlias() + "." + table
	}
	for _, warning := range model.Warnings {
		var queries []string

		// Try audit pattern (post-INSERT, history-aware)
		if sql, err := validation.AuditToSQL(warning, table, historicalTable, prevSnapshot); err == nil {
			queries = append(queries, sql)
		}

		// Try constraint pattern (row-level checks)
		if sql, err := validation.ConstraintToSQL(warning, table); err == nil {
			queries = append(queries, sql)
		}

		if len(queries) == 0 {
			result.Warnings = append(result.Warnings, fmt.Sprintf("warning parse error: unknown pattern: %s", warning))
			continue
		}

		for _, sql := range queries {
			rows, err := r.sess.QueryRows(sql)
			if err != nil {
				result.Warnings = append(result.Warnings, fmt.Sprintf("warning check error: %v", err))
				continue
			}

			for _, row := range rows {
				if row != "" {
					result.Warnings = append(result.Warnings, row)
				}
			}
		}
	}
}

// escapeSQL escapes single quotes for safe SQL string interpolation.
func escapeSQL(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}

// sanitizeTableName converts a target name to a safe temp table name.
func sanitizeTableName(target string) string {
	// Replace dots with underscores
	result := ""
	for _, c := range target {
		if c == '.' {
			result += "_"
		} else {
			result += string(c)
		}
	}
	return result
}

// getTableColumns returns the column names from a temp table.
// Only called with single-part temp table names (e.g. "tmp_staging_orders").
// Uses ondatra_get_column_names macro loaded at session startup.
func (r *Runner) getTableColumns(table string) ([]string, error) {
	query := fmt.Sprintf("SELECT * FROM ondatra_get_column_names('%s')", escapeSQL(table))
	return r.sess.QueryRows(query)
}

// ensureColumnsExist verifies that all named columns are present in the temp
// table's output schema. Used to validate @unique_key, @incremental, etc.
// against the actual SELECT result, instead of letting the model "succeed"
// on first run (backfill) and fail cryptically on the second run when CDC
// or merge tries to reference the missing column. (Bug 16 + 17)
//
// columnList may be a single column ("id") or comma-separated ("year, month").
func (r *Runner) ensureColumnsExist(tmpTable, directive, columnList string) error {
	actual, err := r.getTableColumns(tmpTable)
	if err != nil {
		return fmt.Errorf("get columns to validate %s: %w", directive, err)
	}
	actualSet := make(map[string]bool, len(actual))
	for _, c := range actual {
		actualSet[strings.ToLower(c)] = true
	}
	for _, want := range strings.Split(columnList, ",") {
		want = strings.TrimSpace(want)
		if want == "" {
			continue
		}
		if !actualSet[strings.ToLower(want)] {
			return fmt.Errorf("%s column %q is not in the model output (available columns: %s)",
				directive, want, strings.Join(actual, ", "))
		}
	}
	return nil
}

// ensureUniqueKeyNotNull rejects merge operations where the source temp table
// has NULL values in the @unique_key column(s). NULL keys can't be merged
// reliably (NULL = NULL is FALSE in standard SQL, and IS NOT DISTINCT FROM
// produces ambiguous UPDATEs when multiple source NULL rows match multiple
// target NULL rows). The user should fix the data or add @constraint: <key> NOT NULL.
//
// Supports both single-column keys ("id") and composite keys ("year, month").
func (r *Runner) ensureUniqueKeyNotNull(tmpTable, uniqueKey string) error {
	// Build a WHERE clause that matches any row with at least one NULL key column.
	cols := strings.Split(uniqueKey, ",")
	var conds []string
	for _, c := range cols {
		c = strings.TrimSpace(c)
		if c == "" {
			continue
		}
		conds = append(conds, fmt.Sprintf("%s IS NULL", duckdb.QuoteIdentifier(c)))
	}
	if len(conds) == 0 {
		return nil
	}
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE %s", tmpTable, strings.Join(conds, " OR "))
	result, err := r.sess.QueryValue(query)
	if err != nil {
		return fmt.Errorf("check unique_key NULLs: %w", err)
	}
	if result == "0" || result == "" {
		return nil
	}
	return fmt.Errorf("%s row(s) have NULL in unique_key column(s) [%s] — unique keys must not be NULL (fix the source data or add @constraint: %s NOT NULL)", result, uniqueKey, uniqueKey)
}

// detectSchemaEvolution captures the temp table's schema, compares it
// against the previously-committed schema for the model, classifies the
// change (additive / destructive / type-only / none), optionally detects
// renames via lineage, and returns the resulting *SchemaChange plus the
// (possibly-updated) needsBackfill decision.
//
// Shared by runner.go (SQL models) and script.go (Starlark/YAML script
// models) so the two execution paths can't drift on this critical
// correctness logic. Earlier, script.go inlined a much simpler version
// that:
//   - missed `tracked` kind entirely (and the kind-column filter for both
//     scd2 and tracked, so SCD2 scripts always saw "destructive" because
//     the prevSchema still had `is_current`/`valid_*` columns that the
//     script-emitted temp table never has)
//   - only ran on backfill (additive changes at incremental run were
//     never detected)
//   - had no rename detection
//   - on destructive changes, just warned instead of applying ALTER
//     (breaking the DuckLake snapshot chain)
//
// Views are exempt — they have no persisted schema to evolve.
func (r *Runner) detectSchemaEvolution(
	model *parser.Model,
	tmpTable string,
	needsBackfill bool,
	result *Result,
) (*backfill.SchemaChange, bool) {
	if model.Kind == "view" {
		return nil, needsBackfill
	}

	stepStart := time.Now()
	newSchema, schemaErr := backfill.CaptureSchema(r.sess, tmpTable)
	r.trace(result, "schema.capture_new", stepStart, "ok")
	if schemaErr != nil || len(newSchema) == 0 {
		return nil, needsBackfill
	}

	stepStart = time.Now()
	prevSchema, _ := backfill.GetPreviousSchema(r.sess, model.Target)
	r.trace(result, "schema.get_previous", stepStart, "ok")

	// Filter out kind-specific columns from prevSchema before comparison.
	// These are added by materialization (SCD2 adds is_current etc., tracked
	// adds _content_hash) and never appear in the temp table.
	prevSchema = filterKindColumns(prevSchema, model.Kind)

	if len(prevSchema) == 0 {
		return nil, needsBackfill
	}

	stepStart = time.Now()
	change := backfill.ClassifySchemaChange(prevSchema, newSchema, r.sess)
	r.trace(result, "schema.classify", stepStart, "ok")

	// Detect renames via lineage (only when there are both drops and adds —
	// the only configuration that could plausibly be a rename rather than a
	// real schema change). Skip for script models since they don't have
	// SQL-based lineage to compare against.
	if change.Type == backfill.SchemaChangeDestructive &&
		len(change.Dropped) > 0 && len(change.Added) > 0 &&
		!model.IsScript {
		stepStart = time.Now()
		if prevCommit, err := backfill.GetModelCommitInfo(r.sess, model.Target); err == nil && prevCommit != nil {
			if newLineage, _, err := r.extractLineage(model.SQL); err == nil {
				renames, addedCols, droppedCols := lineage.DetectRenames(prevCommit.ColumnLineage, newLineage)
				if len(renames) > 0 {
					for _, lr := range renames {
						change.Renamed = append(change.Renamed, backfill.ColumnRename{
							OldName: lr.OldName,
							NewName: lr.NewName,
							Source:  lr.Source,
						})
					}
					change.Added = filterColumnsByName(change.Added, addedCols)
					change.Dropped = filterColumnsByName(change.Dropped, droppedCols)
					if len(change.Dropped) == 0 {
						change.Type = backfill.SchemaChangeAdditive
					}
				}
			}
		}
		r.trace(result, "schema.detect_renames", stepStart, "ok")
	}

	switch change.Type {
	case backfill.SchemaChangeDestructive:
		// Apply destructive changes via ALTER (preserves DuckLake snapshot chain).
		if model.Kind == "table" {
			result.RunType = "full"
		} else {
			result.RunType = "incremental"
		}
		result.Warnings = append(result.Warnings,
			fmt.Sprintf("schema evolution: dropped=%d, added=%d, type_changes=%d",
				len(change.Dropped), len(change.Added), len(change.TypeChanged)))
		return &change, false
	case backfill.SchemaChangeAdditive, backfill.SchemaChangeTypeChange:
		result.RunType = "incremental"
		renameMsg := ""
		if len(change.Renamed) > 0 {
			renameMsg = fmt.Sprintf(", renames=%d", len(change.Renamed))
		}
		result.Warnings = append(result.Warnings,
			fmt.Sprintf("schema evolution: added=%d columns, type_changes=%d%s",
				len(change.Added), len(change.TypeChanged), renameMsg))
		return &change, false
	}
	// SchemaChangeNone — keep the backfill decision from NeedsBackfill.
	return nil, needsBackfill
}

// filterKindColumns removes kind-specific columns from a schema before comparison.
// Tracked adds _content_hash, SCD2 adds valid_from_snapshot/valid_to_snapshot/is_current.
// These are added by materialization logic, not user SQL, so they won't appear in temp tables.
func filterKindColumns(schema []backfill.Column, kind string) []backfill.Column {
	var exclude map[string]bool
	switch kind {
	case "tracked":
		exclude = map[string]bool{"_content_hash": true}
	case "scd2":
		exclude = map[string]bool{"valid_from_snapshot": true, "valid_to_snapshot": true, "is_current": true}
	default:
		return schema
	}
	var filtered []backfill.Column
	for _, col := range schema {
		if !exclude[col.Name] {
			filtered = append(filtered, col)
		}
	}
	return filtered
}

// filterColumnsByName filters columns to only include those with names in the given list.
func filterColumnsByName(columns []backfill.Column, names []string) []backfill.Column {
	if len(names) == 0 {
		return nil
	}
	nameSet := make(map[string]bool)
	for _, name := range names {
		nameSet[name] = true
	}
	var filtered []backfill.Column
	for _, col := range columns {
		if nameSet[col.Name] {
			filtered = append(filtered, col)
		}
	}
	return filtered
}
