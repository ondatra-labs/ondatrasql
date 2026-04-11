// OndatraSQL - You don't need a data stack anymore
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package execute

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/ondatra-labs/ondatrasql/internal/backfill"
	"github.com/ondatra-labs/ondatrasql/internal/duckdb"
	"github.com/ondatra-labs/ondatrasql/internal/parser"
	"github.com/ondatra-labs/ondatrasql/internal/script"
	"github.com/ondatra-labs/ondatrasql/internal/validation"
)

// loadExtension installs and loads a DuckDB extension.
// Supports formats: "name", "name FROM community", "name FROM core_nightly", "name FROM 'url'"
func (r *Runner) loadExtension(ext string) error {
	// Parse "name FROM repo" syntax (case-insensitive FROM).
	// Name is lowercased (DuckDB extension names are case-insensitive),
	// but repo is preserved as-is (URLs may be case-sensitive).
	// Split on whitespace first to avoid byte-offset mismatch from ToLower on non-ASCII.
	var name, repo string
	trimmed := strings.TrimSpace(ext)
	parts := strings.Fields(trimmed)
	if len(parts) >= 3 && strings.EqualFold(parts[1], "from") {
		name = strings.ToLower(parts[0])
		repo = strings.Join(parts[2:], " ")
	} else {
		name = strings.ToLower(trimmed)
	}

	// Build INSTALL statement
	var installSQL string
	if repo != "" {
		installSQL = fmt.Sprintf("INSTALL %s FROM %s", name, repo)
	} else {
		installSQL = fmt.Sprintf("INSTALL %s", name)
	}

	// Install (ignore "already installed" errors)
	if err := r.sess.Exec(installSQL); err != nil {
		// Check if it's an "already installed" error - that's OK.
		// Match the full phrase so unrelated errors mentioning either word
		// (e.g. "extension foo not installed for ...") still propagate.
		errStr := strings.ToLower(err.Error())
		if !strings.Contains(errStr, "already installed") {
			return fmt.Errorf("install: %w", err)
		}
	}

	// Load extension
	loadSQL := fmt.Sprintf("LOAD %s", name)
	if err := r.sess.Exec(loadSQL); err != nil {
		// Check if already loaded — match the full phrase, not the
		// individual words, so unrelated errors don't get swallowed.
		errStr := strings.ToLower(err.Error())
		if !strings.Contains(errStr, "already loaded") {
			return fmt.Errorf("load: %w", err)
		}
	}

	return nil
}

// runScript executes a Starlark script model using the same flow as SQL models.
// This ensures scripts get backfill detection, schema evolution, and metadata.
func (r *Runner) runScript(ctx context.Context, model *parser.Model) (*Result, error) {
	start := time.Now()
	result := &Result{
		Target:       model.Target,
		Kind:         model.Kind,
		lastTraceEnd: start,
	}

	// Warn if masking tags are set on script models (not supported)
	for colName, tags := range model.ColumnTags {
		if getMaskingMacro(tags) != "" {
			result.Warnings = append(result.Warnings,
				fmt.Sprintf("@column masking tag on %q is ignored for script models", colName))
		}
	}

	// Hash the script code for backfill detection
	stepStart := time.Now()
	scriptHash := backfill.ModelHash(model.SQL, backfill.ModelDirectives{
		Kind:               model.Kind,
		UniqueKey:          model.UniqueKey,
		PartitionedBy:      model.PartitionedBy,
		Incremental:        model.Incremental,
		IncrementalInitial: model.IncrementalInitial,
		ConfigHash:         r.configHash,
	})
	r.trace(result, "hash_script", stepStart, "ok")

	// Determine run_type using SQL-based logic (same as SQL models)
	stepStart = time.Now()
	decision, err := ComputeSingleRunType(r.sess, model, r.configHash)
	r.trace(result, "run_type.compute", stepStart, "ok")
	if err != nil {
		result.Errors = append(result.Errors, fmt.Sprintf("run_type check: %v", err))
		decision = &RunTypeDecision{RunType: "backfill"} // Safe fallback
	}

	result.RunType = decision.RunType
	needsBackfill := decision.RunType == "backfill"

	// Track schema evolution
	var schemaChange *backfill.SchemaChange

	// Get incremental state if @incremental is specified
	var incrState *backfill.IncrementalState
	if model.Incremental != "" {
		stepStart = time.Now()
		incrState, _ = backfill.GetIncrementalState(
			r.sess, model.Target, model.Incremental, model.IncrementalInitial)
		r.trace(result, "incremental.get_state", stepStart, "ok")

		// Force is_backfill when the runner decided on backfill (e.g. hash changed).
		// GetIncrementalState only checks table existence, but the runner may trigger
		// backfill for other reasons (directive change, schema change, etc.).
		if needsBackfill && incrState != nil {
			incrState.IsBackfill = true
			incrState.LastValue = incrState.InitialValue
		}
	}

	// Execute the Starlark script to collect data into temp table
	rt := script.NewRuntime(r.sess, incrState, r.projectDir)

	// Enable durable Badger-backed buffering when projectDir is set.
	// If projectDir is empty, all kinds fall back to the default in-memory
	// collector (no on-disk staging).
	// Exception: @kind: table uses the in-memory collector even when
	// projectDir is set, because table materialization uses CREATE OR
	// REPLACE (all-or-nothing) — partial Badger recovery between runs
	// would create duplicates against an already-populated target.
	if r.projectDir != "" && model.Kind != "table" {
		rt.SetIngestDir(filepath.Join(r.projectDir, ".ondatra", "ingest"))
	}

	stepStart = time.Now()
	var scriptResult *script.Result
	if model.Source != "" {
		// YAML model: load source function and call it directly
		scriptResult, err = rt.RunSource(ctx, model.Target, model.Source, model.SourceConfig)
	} else {
		// Starlark model: execute script code
		scriptResult, err = rt.Run(ctx, model.Target, model.SQL)
	}
	if err != nil {
		// Check if this is a clean abort (no message) — early exit, not an error
		var abortErr *script.AbortError
		if errors.As(err, &abortErr) {
			r.trace(result, "script_execute", stepStart, "ok")
			result.Duration = time.Since(start)
			return result, nil
		}
		r.trace(result, "script_execute", stepStart, "error")
		result.Errors = append(result.Errors, err.Error())
		result.Duration = time.Since(start)
		return result, err
	}
	defer scriptResult.Close()
	r.trace(result, "script_execute", stepStart, "ok")

	// Create temp table from collected data (DuckDB is now resumed).
	// This must happen before the "no data" check because in Badger mode,
	// pre-existing events from previous runs are included in the claim.
	stepStart = time.Now()
	if err := scriptResult.CreateTempTable(); err != nil {
		return nil, fmt.Errorf("create temp table: %w", err)
	}
	r.trace(result, "create_temp_table", stepStart, "ok")

	// If no data was collected (neither this run nor pre-existing), we're done
	if scriptResult.TempTable == "" {
		result.Duration = time.Since(start)
		result.Warnings = append(result.Warnings, "script collected no data")
		return result, nil
	}

	tmpTable := scriptResult.TempTable

	// Deduplicate temp table by unique_key for kinds that may have Badger duplicates.
	// When a script crashes after save.row() but before materialization, Badger retains
	// the rows. On the next run, the script produces the same rows again, resulting in
	// duplicates in the temp table. Dedup keeps only the last row per unique_key.
	if model.UniqueKey != "" {
		switch model.Kind {
		case "merge", "tracked", "scd2", "partition":
			// Handle composite unique_key (comma-separated, used by partition kind)
			var groupByCols string
			if strings.Contains(model.UniqueKey, ",") {
				var parts []string
				for _, col := range strings.Split(model.UniqueKey, ",") {
					parts = append(parts, duckdb.QuoteIdentifier(strings.TrimSpace(col)))
				}
				groupByCols = strings.Join(parts, ", ")
			} else {
				groupByCols = duckdb.QuoteIdentifier(model.UniqueKey)
			}
			dedupSQL := fmt.Sprintf(
				"DELETE FROM %s WHERE rowid NOT IN (SELECT MAX(rowid) FROM %s GROUP BY %s)",
				tmpTable, tmpTable, groupByCols)
			if err := r.sess.Exec(dedupSQL); err != nil {
				// Non-fatal: if dedup fails, proceed with potential duplicates
				result.Warnings = append(result.Warnings, fmt.Sprintf("dedup warning: %v", err))
			}
		}
	}

	// Schema evolution check — shared with runner.go's SQL-model path so
	// the two execution paths can't drift on this critical correctness logic.
	// Earlier this was a hand-rolled check that missed `tracked` kind, only
	// ran on backfill, lacked the kind-column filter (so SCD2 scripts always
	// saw "destructive" because of is_current/valid_* columns), and didn't
	// preserve the snapshot chain on destructive changes.
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

	// Abort if constraints failed — nack claims so events can be retried
	if len(result.Errors) > 0 {
		scriptResult.NackClaims()
		r.cleanup(tmpTable)
		result.Duration = time.Since(start)
		return result, fmt.Errorf("constraint validation failed")
	}

	// Build ack SQL for Badger claims — included in the materialize transaction
	// so the ack record is atomic with the data commit. On audit failure,
	// the ack record is undone too, and we nack Badger claims for retry.
	var extraPreSQL []string
	if len(scriptResult.ClaimIDs) > 0 {
		if ackErr := script.EnsureAckTable(r.sess); ackErr != nil {
			result.Warnings = append(result.Warnings, fmt.Sprintf("ack table: %v", ackErr))
		} else {
			for _, claimID := range scriptResult.ClaimIDs {
				extraPreSQL = append(extraPreSQL, script.AckSQL(claimID, model.Target, scriptResult.RowCount))
			}
		}
	}

	// Render audits as a transactional pre-commit check (same path as
	// runner.go's SQL flow). Parse errors abort BEFORE materialize so a
	// broken @audit directive doesn't waste a Badger ack cycle.
	stepStart = time.Now()
	auditSQL, auditParseErrors := r.buildAuditSQL(model)
	r.trace(result, "audits.render", stepStart, "ok")
	if len(auditParseErrors) > 0 {
		for _, e := range auditParseErrors {
			result.Errors = append(result.Errors, e.Error())
		}
		scriptResult.NackClaims()
		r.cleanup(tmpTable)
		result.Duration = time.Since(start)
		return result, fmt.Errorf("audit parse errors")
	}

	// Materialize using same logic as SQL models (ack records in same transaction).
	// Audits run inside the same BEGIN/COMMIT, so a failing audit aborts
	// the whole materialize atomically — no separate rollback() needed.
	stepStart = time.Now()
	rowsAffected, err := r.materialize(model, tmpTable, needsBackfill, schemaChange, auditSQL, scriptHash, result.RunType, result, start, extraPreSQL...)
	if err != nil {
		r.trace(result, "materialize", stepStart, "error")
		// A failed audit raises error() inside the BEGIN/COMMIT, which
		// aborts the transaction but leaves the session in an aborted
		// state — explicit ROLLBACK clears it so the next model in the
		// same batch can start its own transaction. Best-effort.
		r.sess.Exec("ROLLBACK")

		// Badger claim handling differs by failure cause:
		//
		//   * Audit failure → ACK the claims. The script's data was
		//     valid output; only the materialize result was reverted
		//     by the transaction abort. Re-running the script would
		//     re-fetch fresh events from the source via the script's
		//     incremental cursor logic, so we mark the consumed
		//     batch as "done" to avoid double-processing on retry.
		//
		//   * Other materialize errors (DDL/DML bug, transient I/O,
		//     constraint violation surfacing during INSERT) → NACK
		//     the claims. The script output never reached the target
		//     in any meaningful way, and the next run should retry
		//     with the same buffered events.
		//
		// We discriminate on the error message because the audit
		// failure path is the only one that goes through DuckDB's
		// `error('audit failed: …')` scalar inside the transaction.
		if strings.Contains(err.Error(), "audit failed") {
			if ackErr := scriptResult.AckClaims(); ackErr != nil {
				result.Warnings = append(result.Warnings,
					fmt.Sprintf("ack after audit failure: %v", ackErr))
			}
		} else {
			scriptResult.NackClaims()
		}

		result.Errors = append(result.Errors, err.Error())
		r.cleanup(tmpTable)
		result.Duration = time.Since(start)
		return result, fmt.Errorf("materialize: %w", err)
	}
	r.trace(result, "materialize", stepStart, "ok")
	result.RowsAffected = rowsAffected

	// Everything succeeded — ack Badger claims (remove from inflight).
	// Then delete ack records — the crash window is closed.
	if ackErr := scriptResult.AckClaims(); ackErr != nil {
		result.Warnings = append(result.Warnings, fmt.Sprintf("ack claims: %v", ackErr))
	} else {
		for _, claimID := range scriptResult.ClaimIDs {
			script.DeleteAck(r.sess, claimID)
		}
	}

	// Run warnings (same as SQL models)
	stepStart = time.Now()
	r.runWarnings(model, model.Target, result)
	if len(model.Warnings) > 0 {
		r.trace(result, "warnings", stepStart, "ok")
	}

	// Cleanup
	stepStart = time.Now()
	r.cleanup(tmpTable)
	r.trace(result, "cleanup", stepStart, "ok")

	result.Duration = time.Since(start)
	return result, nil
}
