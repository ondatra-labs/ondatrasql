// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

// Package execute runs SQL models through the DuckDB session.
package execute

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	duckdbdriver "github.com/duckdb/duckdb-go/v2"

	"github.com/ondatra-labs/ondatrasql/internal/backfill"
	"github.com/ondatra-labs/ondatrasql/internal/duckast"
	"github.com/ondatra-labs/ondatrasql/internal/duckdb"
	"github.com/ondatra-labs/ondatrasql/internal/libregistry"
	"github.com/ondatra-labs/ondatrasql/internal/lineage"
	"github.com/ondatra-labs/ondatrasql/internal/parser"
	"github.com/ondatra-labs/ondatrasql/internal/script"
	"github.com/ondatra-labs/ondatrasql/internal/state"
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
	Target         string
	Kind           string
	RunType        string // "incremental", "backfill", "full", or "skip"
	RunReason      string // Human-readable reason for the run_type decision
	RowsAffected   int64
	Duration       time.Duration
	Errors         []string
	Warnings       []string
	Trace          []TraceStep `json:"trace,omitempty"`
	SyncSucceeded  int64       `json:"sync_succeeded,omitempty"`
	SyncFailed     int64       `json:"sync_failed,omitempty"`
	lastTraceEnd   time.Time   // internal: when last trace was recorded
}

// Runner executes SQL models.
type Runner struct {
	sess             *duckdb.Session
	mode             Mode
	dagRunID         string
	projectDir       string                // Project root directory (for Starlark load())
	configIndex      *backfill.ConfigIndex // config/ decomposed into per-statement buckets
	foreignCatalogs  map[string]bool       // ATTACHed catalogs that are not the lake; nil until first lookup
	configIndexErr   error                 // config/ exists but could not be read (fatal for the run)
	gitInfo          gitInfo               // Cached Git metadata
	runTypeDecisions RunTypeDecisions      // Pre-computed run_type decisions (batch optimization)
	astCache         map[string]string     // Cached AST JSON by SQL hash (reduces duplicate lineage queries)
	libRegistry      *libregistry.Registry // Registered lib functions from lib/*.star
	stateStore       *state.State          // Lazily opened state.duckdb (durable fetch buffer)
	stateOwned       bool                  // True iff this Runner opened stateStore (controls Close)
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

// SetLibRegistry sets the lib function registry for FROM lib_func() support.
func (r *Runner) SetLibRegistry(reg *libregistry.Registry) {
	r.libRegistry = reg
}

// SetProjectDir sets the project root directory for Starlark load() support.
// Also indexes config/ so macro and variable changes are detected per model
// (Bug S21).
func (r *Runner) SetProjectDir(dir string) {
	r.projectDir = dir
	// A read failure is held, not swallowed: Run surfaces it. Treating an
	// unreadable config/ as "no config" would silently disable config-change
	// detection for every model in the project.
	r.configIndex, r.configIndexErr = backfill.BuildConfigIndex(filepath.Join(dir, "config"))
}

// SetProjectDirWithConfigIndex sets the project root and the config index
// together, skipping the config read SetProjectDir would perform.
//
// RunDAG builds the index once and shares it, so a DAG of N models walks and
// parses config/ once instead of N times. Setting the two in one call is what
// makes that true: calling SetProjectDir first and overwriting the index
// afterwards would still do the per-model read, and would additionally
// discard a read error from it — hiding a broken config behind an index that
// happened to be built a moment earlier.
func (r *Runner) SetProjectDirWithConfigIndex(dir string, ix *backfill.ConfigIndex) {
	r.projectDir = dir
	r.configIndex = ix
	r.configIndexErr = nil
}

// modelConfigHash returns the config hash that applies to this model — the
// value compared against `config_hash` in the model's last DuckLake commit.
// It covers only the config fragments this model actually uses, plus the
// session-wide config that applies to everything.
func (r *Runner) modelConfigHash(model *parser.Model) string {
	return r.configIndex.HashFor(configRefText(model))
}

// SetStateStore lets the caller inject an externally-owned state.duckdb
// handle so multiple Runners in the same DAG can share one open file
// (DuckDB takes a process-level file lock; concurrent opens of the same
// path fail). When set, the Runner does not own the handle and never
// closes it.
func (r *Runner) SetStateStore(st *state.State) {
	r.stateStore = st
	r.stateOwned = false
}

// getStateStore returns the shared state store if one was injected via
// SetStateStore. Otherwise (single-Runner usage with projectDir set) it
// lazily opens its own. Returns nil if projectDir isn't set (in-memory
// mode only). Caller should defer CloseState() to release a
// runner-owned handle.
func (r *Runner) getStateStore() (*state.State, error) {
	if r.stateStore != nil {
		return r.stateStore, nil
	}
	if r.projectDir == "" {
		return nil, nil
	}
	st, err := state.Open(filepath.Join(r.projectDir, "config"))
	if err != nil {
		return nil, fmt.Errorf("open state: %w", err)
	}
	r.stateStore = st
	r.stateOwned = true
	return st, nil
}

// stateDB returns the underlying *sql.DB handle for state, or nil if
// no state store is configured. Used to thread the handle into OAuth
// flows via APIHTTPConfig.StateDB without forcing every call site to
// own state-error handling.
func (r *Runner) stateDB() *sql.DB {
	if r.stateStore == nil {
		return nil
	}
	return r.stateStore.DB()
}

// CloseState closes the state.duckdb handle iff this Runner opened it
// itself. A handle injected via SetStateStore is owned by the caller
// and left alone. Safe to call when no state store was ever opened.
func (r *Runner) CloseState() error {
	if r.stateStore == nil || !r.stateOwned {
		r.stateStore = nil
		return nil
	}
	err := r.stateStore.Close()
	r.stateStore = nil
	r.stateOwned = false
	return err
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

	// config/ exists but could not be read. Aborting is the point: proceeding
	// would hash every model as "no config" and quietly stop detecting config
	// changes for the rest of the project's life.
	if r.configIndexErr != nil {
		result.Errors = append(result.Errors, fmt.Sprintf("read config: %v", r.configIndexErr))
		result.Duration = time.Since(start)
		return result, fmt.Errorf("read config for %s: %w", model.Target, r.configIndexErr)
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

	// Starlark script models (.star) are no longer supported as model files.
	// Starlark is used only in lib/ blueprints via the API dict pattern.
	if model.ScriptType != parser.ScriptTypeNone {
		return nil, fmt.Errorf("starlark script models (.star) are no longer supported — use SQL models with lib/ blueprints instead")
	}


	// Trace: Calculate SQL hash (in-memory, should be fast)
	var stepStart time.Time
	stepStart = time.Now()
	sqlHash := backfill.ModelHash(model.SQL, backfill.ModelDirectives{
		Kind:               model.Kind,
		UniqueKey:          model.UniqueKey,
		GroupKey:           model.GroupKey,
		PartitionedBy:      model.PartitionedBy,
		Incremental:        model.Incremental,
		IncrementalInitial: model.IncrementalInitial,
		Fetch:              model.Fetch,
		Push:               model.Push,
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
		decision, err = ComputeSingleRunType(r.sess, model, r.modelConfigHash(model))
		r.trace(result, "run_type.compute", stepStart, "ok")
		if err != nil {
			// Abort rather than fall back to backfill — a transient DB error
			// would otherwise force a destructive full re-fetch.
			result.Errors = append(result.Errors, fmt.Sprintf("run_type check: %v", err))
			result.Duration = time.Since(start)
			return result, fmt.Errorf("run_type check: %w", err)
		}
	}

	result.RunType = decision.RunType
	result.RunReason = decision.Reason

	// v0.12.1+ sandbox override (Bug S5): if a model has any validation- or
	// transformation-relevant directive, never short-circuit to "skip" in
	// sandbox mode. The model SQL hash isn't sensitive to directive-only
	// changes (audit thresholds, constraint values, mask macro names), so a
	// user editing only such a value would otherwise see [OK] skip with the
	// directives silently un-evaluated.
	//
	// What counts as "directive present" for the purposes of this override:
	//   - Audits, Constraints, Warnings: pure validation, must always run
	//   - ColumnTags: drives applyColumnMasking which mutates the data
	//     (mask, hash, redact macros), so an edit changes the output
	//
	// Models without any of these can still skip in sandbox — there's
	// nothing to validate beyond the data, which is already inherited from
	// the prod fork.
	if r.sess.ProdAlias() != "" && result.RunType == "skip" {
		hasDirectives := len(model.Audits) > 0 ||
			len(model.Constraints) > 0 ||
			len(model.Warnings) > 0 ||
			len(model.ColumnTags) > 0
		if hasDirectives {
			result.RunType = "backfill"
			result.RunReason = "sandbox: forced re-run for directive validation"
			decision = &RunTypeDecision{RunType: "backfill", Reason: result.RunReason}
		}
	}

	// Skip: nothing changed, no work to do -- but check for pending sink work
	if result.RunType == "skip" {
		if model.Push != "" {
			// No delta table on skip -- only process existing state-store backlog
			if err := r.executePush(ctx, model, result, nil, 0); err != nil {
				result.Warnings = append(result.Warnings, fmt.Sprintf("sink: %v", err))
			}
		}
		result.Duration = time.Since(start)
		return result, nil
	}

	needsBackfill := decision.RunType == "backfill"

	// Track if we need to apply schema evolution (additive changes)
	var schemaChange *backfill.SchemaChange

	// Set incremental variables if @incremental is specified (for SQL models)
	if model.Incremental != "" {
		stepStart = time.Now()
		incrState, incrErr := backfill.GetIncrementalState(
			r.sess, model.Target, model.Incremental, model.IncrementalInitial)
		if incrErr != nil {
			// State lookup failed (catalog read error, snapshots() down,
			// MAX(cursor) failed). Don't proceed with NULL incr_* vars —
			// the model's `WHERE col > getvariable('incr_last_value')`
			// would evaluate to NULL and silently materialise 0 rows.
			// Abort so the caller decides whether to retry or escalate.
			// (Pre-fix this was masked by GetIncrementalState falling
			// back to InitialValue; the strict-error contract surfaces
			// the failure but the runner must respect it.)
			result.Errors = append(result.Errors, fmt.Sprintf("incremental state: %v", incrErr))
			result.Duration = time.Since(start)
			return result, fmt.Errorf("incremental state for %s: %w", model.Target, incrErr)
		}
		if incrState != nil {
			// Force is_backfill when the runner decided on backfill (e.g. hash changed).
			// GetIncrementalState only checks table existence, but the runner may trigger
			// backfill for other reasons (directive change, schema change, etc.).
			if needsBackfill {
				incrState.IsBackfill = true
				incrState.LastValue = incrState.InitialValue
			}
			if err := applyIncrementalVars(r.sess, incrState); err != nil {
				result.Errors = append(result.Errors, err.Error())
				result.Duration = time.Since(start)
				return result, err
			}
		}
		r.trace(result, "incremental.set_vars", stepStart, "ok")
	}

	// escalateToRebuild records that the model has to materialize as a rebuild
	// rather than an append, which every path that abandons CDC needs: an
	// unbounded query appended to the target grows it without bound.
	//
	// `@incremental` is the exception. Its cursor bounds the read on its own,
	// so the query is not full, appending is correct, and forcing a rebuild
	// would reset the cursor and re-read the whole source every run. Kept in
	// one place because three separate call sites got this wrong before.
	escalateToRebuild := func(isIncremental bool) error {
		if !isIncremental || model.Incremental != "" {
			return nil
		}
		needsBackfill = true
		return r.resetIncrementalForBackfill(model)
	}

	// Track the SQL transformation pipeline through this Run().
	// state.Current() is what we execute; state.Rewritten() is the snapshot
	// preserved as the retry target when CDC must be reverted. See sql_state.go.
	state := newRunSQLState(model.SQL)

	// Auto-detect tables and column lineage from SQL (single AST query)
	stepStart = time.Now()
	astJSON, astErr := r.getAST(model.SQL)
	if astErr != nil {
		// Surface AST serialization failures so degraded lib-call /
		// CDC / lineage paths are at least visible to the operator.
		// Materialization itself still proceeds — DuckDB will surface
		// a clearer error if the SQL is genuinely broken — but a
		// transient json_serialize_sql hiccup would otherwise silently
		// disable AST-driven validators and lineage capture.
		result.Warnings = append(result.Warnings, fmt.Sprintf("AST serialize failed (lineage/CDC degraded): %v", astErr))
	}

	// Parse the AST once for cross-cutting validators and downstream uses.
	// Parse failures are non-fatal here — DuckDB will surface a clearer
	// error when the model actually executes. Validators tolerate nil AST.
	parsedAST, parseErr := duckast.Parse(astJSON)
	if parseErr != nil && astErr == nil {
		// Don't double-report when the upstream serialise already failed.
		result.Warnings = append(result.Warnings, fmt.Sprintf("AST parse failed (lineage/CDC degraded): %v", parseErr))
	}

	// Cross-cutting parser rule: pipeline models must not contain LIMIT or
	// OFFSET. Applies to every kind, every directive, every nesting level.
	if err := validateNoLimitOffset(parsedAST); err != nil {
		return nil, fmt.Errorf("%s: %w", model.Target, err)
	}

	// Detect and execute lib function calls in FROM clause (e.g. FROM gam_fetch(...))
	var libCalls []LibCall
	var hasInferredStubs bool        // true when 0-row lib stubs were created from AST/column inference
	var trackedOpts trackedRunOpts   // populated when tracked + lib + all-empty + no_change semantics
	// Ensure all lib-call state-store stores are cleaned up on any exit path.
	// Claims that were not explicitly acked are nacked (returned to queue).
	// The acked flag is set in the success path after materialize.
	var libClaimsAcked bool
	// ackLibClaims acks all lib-call state-store claims and deletes ack records.
	// Sets libClaimsAcked only if ALL acks succeed. If any ack fails,
	// libClaimsAcked stays false and defer will nack all claims.
	ackLibClaims := func() {
		if libClaimsAcked {
			return
		}
		allOK := true
		for i := range libCalls {
			sr := libCalls[i].ScriptResult
			if sr == nil || len(sr.ClaimIDs) == 0 {
				continue
			}
			if ackErr := sr.AckClaims(); ackErr != nil {
				result.Warnings = append(result.Warnings, fmt.Sprintf("ack claims %s: %v", libCalls[i].FuncName, ackErr))
				allOK = false
				break
			}
			for _, claimID := range sr.ClaimIDs {
				if err := script.DeleteAck(r.sess, claimID); err != nil {
					fmt.Fprintf(os.Stderr, "warning: delete ack record: %v\n", err)
				}
			}
		}
		if allOK {
			libClaimsAcked = true
		}
	}
	defer func() {
		for i := range libCalls {
			sr := libCalls[i].ScriptResult
			if sr == nil {
				continue
			}
			if !libClaimsAcked && len(sr.ClaimIDs) > 0 {
				// Only nack claims that we KNOW are not committed to
				// lake. After materialize commits, _ondatra_acks holds
				// the ack record; nacking those resets staging
				// claim_id but leaves the ack record orphaned, and the
				// next run can re-process the same rows under a new
				// claim_id (silent duplicates). IsAcked discriminates.
				//
				// On IsAcked lookup error we leave the claim alone:
				// claim_id stays set, so newStateCollector on the next
				// pipeline run will retry the IsAcked check (and at
				// that point return an error if it still fails, which
				// is the safer of the two ambiguous outcomes).
				toNack := sr.ClaimIDs[:0]
				for _, cid := range sr.ClaimIDs {
					acked, ackErr := script.IsAcked(r.sess, cid)
					if ackErr != nil {
						result.Warnings = append(result.Warnings,
							fmt.Sprintf("ack lookup for claim %s failed; left claimed for next-run recovery: %v", cid, ackErr))
						continue
					}
					if !acked {
						toNack = append(toNack, cid)
					}
				}
				if len(toNack) > 0 {
					sr.ClaimIDs = toNack
					_ = sr.NackClaims() // claim retries on next run if Nack fails
				}
			}
			_ = sr.Close() // cleanup path; close error secondary
		}
	}()
	if r.libRegistry != nil && !r.libRegistry.Empty() {
		if parsedAST != nil {
			libCalls = detectLibCalls(parsedAST, r.libRegistry)
		}
		// Also run SQL-based detection to find API dict libs hidden by
		// macro expansion. Merge results, skipping duplicates by FuncName+Occurrence.
		sqlCalls := detectLibCallsFromSQL(model.SQL, r.libRegistry)
		if len(sqlCalls) > 0 {
			seen := make(map[string]int)
			for _, c := range libCalls {
				seen[c.FuncName]++
			}
			for _, c := range sqlCalls {
				if seen[c.FuncName] > 0 {
					seen[c.FuncName]--
					continue // already detected via AST
				}
				libCalls = append(libCalls, c)
			}
		}
	}

	// @fetch ↔ lib-call relationship rules. Enforced regardless of
	// whether the lib registry is populated — a model with @fetch but
	// no lib call is invalid even if the registry is empty (the user
	// has declared a lib-backed model but provided no lib).
	if model.Fetch && len(libCalls) == 0 {
		return nil, fmt.Errorf("%s: @fetch model has no lib calls — add `FROM lib_name(...)` to the SELECT (lib functions live in lib/*.star), or remove @fetch", model.Target)
	}
	if !model.Fetch && len(libCalls) > 0 {
		return nil, fmt.Errorf("%s: model contains lib call %q in FROM but lacks @fetch — add `-- @fetch` to the model header to declare it as a lib-backed fetch model", model.Target, libCalls[0].FuncName)
	}

	// Surface lib status-check warnings (fetch funcs that call http.* without
	// checking resp.ok — a 4xx would become a silent 0-row return). Deduped so
	// a lib used in multiple calls warns once.
	seenLibWarn := make(map[string]bool)
	for i := range libCalls {
		if libCalls[i].Lib == nil {
			continue
		}
		for _, w := range libCalls[i].Lib.StatusWarnings {
			if !seenLibWarn[w] {
				seenLibWarn[w] = true
				result.Warnings = append(result.Warnings, w)
			}
		}
	}

	// Strict-schema validator activation is now @fetch-driven (was
	// lib-call-detection-driven in v0.29 and earlier). The @fetch
	// directive is the explicit declaration that this model produces
	// its schema from SQL projections; the validator enforces the
	// shape. After the relationship rules above, model.Fetch and
	// len(libCalls) > 0 are equivalent — but the contract is now
	// declared by the directive, not inferred from the FROM clause.
	if model.Fetch {
		if err := validateStrictLibSchema(parsedAST); err != nil {
			return nil, fmt.Errorf("%s: %w", model.Target, err)
		}
	}

	// Strict-push validator activates on @push. Push-mode scope is the
	// outermost SELECT projection only — the rest of the SQL is free
	// shape construction.
	if model.Push != "" {
		if err := validateStrictPushSchema(parsedAST, len(libCalls) > 0); err != nil {
			return nil, fmt.Errorf("%s: %w", model.Target, err)
		}
	}

	if r.libRegistry != nil && !r.libRegistry.Empty() {
		if len(libCalls) > 0 {
				// Validate supported_kinds for fetch
				for _, c := range libCalls {
					if len(c.Lib.SupportedKinds) > 0 {
						allowed := false
						for _, k := range c.Lib.SupportedKinds {
							if k == model.Kind {
								allowed = true
								break
							}
						}
						if !allowed {
							return nil, fmt.Errorf("lib %s does not support @kind: %s (supported: %v)", c.FuncName, model.Kind, c.Lib.SupportedKinds)
						}
					}
				}

				// Mark model as having lib calls so materialize knows
				// tmpTable contains all rows (not CDC-filtered)
				model.Source = libCalls[0].FuncName

				singleLibSource := len(libCalls) == 1
				argOccurrence := make(map[string]int) // track per-function occurrence for arg extraction
				for i := range libCalls {
					call := &libCalls[i]

					var kwargs map[string]any

					if call.ASTNode != nil {
						// AST-based detection: evaluate args from AST nodes
						args, evalErr := r.evaluateArgs(call)
						if evalErr != nil {
							return nil, fmt.Errorf("lib %s args: %w", call.FuncName, evalErr)
						}
						callAlias := ""
						if call.ASTNode != nil {
							callAlias = call.ASTNode.Alias()
						}
						cols, matchErr := matchColumns(parsedAST, call.Lib, callAlias, singleLibSource)
						if matchErr != nil {
							return nil, fmt.Errorf("lib %s columns: %w", call.FuncName, matchErr)
						}
						var typedCols []any
						if call.Lib.DynamicColumns {
							typedCols = extractTypedSelectColumns(parsedAST)
						}
						kwargs = buildLibCallKwargs(call.Lib, args, cols, typedCols)
					} else {
						// SQL-based detection (macro expansion hid the TABLE_FUNCTION node).
						// Extract args from raw SQL: parse "func_name('arg1', 'arg2')" manually.
						// Track occurrence per function name for multiple calls.
						occ := argOccurrence[call.FuncName]
						argOccurrence[call.FuncName]++
						kwargs = r.extractArgsFromSQL(model.SQL, call.Lib, occ)
					}

					// Inject target name so fetch() can query existing state
					kwargs["target"] = model.Target

					// Inject typed columns for SQL-fallback path (AST path
					// already has them via buildLibCallKwargs)
					if _, hasColumns := kwargs["columns"]; !hasColumns && call.Lib.DynamicColumns {
						if typedCols := extractTypedSelectColumns(parsedAST); typedCols != nil {
							kwargs["columns"] = typedCols
						}
					}

					// Inject incremental state as kwargs.
					//
					// Fail closed on read failure — pre-R7 the code logged a
					// warning and fell through to synthetic
					// is_backfill=true/empty cursor, which silently produced
					// wrong rows (a fetch that should have been incremental
					// would have re-pulled everything OR pulled nothing
					// depending on the lib's defaults). A fetch with
					// corrupted state is never the right answer; it must
					// halt and surface the error so the operator can
					// inspect catalog state.
					incrState, incrErr := backfill.GetIncrementalState(
						r.sess, model.Target, model.Incremental, model.IncrementalInitial)
					if incrErr != nil {
						result.Errors = append(result.Errors, fmt.Sprintf("lib %s incremental state: %v", call.FuncName, incrErr))
						return result, fmt.Errorf("lib %s incremental state for %s: %w", call.FuncName, model.Target, incrErr)
					}
					if incrState != nil && needsBackfill {
						incrState.IsBackfill = true
						incrState.LastValue = incrState.InitialValue
					}
					if incrState != nil {
						kwargs["is_backfill"] = incrState.IsBackfill
						kwargs["last_value"] = incrState.LastValue
						kwargs["last_run"] = incrState.LastRun
						kwargs["cursor"] = incrState.Cursor
						kwargs["initial_value"] = incrState.InitialValue
					} else {
						// GetIncrementalState returned (nil, nil) — no
						// existing target table, so first-run backfill
						// semantics with empty cursor is the correct
						// initial state, NOT a fallback for an error path.
						kwargs["is_backfill"] = true
						kwargs["last_value"] = ""
						kwargs["last_run"] = ""
						kwargs["cursor"] = ""
						kwargs["initial_value"] = ""
					}

					rt := script.NewRuntime(r.sess, nil, r.projectDir)
					if r.projectDir != "" && model.Kind != "table" {
						st, err := r.getStateStore()
						if err != nil {
							return nil, err
						}
						rt.SetStateStore(st)
					}

					// Execute lib function (unique target per call to avoid temp table collision)
					libTarget := fmt.Sprintf("_lib_%s_%d", call.FuncName, call.CallIndex)
					var scriptResult *script.Result
					var runErr error
					if call.Lib.APIConfig != nil {
						httpCfg := httpConfigFromLib(call.Lib.APIConfig, ctx, r.stateDB())
						if call.Lib.FetchMode == "async" {
							// Async fetch: submit() → check() → fetch_result()
							pollInterval := 5 * time.Second
							pollTimeout := 5 * time.Minute
							pollBackoff := 1
							// Reject zero/negative durations — pre-R8 a
							// blueprint that set fetch_poll_interval="0s"
							// (or a malformed value that parsed but landed
							// negative) would either busy-spin in
							// time.After(0) or trigger immediate timeout
							// before the first poll. Fail closed instead.
							if call.Lib.APIConfig.FetchPollInterval != "" {
								d, err := time.ParseDuration(call.Lib.APIConfig.FetchPollInterval)
								if err != nil {
									return nil, fmt.Errorf("lib %s fetch_poll_interval %q: %w", call.FuncName, call.Lib.APIConfig.FetchPollInterval, err)
								}
								if d <= 0 {
									return nil, fmt.Errorf("lib %s fetch_poll_interval must be > 0 (got %v)", call.FuncName, d)
								}
								pollInterval = d
							}
							if call.Lib.APIConfig.FetchPollTimeout != "" {
								d, err := time.ParseDuration(call.Lib.APIConfig.FetchPollTimeout)
								if err != nil {
									return nil, fmt.Errorf("lib %s fetch_poll_timeout %q: %w", call.FuncName, call.Lib.APIConfig.FetchPollTimeout, err)
								}
								if d <= 0 {
									return nil, fmt.Errorf("lib %s fetch_poll_timeout must be > 0 (got %v)", call.FuncName, d)
								}
								pollTimeout = d
							}
							if call.Lib.APIConfig.FetchPollBackoff > 0 {
								pollBackoff = call.Lib.APIConfig.FetchPollBackoff
							}
							scriptResult, runErr = rt.RunSourceAsync(ctx, libTarget, call.FuncName, kwargs, call.Lib.PageSize, pollInterval, pollTimeout, pollBackoff, httpCfg)
						} else {
							// Sync fetch: paginated path (page_size=0 means single call)
							scriptResult, runErr = rt.RunSourcePaginated(ctx, libTarget, call.FuncName, kwargs, call.Lib.PageSize, httpCfg)
						}
					} else {
						// Legacy TABLE dict: use save.row() contract
						scriptResult, runErr = rt.RunSource(ctx, libTarget, call.FuncName, kwargs)
					}
					if runErr != nil {
						// Check for clean abort
						var abortErr *script.AbortError
						if errors.As(runErr, &abortErr) {
							result.RowsAffected = 0
							result.Duration = time.Since(start)
							return result, nil
						}
						return nil, fmt.Errorf("lib %s: %w", call.FuncName, runErr)
					}

					// Create temp table from collected rows
					if err := scriptResult.CreateTempTable(); err != nil {
						return nil, fmt.Errorf("lib %s temp table: %w", call.FuncName, err)
					}
					call.TempTable = scriptResult.TempTable
					call.ScriptResult = scriptResult
					r.trace(result, "lib."+call.FuncName, stepStart, "ok")
					stepStart = time.Now()
				}

				// Tracked/scd2 + lib + all 0-row returns + every lib's
				// empty_result is "no_change" (the default) → tell materialize
				// to suppress the delete-on-missing branch so target rows are
				// preserved (tracked: missing groups; scd2: missing current
				// versions). The rest of the pipeline (stubs, rewrite, schema
				// evolution, audits, materialize) still runs so that SQL-only
				// changes (new columns, audit changes) are applied.
				trackedOpts.noDeleteOnMissingGroups = (model.Kind == "tracked" || model.Kind == "scd2") && allLibsReturnedNoChange(libCalls)

				// For lib calls that returned 0 rows, create empty stub
				// temp tables using the input-shape columns the query
				// references for each specific lib call — not the output
				// projection from SELECT. This ensures columns used in
				// JOIN ON, WHERE, LATERAL, etc. are present in the stub.
				for i := range libCalls {
					call := &libCalls[i]
					if call.TempTable != "" {
						continue // already has data
					}
					stubName := fmt.Sprintf("_lib_%s_%d", call.FuncName, call.CallIndex)

					// 1. Static-column libs: use declared columns
					if len(call.Lib.Columns) > 0 {
						var colDefs []string
						for _, c := range call.Lib.Columns {
							colDefs = append(colDefs, fmt.Sprintf("%s %s", duckdb.QuoteIdentifier(c.Name), c.Type))
						}
						_ = r.sess.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", stubName)) // IF EXISTS makes non-existence OK; real catalog errors break next CREATE more visibly
						if err := r.sess.Exec(fmt.Sprintf("CREATE TEMP TABLE %s (%s)", stubName, strings.Join(colDefs, ", "))); err != nil {
							return nil, fmt.Errorf("create empty lib stub %s: %w", stubName, err)
						}
						call.TempTable = stubName
						hasInferredStubs = true
						continue
					}

					// 2. Dynamic-column libs: build the stub from the model
					//    SQL's column shape — typed from explicit casts in
					//    the SELECT projection (validated upstream), plus
					//    input-only refs (JOIN ON, WHERE, etc) as VARCHAR.
					//    SQL is the schema authority for lib-backed models;
					//    target-type lookups and VARCHAR-on-projection
					//    fallbacks are no longer needed because every
					//    projected column is already cast.
					alias := ""
					if call.ASTNode != nil {
						alias = call.ASTNode.Alias()
					}
					if shape := extractColShapeForLib(parsedAST, alias, singleLibSource); len(shape) > 0 {
						var colDefs []string
						for _, c := range shape {
							colDefs = append(colDefs, fmt.Sprintf("%s %s", duckdb.QuoteIdentifier(c.name), c.sqlType))
						}
						_ = r.sess.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", stubName)) // IF EXISTS makes non-existence OK; real catalog errors break next CREATE more visibly
						if err := r.sess.Exec(fmt.Sprintf("CREATE TEMP TABLE %s (%s)", stubName, strings.Join(colDefs, ", "))); err != nil {
							return nil, fmt.Errorf("create empty lib stub %s: %w", stubName, err)
						}
						call.TempTable = stubName
						hasInferredStubs = true
						continue
					}

					// 3. SQL didn't reference any columns belonging to this
					//    lib (e.g. `SELECT *` against a single lib source).
					//    Fall back to cloning the existing target.
					targetExists, existsErr := r.tableExistsCheck(model.Target)
					if existsErr != nil {
						return nil, fmt.Errorf("check target exists for empty-lib fallback: %w", existsErr)
					}
					if targetExists {
						_ = r.sess.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", stubName)) // IF EXISTS makes non-existence OK; real catalog errors break next CREATE more visibly
						if err := r.sess.Exec(fmt.Sprintf("CREATE TEMP TABLE %s AS SELECT * FROM %s WHERE false", stubName, model.Target)); err != nil {
							return nil, fmt.Errorf("create empty lib stub from target %s: %w", model.Target, err)
						}
						call.TempTable = stubName
						hasInferredStubs = true
						continue
					}

					// 4. No SQL shape, no target — first run, skip.
					ackLibClaims()
					result.RowsAffected = 0
					result.RunType = "skip"
					result.Warnings = append(result.Warnings, "lib returned 0 rows on first run — target table not created (will be created when data arrives)")
					result.Duration = time.Since(start)
					return result, nil
				}

				// Rewrite SQL: replace lib function calls with temp table references
				// Check if AST-based rewrite is possible. API dict libs use
				// macros which DuckDB expands and reconstructs -- AST rewrite
				// produces the original SQL, not the rewritten version. Use
				// string-based rewrite for those.
				hasASTNodes := false
				for _, c := range libCalls {
					if c.ASTNode != nil && c.Lib.APIConfig == nil {
						hasASTNodes = true
						break
					}
				}
				if hasASTNodes {
					// AST-based rewrite (legacy TABLE libs)
					rewriteLibCalls(parsedAST, libCalls)
					modified, serErr := parsedAST.Serialize()
					if serErr != nil {
						return nil, fmt.Errorf("serialize rewritten AST: %w", serErr)
					}
					rewrittenSQL, desErr := r.deserializeAST(modified)
					if desErr != nil {
						return nil, fmt.Errorf("deserialize rewritten AST: %w", desErr)
					}
					state.Promote(rewrittenSQL)
				}

				// String-based rewrite for API dict libs (macro-expanded, not
				// visible in AST). Also handles the case where AST-based rewrite
				// handled some calls but API dict calls remain unrewritten.
				{
					var stringCalls []LibCall
					for _, c := range libCalls {
						if c.TempTable != "" && (c.ASTNode == nil || c.Lib.APIConfig != nil) {
							stringCalls = append(stringCalls, c)
						}
					}
					if len(stringCalls) > 0 {
						state.Promote(rewriteLibCallsByString(state.Current(), stringCalls))
					}
				}

				// Re-parse AST for lineage/CDC (now references temp tables, not lib functions)
				var rewriteASTErr error
				astJSON, rewriteASTErr = r.getAST(state.Current())
				if rewriteASTErr != nil {
					// Post-rewrite AST drives lineage + CDC. A failure
					// here silently strips column lineage from the
					// downstream impact analysis — surface it.
					result.Warnings = append(result.Warnings, fmt.Sprintf("post-rewrite AST serialize failed (lineage/CDC degraded): %v", rewriteASTErr))
				}
			}
	}  // end if r.libRegistry != nil
	// state.Rewritten() is now the post-lib-rewrite SQL — used as retry target
	// when CDC must be reverted.

	tables, _ := lineage.ExtractTablesFromAST(astJSON)
	colLineage, _ := lineage.ExtractFromAST(astJSON)

	// Determine which tables need CDC:
	// 1. Primary table (first FROM) always gets CDC
	// 2. JOIN tables with AGGREGATION get CDC (new rows affect aggregates)
	// 3. JOIN tables with only IDENTITY are dimension lookups (full scan)
	var cdcTables []string
	var allTableNames []string
	hasForeignSource := false
	aggregationTables := lineage.GetCDCTables(colLineage)

	// Smart CDC: auto-detect tables, apply CDC to fact tables and aggregated joins
	// Note: SCD2 is excluded because it needs full source data for proper change detection
	// tracked excluded: it does its own hash-based change detection and needs full source data
	isIncremental := model.Kind == "append" || model.Kind == "merge"

	// Collect lib-call temp table names so CDC can skip them.
	libTempTables := make(map[string]bool)
	for _, c := range libCalls {
		if c.TempTable != "" {
			libTempTables[c.TempTable] = true
		}
	}

	for _, t := range tables {
		allTableNames = append(allTableNames, t.Table)
		// Skip lib-call rewrite temp tables — they are internal temp tables,
		// not DuckLake tables, and cannot be used with table_changes().
		// Uses the exact set of rewritten table names, not a prefix match,
		// so legitimate tables like raw.tmp_orders are not affected.
		if libTempTables[t.Table] {
			continue
		}
		// Note a source living in another attached catalog (e.g. a Postgres
		// source ATTACHed alongside the lake). table_changes() is a DuckLake
		// function, so it cannot work on them: the gate would qualify the
		// name as <lakeAlias>.<schema>.<table>, fail to resolve it, and fall
		// back to a full query anyway — after emitting two warnings per model
		// per run.
		//
		// Unlike libTempTables this cannot be a per-table skip. Dropping just
		// the foreign table would leave CDC running on the model's remaining
		// lake sources, and the gate would then answer "nothing changed"
		// whenever only the foreign source moved — the model would read
		// nothing and silently lose the change. Not knowing whether a source
		// changed means the whole model has to run a full query, which is
		// what the failing gate used to achieve by accident.
		//
		// Only incremental kinds consult CDC, so only they pay for the lookup.
		// The skip is traced rather than silent: it removes the warnings that
		// used to signal the fallback, so something has to take their place.
		if isIncremental {
			inLake, lakeErr := r.tableInLakeCatalog(t.Table)
			if lakeErr != nil {
				// Unclassifiable is the same answer as foreign: the question
				// CDC asks is "can I observe this source's changes?", and an
				// unanswered question must not be read as yes. Letting the
				// table into cdcTables would put it under the gate, which on
				// an unchanged-snapshot run rewrites it to an empty delta.
				result.Warnings = append(result.Warnings,
					fmt.Sprintf("cdc catalog check for %s, running a full query: %v", t.Table, lakeErr))
				hasForeignSource = true
			} else if !inLake {
				r.trace(result, "cdc.skip_foreign_catalog:"+t.Table, time.Now(), "skip")
				hasForeignSource = true
			}
		}
		if t.IsFirstFrom {
			// Primary table always gets CDC
			cdcTables = append(cdcTables, t.Table)
		} else if aggregationTables[t.Table] || aggregationTables[stripLakeAlias(t.Table, r.sess.CatalogAlias(), r.sess.ProdAlias())] {
			// JOIN table with aggregations gets CDC.
			//
			// Two lookups because the extractors disagree on name shape:
			// ExtractTablesFromAST keeps catalog_name, the column lineage
			// behind aggregationTables drops it. Measured: a join written
			// `lake.raw.events` yields cdcTables=[raw.base] where the same
			// join written `raw.events` yields [raw.base raw.events], so the
			// aggregated source silently loses its change detection.
			cdcTables = append(cdcTables, t.Table)
		}
		// JOIN tables without aggregations get full scan (dimension lookups)
	}
	if hasForeignSource {
		// One unobservable source disables CDC for the model, not just for
		// itself. See the reasoning at the skip above.
		cdcTables = nil
		// Abandoning CDC on an incremental kind means the model runs its query
		// without a delta, and an unbounded query has to materialize as a
		// rebuild rather than an append — otherwise every run appends the
		// whole source again and the target grows without bound.
		//
		// `@incremental` is the exception, and the one case that matters here:
		// its cursor bounds the read on its own, so the query is not full,
		// appending is correct, and forcing a rebuild would reset the cursor
		// and re-read the entire foreign source on every run — defeating the
		// mechanism the docs recommend for exactly these sources.
		if err := escalateToRebuild(isIncremental); err != nil {
			result.Errors = append(result.Errors, err.Error())
			result.Duration = time.Since(start)
			return result, err
		}
	}
	tableExtractTime := time.Since(stepStart)

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

		// Sub-step: Check for CDC changes using table_changes() gate.
		// Instead of global snapshot comparison, check each source table
		// individually. This skips when unrelated tables created new snapshots.
		stepStart = time.Now()
		hasChanges := false
		insertOnly := true
		// An aggregate is only correct over a delta when the affected group is
		// entirely new, which cannot be known cheaply — so a changed aggregated
		// source disqualifies delta CDC for the model. The two facts needed for
		// that decision are both already computed: which sources are aggregated
		// (AST column lineage) and which ones changed (this gate).
		changedAggregated := false
		// True only when the per-table loop below ran to completion. Every
		// bail-out sets hasChanges without classifying anything, and an
		// unclassified aggregate must not be assumed unchanged.
		gateClassified := false
		isAggregatedSource := func(name string) bool {
			return aggregationTables[name] ||
				aggregationTables[stripLakeAlias(name, r.sess.CatalogAlias(), r.sess.ProdAlias())]
		}
		snapshotID, snapshotErr := r.sess.GetDagStartSnapshot()
		if snapshotErr != nil {
			result.Warnings = append(result.Warnings, fmt.Sprintf("get snapshot warning: %v", snapshotErr))
			// Cannot determine start snapshot — skip CDC gate, assume changes exist
			hasChanges = true
			insertOnly = false
		}
		r.trace(result, "cdc.get_snapshot", stepStart, "ok")

		stepStart = time.Now()
		currSnap, currSnapErr := r.sess.GetCurrentSnapshot()
		if currSnapErr != nil {
			// Cannot determine current snapshot — assume changes exist, use full EXCEPT
			result.Warnings = append(result.Warnings, fmt.Sprintf("get current snapshot: %v", currSnapErr))
			hasChanges = true
			insertOnly = false
		} else if snapshotErr != nil {
			// Already handled above — skip table-level checks
		} else if snapshotID < currSnap {
			gateClassified = true
			for _, t := range cdcTables {
				catalog, schema, table, ok := splitCDCTableName(t, r.sess.CatalogAlias())
				if !ok {
					hasChanges = true
					insertOnly = false
					gateClassified = false
					break
				}
				// Set search_path to include the source schema for table_changes()
				// (which takes bare table name). Restore default search_path after.
				// A failure on either set or restore means subsequent
				// table_changes() calls or model SQL would run against
				// the wrong scope — propagate so the iteration aborts
				// rather than silently corrupting the gate.
				//
				// The catalog comes from the name when the SQL wrote one, so a
				// source read as lake.raw.src is gated against the same catalog
				// applySmartCDC will rewrite it to. Hardcoding the active alias
				// here would gate the sandbox fork while the rewrite read prod.
				if err := r.sess.Exec(fmt.Sprintf("SET search_path = '%s.%s,%s'",
					escapeSQL(catalog), escapeSQL(schema), escapeSQL(r.sess.DefaultSearchPath()))); err != nil {
					result.Warnings = append(result.Warnings, fmt.Sprintf("set search_path for table_changes gate (%s): %v", t, err))
					hasChanges = true
					insertOnly = false
					gateClassified = false
					break
				}
				cnt, err := r.sess.TableHasChanges(table, snapshotID+1, currSnap)
				if err != nil {
					result.Warnings = append(result.Warnings, fmt.Sprintf("table_changes gate failed for %s: %v", t, err))
					hasChanges = true
					insertOnly = false
					gateClassified = false
					if restoreErr := r.sess.Exec(fmt.Sprintf("SET search_path = '%s'", escapeSQL(r.sess.DefaultSearchPath()))); restoreErr != nil {
						result.Warnings = append(result.Warnings, fmt.Sprintf("restore search_path after gate failure: %v", restoreErr))
					}
					break
				}
				if cnt > 0 {
					hasChanges = true
					if isAggregatedSource(t) {
						changedAggregated = true
					}
					isInsertOnly, ioErr := r.sess.TableChangesInsertOnly(table, snapshotID+1, currSnap)
					if ioErr != nil || !isInsertOnly {
						insertOnly = false
					}
				}
				if err := r.sess.Exec(fmt.Sprintf("SET search_path = '%s'", escapeSQL(r.sess.DefaultSearchPath()))); err != nil {
					result.Warnings = append(result.Warnings, fmt.Sprintf("restore search_path after CDC gate (%s): %v", t, err))
					// Don't break — the gate decision is already made;
					// surface the warning so an operator notices the
					// scope leak.
				}
			}
		}
		r.trace(result, "cdc.table_changes_gate", stepStart, "ok")

		// When the gate could not classify each source — an unreadable
		// snapshot, a name it cannot split, a failed table_changes() — it
		// falls back to "assume changes exist". For an aggregated source that
		// assumption has to carry through to the delta decision as well:
		// substituting a delta for an aggregate whose change status is unknown
		// is the same corruption the classified path avoids.
		if hasChanges && !gateClassified {
			for _, t := range cdcTables {
				if isAggregatedSource(t) {
					changedAggregated = true
					break
				}
			}
		}

		// Every path that abandons CDC falls back to the full query, and a full
		// query on an incremental kind has to materialize as a rebuild — an
		// append would put the whole source on top of the rows already there.
		// Shared so a new fallback cannot quietly skip the escalation.
		revertToFullQuery := func() error {
			state.RevertToRewritten()
			return escalateToRebuild(isIncremental)
		}

		if changedAggregated && model.Kind == "merge" {
			// Leave state.Current() as the full query. Substituting a delta
			// here would aggregate only the new rows and then merge that
			// partial total over a correct one — corrupting the target rather
			// than merely leaving it stale.
			//
			// Only `merge` is affected. It replaces a row by unique_key, so a
			// partial total lands on top of a correct one. An `append` model
			// overwrites nothing: accumulating one aggregated row per run is
			// what that kind is for, and forcing a full query there would
			// re-append every unchanged group as well. No rebuild either —
			// a merge converges on its own, and escalating would reset the
			// incremental cursor for no gain.
			r.trace(result, "cdc.skip_changed_aggregate", stepStart, "skip")
		} else if hasChanges {
			// Check if upstream schema changed — CDC EXCEPT requires matching column counts.
			stepStart = time.Now()
			schemaChanged := r.cdcSchemaChanged(cdcTables, snapshotID)
			r.trace(result, "cdc.schema_check", stepStart, "ok")

			if schemaChanged {
				result.Warnings = append(result.Warnings, "upstream schema changed, skipping CDC")
				if err := revertToFullQuery(); err != nil {
					result.Errors = append(result.Errors, err.Error())
					result.Duration = time.Since(start)
					return result, err
				}
			} else if insertOnly {
				// Phase 2: all source changes are inserts — apply CDC with
				// the same EXCEPT approach but log the optimization.
				cdcSQL, cdcErr := r.applySmartCDC(astJSON, model.Kind, cdcTables, snapshotID)
				if cdcErr != nil {
					result.Warnings = append(result.Warnings, fmt.Sprintf("CDC failed, using full query: %v", cdcErr))
					if err := revertToFullQuery(); err != nil {
						result.Errors = append(result.Errors, err.Error())
						result.Duration = time.Since(start)
						return result, err
					}
				} else {
					state.SetCurrent(cdcSQL)
					r.trace(result, "cdc.applied_insert_only:"+strings.Join(cdcTables, ","), stepStart, "ok")
				}
			} else {
				// Mixed changes (updates/deletes) — full EXCEPT
				cdcSQL, cdcErr := r.applySmartCDC(astJSON, model.Kind, cdcTables, snapshotID)
				if cdcErr != nil {
					result.Warnings = append(result.Warnings, fmt.Sprintf("CDC failed, using full query: %v", cdcErr))
					if err := revertToFullQuery(); err != nil {
						result.Errors = append(result.Errors, err.Error())
						result.Duration = time.Since(start)
						return result, err
					}
				} else {
					state.SetCurrent(cdcSQL)
					r.trace(result, "cdc.applied:"+strings.Join(cdcTables, ","), stepStart, "ok")
				}
			}
		} else {
			// No upstream changes — sources unchanged since last run
			emptySQL, emptyErr := r.applyEmptySmartCDC(astJSON, cdcTables)
			if emptyErr != nil {
				result.Warnings = append(result.Warnings, fmt.Sprintf("CDC empty failed, using full query: %v", emptyErr))
				if err := revertToFullQuery(); err != nil {
					result.Errors = append(result.Errors, err.Error())
					result.Duration = time.Since(start)
					return result, err
				}
			} else {
				state.SetCurrent(emptySQL)
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
			// A table in another ATTACHed catalog is not in sandbox and not in
			// prod either. Qualifying it would rewrite it to <prodAlias>.<name>,
			// which does not exist, and the CREATE TEMP would fail into the
			// revert path. Leave it alone; DuckDB resolves it via its own
			// catalog.
			//
			// This is a behaviour change, not a no-op. cdcHandled shielded such
			// tables only under `!needsBackfill && isIncremental &&
			// len(cdcTables) > 0`, so table, scd2 and tracked kinds — and every
			// backfill run — did reach the qualifier and were rewritten to a
			// catalog that does not hold them.
			//
			// A lookup failure is treated as "in the lake" here, which is the
			// opposite of the CDC loop above, because the two ask different
			// questions. CDC asks "can I observe this source's changes?" and
			// an unanswered question must mean no, so it degrades to a full
			// query. Qualification asks "should this name be pointed at
			// prod?", and for everything except a foreign catalog the answer
			// is yes — declining on an unanswered question would strip the
			// qualification every sandbox read of a prod table depends on.
			// Aborting instead would let one transient duckdb_databases()
			// error kill the run outright.
			inLake, lakeErr := r.tableInLakeCatalog(t)
			if lakeErr != nil {
				result.Warnings = append(result.Warnings,
					fmt.Sprintf("catalog check for %s during sandbox qualification: %v", t, lakeErr))
				inLake = true
			}
			if !inLake {
				continue
			}
			exists, existsErr := r.tableExistsInCatalog(t, r.sess.CatalogAlias())
			if existsErr != nil {
				return nil, existsErr
			}
			if !exists {
				tablesToQualify[strings.ToLower(t)] = true
			}
		}
		if len(tablesToQualify) > 0 {
			// Re-parse the current SQL AST for qualification
			qualified := false
			if execAST, qualErr := r.getAST(state.Current()); qualErr == nil {
				if root, parseErr := parseASTJSON(execAST); parseErr == nil {
					qualifyTablesInAST(root, tablesToQualify, r.sess.ProdAlias())
					if modified, marshalErr := json.Marshal(root); marshalErr == nil {
						if deserialized, deserErr := r.deserializeAST(string(modified)); deserErr == nil {
							state.SetCurrent(deserialized)
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
	state.SetCurrent(applyColumnMasking(state.Current(), model))

	// Create temp model
	state.SetTmpTable("tmp_" + sanitizeTableName(model.Target))
	tmpTable := state.TmpTable()
	createSQL := fmt.Sprintf("CREATE TEMP TABLE %s AS %s", tmpTable, state.Current())
	stepStart = time.Now()
	if err := r.sess.Exec(createSQL); err != nil {
		// CDC EXCEPT can fail on edge cases (e.g. DuckDB unicode stats after TRUNCATE+INSERT).
		// Fall back to full query without CDC.
		if isIncremental && !state.CurrentMatchesRewritten() {
			r.trace(result, "create_temp", stepStart, "retry")
			result.Warnings = append(result.Warnings, fmt.Sprintf("CDC query failed (%v), retrying with full query", err))
			state.RevertToRewritten()
			// Rewritten predates applyColumnMasking, so reverting to it drops
			// the masking. Without re-applying it the retry materializes raw
			// values for every @column-masked column. The mismatch this branch
			// tests for is also satisfied by masking alone, so a masked model
			// reaches here on any create_temp failure, not only a CDC one.
			state.SetCurrent(applyColumnMasking(state.Current(), model))
			if resetErr := escalateToRebuild(isIncremental); resetErr != nil {
				result.Errors = append(result.Errors, resetErr.Error())
				result.Duration = time.Since(start)
				return result, resetErr
			}
			createSQL = fmt.Sprintf("CREATE TEMP TABLE %s AS %s", tmpTable, state.Current())
			stepStart = time.Now()
			if retryErr := r.sess.Exec(createSQL); retryErr != nil {
				r.trace(result, "create_temp", stepStart, "error")
				return nil, fmt.Errorf("create temp table: %w", retryErr)
			}
		} else if hasInferredStubs {
			// Inferred VARCHAR stubs let the SQL rewrite work, but the
			// full SQL may fail on LATERAL unnest, alias-colliding casts,
			// etc. Use DESCRIBE to get the correct output schema from the
			// rewritten SQL, then create an empty temp table with that
			// schema. DESCRIBE plans but does not execute the query.
			r.trace(result, "create_temp", stepStart, "retry_describe")
			descRows, descErr := r.sess.QueryRows(fmt.Sprintf("SELECT column_name || ' ' || column_type FROM (DESCRIBE %s)", state.Current()))
			if descErr == nil && len(descRows) > 0 {
				_ = r.sess.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", tmpTable)) // IF EXISTS makes non-existence OK; real catalog errors break next CREATE more visibly
				descCreateSQL := fmt.Sprintf("CREATE TEMP TABLE %s (%s)", tmpTable, strings.Join(descRows, ", "))
				if createErr := r.sess.Exec(descCreateSQL); createErr != nil {
					return nil, fmt.Errorf("create temp table from DESCRIBE: %w", createErr)
				}
			} else {
				// DESCRIBE also failed — fall back to target clone
				targetExists, existsErr := r.tableExistsCheck(model.Target)
				if existsErr != nil {
					return nil, fmt.Errorf("check target exists after DESCRIBE failure: %w", existsErr)
				}
				if !targetExists {
					return nil, fmt.Errorf("create temp table: %w", err)
				}
				_ = r.sess.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", tmpTable)) // IF EXISTS makes non-existence OK; real catalog errors break next CREATE more visibly
				cloneSQL := fmt.Sprintf("CREATE TEMP TABLE %s AS SELECT * FROM %s WHERE false", tmpTable, model.Target)
				if cloneErr := r.sess.Exec(cloneSQL); cloneErr != nil {
					return nil, fmt.Errorf("create temp table (fallback clone): %w", cloneErr)
				}
			}
		} else {
			r.trace(result, "create_temp", stepStart, "error")
			return nil, fmt.Errorf("create temp table: %w", err)
		}
	}
	r.trace(result, "create_temp", stepStart, "ok")

	// v0.12.0+: with the catalog-fork sandbox, the target always exists in
	// the sandbox catalog (inherited from prod). The old empty-sandbox skip
	// path is no longer reachable, so we drop it. Models that genuinely have
	// nothing to write (0 incremental rows) will still execute the materialize
	// step but it becomes a no-op INSERT.

	// Schema evolution check — shared with script.go's runScript so the
	// two execution paths can't drift on this critical correctness logic.
	// A run whose libs all smart-skipped has no data to rebuild from, so a
	// rebuild would empty the target instead of refreshing it. materialize
	// already preserves target rows for that case; the rebuild decision has to
	// agree, or it truncates before materialize gets the chance.
	//
	// Applied once here rather than at each escalation. needsBackfill can be
	// set by the run-type decision itself and by three separate mid-run
	// escalations — a foreign source, an upstream schema change, a failed CDC
	// query — and guarding only one of them leaves the others able to empty
	// the target. This sits after all of them and before the only two
	// consumers, detectSchemaEvolution and materialize.
	if len(libCalls) > 0 && allLibsReturnedNoChange(libCalls) && needsBackfill {
		// The signal alone is not enough. A model can smart-skip its lib and
		// still have real rows from its other sources, and cancelling the
		// rebuild there would append a full result on top of the target. Only
		// an empty result has nothing to rebuild from, which is the case that
		// would otherwise wipe the target.
		//
		// An unreadable count is treated as empty: preserving rows costs a
		// missed refresh, while rebuilding from an unknown result risks
		// emptying the target outright.
		empty := true
		if cnt, err := r.sess.QueryValue(fmt.Sprintf("SELECT count(*) FROM %s", tmpTable)); err != nil {
			result.Warnings = append(result.Warnings,
				fmt.Sprintf("count temp rows for smart-skip check: %v", err))
		} else {
			empty = cnt == "0"
		}
		if empty {
			result.Warnings = append(result.Warnings,
				"all libs reported no change and the result is empty, keeping existing rows instead of rebuilding")
			needsBackfill = false
		}
	}
	schemaChange, needsBackfill = r.detectSchemaEvolution(model, tmpTable, needsBackfill, decision.RunType == "backfill", result)

	// Run constraints (batched - single query for all constraints)
	stepStart = time.Now()
	if len(model.Constraints) > 0 {
		batchSQL, parseErrors := validation.DispatchConstraintsBatch(model.Constraints, tmpTable)

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

	// If constraints failed, abort (defer nacks claims + closes stores)
	if len(result.Errors) > 0 {
		r.cleanup(tmpTable)
		result.Duration = time.Since(start)
		return result, fmt.Errorf("constraint validation failed")
	}

	// Render audits as a transactional pre-commit check. The result is
	// a SELECT error(...) wrapper that aborts the materialize transaction
	// if any audit fails — so failing audits roll back the schema ALTER,
	// the data write, and the commit metadata together.
	//
	// Parse errors abort BEFORE materialize: there's no point trying to
	// materialize a model whose audit directives are syntactically broken.
	stepStart = time.Now()
	auditSQL, auditParseErrors := r.buildAuditSQL(model)
	r.trace(result, "audits.render", stepStart, "ok")
	if len(auditParseErrors) > 0 {
		for _, e := range auditParseErrors {
			result.Errors = append(result.Errors, e.Error())
		}
		// defer nacks claims + closes stores
		r.cleanup(tmpTable)
		result.Duration = time.Since(start)
		return result, fmt.Errorf("audit parse errors")
	}

	// Build ack SQL for lib-call state-store claims — included in the materialize
	// transaction so the ack record is atomic with the data commit (same as script.go).
	var libExtraPreSQL []string
	for i := range libCalls {
		sr := libCalls[i].ScriptResult
		if sr == nil || len(sr.ClaimIDs) == 0 {
			continue
		}
		// EnsureAckTable failure is fatal: see corresponding comment in
		// internal/execute/script.go. Without the ack-marker the next
		// run can't tell that these rows were already committed and will
		// replay them.
		if ackErr := script.EnsureAckTable(r.sess); ackErr != nil {
			r.cleanup(tmpTable)
			result.Duration = time.Since(start)
			return result, fmt.Errorf("ensure ack table: %w", ackErr)
		}
		for _, claimID := range sr.ClaimIDs {
			libExtraPreSQL = append(libExtraPreSQL, script.AckSQL(claimID, model.Target, sr.RowCount))
		}
	}

	// Capture pre-commit snapshot for sink delta (table_changes needs the range).
	var preCommitSnapshot int64
	if model.Push != "" {
		var snapErr error
		preCommitSnapshot, snapErr = r.sess.GetCurrentSnapshot()
		if snapErr != nil {
			r.cleanup(tmpTable)
			result.Duration = time.Since(start)
			return result, fmt.Errorf("get pre-commit snapshot for sink delta: %w", snapErr)
		}
	}

	// Execute based on kind (includes audits + commit metadata in same transaction)
	stepStart = time.Now()
	rowsAffected, err := r.materialize(model, tmpTable, needsBackfill, schemaChange, auditSQL, sqlHash, result.RunType, result, start, trackedOpts, libExtraPreSQL...)
	if err != nil {
		r.trace(result, "materialize", stepStart, "error")
		// A failed audit raises error() inside the BEGIN/COMMIT, which
		// aborts the transaction — but DuckDB leaves the session in an
		// "aborted transaction" state. Without an explicit ROLLBACK the
		// next model in the same batch will fail with "cannot start a
		// transaction within a transaction". Best-effort: ignore any
		// error from the ROLLBACK itself (the session might already be
		// clean if the error came from a non-transactional path).
		_ = r.sess.Exec("ROLLBACK") // session is in error state from upstream Exec; next Exec surfaces a clearer error

		// Lib-call state-store claim handling on materialize failure
		// (same logic as script.go: audit fail → ack, other fail → nack)
		if strings.Contains(err.Error(), "audit failed") {
			// Audit failure: ack claims (data was valid, just reverted).
			// Only set flag if all acks succeed — otherwise defer nacks.
			allAcked := true
			for i := range libCalls {
				sr := libCalls[i].ScriptResult
				if sr != nil && len(sr.ClaimIDs) > 0 {
					if ackErr := sr.AckClaims(); ackErr != nil {
						result.Warnings = append(result.Warnings, fmt.Sprintf("ack after audit failure: %v", ackErr))
						allAcked = false
						break
					}
				}
			}
			if allAcked {
				libClaimsAcked = true
			}
		}
		// Other failures: defer will nack (libClaimsAcked stays false)

		result.Errors = append(result.Errors, err.Error())
		r.cleanup(tmpTable)
		result.Duration = time.Since(start)
		return result, fmt.Errorf("materialize: %w", err)
	}
	r.trace(result, "materialize", stepStart, "ok")
	result.RowsAffected = rowsAffected

	// Success — ack lib-call state-store claims and delete ack records.
	// Same lifecycle as script.go. Stores are closed by defer.
	ackLibClaims()

	// Run warnings (soft validations, log only)
	stepStart = time.Now()
	r.runWarnings(model, model.Target, result)
	if len(model.Warnings) > 0 {
		r.trace(result, "warnings", stepStart, "ok")
	}

	// Outbound sync: compute delta AFTER commit, then push.
	// Delta is a list of SyncEvents (rowid + operation + snapshot).
	// Row data is read from DuckLake at push time, not stored in state-store.
	if model.Push != "" {
		// Get post-commit snapshot. All sink kinds need this:
		// All sink-enabled kinds need table_changes() range
		// A failure here is fatal for the push step — postCommitSnapshot=0
		// would make createPushDelta interpret as "no delta" and silently
		// omit newly-committed rows from outbound sync. Abort with an
		// error rather than letting the push silently push zero rows.
		var postCommitSnapshot int64
		if err := r.sess.RefreshSnapshot(); err != nil {
			result.Errors = append(result.Errors, fmt.Sprintf("refresh snapshot for push delta: %v", err))
			result.Duration = time.Since(start)
			return result, fmt.Errorf("refresh snapshot for %s push: %w", model.Target, err)
		}
		var snapErr error
		postCommitSnapshot, snapErr = r.sess.GetCurrentSnapshot()
		if snapErr != nil {
			result.Errors = append(result.Errors, fmt.Sprintf("read current snapshot for push delta: %v", snapErr))
			result.Duration = time.Since(start)
			return result, fmt.Errorf("get current snapshot for %s push: %w", model.Target, snapErr)
		}

		// Set search_path so table_changes() can resolve bare table name
		schema, _ := splitSchemaTable(model.Target)
		if schema != "" {
			_ = r.sess.Exec(fmt.Sprintf("SET search_path = '%s.%s,%s'", // search_path is restored on a later set; failure here means subsequent queries hit the previous scope (pre-existing pattern)
				escapeSQL(r.sess.CatalogAlias()), escapeSQL(schema), escapeSQL(r.sess.DefaultSearchPath())))
		}

		stepStart = time.Now()
		sinkEvents, deltaErr := r.createPushDelta(model, tmpTable, preCommitSnapshot, postCommitSnapshot)

		if schema != "" {
			_ = r.sess.Exec(fmt.Sprintf("SET search_path = '%s'", escapeSQL(r.sess.DefaultSearchPath()))) // search_path is restored on a later set; failure here means subsequent queries hit the previous scope (pre-existing pattern)
		}

		if deltaErr != nil {
			r.trace(result, "sink.delta", stepStart, "error")
			result.Warnings = append(result.Warnings, fmt.Sprintf("sink: delta failed: %v", deltaErr))
		} else if len(sinkEvents) > 0 {
			r.trace(result, "sink.delta", stepStart, "ok")
		}

		// Run sink: processes new delta AND existing state-store backlog.
		stepStart = time.Now()
		if err := r.executePush(ctx, model, result, sinkEvents, postCommitSnapshot); err != nil {
			r.trace(result, "sink", stepStart, "error")
			result.Warnings = append(result.Warnings, fmt.Sprintf("sink: %v", err))
		} else {
			r.trace(result, "sink", stepStart, "ok")
		}
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

// buildAuditSQL renders the model's audits as a transactional pre-commit
// check. The returned SQL, when executed inside a BEGIN/COMMIT, raises a
// DuckDB error() (and aborts the surrounding transaction) the moment any
// audit query produces an error row. Returns "" when the model has no
// audits.
//
// Parse errors are returned to the caller so they can abort BEFORE
// materialize runs — there's no value in trying to materialize a model
// whose audit directives are syntactically broken.
func (r *Runner) buildAuditSQL(model *parser.Model) (string, []error) {
	if len(model.Audits) == 0 {
		return "", nil
	}
	return validation.DispatchAuditsTransactional(model.Audits, model.Target)
}

// cdcSchemaChanged checks if any CDC table's schema differs between current and snapshot.
// CDC uses EXCEPT which requires matching column names and types on both sides.
// Returns true if any table has different columns at the snapshot version.
func (r *Runner) cdcSchemaChanged(cdcTables []string, snapshotID int64) bool {
	for _, table := range cdcTables {
		qt := quoteTableName(table)
		// Compare column names and types: current vs snapshot version.
		// A renamed or retyped column with the same count would break EXCEPT.
		query := fmt.Sprintf(
			"SELECT (SELECT list(column_name||':'||column_type ORDER BY column_name) FROM (DESCRIBE SELECT * FROM %s)) != "+
				"(SELECT list(column_name||':'||column_type ORDER BY column_name) FROM (DESCRIBE SELECT * FROM %s AT (VERSION => %d)))",
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
//
// Returns (false, nil) for unqualified names — they resolve via search_path
// and don't need explicit catalog qualification.
//
// Returns the underlying query error rather than collapsing it to "not exists".
// The earlier behaviour silently treated transient information_schema failures
// as "table missing", which fed wrong inputs into schema-evolution and skip
// logic (e.g. forcing a backfill on a table that actually had data).
func (r *Runner) tableExistsInCatalog(table, catalog string) (bool, error) {
	parts := strings.SplitN(table, ".", 2)
	if len(parts) != 2 {
		return false, nil // Unqualified names resolve via search path, no fix needed
	}
	schema, tbl := parts[0], parts[1]
	q := fmt.Sprintf(
		"SELECT COUNT(*) FROM information_schema.tables WHERE table_catalog = '%s' AND table_schema = '%s' AND table_name = '%s'",
		escapeSQL(catalog), escapeSQL(schema), escapeSQL(tbl))
	val, err := r.sess.QueryValue(q)
	if err != nil {
		return false, fmt.Errorf("check table %s in catalog %s: %w", table, catalog, err)
	}
	return val != "0", nil
}

// splitCDCTableName breaks a cdcTables entry into the catalog, schema and table
// the CDC gate needs. Names carry a catalog only when the SQL wrote one, so the
// active catalog is the default.
//
// ok is false for an unqualified name: table_changes() needs a schema to scope
// against, and guessing one would gate the wrong table.
func splitCDCTableName(name, activeCatalog string) (catalog, schema, table string, ok bool) {
	parts := strings.Split(name, ".")
	switch len(parts) {
	case 2:
		return activeCatalog, parts[0], parts[1], true
	case 3:
		return parts[0], parts[1], parts[2], true
	default:
		return "", "", "", false
	}
}

// tableInLakeCatalog reports whether a source table lives in the lake catalog
// — or, in sandbox, in the prod catalog the fork inherits from.
//
// Used to keep tables in another ATTACHed catalog (e.g. `ATTACH 'postgresql://…'
// AS crm`) out of two lake-specific code paths: the CDC gate, whose
// table_changes() cannot read them, and the sandbox qualifier, which would
// rewrite them to <prodAlias>.<name> and break the query.
//
// Name shapes, per internal/lineage/extractor.go which builds TableRef.Table
// from whatever the SQL wrote:
//
//	orders                 unqualified — resolves via search_path
//	raw.orders             schema.table, or catalog.table for an attached database
//	lake.raw.orders        catalog.schema.table
//
// The three-part case is answered from the name alone. The two-part case is
// genuinely ambiguous: DuckDB's parser records `crm.decision` as schema `crm`
// (catalog_name is empty) and only the binder resolves `crm` against attached
// catalogs. So we ask the question the binder would — is the leading segment an
// attached catalog? — rather than probing for the table, which would also
// answer "foreign" for a lake table that simply does not exist yet.
//
// Unqualified names answer true: not classifiable, and answering false would
// silently strip CDC from every model that writes a bare table name.
// stripLakeAlias removes a leading lake (or prod) catalog segment, giving the
// schema.table shape the column-lineage extractor keys on.
//
// Only a lake alias is stripped. A foreign catalog's leading segment stays in
// place so that `crm.raw.events` cannot be folded onto the lake's own
// `raw.events` — the collision that makes general name-flattening unsafe.
func stripLakeAlias(name, catalogAlias, prodAlias string) string {
	first, rest, found := strings.Cut(name, ".")
	if !found || !isLakeAlias(first, catalogAlias, prodAlias) {
		return name
	}
	return rest
}

// resetIncrementalForBackfill re-points the incremental cursor at its initial
// value after a mid-run decision to rebuild instead of append.
//
// applyIncrementalVars runs early, off the run-type decision. Every later
// escalation to backfill leaves the model's SQL still filtered by
// `getvariable('incr_last_value')` while materialize switches to
// TRUNCATE + INSERT, so the rebuild sees only the rows after the cursor and
// destroys every row before it. Measured on an @incremental append over an
// ATTACHed source: a target holding 1 and 2 came back holding only 3.
//
// The variable is read when the query executes, not when it was rendered, so
// re-applying it before create_temp is enough. A failure to reset is returned
// rather than warned about: continuing would run the destructive rebuild.
func (r *Runner) resetIncrementalForBackfill(model *parser.Model) error {
	if model.Incremental == "" {
		return nil
	}
	incrState, err := backfill.GetIncrementalState(
		r.sess, model.Target, model.Incremental, model.IncrementalInitial)
	if err != nil {
		return fmt.Errorf("reset incremental cursor for backfill of %s: %w", model.Target, err)
	}
	if incrState == nil {
		return nil
	}
	incrState.IsBackfill = true
	incrState.LastValue = incrState.InitialValue
	if err := applyIncrementalVars(r.sess, incrState); err != nil {
		return fmt.Errorf("apply reset incremental cursor for %s: %w", model.Target, err)
	}
	return nil
}

// isLakeAlias reports whether name is the lake catalog alias or, in a sandbox
// run, the prod alias the fork reads through. Empty aliases never match, so a
// session without a prod alias cannot mistake an unqualified "" for one.
func isLakeAlias(name, catalogAlias, prodAlias string) bool {
	if catalogAlias != "" && strings.EqualFold(name, catalogAlias) {
		return true
	}
	if prodAlias != "" && strings.EqualFold(name, prodAlias) {
		return true
	}
	return false
}

func (r *Runner) tableInLakeCatalog(table string) (bool, error) {
	parts := strings.Split(table, ".")
	if len(parts) == 1 {
		return true, nil
	}

	if isLakeAlias(parts[0], r.sess.CatalogAlias(), r.sess.ProdAlias()) {
		return true, nil
	}

	// Only a name whose leading segment is a catalog we can see ATTACHed is
	// treated as foreign. Absence of a lake alias is not evidence: a two-part
	// name is usually schema.table in the lake, and a name with three or more
	// segments can also be a quoted identifier containing dots
	// (`raw."odd.name"` splits into three). Guessing "foreign" there would
	// strip the prod qualification a sandbox run needs and break the query.
	foreign, err := r.isAttachedCatalog(parts[0])
	if err != nil {
		return false, err
	}
	return !foreign, nil
}

// isAttachedCatalog reports whether name is an ATTACHed catalog other than the
// lake (or, in sandbox, prod).
//
// Resolved once and cached on the Runner. A Runner executes a single model —
// RunDAG constructs one per model — and catalogs are ATTACHed by config/ during
// session init, before any model runs, so the set cannot change within a
// Runner's lifetime. A caller that reused one Runner across ATTACH/DETACH would
// read stale state.
//
// Splitting on "." is not identifier-aware, so a table whose quoted name
// contains a dot (`"odd.name"`) is misread as qualified and may be classified
// foreign. That costs a full query instead of CDC — the safe direction — and is
// not worth an SQL identifier parser here.
func (r *Runner) isAttachedCatalog(name string) (bool, error) {
	if r.foreignCatalogs == nil {
		rows, err := r.sess.QueryRows("SELECT database_name FROM duckdb_databases()")
		if err != nil {
			return false, fmt.Errorf("list attached catalogs: %w", err)
		}
		found := make(map[string]bool, len(rows))
		for _, db := range rows {
			db = strings.ToLower(strings.TrimSpace(db))
			switch db {
			case "", "system", "temp", "memory":
				// DuckDB's own catalogs, never a user source.
				continue
			}
			if strings.EqualFold(db, r.sess.CatalogAlias()) || strings.EqualFold(db, r.sess.ProdAlias()) {
				continue
			}
			found[db] = true
		}
		r.foreignCatalogs = found
	}
	return r.foreignCatalogs[strings.ToLower(name)], nil
}

// cleanup removes the temp table.
func (r *Runner) cleanup(tmpTable string) {
	_ = r.sess.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", tmpTable)) // IF EXISTS makes non-existence OK; real catalog errors break next CREATE more visibly
}

// runWarnings runs warning validations (log only, no rollback).
// Warnings support both audit patterns (post-INSERT, history-aware) and
// constraint patterns (row-level checks). Both are tried for each directive.
func (r *Runner) runWarnings(model *parser.Model, table string, result *Result) {
	// Load per-model variables (prev_model_snapshot, curr_snapshot).
	// Errors here mean delta warnings will use stale values — log as warning.
	configPath := filepath.Join(r.projectDir, "config")
	if err := r.sess.LoadPerModelVars(configPath, model.Target, r.sess.CatalogAlias()); err != nil {
		result.Warnings = append(result.Warnings, fmt.Sprintf("load per-model variables: %v", err))
	}

	// Set search_path so delta macros can resolve table_changes() + memory macros.
	parts := strings.SplitN(model.Target, ".", 2)
	if len(parts) == 2 {
		// search_path scoping; restore via deferred set below.
		_ = r.sess.Exec(fmt.Sprintf("SET search_path = '%s.%s,%s'", escapeSQL(r.sess.CatalogAlias()), escapeSQL(parts[0]), escapeSQL(r.sess.DefaultSearchPath())))
		defer func() {
			// Restore search_path on function exit; if this fails the caller
			// sees the temp scope until they next set it.
			_ = r.sess.Exec(fmt.Sprintf("SET search_path = '%s'", escapeSQL(r.sess.DefaultSearchPath())))
		}()
	}

	for _, warning := range model.Warnings {
		var queries []string

		// Dispatch warning via macro
		sql, err := validation.DispatchWarning(warning, table)
		if err != nil {
			result.Warnings = append(result.Warnings, fmt.Sprintf("warning dispatch error: %v", err))
			continue
		}
		queries = append(queries, sql)

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

// formatSchemaEvolution builds a human-readable description of schema changes
// with specific column names instead of just counts.
func formatSchemaEvolution(change backfill.SchemaChange) string {
	var parts []string
	if len(change.Renamed) > 0 {
		for _, r := range change.Renamed {
			parts = append(parts, fmt.Sprintf("renamed %s → %s", r.OldName, r.NewName))
		}
	}
	if len(change.Added) > 0 {
		var names []string
		for _, c := range change.Added {
			names = append(names, fmt.Sprintf("%s (%s)", c.Name, c.Type))
		}
		parts = append(parts, fmt.Sprintf("+ %s", strings.Join(names, ", ")))
	}
	if len(change.Dropped) > 0 {
		var names []string
		for _, c := range change.Dropped {
			names = append(names, c.Name)
		}
		parts = append(parts, fmt.Sprintf("- %s", strings.Join(names, ", ")))
	}
	if len(change.TypeChanged) > 0 {
		for _, tc := range change.TypeChanged {
			parts = append(parts, fmt.Sprintf("%s: %s → %s", tc.Column, tc.OldType, tc.NewType))
		}
	}
	if len(parts) == 0 {
		return "schema evolution: no changes"
	}
	return "schema evolution: " + strings.Join(parts, "; ")
}

// escapeSQL escapes single quotes for safe SQL string interpolation.
func escapeSQL(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}

// sanitizeTableName converts a target name to a safe temp table name.
// checkDeleteThresholdPreMaterialize counts how many target rows would be
// extractArgsFromSQL parses lib function args from raw SQL text.
// occurrence selects which match to extract (0-based) when the same
// function appears multiple times in the query.
func (r *Runner) extractArgsFromSQL(sql string, lib *libregistry.LibFunc, occurrence int) map[string]any {
	kwargs := make(map[string]any)

	// Find the N-th "func_name(" in SQL (skipping strings/comments)
	cleaned := stripStringsAndComments(sql)
	lower := strings.ToLower(cleaned)
	pattern := strings.ToLower(lib.Name) + "("
	searchFrom := 0
	idx := -1
	for n := 0; n <= occurrence; n++ {
		i := strings.Index(lower[searchFrom:], pattern)
		if i < 0 {
			return kwargs
		}
		idx = searchFrom + i
		searchFrom = idx + len(pattern)
	}
	// idx now points to the occurrence-th match in cleaned,
	// but we need to extract args from the original SQL at the same position

	// Extract content between parentheses, respecting string literals
	start := idx + len(pattern)
	end := findMatchingParen(sql, start)
	// findMatchingParen returns position after the closing paren
	if end > start {
		end-- // point to the closing paren, not past it
	}

	argsStr := strings.TrimSpace(sql[start:end])
	if argsStr == "" {
		return kwargs
	}

	// Split by comma, respecting parentheses and quoted strings.
	// Handles cases like: func('a,b'), func(json('{"x":1}'))
	parts := splitArgsRespectingNesting(argsStr)
	for i, part := range parts {
		part = strings.TrimSpace(part)
		// Remove surrounding quotes
		if len(part) >= 2 && ((part[0] == '\'' && part[len(part)-1] == '\'') || (part[0] == '"' && part[len(part)-1] == '"')) {
			part = part[1 : len(part)-1]
		}
		if i < len(lib.Args) {
			kwargs[lib.Args[i]] = part
		}
	}

	return kwargs
}

// splitArgsRespectingNesting splits a comma-separated argument string while
// respecting parentheses nesting and quoted strings. This correctly handles
// cases like: 'a,b', json('{"x":1,"y":2}'), concat('a', 'b')
func splitArgsRespectingNesting(s string) []string {
	var parts []string
	var current strings.Builder
	depth := 0
	inString := false
	stringChar := byte(0)

	for i := 0; i < len(s); i++ {
		c := s[i]
		if inString {
			current.WriteByte(c)
			if c == stringChar {
				// Handle escaped quotes ('')
				if i+1 < len(s) && s[i+1] == stringChar {
					current.WriteByte(s[i+1])
					i++
				} else {
					inString = false
				}
			}
		} else if c == '\'' || c == '"' {
			inString = true
			stringChar = c
			current.WriteByte(c)
		} else if c == '(' {
			depth++
			current.WriteByte(c)
		} else if c == ')' {
			depth--
			current.WriteByte(c)
		} else if c == ',' && depth == 0 {
			parts = append(parts, current.String())
			current.Reset()
		} else {
			current.WriteByte(c)
		}
	}
	if current.Len() > 0 {
		parts = append(parts, current.String())
	}
	return parts
}


func sanitizeTableName(target string) string {
	// Replace non-alphanumeric characters with underscores.
	// Temp table names must be safe for unquoted SQL identifiers.
	result := ""
	for _, c := range target {
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_' {
			result += string(c)
		} else {
			result += "_"
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
func (r *Runner) detectSchemaEvolution(
	model *parser.Model,
	tmpTable string,
	needsBackfill bool,
	decidedBackfill bool,
	result *Result,
) (*backfill.SchemaChange, bool) {
	stepStart := time.Now()
	newSchema, schemaErr := backfill.CaptureSchema(r.sess, tmpTable)
	r.trace(result, "schema.capture_new", stepStart, "ok")
	if schemaErr != nil || len(newSchema) == 0 {
		return nil, needsBackfill
	}

	stepStart = time.Now()
	prevSchema, prevErr := backfill.GetPreviousSchema(r.sess, model.Target)
	r.trace(result, "schema.get_previous", stepStart, "ok")
	if prevErr != nil {
		// Surface metadata-read failures as a warning rather than
		// silently treating "no previous schema" as the equivalent of
		// "fresh table" — a transient catalog read error would otherwise
		// suppress legitimate schema-change warnings and rebuild
		// decisions for an existing table.
		result.Warnings = append(result.Warnings, fmt.Sprintf("read previous schema for %s: %v", model.Target, prevErr))
	}

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
		model.ScriptType == parser.ScriptTypeNone {
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

	// If the unique_key column has a type change, force backfill. Type changes
	// use DROP+ADD which NULLs existing rows, breaking incremental detection
	// (e.g. SCD2 JOIN on id compares '1' = NULL → no match → silent data loss).
	// The schema evolution is returned so it runs inside the materialize
	// transaction (atomic with audit + data write).
	keyCol := model.UniqueKey
	if model.Kind == "tracked" {
		keyCol = model.GroupKey
	}
	if keyCol != "" && len(change.TypeChanged) > 0 {
		ukCols := make(map[string]bool)
		for _, part := range strings.Split(keyCol, ",") {
			ukCols[strings.TrimSpace(part)] = true
		}
		for _, tc := range change.TypeChanged {
			if ukCols[tc.Column] {
				result.RunType = "backfill"
				result.RunReason = "unique_key type changed"
				result.Warnings = append(result.Warnings,
					fmt.Sprintf("schema evolution: unique_key column %q type changed from %s to %s, forcing backfill",
						tc.Column, tc.OldType, tc.NewType))
				return &change, true
			}
		}
	}

	// Neither branch below demands a rebuild of its own, and both used to
	// return a bare false — which cancelled one the caller had already decided
	// on. Adding a column to an `append` model changes its hash, so the run is
	// a backfill, and the downgrade made it append the full source over the
	// rows already there: two rows became four.
	//
	// Only the up-front decision is preserved, not a mid-run escalation. When
	// CDC abandons a delta because an upstream schema changed, an incremental
	// append is still meant to append its new rows — the e2e goldens pin a
	// target accumulating across six runs — so escalating there would rebuild
	// a target that is supposed to grow.
	//
	// result.RunType is deliberately left reporting "incremental": no late
	// escalation relabels the run anywhere else either.
	keepBackfill := needsBackfill && decidedBackfill

	switch change.Type {
	case backfill.SchemaChangeDestructive:
		// Apply destructive changes via ALTER (preserves DuckLake snapshot chain).
		if model.Kind == "table" {
			result.RunType = "full"
		} else {
			result.RunType = "incremental"
		}
		result.Warnings = append(result.Warnings, formatSchemaEvolution(change))
		return &change, keepBackfill
	case backfill.SchemaChangeAdditive, backfill.SchemaChangeTypeChange:
		result.RunType = "incremental"
		result.Warnings = append(result.Warnings, formatSchemaEvolution(change))
		return &change, keepBackfill
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
