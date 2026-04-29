// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

// Package execute runs SQL models through the DuckDB session.
package execute

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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
	projectDir       string               // Project root directory (for Starlark load())
	configHash       string               // SHA256 of config/*.sql files (Bug S21: macros/variables bust hash)
	gitInfo          gitInfo              // Cached Git metadata
	runTypeDecisions RunTypeDecisions     // Pre-computed run_type decisions (batch optimization)
	astCache         map[string]string    // Cached AST JSON by SQL hash (reduces duplicate lineage queries)
	adminPort        string               // Admin port for event daemon (flush operations)
	claimLimit       int                  // Max events per claim batch (0 = default 1000)
	libRegistry      *libregistry.Registry // Registered lib functions from lib/*.star
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

// SetLibRegistry sets the lib function registry for FROM lib_func() support.
func (r *Runner) SetLibRegistry(reg *libregistry.Registry) {
	r.libRegistry = reg
}

// SetProjectDir sets the project root directory for Starlark load() support.
// Also computes the config hash for Bug S21 (macros/variables change detection).
func (r *Runner) SetProjectDir(dir string) {
	r.projectDir = dir
	r.configHash = backfill.ConfigHash(filepath.Join(dir, "config"))
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

	// Starlark script models (.star) are no longer supported as model files.
	// Starlark is used only in lib/ blueprints via the API dict pattern.
	if model.ScriptType != parser.ScriptTypeNone {
		return nil, fmt.Errorf("Starlark script models (.star) are no longer supported. Use SQL models with lib/ blueprints instead")
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
		GroupKey:           model.GroupKey,
		PartitionedBy:      model.PartitionedBy,
		Incremental:        model.Incremental,
		IncrementalInitial: model.IncrementalInitial,
		ConfigHash:         r.configHash,
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
		decision, err = ComputeSingleRunType(r.sess, model, r.configHash)
		r.trace(result, "run_type.compute", stepStart, "ok")
		if err != nil {
			result.Errors = append(result.Errors, fmt.Sprintf("run_type check: %v", err))
			decision = &RunTypeDecision{RunType: "backfill"} // Safe fallback
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
		if model.Sink != "" {
			// No delta table on skip -- only process existing Badger backlog
			if err := r.executeSink(ctx, model, result, nil, 0); err != nil {
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
			result.Warnings = append(result.Warnings, fmt.Sprintf("incremental state: %v", incrErr))
		}
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

	// Track the SQL transformation pipeline through this Run().
	// state.Current() is what we execute; state.Rewritten() is the snapshot
	// preserved as the retry target when CDC must be reverted. See sql_state.go.
	state := newRunSQLState(model.SQL)

	// Auto-detect tables and column lineage from SQL (single AST query)
	stepStart = time.Now()
	astJSON, _ := r.getAST(model.SQL)

	// Detect and execute lib function calls in FROM clause (e.g. FROM gam_fetch(...))
	var libCalls []LibCall
	var hasInferredStubs bool        // true when 0-row lib stubs were created from AST/column inference
	var trackedOpts trackedRunOpts   // populated when tracked + lib + all-empty + no_change semantics
	// Ensure all lib-call Badger stores are cleaned up on any exit path.
	// Claims that were not explicitly acked are nacked (returned to queue).
	// The acked flag is set in the success path after materialize.
	var libClaimsAcked bool
	// ackLibClaims acks all lib-call Badger claims and deletes ack records.
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
				script.DeleteAck(r.sess, claimID)
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
				sr.NackClaims()
			}
			sr.Close()
		}
	}()
	if r.libRegistry != nil && !r.libRegistry.Empty() {
		parsedAST, parseErr := duckast.Parse(astJSON)
		if parseErr == nil {
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

					// Inject incremental state as kwargs
					incrState, incrErr := backfill.GetIncrementalState(
						r.sess, model.Target, model.Incremental, model.IncrementalInitial)
					if incrErr != nil {
						result.Warnings = append(result.Warnings, fmt.Sprintf("lib %s incremental state: %v", call.FuncName, incrErr))
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
						kwargs["is_backfill"] = true
						kwargs["last_value"] = ""
						kwargs["last_run"] = ""
						kwargs["cursor"] = ""
						kwargs["initial_value"] = ""
					}

					rt := script.NewRuntime(r.sess, nil, r.projectDir)
					if r.projectDir != "" && model.Kind != "table" {
						rt.SetIngestDir(filepath.Join(r.projectDir, ".ondatra", "ingest"))
					}

					// Execute lib function (unique target per call to avoid temp table collision)
					libTarget := fmt.Sprintf("_lib_%s_%d", call.FuncName, call.CallIndex)
					var scriptResult *script.Result
					var runErr error
					if call.Lib.APIConfig != nil {
						httpCfg := httpConfigFromLib(call.Lib.APIConfig, ctx, r.projectDir)
						if call.Lib.FetchMode == "async" {
							// Async fetch: submit() → check() → fetch_result()
							pollInterval := 5 * time.Second
							pollTimeout := 5 * time.Minute
							pollBackoff := 1
							if call.Lib.APIConfig.FetchPollInterval != "" {
								if d, err := time.ParseDuration(call.Lib.APIConfig.FetchPollInterval); err == nil {
									pollInterval = d
								}
							}
							if call.Lib.APIConfig.FetchPollTimeout != "" {
								if d, err := time.ParseDuration(call.Lib.APIConfig.FetchPollTimeout); err == nil {
									pollTimeout = d
								}
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

				// Tracked + lib + all 0-row returns + every lib's empty_result
				// is "no_change" (the default) → tell tracked materialize to
				// suppress the delete-missing-groups branch so target rows
				// are preserved. The rest of the pipeline (stubs, rewrite,
				// schema evolution, audits, materialize) still runs so that
				// SQL-only changes (new columns, audit changes) are applied.
				trackedOpts.noDeleteOnMissingGroups = model.Kind == "tracked" && allLibsReturnedNoChange(libCalls)

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
						r.sess.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", stubName))
						if err := r.sess.Exec(fmt.Sprintf("CREATE TEMP TABLE %s (%s)", stubName, strings.Join(colDefs, ", "))); err != nil {
							return nil, fmt.Errorf("create empty lib stub %s: %w", stubName, err)
						}
						call.TempTable = stubName
						hasInferredStubs = true
						continue
					}

					// 2. Dynamic-column libs: build the stub from the model
					//    SQL's column shape — typed projection where there
					//    are explicit casts, plus input-only refs (JOIN ON,
					//    WHERE, etc) as VARCHAR. SQL is the schema authority;
					//    the runner already extracts this AST info elsewhere.
					//    This catches schema evolution on 0-row runs that
					//    target-clone would have hidden.
					//
					//    When target exists, prefer the target's established
					//    type for VARCHAR fallback columns (no explicit cast)
					//    so we don't trigger false schema-evolution warnings.
					//    Explicit SQL casts always win.
					alias := ""
					if call.ASTNode != nil {
						alias = call.ASTNode.Alias()
					}
					if shape := extractColShapeForLib(parsedAST, alias, singleLibSource); len(shape) > 0 {
						if targetExists, _ := r.tableExistsCheck(model.Target); targetExists {
							targetTypes, fetchErr := r.fetchTargetColumnTypes(model.Target)
							if fetchErr != nil {
								// Surface the failure rather than silently degrading
								// to all-VARCHAR, which would trigger false schema
								// evolution warnings on subsequent runs with data.
								result.Warnings = append(result.Warnings,
									fmt.Sprintf("lib stub: target type lookup failed for %s: %v (falling back to SQL-shape types)",
										model.Target, fetchErr))
							}
							for i := range shape {
								if shape[i].sqlType != "VARCHAR" {
									continue
								}
								if t, ok := targetTypes[shape[i].name]; ok && t != "" {
									shape[i].sqlType = t
								}
							}
						}
						var colDefs []string
						for _, c := range shape {
							colDefs = append(colDefs, fmt.Sprintf("%s %s", duckdb.QuoteIdentifier(c.name), c.sqlType))
						}
						r.sess.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", stubName))
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
					if targetExists, _ := r.tableExistsCheck(model.Target); targetExists {
						r.sess.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", stubName))
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
				astJSON, _ = r.getAST(state.Current())
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
	aggregationTables := lineage.GetCDCTables(colLineage)

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
	isIncremental := model.Kind == "append" || model.Kind == "merge"
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
			for _, t := range cdcTables {
				parts := strings.SplitN(t, ".", 2)
				if len(parts) != 2 {
					hasChanges = true
					insertOnly = false
					break
				}
				schema, table := parts[0], parts[1]
				// Set search_path to include the source schema for table_changes()
				// (which takes bare table name). Restore default search_path after.
				r.sess.Exec(fmt.Sprintf("SET search_path = '%s.%s,%s'",
					escapeSQL(r.sess.CatalogAlias()), escapeSQL(schema), escapeSQL(r.sess.DefaultSearchPath())))
				cnt, err := r.sess.TableHasChanges(table, snapshotID+1, currSnap)
				if err != nil {
					result.Warnings = append(result.Warnings, fmt.Sprintf("table_changes gate failed for %s: %v", t, err))
					hasChanges = true
					insertOnly = false
					r.sess.Exec(fmt.Sprintf("SET search_path = '%s'", escapeSQL(r.sess.DefaultSearchPath())))
					break
				}
				if cnt > 0 {
					hasChanges = true
					isInsertOnly, ioErr := r.sess.TableChangesInsertOnly(table, snapshotID+1, currSnap)
					if ioErr != nil || !isInsertOnly {
						insertOnly = false
					}
				}
				r.sess.Exec(fmt.Sprintf("SET search_path = '%s'", escapeSQL(r.sess.DefaultSearchPath())))
			}
		}
		r.trace(result, "cdc.table_changes_gate", stepStart, "ok")

		if hasChanges {
			// Check if upstream schema changed — CDC EXCEPT requires matching column counts.
			stepStart = time.Now()
			schemaChanged := r.cdcSchemaChanged(cdcTables, snapshotID)
			r.trace(result, "cdc.schema_check", stepStart, "ok")

			if schemaChanged {
				result.Warnings = append(result.Warnings, "upstream schema changed, skipping CDC")
				state.RevertToRewritten()
				needsBackfill = true
			} else if insertOnly {
				// Phase 2: all source changes are inserts — apply CDC with
				// the same EXCEPT approach but log the optimization.
				cdcSQL, cdcErr := r.applySmartCDC(astJSON, model.Kind, cdcTables, snapshotID)
				if cdcErr != nil {
					result.Warnings = append(result.Warnings, fmt.Sprintf("CDC failed, using full query: %v", cdcErr))
					state.RevertToRewritten()
				} else {
					state.SetCurrent(cdcSQL)
					r.trace(result, "cdc.applied_insert_only:"+strings.Join(cdcTables, ","), stepStart, "ok")
				}
			} else {
				// Mixed changes (updates/deletes) — full EXCEPT
				cdcSQL, cdcErr := r.applySmartCDC(astJSON, model.Kind, cdcTables, snapshotID)
				if cdcErr != nil {
					result.Warnings = append(result.Warnings, fmt.Sprintf("CDC failed, using full query: %v", cdcErr))
					state.RevertToRewritten()
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
				state.RevertToRewritten()
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
			needsBackfill = true
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
				r.sess.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", tmpTable))
				descCreateSQL := fmt.Sprintf("CREATE TEMP TABLE %s (%s)", tmpTable, strings.Join(descRows, ", "))
				if createErr := r.sess.Exec(descCreateSQL); createErr != nil {
					return nil, fmt.Errorf("create temp table from DESCRIBE: %w", createErr)
				}
			} else {
				// DESCRIBE also failed — fall back to target clone
				targetExists, _ := r.tableExistsCheck(model.Target)
				if !targetExists {
					return nil, fmt.Errorf("create temp table: %w", err)
				}
				r.sess.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", tmpTable))
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
	schemaChange, needsBackfill = r.detectSchemaEvolution(model, tmpTable, needsBackfill, result)

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

	// Build ack SQL for lib-call Badger claims — included in the materialize
	// transaction so the ack record is atomic with the data commit (same as script.go).
	var libExtraPreSQL []string
	for i := range libCalls {
		sr := libCalls[i].ScriptResult
		if sr == nil || len(sr.ClaimIDs) == 0 {
			continue
		}
		if ackErr := script.EnsureAckTable(r.sess); ackErr != nil {
			result.Warnings = append(result.Warnings, fmt.Sprintf("ack table: %v", ackErr))
		} else {
			for _, claimID := range sr.ClaimIDs {
				libExtraPreSQL = append(libExtraPreSQL, script.AckSQL(claimID, model.Target, sr.RowCount))
			}
		}
	}

	// Capture pre-commit snapshot for sink delta (table_changes needs the range).
	var preCommitSnapshot int64
	if model.Sink != "" {
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
		r.sess.Exec("ROLLBACK")

		// Lib-call Badger claim handling on materialize failure
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

	// Success — ack lib-call Badger claims and delete ack records.
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
	// Row data is read from DuckLake at push time, not stored in Badger.
	if model.Sink != "" {
		// Get post-commit snapshot. All sink kinds need this:
		// All sink-enabled kinds need table_changes() range
		var postCommitSnapshot int64
		if err := r.sess.RefreshSnapshot(); err == nil {
			postCommitSnapshot, _ = r.sess.GetCurrentSnapshot()
		}

		// Set search_path so table_changes() can resolve bare table name
		schema, _ := splitSchemaTable(model.Target)
		if schema != "" {
			r.sess.Exec(fmt.Sprintf("SET search_path = '%s.%s,%s'",
				escapeSQL(r.sess.CatalogAlias()), escapeSQL(schema), escapeSQL(r.sess.DefaultSearchPath())))
		}

		stepStart = time.Now()
		sinkEvents, deltaErr := r.createSinkDelta(model, tmpTable, preCommitSnapshot, postCommitSnapshot)

		if schema != "" {
			r.sess.Exec(fmt.Sprintf("SET search_path = '%s'", escapeSQL(r.sess.DefaultSearchPath())))
		}

		if deltaErr != nil {
			r.trace(result, "sink.delta", stepStart, "error")
			result.Warnings = append(result.Warnings, fmt.Sprintf("sink: delta failed: %v", deltaErr))
		} else if len(sinkEvents) > 0 {
			r.trace(result, "sink.delta", stepStart, "ok")
		}

		// Run sink: processes new delta AND existing Badger backlog.
		stepStart = time.Now()
		if err := r.executeSink(ctx, model, result, sinkEvents, postCommitSnapshot); err != nil {
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

// cleanup removes the temp table.
func (r *Runner) cleanup(tmpTable string) {
	r.sess.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", tmpTable))
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
		r.sess.Exec(fmt.Sprintf("SET search_path = '%s.%s,%s'", escapeSQL(r.sess.CatalogAlias()), escapeSQL(parts[0]), escapeSQL(r.sess.DefaultSearchPath())))
		defer r.sess.Exec(fmt.Sprintf("SET search_path = '%s'", escapeSQL(r.sess.DefaultSearchPath())))
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
	result *Result,
) (*backfill.SchemaChange, bool) {
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

	switch change.Type {
	case backfill.SchemaChangeDestructive:
		// Apply destructive changes via ALTER (preserves DuckLake snapshot chain).
		if model.Kind == "table" {
			result.RunType = "full"
		} else {
			result.RunType = "incremental"
		}
		result.Warnings = append(result.Warnings, formatSchemaEvolution(change))
		return &change, false
	case backfill.SchemaChangeAdditive, backfill.SchemaChangeTypeChange:
		result.RunType = "incremental"
		result.Warnings = append(result.Warnings, formatSchemaEvolution(change))
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
