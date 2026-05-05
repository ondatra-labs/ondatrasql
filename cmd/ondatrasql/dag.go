// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/ondatra-labs/ondatrasql/internal/config"
	"github.com/ondatra-labs/ondatrasql/internal/dag"
	"github.com/ondatra-labs/ondatrasql/internal/duckdb"
	"github.com/ondatra-labs/ondatrasql/internal/execute"
	"github.com/ondatra-labs/ondatrasql/internal/git"
	"github.com/ondatra-labs/ondatrasql/internal/libregistry"
	"github.com/ondatra-labs/ondatrasql/internal/output"
	"github.com/ondatra-labs/ondatrasql/internal/parser"
)

func runAll(ctx context.Context, cfg *config.Config, sandboxMode bool) error {
	// Load all models from files
	models, err := loadModelsFromDir(cfg)
	if err != nil {
		return fmt.Errorf("load models: %w", err)
	}

	if len(models) == 0 {
		output.Fprintf("No models found in %s\n", cfg.ModelsPath)
		return nil
	}

	// Allocate a unique per-pid sandbox directory.
	var sandboxDir string
	if sandboxMode {
		var err error
		sandboxDir, err = createSandbox(cfg)
		if err != nil {
			return fmt.Errorf("create sandbox: %w", err)
		}
		// Defer cleanup so the sandbox directory is removed on every exit
		// path — including failures during InitSandbox or model execution.
		// Defers are LIFO, so this runs AFTER the sess.Close() defer below.
		defer func() { _ = os.RemoveAll(sandboxDir) }() // ignored: best-effort temp-dir cleanup
	}

	// Generate a single dag_run_id for the entire DAG run
	dagRunID := dag.GenerateRunID()

	// Create a SINGLE session for the entire DAG run
	// Used for both AST-based DAG building AND model execution
	sess, err := duckdb.NewSession("")
	if err != nil {
		return fmt.Errorf("create session: %w", err)
	}
	defer closeSessionOrLog(sess)

	// Scan lib/ for TABLE/SINK functions and register dummy macros
	// so json_serialize_sql() accepts FROM lib_func(...) syntax
	libReg, err := libregistry.Scan(cfg.ProjectDir)
	if err != nil {
		return fmt.Errorf("scan lib/: %w", err)
	}
	if err := libReg.RegisterMacros(sess); err != nil {
		return fmt.Errorf("register lib macros: %w", err)
	}

	// Build DAG with AST-based dependency extraction (before catalog attach)
	// This uses DuckDB's parser which doesn't need the lake attached
	graph := dag.NewGraph(sess, cfg.ProjectDir)
	for _, m := range models {
		graph.Add(m)
	}

	// Sort by dependencies
	sortedModels, err := graph.Sort()
	if err != nil {
		return fmt.Errorf("build DAG: %w", err)
	}

	// Build reverse-dependency map for skip propagation
	dependents := graph.Dependents()

	// Initialize session with catalog (after DAG is built)
	if sandboxMode {
		if n := duckdb.SandboxPgActiveConnections(cfg.Catalog.ConnStr); n > 0 {
			fmt.Fprintf(os.Stderr, "warning: sandbox will terminate %d active connection(s) to postgres catalog %q\n", n, cfg.Catalog.Alias)
			if !output.JSONEnabled {
				fmt.Fprintf(os.Stderr, "Continue? [y/N] ")
				var answer string
				_, _ = fmt.Scanln(&answer) // empty input falls through to safety default below
				if answer != "y" && answer != "Y" {
					return fmt.Errorf("sandbox cancelled by user")
				}
			}
		}
		sandboxCatalog := filepath.Join(sandboxDir, "sandbox.sqlite")
		if err := sess.InitSandbox(cfg.ConfigPath, cfg.Catalog.ConnStr, cfg.Catalog.DataPath, sandboxCatalog, cfg.Catalog.Alias); err != nil {
			return fmt.Errorf("init sandbox session: %w", err)
		}
	} else {
		if err := sess.InitWithCatalog(cfg.ConfigPath); err != nil {
			return fmt.Errorf("init session: %w", err)
		}
	}

	// Show banner for sandbox mode
	if sandboxMode {
		printTopBorder()
		printCenteredLine("SANDBOX DAG RUN")
		printSectionBorder("")
		printEmptyLine()
		printPaddedLine(fmt.Sprintf("Models: %d", len(sortedModels)))
		printEmptyLine()
		printSectionBorder("Execution")
		printEmptyLine()
	} else {
		output.Fprintf("Running %d models...\n", len(sortedModels))
	}

	// Validate model + sink compatibility before execution
	if err := execute.ValidateModelPushCompat(sortedModels, libReg); err != nil {
		return err
	}

	// Get git info once for the entire run
	gitInfo := git.GetInfo(cfg.ProjectDir)

	// Execute DAG using shared logic
	failedTargets := make(map[string]string)
	var failed, skipped int
	var totalRows int64
	dagStart := time.Now()

	_, dagErrs := execute.RunDAG(ctx, sess, sortedModels, dependents, dagRunID,
		gitInfo.Commit, gitInfo.Branch, gitInfo.RepoURL,
		cfg.ProjectDir,
		libReg,
		func(model *parser.Model, result *execute.Result, err error) bool {
			// Several runner.Run paths return (result, err) without
			// having appended err to result.Errors first (e.g. ensure
			// ack table, get pre-commit snapshot, materialize). Without
			// this, printResult shows the existing result.Errors but
			// the actual returned err is invisible — the model is
			// counted as failed yet the error text never reaches human
			// or JSON output. Fold it in once here so all downstream
			// renderers see the same error set.
			if err != nil && result != nil {
				errMsg := err.Error()
				alreadyPresent := false
				for _, existing := range result.Errors {
					if existing == errMsg {
						alreadyPresent = true
						break
					}
				}
				if !alreadyPresent {
					result.Errors = append(result.Errors, errMsg)
				}
			}

			if sandboxMode {
				printSandboxResult(result, model.Target, err)
			} else if result != nil {
				printResult(result)
			} else if err != nil {
				output.Fprintf("[FAILED] %s\n  ERROR: %s\n", model.Target, cleanErrorMessage(err.Error()))
			}
			// If runner.Run failed before producing a Result (e.g.
			// state-store open failure, blueprint resolution),
			// emitModelResultJSON(nil, ...) is a no-op and the failure
			// disappears from the JSON stream even though it's counted
			// in `failed` below. Synthesize a minimal error envelope so
			// JSON consumers see the same set of model rows the human
			// output saw.
			emitResult := result
			if emitResult == nil && err != nil {
				emitResult = &execute.Result{
					Target: model.Target,
					Kind:   model.Kind,
					Errors: []string{err.Error()},
				}
			}
			emitModelResultJSON(emitResult, dagRunID, sandboxMode)

			if err != nil {
				failed++
				failedTargets[model.Target] = err.Error()
			}
			if result != nil {
				totalRows += result.RowsAffected
				if result.RunType == "skip" {
					skipped++
				}
			}

			// Check context cancellation
			select {
			case <-ctx.Done():
				return false
			default:
				return true
			}
		},
	)

	// Surface batch-level errors that aren't tied to a specific model
	// (e.g. ComputeRunTypeDecisions failure under the _validation key).
	// These are caught here rather than per-callback because the
	// callback only fires per model that the runner actually attempted.
	if validationErr, ok := dagErrs["_validation"]; ok {
		return fmt.Errorf("dag pre-flight: %w", validationErr)
	}
	// _gc errors come from RunDAG's pre-flight orphan recovery + GC
	// pass. The pipeline still ran, but a failed GC means stale inflight
	// rows or unconsumed apply_log entries are accumulating in
	// state.duckdb — a real runtime regression. Surface in human and
	// JSON, and propagate as a non-zero exit per the documented
	// "any non-invocation runtime error → exit 1" contract in
	// docs/reference/pipeline/cli.md. Without this exit propagation a
	// CI gate would treat a degrading state.duckdb as a clean run.
	gcErr, hasGCErr := dagErrs["_gc"]
	if hasGCErr {
		if output.JSONEnabled {
			output.EmitJSON(map[string]any{
				"schema_version": 1,
				"kind":           "dag_warning",
				"source":         "_gc",
				"message":        gcErr.Error(),
			})
		} else {
			output.Fprintf("warning: %v (pipeline ran anyway; rerun GC at next pipeline start)\n", gcErr)
		}
	}

	// Honour ctx cancellation. RunDAG returns partial results on SIGINT
	// without a per-target error, so without this check the run would
	// exit 0 even though only a prefix of the DAG committed. Surface
	// the cancellation so the caller's exit code reflects an aborted
	// run, not a clean one.
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("dag run interrupted: %w", err)
	}

	// Print compact summary for non-sandbox runs
	if !sandboxMode && !output.JSONEnabled {
		ran := len(sortedModels) - skipped - failed
		output.Fprintf("\nDone: %d ran, %d skipped, %d failed (%d rows, %v)\n",
			ran, skipped, failed, totalRows, time.Since(dagStart).Round(time.Millisecond))
	}

	// Print summary
	if sandboxMode {
		// Show sandbox diff summary
		printEmptyLine()
		showDagSandboxSummary(sess, sortedModels, failedTargets)
		printBottomBorder()
	}

	if failed > 0 {
		// In sandbox mode the diff summary already showed each failure,
		// so we return the silent sentinel — main() suppresses the error
		// message but still exits with the right non-zero code so
		// automation detects the failed run. Without the sentinel the
		// command exited 0 in sandbox mode, hiding failures.
		if sandboxMode {
			return errFindings
		}
		return fmt.Errorf("%d model(s) failed", failed)
	}
	// _gc was already reported above (human + JSON). Returning the
	// silent sentinel propagates exit 1 without re-printing the
	// warning — matches the documented "non-invocation runtime error"
	// contract while keeping output deduplicated.
	if hasGCErr {
		return errFindings
	}

	return nil
}

