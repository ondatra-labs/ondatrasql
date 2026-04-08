// OndatraSQL - You don't need a data stack anymore
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/ondatra-labs/ondatrasql/internal/config"
	"github.com/ondatra-labs/ondatrasql/internal/dag"
	"github.com/ondatra-labs/ondatrasql/internal/duckdb"
	"github.com/ondatra-labs/ondatrasql/internal/execute"
	"github.com/ondatra-labs/ondatrasql/internal/git"
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

	// Auto-create sandbox if needed
	var sandboxDir string
	if sandboxMode {
		sandboxDir = filepath.Join(cfg.ProjectDir, ".sandbox")
		if _, err := os.Stat(sandboxDir); os.IsNotExist(err) {
			if err := createSandbox(cfg); err != nil {
				return fmt.Errorf("create sandbox: %w", err)
			}
		}
		// Defer cleanup so the sandbox directory is removed on every exit
		// path — including failures during InitSandbox or model execution.
		// Without this, a failed run leaves a stale .sandbox/ behind that
		// the next run would silently reuse instead of starting fresh.
		// Defers are LIFO, so this runs AFTER the sess.Close() defer below.
		defer os.RemoveAll(sandboxDir)
	}

	// Generate a single dag_run_id for the entire DAG run
	dagRunID := dag.GenerateRunID()

	// Create a SINGLE session for the entire DAG run
	// Used for both AST-based DAG building AND model execution
	sess, err := duckdb.NewSession("")
	if err != nil {
		return fmt.Errorf("create session: %w", err)
	}
	defer sess.Close()

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
		sandboxCatalog := filepath.Join(cfg.ProjectDir, ".sandbox", "sandbox.sqlite")
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

	// Get git info once for the entire run
	gitInfo := git.GetInfo(cfg.ProjectDir)

	// Resolve admin port for event daemon (empty if events not running)
	adminPort := resolveAdminPort(cfg)

	// Execute DAG using shared logic
	failedTargets := make(map[string]string)
	var failed int

	execute.RunDAG(ctx, sess, sortedModels, dependents, dagRunID,
		gitInfo.Commit, gitInfo.Branch, gitInfo.RepoURL,
		adminPort,
		cfg.ProjectDir,
		func(model *parser.Model, result *execute.Result, err error) bool {
			if sandboxMode {
				printSandboxResult(result, model.Target, err)
			} else {
				printResult(result)
			}
			emitModelResultJSON(result, dagRunID, sandboxMode)

			if err != nil {
				failed++
				failedTargets[model.Target] = err.Error()
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

	// Print summary
	if sandboxMode {
		// Show sandbox diff summary
		printEmptyLine()
		showDagSandboxSummary(sess, sortedModels, failedTargets)
		printBottomBorder()
	}

	if failed > 0 {
		err := fmt.Errorf("%d model(s) failed", failed)
		// In sandbox mode, failures are shown in summary - don't print error again
		if sandboxMode {
			return nil
		}
		return err
	}

	return nil
}

// resolveAdminPort returns the event daemon admin port from the runtime file
// written by `ondatrasql events`. Returns empty if events daemon is not running.
func resolveAdminPort(cfg *config.Config) string {
	portFile := filepath.Join(cfg.ProjectDir, ".ondatra", "events.admin.port")
	data, err := os.ReadFile(portFile)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(data))
}
