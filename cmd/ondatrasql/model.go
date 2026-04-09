// OndatraSQL - You don't need a data stack anymore
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package main

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strings"

	"github.com/ondatra-labs/ondatrasql/internal/backfill"
	"github.com/ondatra-labs/ondatrasql/internal/config"
	"github.com/ondatra-labs/ondatrasql/internal/dag"
	"github.com/ondatra-labs/ondatrasql/internal/duckdb"
	"github.com/ondatra-labs/ondatrasql/internal/execute"
	"github.com/ondatra-labs/ondatrasql/internal/git"
	"github.com/ondatra-labs/ondatrasql/internal/output"
	"github.com/ondatra-labs/ondatrasql/internal/parser"
)

// loadModelsFromDir loads all SQL models from the configured models directory.
func loadModelsFromDir(cfg *config.Config) ([]*parser.Model, error) {
	modelsDir := cfg.ModelsPath

	if _, err := os.Stat(modelsDir); os.IsNotExist(err) {
		return nil, fmt.Errorf("models directory not found: %s", modelsDir)
	}

	var models []*parser.Model

	err := filepath.Walk(modelsDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if info.IsDir() || !parser.IsModelFile(path) {
			return nil
		}

		model, err := parser.ParseModel(path, cfg.ProjectDir)
		if err != nil {
			return fmt.Errorf("parse %s: %w", path, err)
		}

		models = append(models, model)
		return nil
	})

	return models, err
}

// findModel finds a model by target name or file path.
func findModel(cfg *config.Config, target string) (*parser.Model, error) {
	// Check if it's a file path with a model extension.
	if parser.IsModelFile(target) {
		if _, err := os.Stat(target); err == nil {
			// For .yaml/.yml files, only treat as model if under models/ directory —
			// prevents config.yml and other non-model YAML from being parsed as models.
			if isYAMLModelPath(target, cfg.ModelsPath) || !isYAMLExt(target) {
				return parser.ParseModel(target, cfg.ProjectDir)
			}
		}
	}

	// Load all models and find by target
	models, err := loadModelsFromDir(cfg)
	if err != nil {
		return nil, err
	}

	for _, m := range models {
		if m.Target == target {
			return m, nil
		}
	}

	return nil, fmt.Errorf("model not found: %s", target)
}

func runModel(ctx context.Context, cfg *config.Config, target string, sandboxMode bool) error {
	model, err := findModel(cfg, target)
	if err != nil {
		return err
	}

	// Check sandbox mode - allocate a unique per-pid sandbox directory.
	var sandboxDir string
	if sandboxMode {
		var err error
		sandboxDir, err = createSandbox(cfg)
		if err != nil {
			return fmt.Errorf("create sandbox: %w", err)
		}
		// Defer cleanup so the sandbox directory is removed on every exit
		// path — including failures during InitSandbox or model execution.
		// Without this, a failed run leaves a stale .sandbox/<pid>-<rand>
		// subdir behind. Defers are LIFO, so this runs AFTER sess.Close().
		defer os.RemoveAll(sandboxDir)
	}

	// Generate dag_run_id
	dagRunID := dag.GenerateRunID()

	// Create session (in-memory, DuckLake attaches separately)
	sess, err := duckdb.NewSession("")
	if err != nil {
		return fmt.Errorf("create session: %w", err)
	}
	defer sess.Close()

	// Initialize session with DuckLake catalog
	if sandboxMode {
		sandboxCatalog := filepath.Join(sandboxDir, "sandbox.sqlite")
		if err := sess.InitSandbox(cfg.ConfigPath, cfg.Catalog.ConnStr, cfg.Catalog.DataPath, sandboxCatalog, cfg.Catalog.Alias); err != nil {
			return fmt.Errorf("init sandbox session: %w", err)
		}
		// Show sandbox banner after session init (so we can query dependencies)
		printSandboxBanner(cfg, sess, model)
	} else {
		if err := sess.InitWithCatalog(cfg.ConfigPath); err != nil {
			return fmt.Errorf("init session: %w", err)
		}
	}

	// Run model with Git metadata
	gitInfo := git.GetInfo(cfg.ProjectDir)

	runner := execute.NewRunner(sess, execute.ModeRun, dagRunID)
	runner.SetGitInfo(gitInfo.Commit, gitInfo.Branch, gitInfo.RepoURL)
	runner.SetAdminPort(resolveAdminPort(cfg))
	runner.SetProjectDir(cfg.ProjectDir)

	result, err := runner.Run(ctx, model)

	// Show automatic diff and impact in sandbox mode. v0.12.1 (Bug S7 fix):
	// the box is rendered for both success and failure paths so the user
	// always sees a complete, properly-closed box. Pre-fix the failure path
	// fell through to printResult and left the box half-open after the
	// SANDBOX MODE banner had already been printed.
	if sandboxMode {
		printSectionBorder("Result")
		printEmptyLine()
		switch {
		case err != nil:
			printPaddedLine(fmt.Sprintf("[FAILED] %s", model.Target))
			for _, line := range wrapErrorMessage(err.Error(), 60) {
				printPaddedLine("  " + line)
			}
		case result != nil:
			printPaddedLine(fmt.Sprintf("[OK] %s (%s, %s, %d rows, %v)",
				result.Target, result.Kind, result.RunType,
				result.RowsAffected, result.Duration.Round(1e6)))
			showValidationStatus(model, result)
		}
		printEmptyLine()

		// Diff and impact only make sense on success — failed runs may have
		// no sandbox table at all.
		if err == nil {
			showSandboxDiff(sess, model.Target, model.Kind)
			showSandboxImpact(cfg, model.Target)
		}

		// Close box (cleanup is handled by deferred RemoveAll above)
		printBottomBorder()
	} else {
		// Normal mode - print simple result
		printResult(result)
	}

	emitModelResultJSON(result, dagRunID, sandboxMode)

	if err != nil {
		return err
	}

	return nil
}

// runModelInSession executes a model using an existing shared session.
// Used by run_all for efficient single-session DAG execution.
func runModelInSession(ctx context.Context, cfg *config.Config, sess *duckdb.Session, model *parser.Model,
	dagRunID string, runTypeDecisions execute.RunTypeDecisions) (*execute.Result, error) {

	gitInfo := git.GetInfo(cfg.ProjectDir)

	runner := execute.NewRunner(sess, execute.ModeRun, dagRunID)
	runner.SetGitInfo(gitInfo.Commit, gitInfo.Branch, gitInfo.RepoURL)
	runner.SetRunTypeDecisions(runTypeDecisions) // Use pre-computed decisions
	runner.SetProjectDir(cfg.ProjectDir)

	result, err := runner.Run(ctx, model)
	return result, err
}

func printResult(result *execute.Result) {
	if result == nil {
		return
	}

	status := "OK"
	if len(result.Errors) > 0 {
		status = "FAILED"
	}

	reason := ""
	if result.RunReason != "" {
		reason = " — " + result.RunReason
	}
	output.Fprintf("[%s] %s (%s, %s, %d rows, %v%s)\n",
		status, result.Target, result.Kind, result.RunType,
		result.RowsAffected, result.Duration.Round(1e6), reason)

	for _, err := range result.Errors {
		output.Fprintf("  ERROR: %s\n", err)
	}
	for _, warn := range result.Warnings {
		output.Fprintf("  WARN: %s\n", warn)
	}
}

// emitModelResultJSON emits a JSON line for --json mode after a model run.
//
// Status mapping (Bug S11 fix): "ok" was previously emitted for any run with
// no errors, including skipped models. CI scripts that gate on `status: "ok"`
// would then deploy changes whose audits/constraints/warnings were never
// evaluated. Sandbox v0.12.1 forces non-skip in sandbox so this is mostly
// theoretical there, but the rule still applies for prod skips: distinguish
// "ran successfully and produced data" from "didn't run at all".
func emitModelResultJSON(result *execute.Result, dagRunID string, sandbox bool) {
	if result == nil {
		return
	}
	status := "ok"
	switch {
	case len(result.Errors) > 0:
		status = "error"
	case result.RunType == "skip":
		status = "skip"
	}
	output.EmitJSON(output.ModelResult{
		Model:        result.Target,
		Kind:         result.Kind,
		RunType:      result.RunType,
		RunReason:    result.RunReason,
		RowsAffected: result.RowsAffected,
		DurationMs:   result.Duration.Milliseconds(),
		Status:       status,
		Errors:       result.Errors,
		Warnings:     result.Warnings,
		DagRunID:     dagRunID,
		Sandbox:      sandbox,
	})
}

// printSandboxBanner prints sandbox mode header with model info.
func printSandboxBanner(cfg *config.Config, sess *duckdb.Session, model *parser.Model) {
	// Use same box style as describe command
	printTopBorder()
	printCenteredLine("SANDBOX MODE")
	printSectionBorder("")
	printEmptyLine()
	printPaddedLine(fmt.Sprintf("Target:  %s", model.Target))

	// Get dependencies from catalog
	info, err := backfill.GetModelCommitInfo(sess, model.Target)
	if err == nil && info != nil && len(info.Depends) > 0 {
		deps := strings.Join(info.Depends, ", ")
		printPaddedLine(fmt.Sprintf("Depends: %s", truncateStr(deps, 50)))
	}
	printEmptyLine()
}

// showSandboxImpact shows downstream impact after running a model in sandbox.
func showSandboxImpact(cfg *config.Config, target string) {
	printSectionBorder("Downstream Impact")
	printEmptyLine()

	// Create a new session for impact analysis
	sess, err := duckdb.NewSession("")
	if err != nil {
		printPaddedLine("(unable to analyze)")
		printEmptyLine()
		return
	}
	defer sess.Close()

	if err := sess.InitWithCatalog(cfg.ConfigPath); err != nil {
		printPaddedLine("(unable to analyze)")
		printEmptyLine()
		return
	}

	analysis, err := dag.AnalyzeImpact(sess, target)
	if err != nil {
		printPaddedLine("(unable to analyze)")
		printEmptyLine()
		return
	}

	if len(analysis.Impacts) == 0 {
		printPaddedLine("No downstream models affected")
		printEmptyLine()
		return
	}

	printPaddedLine(fmt.Sprintf("%d model(s) affected:", len(analysis.Impacts)))
	printEmptyLine()

	for _, impact := range analysis.Impacts {
		rebuildStr := ""
		if impact.RebuildNeeded {
			rebuildStr = " [REBUILD]"
		}
		printPaddedLine(fmt.Sprintf("→ %s%s", impact.Target, rebuildStr))

		// Show reason for impact
		if impact.Reason != "" {
			printPaddedLine(fmt.Sprintf("    Reason: %s", truncateStr(impact.Reason, 50)))
		}

		// Show affected columns (from target model)
		if len(impact.AffectedColumns) > 0 {
			cols := strings.Join(impact.AffectedColumns, ", ")
			printPaddedLine(fmt.Sprintf("    Affected: %s", truncateStr(cols, 48)))
		}

		// Show source columns (from changed model)
		if len(impact.SourceColumns) > 0 {
			cols := strings.Join(unique(impact.SourceColumns), ", ")
			printPaddedLine(fmt.Sprintf("    Source:   %s", truncateStr(cols, 48)))
		}

		// Show transformations (if any non-identity)
		if len(impact.Transformations) > 0 {
			var transforms []string
			for col, t := range impact.Transformations {
				transforms = append(transforms, fmt.Sprintf("%s→%s", col, t))
			}
			printPaddedLine(fmt.Sprintf("    Transforms: %s", truncateStr(strings.Join(transforms, ", "), 44)))
		}
	}

	// Show transitive impacts with full details
	fullTree, err := dag.GetFullImpactTree(sess, target)
	if err == nil && len(fullTree) > len(analysis.Impacts) {
		printEmptyLine()
		transitiveCount := len(fullTree) - len(analysis.Impacts)
		printPaddedLine(fmt.Sprintf("Transitive impact: +%d model(s)", transitiveCount))
		printEmptyLine()

		// Build set of direct impact targets
		directTargets := make(map[string]bool)
		for _, imp := range analysis.Impacts {
			directTargets[imp.Target] = true
		}

		for _, model := range fullTree {
			if directTargets[model] {
				continue // Skip direct impacts, already shown
			}

			// For each transitive model, find which direct impact it depends on
			// and show that relationship
			for _, directImp := range analysis.Impacts {
				impact, err := dag.AnalyzeTransitiveImpact(sess, directImp.Target, model)
				if err != nil || impact == nil {
					continue
				}

				printPaddedLine(fmt.Sprintf("↳ %s [REBUILD]", model))
				printPaddedLine(fmt.Sprintf("    Via: %s", directImp.Target))
				if len(impact.AffectedColumns) > 0 {
					cols := strings.Join(impact.AffectedColumns, ", ")
					printPaddedLine(fmt.Sprintf("    Affected: %s", truncateStr(cols, 48)))
				}
				if len(impact.SourceColumns) > 0 {
					cols := strings.Join(unique(impact.SourceColumns), ", ")
					printPaddedLine(fmt.Sprintf("    Source:   %s", truncateStr(cols, 48)))
				}
				if len(impact.Transformations) > 0 {
					var transforms []string
					for col, t := range impact.Transformations {
						transforms = append(transforms, fmt.Sprintf("%s→%s", col, t))
					}
					printPaddedLine(fmt.Sprintf("    Transforms: %s", truncateStr(strings.Join(transforms, ", "), 44)))
				}
				break // Only show first matching path
			}
		}
	}
	printEmptyLine()
}

// wrapErrorMessage breaks a long error string into wrapped lines that fit
// inside the sandbox box layout. Used by the sandbox failure rendering path
// to keep the box close-borders aligned even when the underlying error
// (often a multi-line DuckDB diagnostic) is wider than the box.
func wrapErrorMessage(msg string, width int) []string {
	// First flatten newlines in the error to spaces — DuckDB errors include
	// hint lines we don't have room for inside the box.
	flat := strings.ReplaceAll(msg, "\n", " ")
	flat = strings.Join(strings.Fields(flat), " ")
	if width <= 0 {
		width = 60
	}
	var lines []string
	for len(flat) > width {
		// break at the last space before width
		cut := strings.LastIndex(flat[:width], " ")
		if cut <= 0 {
			cut = width
		}
		lines = append(lines, flat[:cut])
		flat = strings.TrimSpace(flat[cut:])
	}
	if flat != "" {
		lines = append(lines, flat)
	}
	return lines
}

// createSandbox creates a unique per-invocation sandbox directory and
// returns its absolute path. The directory is nested under .sandbox/ as
// .sandbox/<pid>-<random> so that two parallel `ondatrasql sandbox` runs
// against the same project don't collide on the same sqlite catalog file
// (Bug S12 fix). Cleanup is the caller's responsibility — typically a
// defer os.RemoveAll(...) on the returned path.
func createSandbox(cfg *config.Config) (string, error) {
	// 8 hex chars of randomness keeps collisions astronomically unlikely
	// across multiple sandbox sessions per pid.
	suffix := fmt.Sprintf("%d-%08x", os.Getpid(), rand.Uint32())
	dir := filepath.Join(cfg.ProjectDir, ".sandbox", suffix)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", err
	}
	return dir, nil
}

// unique removes duplicates from a string slice.
func unique(s []string) []string {
	seen := make(map[string]bool)
	var result []string
	for _, v := range s {
		if !seen[v] {
			seen[v] = true
			result = append(result, v)
		}
	}
	return result
}

// truncateStr truncates a string to maxLen with ellipsis (rune-safe).
func truncateStr(s string, maxLen int) string {
	runes := []rune(s)
	if len(runes) <= maxLen {
		return s
	}
	return string(runes[:maxLen-3]) + "..."
}

// showValidationStatus shows constraint/audit/warning results.
func showValidationStatus(model *parser.Model, result *execute.Result) {
	hasValidations := len(model.Constraints) > 0 || len(model.Audits) > 0 || len(model.Warnings) > 0
	if !hasValidations {
		return
	}

	printEmptyLine()

	// If the run was skipped, validations were not evaluated. Saying
	// "N passed" would be misleading since they never ran. (Bug 27)
	skipped := result != nil && result.RunType == "skip"

	// Constraints
	if len(model.Constraints) > 0 {
		if skipped {
			printPaddedLine(fmt.Sprintf("Constraints: %d not evaluated (model skipped)", len(model.Constraints)))
		} else {
			printPaddedLine(fmt.Sprintf("Constraints: %d passed", len(model.Constraints)))
		}
	}

	// Audits
	if len(model.Audits) > 0 {
		if skipped {
			printPaddedLine(fmt.Sprintf("Audits: %d not evaluated (model skipped)", len(model.Audits)))
		} else {
			printPaddedLine(fmt.Sprintf("Audits: %d passed", len(model.Audits)))
		}
	}

	// Warnings
	if len(model.Warnings) > 0 {
		// Check if any warnings were triggered
		warningCount := 0
		if result != nil {
			for _, w := range result.Warnings {
				// Warning validations produce specific messages
				if strings.Contains(w, "rows") || strings.Contains(w, "warning") {
					warningCount++
				}
			}
		}
		if warningCount > 0 {
			printPaddedLine(fmt.Sprintf("Warnings: %d triggered", warningCount))
			for _, w := range result.Warnings {
				printPaddedLine(fmt.Sprintf("  ! %s", truncateStr(w, 54)))
			}
		} else {
			printPaddedLine(fmt.Sprintf("Warnings: %d checked, 0 triggered", len(model.Warnings)))
		}
	}
}

// isYAMLExt returns true if the file has a .yaml or .yml extension.
func isYAMLExt(path string) bool {
	ext := filepath.Ext(path)
	return ext == ".yaml" || ext == ".yml"
}

// isYAMLModelPath returns true if a YAML file is under the models directory.
func isYAMLModelPath(path, modelsPath string) bool {
	absTarget, err := filepath.Abs(path)
	if err != nil {
		return false
	}
	absModels, err := filepath.Abs(modelsPath)
	if err != nil {
		return false
	}
	return strings.HasPrefix(absTarget, absModels+string(filepath.Separator))
}
