// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package execute

import (
	"context"
	"fmt"
	"path/filepath"

	"github.com/ondatra-labs/ondatrasql/internal/backfill"
	"github.com/ondatra-labs/ondatrasql/internal/duckdb"
	"github.com/ondatra-labs/ondatrasql/internal/libregistry"
	"github.com/ondatra-labs/ondatrasql/internal/parser"
	"github.com/ondatra-labs/ondatrasql/internal/state"
)

// DAGRunResult holds the outcome of a single model execution in a DAG run.
type DAGRunResult struct {
	Model  *parser.Model
	Result *Result
	Err    error
}

// DAGCallback is called after each model executes. Return false to abort the DAG.
type DAGCallback func(model *parser.Model, result *Result, err error) bool

// RunDAG executes models in DAG order with batch decisions, propagation,
// and an optional per-model callback. This is the single source of truth for
// DAG execution logic — used by both the CLI and tests.
//
// Parameters:
//   - sess: DuckDB session (prod or sandbox)
//   - sorted: models in topological order
//   - dependents: reverse-dependency map (target → downstream targets)
//   - dagRunID: shared run ID for all models
//   - gitCommit, gitBranch, gitRepoURL: git metadata (empty strings if unavailable)
//   - callback: called after each model; return false to stop early
//
// Returns all results and errors keyed by target.
func RunDAG(ctx context.Context, sess *duckdb.Session, sorted []*parser.Model,
	dependents map[string][]string, dagRunID string,
	gitCommit, gitBranch, gitRepoURL string,
	projectDir string,
	libReg *libregistry.Registry,
	callback DAGCallback,
) (map[string]*Result, map[string]error) {

	// Validate model + sink compatibility before any execution
	if err := ValidateModelPushCompat(sorted, libReg); err != nil {
		errors := make(map[string]error)
		errors["_validation"] = err
		return nil, errors
	}

	// Open .ondatra/state.duckdb once for the entire DAG. DuckDB takes a
	// process-level file lock; opening per-Runner would conflict mid-DAG.
	// Run GC before sharing the handle with model runners.
	//
	// State open is fatal — without state.duckdb every fetch/push model
	// will fail anyway. GC is non-fatal: a failed cleanup pass is
	// surfaced as a `_gc` error key in the result map but the DAG runs
	// to completion.
	var sharedState *state.State
	gcErrToSurface := error(nil)
	if projectDir != "" {
		st, err := state.Open(projectDir)
		if err != nil {
			errors := make(map[string]error)
			errors["_validation"] = fmt.Errorf("open state.duckdb: %w", err)
			return nil, errors
		}
		gcErrToSurface = state.GC(st)
		sharedState = st
		defer func() { _ = sharedState.Close() }()
	}

	cfgHash := backfill.ConfigHash(filepath.Join(projectDir, "config"))
	decisions, decisionErr := ComputeRunTypeDecisions(sess, sorted, cfgHash)
	if decisionErr != nil {
		errors := make(map[string]error)
		errors["_validation"] = decisionErr
		return nil, errors
	}

	results := make(map[string]*Result, len(sorted))
	errors := make(map[string]error, len(sorted))
	if gcErrToSurface != nil {
		errors["_gc"] = fmt.Errorf("state GC: %w", gcErrToSurface)
	}
	failed := make(map[string]bool) // targets that failed or were skipped due to upstream failure

	for _, model := range sorted {
		// Check context cancellation
		select {
		case <-ctx.Done():
			return results, errors
		default:
		}

		// Skip if any upstream dependency failed
		if hasFailedUpstream(model, dependents, failed) {
			result := &Result{
				Target:  model.Target,
				Kind:    model.Kind,
				RunType: "skip",
			}
			result.Warnings = append(result.Warnings, "skipped: upstream model failed")
			results[model.Target] = result
			failed[model.Target] = true
			if callback != nil {
				if !callback(model, result, nil) {
					break
				}
			}
			continue
		}

		runner := NewRunner(sess, ModeRun, dagRunID)
		if gitCommit != "" {
			runner.SetGitInfo(gitCommit, gitBranch, gitRepoURL)
		}
		runner.SetRunTypeDecisions(decisions)
		if projectDir != "" {
			runner.SetProjectDir(projectDir)
		}
		if sharedState != nil {
			runner.SetStateStore(sharedState)
		}
		if libReg != nil {
			runner.SetLibRegistry(libReg)
		}

		result, err := runner.Run(ctx, model)
		results[model.Target] = result
		if err != nil {
			errors[model.Target] = err
			failed[model.Target] = true
		}

		// Callback
		if callback != nil {
			if !callback(model, result, err) {
				break
			}
		}

		// DAG propagation: if this model ran successfully (not skipped, not failed),
		// invalidate downstream so they recompute their run_type. v0.12.0+: in
		// sandbox mode, the sandbox catalog has both inherited prod commits and
		// new sandbox commits, so the standard recompute path sees the upstream
		// change and there is no need to force "full" — recompute on its own
		// will pick the right run type from the active catalog.
		if err == nil && result != nil && result.RunType != "skip" {
			for _, dep := range dependents[model.Target] {
				delete(decisions, dep)
			}
		}
	}

	return results, errors
}

// hasFailedUpstream checks if any upstream dependency of this model has failed.
// dependents maps target → its downstream models. We check if model.Target
// appears as a downstream of any failed target.
func hasFailedUpstream(model *parser.Model, dependents map[string][]string, failed map[string]bool) bool {
	for failedTarget := range failed {
		for _, dep := range dependents[failedTarget] {
			if dep == model.Target {
				return true
			}
		}
	}
	return false
}
