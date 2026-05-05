// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

// Package execute provides batch query capabilities for run_type decisions.
package execute

import (
	"fmt"
	"strings"

	"github.com/ondatra-labs/ondatrasql/internal/backfill"
	"github.com/ondatra-labs/ondatrasql/internal/duckdb"
	"github.com/ondatra-labs/ondatrasql/internal/parser"
	"github.com/ondatra-labs/ondatrasql/internal/sql"
)

// RunTypeDecision contains the pre-computed run type for a model.
type RunTypeDecision struct {
	RunType string // "incremental", "backfill", "full", or "skip"
	Reason  string // Human-readable reason for the decision
}

// RunTypeDecisions maps target names to their run type decisions.
type RunTypeDecisions map[string]*RunTypeDecision

// ComputeRunTypeDecisions executes a single batch query to determine run_type
// for all models, reducing N database round-trips to 1.
//
// The query builds a CTE with model info (target, current_hash, kind) and joins
// with DuckDB macros to compute decisions in one pass.
func ComputeRunTypeDecisions(sess *duckdb.Session, models []*parser.Model, configHash ...string) (RunTypeDecisions, error) {
	if len(models) == 0 {
		return make(RunTypeDecisions), nil
	}

	// Build model info: compute SQL hash for each model
	type modelInfo struct {
		target      string
		currentHash string
		kind        string
		fetch       bool
	}

	cfgHash := ""
	if len(configHash) > 0 {
		cfgHash = configHash[0]
	}

	var infos []modelInfo
	for _, m := range models {
		infos = append(infos, modelInfo{
			target:      m.Target,
			currentHash: backfill.ModelHash(m.SQL, backfill.ModelDirectives{
				Kind:               m.Kind,
				UniqueKey:          m.UniqueKey,
				GroupKey:           m.GroupKey,
				PartitionedBy:      m.PartitionedBy,
				Incremental:        m.Incremental,
				IncrementalInitial: m.IncrementalInitial,
				Fetch:              m.Fetch,
				Push:               m.Push,
				ConfigHash:         cfgHash,
			}),
			kind:  m.Kind,
			fetch: m.Fetch,
		})
	}

	if len(infos) == 0 {
		return make(RunTypeDecisions), nil
	}

	// Build VALUES list for model input: ('t1','h1','k1',f1),('t2','h2','k2',f2)
	var valueRows []string
	for _, info := range infos {
		// Escape values for SQL safety
		target := strings.ReplaceAll(info.target, "'", "''")
		hash := strings.ReplaceAll(info.currentHash, "'", "''")
		kind := strings.ReplaceAll(info.kind, "'", "''")
		valueRows = append(valueRows, fmt.Sprintf("('%s','%s','%s',%t)", target, hash, kind, info.fetch))
	}

	// v0.12.0+: snapshots() resolves via USE to the active catalog. In sandbox
	// mode the catalog is a fork of prod with both inherited prod commits
	// and new sandbox commits, so the recompute path correctly sees
	// upstream models that ran in this sandbox session.
	query := sql.MustFormat("execute/batch_run_type.sql", strings.Join(valueRows, ","))

	// Execute batch query
	rows, err := sess.QueryRowsMap(query)
	if err != nil {
		return nil, fmt.Errorf("batch run_type query: %w", err)
	}

	// Build decisions map - run_type is computed entirely in SQL
	decisions := make(RunTypeDecisions)

	for _, row := range rows {
		decisions[row["target"]] = &RunTypeDecision{
			RunType: row["run_type"],
			Reason:  row["run_reason"],
		}
	}

	return decisions, nil
}

// GetDecision returns the full decision for a model.
// Returns nil if not found or if decisions map is nil.
func (d RunTypeDecisions) GetDecision(target string) *RunTypeDecision {
	if d == nil {
		return nil
	}
	return d[target]
}

// ComputeSingleRunType computes run_type for a single model using the same SQL logic as batch.
// This provides consistency between single model runs and run_all.
func ComputeSingleRunType(sess *duckdb.Session, model *parser.Model, configHash ...string) (*RunTypeDecision, error) {
	cfgHash := ""
	if len(configHash) > 0 {
		cfgHash = configHash[0]
	}
	// Use the same SQL template with a single VALUE row
	target := strings.ReplaceAll(model.Target, "'", "''")
	hash := strings.ReplaceAll(backfill.ModelHash(model.SQL, backfill.ModelDirectives{
		Kind:               model.Kind,
		UniqueKey:          model.UniqueKey,
		GroupKey:           model.GroupKey,
		PartitionedBy:      model.PartitionedBy,
		Incremental:        model.Incremental,
		IncrementalInitial: model.IncrementalInitial,
		Fetch:              model.Fetch,
		Push:               model.Push,
		ConfigHash:         cfgHash,
	}), "'", "''")
	kind := strings.ReplaceAll(model.Kind, "'", "''")
	valueRow := fmt.Sprintf("('%s','%s','%s',%t)", target, hash, kind, model.Fetch)

	query := sql.MustFormat("execute/batch_run_type.sql", valueRow)

	rows, err := sess.QueryRowsMap(query)
	if err != nil {
		return nil, fmt.Errorf("run_type query: %w", err)
	}

	if len(rows) == 0 {
		// Fallback if no result (shouldn't happen)
		return &RunTypeDecision{RunType: "backfill"}, nil
	}

	row := rows[0]
	decision := &RunTypeDecision{
		RunType: row["run_type"],
		Reason:  row["run_reason"],
	}

	return decision, nil
}
