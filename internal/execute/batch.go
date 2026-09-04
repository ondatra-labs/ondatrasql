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

// runTypeValueRow renders one model as a VALUES tuple for
// execute/batch_run_type.sql: (target, current_hash, kind, is_fetch,
// current_config_hash). Shared by the batch and single-model paths so both
// compare exactly the same inputs.
func runTypeValueRow(m *parser.Model, cfgHash string) string {
	hash := backfill.ModelHash(m.SQL, backfill.ModelDirectives{
		Kind:               m.Kind,
		UniqueKey:          m.UniqueKey,
		GroupKey:           m.GroupKey,
		PartitionedBy:      m.PartitionedBy,
		Incremental:        m.Incremental,
		IncrementalInitial: m.IncrementalInitial,
		Fetch:              m.Fetch,
		Push:               m.Push,
	})
	q := func(v string) string { return strings.ReplaceAll(v, "'", "''") }
	return fmt.Sprintf("('%s','%s','%s',%t,'%s')",
		q(m.Target), q(hash), q(m.Kind), m.Fetch, q(cfgHash))
}

// ComputeRunTypeDecisions executes a single batch query to determine run_type
// for all models, reducing N database round-trips to 1.
//
// The query builds a CTE with model info (target, current_hash, kind) and joins
// with DuckDB macros to compute decisions in one pass.
//
// hashFor optionally supplies the config hash that applies to each individual
// model — see Runner.modelConfigHash. It is compared against that model's
// stored `config_hash`, NOT folded into its model hash, which is what the
// config argument meant before config indexing. Models in one project
// legitimately have different config hashes, so a caller with config must
// resolve per model; supplying one value for all of them would yield a
// permanent "config changed" backfill loop. Omit it for callers with no
// config at all (tests, in-memory sessions), where every hash is "".
func ComputeRunTypeDecisions(sess *duckdb.Session, models []*parser.Model, hashFor ...func(*parser.Model) string) (RunTypeDecisions, error) {
	if len(models) == 0 {
		return make(RunTypeDecisions), nil
	}

	resolve := func(*parser.Model) string { return "" }
	if len(hashFor) > 0 && hashFor[0] != nil {
		resolve = hashFor[0]
	}

	// Build VALUES list for model input:
	//   ('t1','h1','k1',f1,'c1'),('t2','h2','k2',f2,'c2')
	// The config hash travels as its own column rather than being folded
	// into the model hash, so the query can report "config changed"
	// separately from "sql changed".
	var valueRows []string
	for _, m := range models {
		valueRows = append(valueRows, runTypeValueRow(m, resolve(m)))
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
//
// configHash must be the hash that applies to THIS model — see
// Runner.modelConfigHash. It is compared against the model's stored
// `config_hash`, not folded into its model hash.
func ComputeSingleRunType(sess *duckdb.Session, model *parser.Model, configHash ...string) (*RunTypeDecision, error) {
	cfgHash := ""
	if len(configHash) > 0 {
		cfgHash = configHash[0]
	}
	// Use the same SQL template with a single VALUE row
	query := sql.MustFormat("execute/batch_run_type.sql", runTypeValueRow(model, cfgHash))

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
