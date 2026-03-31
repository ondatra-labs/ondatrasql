// OndatraSQL - A data pipeline framework for DuckDB + DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

// Package dag provides impact analysis and smart rebuild functionality.
package dag

import (
	"strings"

	"github.com/ondatra-labs/ondatrasql/internal/backfill"
	"github.com/ondatra-labs/ondatrasql/internal/duckdb"
)

// Impact represents the impact on a downstream model.
type Impact struct {
	Target          string            // The downstream model target
	AffectedColumns []string          // Which output columns are affected
	SourceColumns   []string          // Which source columns from the changed model are used
	Transformations map[string]string // Map of affected column -> transformation type
	RebuildNeeded   bool              // Whether a rebuild is needed
	Reason          string            // Human-readable reason
}

// ImpactAnalysis represents the full impact analysis for a model change.
type ImpactAnalysis struct {
	ChangedModel string
	ChangeType   backfill.SchemaChangeType
	Impacts      []Impact
}

// AnalyzeImpact analyzes what downstream models would be affected if the given model changes.
// It uses the stored column lineage to trace which columns flow downstream.
func AnalyzeImpact(sess *duckdb.Session, changedModel string) (*ImpactAnalysis, error) {
	analysis := &ImpactAnalysis{
		ChangedModel: changedModel,
		ChangeType:   backfill.SchemaChangeNone,
	}

	// Get all downstream models
	downstream, err := backfill.GetDownstreamModels(sess, changedModel)
	if err != nil {
		return nil, err
	}

	// For each downstream model, analyze the impact
	for _, model := range downstream {
		impact, err := analyzeModelImpact(sess, changedModel, model)
		if err != nil {
			// Log warning but continue
			continue
		}
		if impact != nil {
			analysis.Impacts = append(analysis.Impacts, *impact)
		}
	}

	return analysis, nil
}

// analyzeModelImpact analyzes the impact on a single downstream model.
func analyzeModelImpact(sess *duckdb.Session, changedModel, downstreamModel string) (*Impact, error) {
	// Get the commit info for the downstream model (contains column lineage)
	info, err := backfill.GetModelCommitInfo(sess, downstreamModel)
	if err != nil || info == nil {
		return nil, err
	}

	impact := &Impact{
		Target:          downstreamModel,
		RebuildNeeded:   false,
		Transformations: make(map[string]string),
	}

	// Check which columns in downstream come from the changed model
	for _, col := range info.ColumnLineage {
		for _, source := range col.Sources {
			if strings.EqualFold(source.Table, changedModel) {
				impact.AffectedColumns = append(impact.AffectedColumns, col.Column)
				impact.SourceColumns = append(impact.SourceColumns, source.Column)
				impact.RebuildNeeded = true

				// Capture transformation info
				transform := string(source.Transformation)
				if source.FunctionName != "" {
					transform = source.FunctionName + "()"
				}
				if transform != "" && transform != "IDENTITY" {
					impact.Transformations[col.Column] = transform
				}
			}
		}
	}

	if len(impact.AffectedColumns) > 0 {
		impact.Reason = formatReason(impact.AffectedColumns, changedModel)
		return impact, nil
	}

	// Model depends on changedModel but no column lineage found
	// This could mean it's a dependency via FROM without specific columns traced
	impact.RebuildNeeded = true
	impact.Reason = "depends on " + changedModel + " (full table reference)"
	return impact, nil
}

// formatReason creates a human-readable reason string.
func formatReason(affectedColumns []string, changedModel string) string {
	if len(affectedColumns) == 1 {
		return "uses column " + affectedColumns[0] + " from " + changedModel
	}
	return "uses " + strings.Join(affectedColumns, ", ") + " from " + changedModel
}

// AnalyzeTransitiveImpact analyzes how a transitive model is affected through an intermediate model.
// changedModel is the direct impact, transitiveModel is the model we're analyzing.
func AnalyzeTransitiveImpact(sess *duckdb.Session, changedModel, transitiveModel string) (*Impact, error) {
	// Check if transitiveModel directly depends on changedModel
	downstream, err := backfill.GetDownstreamModels(sess, changedModel)
	if err != nil {
		return nil, err
	}

	isDirectDep := false
	for _, d := range downstream {
		if d == transitiveModel {
			isDirectDep = true
			break
		}
	}

	if !isDirectDep {
		return nil, nil // Not a direct dependency
	}

	// Analyze the impact
	return analyzeModelImpact(sess, changedModel, transitiveModel)
}

// GetFullImpactTree returns all models affected by a change, including transitive dependencies.
// Returns models in topological order (direct dependencies first, then their dependents).
func GetFullImpactTree(sess *duckdb.Session, changedModel string) ([]string, error) {
	var result []string
	visited := make(map[string]bool)

	var traverse func(model string) error
	traverse = func(model string) error {
		if visited[model] {
			return nil
		}
		visited[model] = true

		downstream, err := backfill.GetDownstreamModels(sess, model)
		if err != nil {
			return err
		}

		for _, d := range downstream {
			result = append(result, d)
			if err := traverse(d); err != nil {
				return err
			}
		}
		return nil
	}

	if err := traverse(changedModel); err != nil {
		return nil, err
	}

	return result, nil
}
