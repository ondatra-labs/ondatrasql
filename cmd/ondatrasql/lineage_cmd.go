// OndatraSQL - A data pipeline framework for DuckDB + DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package main

import (
	"fmt"
	"strings"

	"github.com/ondatra-labs/ondatrasql/internal/backfill"
	"github.com/ondatra-labs/ondatrasql/internal/config"
	"github.com/ondatra-labs/ondatrasql/internal/duckdb"
	"github.com/ondatra-labs/ondatrasql/internal/lineage"
	"github.com/ondatra-labs/ondatrasql/internal/output"
	"github.com/ondatra-labs/ondatrasql/internal/sql"
)

func runLineage(cfg *config.Config, args []string) error {
	sess, err := duckdb.NewSession("")
	if err != nil {
		return fmt.Errorf("create session: %w", err)
	}
	defer sess.Close()

	if err := sess.InitWithCatalog(cfg.ConfigPath); err != nil {
		return fmt.Errorf("init session: %w", err)
	}

	// Parse args to determine mode
	var target string
	overview := false

	for _, arg := range args {
		if arg == "overview" {
			overview = true
		} else {
			target = arg
		}
	}

	if overview {
		// Overview mode - show all models
		return runLineageOverview(sess)
	}

	if target == "" {
		return fmt.Errorf("usage: ondatrasql lineage overview | <model> | <model.column>")
	}

	// Determine if this is model or column focus
	parts := strings.Split(target, ".")
	if len(parts) >= 3 {
		// Column focus: schema.table.column
		modelName := parts[0] + "." + parts[1]
		return runLineageColumnFocus(sess, modelName, target)
	}

	// Model focus: schema.table
	return runLineageModelFocus(sess, target)
}

// runLineageOverview shows all models collapsed
func runLineageOverview(sess *duckdb.Session) error {
	// Get all models from snapshots
	query, err := sql.LoadQuery("lineage_all_models")
	if err != nil {
		return fmt.Errorf("load lineage_all_models query: %w", err)
	}
	rows, err := sess.QueryRows(query)
	if err != nil {
		return fmt.Errorf("list models: %w", err)
	}

	var models []*lineage.ModelLineage
	for _, m := range rows {
		info, err := backfill.GetModelCommitInfo(sess, m)
		if err != nil || info == nil {
			continue
		}
		models = append(models, convertToModelLineage(m, info))
	}

	view := lineage.NewLineageView(models)
	view.Mode = lineage.ViewOverview
	output.Println(view.Render())
	return nil
}

// runLineageModelFocus shows selected model expanded
func runLineageModelFocus(sess *duckdb.Session, target string) error {
	info, err := backfill.GetModelCommitInfo(sess, target)
	if err != nil {
		return fmt.Errorf("get model info: %w", err)
	}
	if info == nil {
		return fmt.Errorf("no commit info found for %s", target)
	}

	// Build full lineage chain
	models := buildLineageChain(sess, target, info, make(map[string]bool))

	view := lineage.NewLineageView(models)
	view.SetTarget(target)
	output.Println(view.Render())
	return nil
}

// runLineageColumnFocus shows column trace through all models
func runLineageColumnFocus(sess *duckdb.Session, modelName, fullTarget string) error {
	info, err := backfill.GetModelCommitInfo(sess, modelName)
	if err != nil {
		return fmt.Errorf("get model info: %w", err)
	}
	if info == nil {
		return fmt.Errorf("no commit info found for %s", modelName)
	}

	// Build full lineage chain
	models := buildLineageChain(sess, modelName, info, make(map[string]bool))

	view := lineage.NewLineageView(models)
	view.SetTarget(fullTarget)
	output.Println(view.Render())
	return nil
}

// convertToModelLineage converts CommitInfo to ModelLineage
func convertToModelLineage(name string, info *backfill.CommitInfo) *lineage.ModelLineage {
	cols := make([]string, len(info.Columns))
	for i, c := range info.Columns {
		cols[i] = c.Name
	}

	colLineage := make([]lineage.ColumnLineageInfo, len(info.ColumnLineage))
	for i, cl := range info.ColumnLineage {
		sources := make([]lineage.ColumnSource, len(cl.Sources))
		for j, src := range cl.Sources {
			sources[j] = lineage.ColumnSource{
				Table:          src.Table,
				Column:         src.Column,
				Transformation: src.Transformation,
				FunctionName:   src.FunctionName,
			}
		}
		colLineage[i] = lineage.ColumnLineageInfo{
			Column:  cl.Column,
			Sources: sources,
		}
	}

	return &lineage.ModelLineage{
		Name:          name,
		Columns:       cols,
		ColumnLineage: colLineage,
		Dependencies:  info.Depends,
	}
}

// buildLineageChain recursively builds the full lineage chain
func buildLineageChain(sess *duckdb.Session, target string, info *backfill.CommitInfo, visited map[string]bool) []*lineage.ModelLineage {
	if visited[target] {
		return nil
	}
	visited[target] = true

	// Convert columns to string slice
	cols := make([]string, len(info.Columns))
	for i, c := range info.Columns {
		cols[i] = c.Name
	}

	// Convert column lineage
	colLineage := make([]lineage.ColumnLineageInfo, len(info.ColumnLineage))
	for i, cl := range info.ColumnLineage {
		sources := make([]lineage.ColumnSource, len(cl.Sources))
		for j, src := range cl.Sources {
			sources[j] = lineage.ColumnSource{
				Table:          src.Table,
				Column:         src.Column,
				Transformation: src.Transformation,
				FunctionName:   src.FunctionName,
			}
		}
		colLineage[i] = lineage.ColumnLineageInfo{
			Column:  cl.Column,
			Sources: sources,
		}
	}

	model := &lineage.ModelLineage{
		Name:          target,
		Columns:       cols,
		ColumnLineage: colLineage,
		Dependencies:  info.Depends,
	}

	result := []*lineage.ModelLineage{model}

	// Recursively get dependencies
	for _, dep := range info.Depends {
		depInfo, err := backfill.GetModelCommitInfo(sess, dep)
		if err != nil || depInfo == nil {
			// Add placeholder for missing dependency
			result = append(result, &lineage.ModelLineage{
				Name:    dep,
				Columns: []string{"?"},
			})
			continue
		}
		depModels := buildLineageChain(sess, dep, depInfo, visited)
		result = append(result, depModels...)
	}

	return result
}

// renderLineageGraph renders the full lineage as ASCII art
func renderLineageGraph(target string, models []*lineage.ModelLineage) string {
	if len(models) == 0 {
		return "No lineage data available"
	}

	graph := lineage.BuildLineageGraph(models)
	return graph.Render()
}
