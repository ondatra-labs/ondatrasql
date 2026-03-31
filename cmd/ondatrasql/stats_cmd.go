// OndatraSQL - A data pipeline framework for DuckDB + DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package main

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/ondatra-labs/ondatrasql/internal/config"
	"github.com/ondatra-labs/ondatrasql/internal/duckdb"
	"github.com/ondatra-labs/ondatrasql/internal/output"
	"github.com/ondatra-labs/ondatrasql/internal/sql"
)

// ProjectStats holds project-level statistics.
type ProjectStats struct {
	// Counts
	ModelCount int
	Snapshots  int
	TotalRuns  int

	// Processing
	TotalRows   int64
	AvgDuration float64

	// Timing
	LastRun string

	// By kind breakdown (ordered)
	KindBreakdown []KindCount

	// All models
	AllModels []ModelStats
}

// KindCount holds count for a kind.
type KindCount struct {
	Kind  string
	Count int
}

// ModelStats holds stats for a single model.
type ModelStats struct {
	Name     string
	Kind     string
	RunType  string
	Rows     int64
	Duration int64
	LastRun  string
}

// runStats executes the stats command with nice formatting.
func runStats(cfg *config.Config) error {
	sess, err := duckdb.NewSession("")
	if err != nil {
		return fmt.Errorf("create session: %w", err)
	}
	defer sess.Close()

	if err := sess.InitWithCatalog(cfg.ConfigPath); err != nil {
		return fmt.Errorf("init session: %w", err)
	}

	stats, err := gatherProjectStats(sess)
	if err != nil {
		return fmt.Errorf("gather stats: %w", err)
	}

	printStatsBox(stats)
	return nil
}

// gatherProjectStats collects project-level statistics.
func gatherProjectStats(sess *duckdb.Session) (*ProjectStats, error) {
	stats := &ProjectStats{}

	// Get basic counts
	basicQuery, err := sql.LoadQuery("stats_basic")
	if err != nil {
		return nil, fmt.Errorf("load stats_basic query: %w", err)
	}

	rows, err := sess.QueryRowsMap(basicQuery)
	if err != nil {
		return nil, fmt.Errorf("basic stats: %w", err)
	}
	if len(rows) > 0 {
		row := rows[0]
		stats.ModelCount, _ = strconv.Atoi(row["models"])
		stats.TotalRuns, _ = strconv.Atoi(row["total_runs"])
		stats.TotalRows, _ = strconv.ParseInt(row["total_rows"], 10, 64)
		stats.AvgDuration, _ = strconv.ParseFloat(row["avg_duration"], 64)
		stats.LastRun = row["last_run"]
	}

	// Get snapshot count
	snapshotQuery := fmt.Sprintf("SELECT COUNT(*) as cnt FROM %s.snapshots()", sess.CatalogAlias())
	snapRows, err := sess.QueryRowsMap(snapshotQuery)
	if err == nil && len(snapRows) > 0 {
		stats.Snapshots, _ = strconv.Atoi(snapRows[0]["cnt"])
	}

	// Get kind breakdown (ordered by count DESC)
	kindQuery, err := sql.LoadQuery("stats_kind_breakdown")
	if err != nil {
		return nil, fmt.Errorf("load stats_kind_breakdown query: %w", err)
	}

	kindRows, err := sess.QueryRowsMap(kindQuery)
	if err == nil {
		for _, row := range kindRows {
			kind := row["kind"]
			cnt, _ := strconv.Atoi(row["cnt"])
			stats.KindBreakdown = append(stats.KindBreakdown, KindCount{Kind: kind, Count: cnt})
		}
	}

	// Get all models with latest stats
	modelsQuery, err := sql.LoadQuery("stats_all_models")
	if err != nil {
		return nil, fmt.Errorf("load stats_all_models query: %w", err)
	}

	modelRows, err := sess.QueryRowsMap(modelsQuery)
	if err == nil {
		for _, row := range modelRows {
			rows, _ := strconv.ParseInt(row["rows"], 10, 64)
			duration, _ := strconv.ParseInt(row["duration"], 10, 64)
			stats.AllModels = append(stats.AllModels, ModelStats{
				Name:     row["model"],
				Kind:     row["kind"],
				RunType:  row["run_type"],
				Rows:     rows,
				Duration: duration,
				LastRun:  row["last_run"],
			})
		}
	}

	return stats, nil
}

// printStatsBox prints project stats in a nice box format.
func printStatsBox(stats *ProjectStats) {
	// Title
	printTopBorder()
	printCenteredLine("Project Statistics")
	printSectionBorder("")

	// Overview section
	printEmptyLine()
	printTwoColumns("Models", fmt.Sprintf("%d", stats.ModelCount), "Snapshots", fmt.Sprintf("%d", stats.Snapshots))
	printTwoColumns("Total Runs", fmt.Sprintf("%d", stats.TotalRuns), "Avg Duration", fmt.Sprintf("%.0fms", stats.AvgDuration))
	printTwoColumns("Rows Processed", formatNumber(stats.TotalRows), "Last Run", stats.LastRun)
	printEmptyLine()

	// Kind breakdown section
	if len(stats.KindBreakdown) > 0 {
		printSectionBorder("Models by Kind")
		printEmptyLine()

		// Build kind string with bars
		total := 0
		for _, kc := range stats.KindBreakdown {
			total += kc.Count
		}

		for _, kc := range stats.KindBreakdown {
			pct := float64(kc.Count) / float64(total) * 100
			barWidth := int(pct / 100 * 30)
			if barWidth < 1 && kc.Count > 0 {
				barWidth = 1
			}
			bar := strings.Repeat("█", barWidth)
			line := fmt.Sprintf("%-12s %s %d (%.0f%%)", kc.Kind, bar, kc.Count, pct)
			printPaddedLine(line)
		}
		printEmptyLine()
	}

	// All models section - grouped by schema
	if len(stats.AllModels) > 0 {
		// Group models by schema
		schemaModels := make(map[string][]ModelStats)
		schemaOrder := []string{}
		for _, m := range stats.AllModels {
			parts := strings.SplitN(m.Name, ".", 2)
			schema := parts[0]
			if _, exists := schemaModels[schema]; !exists {
				schemaOrder = append(schemaOrder, schema)
			}
			schemaModels[schema] = append(schemaModels[schema], m)
		}

		printSectionBorder("All Models")
		printEmptyLine()
		printPaddedLine(fmt.Sprintf("  %-22s %-9s %5s %5s   %5s", "Name", "Kind", "Rows", "ms", "Last"))

		for _, schema := range schemaOrder {
			models := schemaModels[schema]
			printEmptyLine()
			printPaddedLine(fmt.Sprintf("── %s ──", schema))
			printEmptyLine()

			for _, m := range models {
				// Extract just the table name (without schema)
				parts := strings.SplitN(m.Name, ".", 2)
				name := parts[0]
				if len(parts) > 1 {
					name = parts[1]
				}
				if len(name) > 22 {
					name = name[:19] + "..."
				}
				line := fmt.Sprintf("  %-22s %-9s %5d %5d   %5s", name, m.Kind, m.Rows, m.Duration, m.LastRun)
				printPaddedLine(line)
			}
		}
		printEmptyLine()
	}

	printBottomBorder()

	// Commands
	output.Println()
	output.Println("Commands:")
	output.Println("  ondatrasql run             # Run all models")
	output.Println("  ondatrasql history         # Show run history")
	output.Println("  ondatrasql describe <m>    # Model details")
}
