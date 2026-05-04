// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
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

// statsSchemaVersion is the JSON schema version for the `stats` command
// output. Bump on breaking changes to ProjectStats / ModelStats /
// KindCount field shape.
//
// Version history:
//   - 1: initial release with snake_case JSON fields
const statsSchemaVersion = 1

// ProjectStats holds project-level statistics.
//
// JSON field names use snake_case for consistency with the rest of the
// CLI's machine-readable output (validate, describe, history).
type ProjectStats struct {
	// SchemaVersion lets typed clients detect breaking changes.
	SchemaVersion int `json:"schema_version"`

	// Counts
	ModelCount int `json:"model_count"`
	Snapshots  int `json:"snapshots"`
	TotalRuns  int `json:"total_runs"`

	// Processing
	TotalRows   int64   `json:"total_rows"`
	AvgDuration float64 `json:"avg_duration_ms"`

	// Timing
	LastRun string `json:"last_run,omitempty"`

	// By kind breakdown (ordered). Always emitted as an array (possibly
	// empty) so JSON consumers see a stable shape — empty project must
	// look the same as populated project.
	KindBreakdown []KindCount `json:"kind_breakdown"`

	// All models. Always emitted as an array (see KindBreakdown).
	AllModels []ModelStats `json:"all_models"`

	// DuckLake info
	CatalogType string `json:"catalog_type,omitempty"`
	DataPath    string `json:"data_path,omitempty"`
	DuckLakeVer string `json:"ducklake_version,omitempty"`

	// Warnings surface non-fatal partial-output failures (a sub-query
	// failed but the rest of the report is intact). Always emitted as
	// `[]` when empty so typed clients can decode unconditionally —
	// removed `omitempty` for shape parity with history/run envelopes
	// (R7 #5). The runtime path appends to the slice via append, which
	// promotes a nil slice to a populated one; add an explicit
	// initialisation in runStats so the encoded JSON is `[]` rather
	// than `null` on the no-warnings path.
	Warnings []string `json:"warnings"`
}

// KindCount holds count for a kind.
type KindCount struct {
	Kind  string `json:"kind"`
	Count int    `json:"count"`
}

// ModelStats holds stats for a single model.
//
//lintcheck:nojsonversion nested under ProjectStats.AllModels; versioned via the wrapping payload.
type ModelStats struct {
	Name     string `json:"name"`
	Kind     string `json:"kind"`
	RunType  string `json:"run_type,omitempty"`
	Rows     int64  `json:"rows"`
	Duration int64  `json:"duration_ms"`
	LastRun  string `json:"last_run,omitempty"`
	// Orphaned is true when the table exists in the catalog but its
	// model file no longer exists on disk. Kept as a separate flag so
	// JSON consumers can rely on Name as a stable identifier instead
	// of parsing presentation labels.
	Orphaned bool `json:"orphaned,omitempty"`
}

// runStats executes the stats command with nice formatting.
func runStats(cfg *config.Config) error {
	sess, err := duckdb.NewSession("")
	if err != nil {
		return fmt.Errorf("create session: %w", err)
	}
	defer closeSessionOrLog(sess)

	if err := sess.InitWithCatalog(cfg.ConfigPath); err != nil {
		return fmt.Errorf("init session: %w", err)
	}

	stats, err := gatherProjectStats(sess)
	if err != nil {
		return fmt.Errorf("gather stats: %w", err)
	}

	// Annotate orphans: tables in the catalog that no longer have a model
	// file on disk. Visible to the user with an [orphaned] tag so they
	// notice and can clean up. (Bug 14)
	annotateOrphans(stats, cfg)

	// In --json mode, emit the structured stats to stdout. The box rendering
	// still runs but its output is routed to stderr by the output package.
	if output.JSONEnabled {
		output.EmitJSON(stats)
		return nil
	}

	printStatsBox(stats)
	return nil
}

// gatherProjectStats collects project-level statistics.
func gatherProjectStats(sess *duckdb.Session) (*ProjectStats, error) {
	// Initialise slice fields to non-nil empty slices so they JSON-encode
	// as `[]` rather than `null` when the project has no models / kinds.
	// Stable shape lets typed clients rely on the field always being an
	// array, populated or not.
	stats := &ProjectStats{
		SchemaVersion: statsSchemaVersion,
		KindBreakdown: []KindCount{},
		AllModels:     []ModelStats{},
		Warnings:      []string{},
	}

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
		// Surface parse failures as warnings (matching describe.GatherWarnings).
		// COUNT(*)/COALESCE(...) results should always parse, but a malformed
		// snapshot extra-info JSON could yield a non-numeric string — silent
		// fallback to 0 would hide that the report is partial.
		if v, perr := strconv.Atoi(row["models"]); perr == nil {
			stats.ModelCount = v
		} else if row["models"] != "" {
			stats.Warnings = append(stats.Warnings, fmt.Sprintf("parse models: %v", perr))
		}
		if v, perr := strconv.Atoi(row["total_runs"]); perr == nil {
			stats.TotalRuns = v
		} else if row["total_runs"] != "" {
			stats.Warnings = append(stats.Warnings, fmt.Sprintf("parse total_runs: %v", perr))
		}
		if v, perr := strconv.ParseInt(row["total_rows"], 10, 64); perr == nil {
			stats.TotalRows = v
		} else if row["total_rows"] != "" {
			stats.Warnings = append(stats.Warnings, fmt.Sprintf("parse total_rows: %v", perr))
		}
		if v, perr := strconv.ParseFloat(row["avg_duration"], 64); perr == nil {
			stats.AvgDuration = v
		} else if row["avg_duration"] != "" {
			stats.Warnings = append(stats.Warnings, fmt.Sprintf("parse avg_duration: %v", perr))
		}
		stats.LastRun = row["last_run"]
	}

	// Get snapshot count
	snapshotQuery := "SELECT COUNT(*) as cnt FROM snapshots()"
	snapRows, err := sess.QueryRowsMap(snapshotQuery)
	if err != nil {
		stats.Warnings = append(stats.Warnings, fmt.Sprintf("snapshot count: %v", err))
	} else if len(snapRows) > 0 {
		if v, perr := strconv.Atoi(snapRows[0]["cnt"]); perr == nil {
			stats.Snapshots = v
		} else if snapRows[0]["cnt"] != "" {
			stats.Warnings = append(stats.Warnings, fmt.Sprintf("parse snapshot count: %v", perr))
		}
	}

	// Get kind breakdown (ordered by count DESC)
	kindQuery, err := sql.LoadQuery("stats_kind_breakdown")
	if err != nil {
		return nil, fmt.Errorf("load stats_kind_breakdown query: %w", err)
	}

	kindRows, err := sess.QueryRowsMap(kindQuery)
	if err != nil {
		stats.Warnings = append(stats.Warnings, fmt.Sprintf("kind breakdown: %v", err))
	} else {
		for _, row := range kindRows {
			kind := row["kind"]
			if v, perr := strconv.Atoi(row["cnt"]); perr == nil {
				stats.KindBreakdown = append(stats.KindBreakdown, KindCount{Kind: kind, Count: v})
			} else if row["cnt"] != "" {
				stats.Warnings = append(stats.Warnings, fmt.Sprintf("parse kind_breakdown[%s].count: %v", kind, perr))
			} else {
				stats.KindBreakdown = append(stats.KindBreakdown, KindCount{Kind: kind, Count: 0})
			}
		}
	}

	// Get all models with latest stats
	modelsQuery, err := sql.LoadQuery("stats_all_models")
	if err != nil {
		return nil, fmt.Errorf("load stats_all_models query: %w", err)
	}

	modelRows, err := sess.QueryRowsMap(modelsQuery)
	if err != nil {
		stats.Warnings = append(stats.Warnings, fmt.Sprintf("all models: %v", err))
	} else {
		for _, row := range modelRows {
			modelName := row["model"]
			rows, rowsErr := strconv.ParseInt(row["rows"], 10, 64)
			if rowsErr != nil && row["rows"] != "" {
				stats.Warnings = append(stats.Warnings, fmt.Sprintf("parse all_models[%s].rows: %v", modelName, rowsErr))
			}
			duration, durErr := strconv.ParseInt(row["duration"], 10, 64)
			if durErr != nil && row["duration"] != "" {
				stats.Warnings = append(stats.Warnings, fmt.Sprintf("parse all_models[%s].duration_ms: %v", modelName, durErr))
			}
			stats.AllModels = append(stats.AllModels, ModelStats{
				Name:     modelName,
				Kind:     row["kind"],
				RunType:  row["run_type"],
				Rows:     rows,
				Duration: duration,
				LastRun:  row["last_run"],
			})
		}
	}

	// Get DuckLake settings
	alias := sess.CatalogAlias()
	if alias == "" {
		alias = "lake"
	}
	dlRows, err := sess.QueryRowsMap(fmt.Sprintf("SELECT * FROM ducklake_settings('%s')", alias))
	if err != nil {
		stats.Warnings = append(stats.Warnings, fmt.Sprintf("ducklake settings: %v", err))
	} else if len(dlRows) > 0 {
		stats.CatalogType = dlRows[0]["catalog_type"]
		stats.DataPath = dlRows[0]["data_path"]
		stats.DuckLakeVer = dlRows[0]["extension_version"]
	}

	return stats, nil
}

// annotateOrphans flags rows in stats.AllModels whose target no longer
// has a model file on disk. The catalog table persists after the file
// is deleted; without annotation users see no warning. (Bug 14)
//
// Sets the Orphaned bool only — Name stays as the canonical schema.table
// identifier so JSON consumers (--json mode) can rely on it. The box
// renderer appends a visual " [orphaned]" suffix at print time. (Review
// finding 3)
func annotateOrphans(stats *ProjectStats, cfg *config.Config) {
	models, err := loadModelsFromDir(cfg)
	if err != nil {
		// Surface the failure so the user knows orphan detection didn't
		// run — silently leaving stats untouched would hide stale
		// catalog rows that should have been flagged.
		stats.Warnings = append(stats.Warnings, fmt.Sprintf("orphan detection skipped: %v", err))
		return
	}
	known := make(map[string]bool, len(models))
	for _, m := range models {
		known[m.Target] = true
	}
	for i, m := range stats.AllModels {
		if !known[m.Name] {
			stats.AllModels[i].Orphaned = true
		}
	}
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
	if stats.CatalogType != "" {
		printTwoColumns("Catalog", stats.CatalogType, "DuckLake", stats.DuckLakeVer)
		if stats.DataPath != "" {
			printPaddedLine(fmt.Sprintf("Data Path: %s", stats.DataPath))
		}
	}
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
			var pct float64
			if total > 0 {
				pct = float64(kc.Count) / float64(total) * 100
			}
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
				// Visual-only orphan tag — Name in the struct stays clean
				// so --json consumers can use it as a stable identifier.
				if m.Orphaned {
					name += " [orphaned]"
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

	if len(stats.Warnings) > 0 {
		printSectionBorder("Warnings")
		printEmptyLine()
		for _, w := range stats.Warnings {
			printPaddedLine("  " + w)
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
