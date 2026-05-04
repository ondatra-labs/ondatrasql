// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package main

import (
	"fmt"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/ondatra-labs/ondatrasql/internal/backfill"
	"github.com/ondatra-labs/ondatrasql/internal/config"
	"github.com/ondatra-labs/ondatrasql/internal/duckast"
	"github.com/ondatra-labs/ondatrasql/internal/duckdb"
	"github.com/ondatra-labs/ondatrasql/internal/libcall"
	"github.com/ondatra-labs/ondatrasql/internal/libregistry"
	"github.com/ondatra-labs/ondatrasql/internal/lineage"
	"github.com/ondatra-labs/ondatrasql/internal/output"
	"github.com/ondatra-labs/ondatrasql/internal/parser"
)

const boxWidth = 64

// Box-drawing characters
const (
	topLeft     = "╔"
	topRight    = "╗"
	bottomLeft  = "╚"
	bottomRight = "╝"
	horizontal  = "═"
	vertical    = "║"
	leftT       = "╠"
	rightT      = "╣"
	topT        = "╦"
	bottomT     = "╩"
)

// describeModelSchemaVersion is the JSON schema version for the
// `describe <model>` output. Bump on breaking changes (field renames,
// type changes, removed fields).
//
// Version history:
//   - 1: initial release with snake_case fields, blueprint cross-link,
//     blueprint_error degradation field
const describeModelSchemaVersion = 1

// ModelInfo holds all information about a model for display.
//
// JSON field names use snake_case for consistency with the rest of the
// CLI's machine-readable output (validate, describe blueprint).
type ModelInfo struct {
	// SchemaVersion lets typed clients detect breaking changes to the
	// describe output. Always emitted, never omitempty.
	SchemaVersion int `json:"schema_version"`

	// Basic info
	Target          string             `json:"target"`
	Kind            string             `json:"kind"`
	Schema          string             `json:"schema,omitempty"`
	Materialization string             `json:"materialization,omitempty"`
	SourceFile      string             `json:"source_file,omitempty"`
	ScriptType      parser.ScriptType  `json:"script_type,omitempty"`

	// Blueprint cross-link: name of the lib function this model fetches
	// from (e.g. "mistral_ocr") so agents can follow up with
	// `describe blueprint <name>`. Empty for models without a lib call.
	Blueprint string `json:"blueprint,omitempty"`

	// BlueprintError captures why blueprint detection couldn't run for
	// a model that may have a lib call. Empty when detection ran cleanly
	// (which includes "no lib call detected" and "blueprint resolved").
	// Populated when ScanLenient saw the project's libs as broken or
	// AST serialization for the model SQL failed — the user gets a
	// structured signal that the cross-link is unverified rather than
	// a silent missing field.
	BlueprintError string `json:"blueprint_error,omitempty"`

	// Statistics
	TotalRuns   int    `json:"total_runs,omitempty"`
	AvgDuration int64  `json:"avg_duration_ms,omitempty"`
	TotalRows   int64  `json:"total_rows,omitempty"`
	TableSize   string `json:"table_size,omitempty"`
	SuccessRate int    `json:"success_rate,omitempty"`
	LastRun     string `json:"last_run,omitempty"`
	FirstRun    string `json:"first_run,omitempty"`

	// Dependencies
	Dependencies []string `json:"dependencies,omitempty"`
	Downstream   []string `json:"downstream,omitempty"`

	// Columns
	Columns []ColumnInfo `json:"columns,omitempty"`

	// Recent runs
	RecentRuns []RunInfo `json:"recent_runs,omitempty"`

	// Definition (SQL or Starlark code)
	Definition string `json:"definition,omitempty"`

	// Description is the table-level comment from @description directive.
	Description string `json:"description,omitempty"`

	// Data Quality Rules
	Constraints []string `json:"constraints,omitempty"`
	Audits      []string `json:"audits,omitempty"`
	Warnings    []string `json:"warnings,omitempty"`

	// Execution profile (from last run)
	Steps     []StepInfo `json:"steps,omitempty"`
	DuckDBVer string     `json:"duckdb_version,omitempty"`
	GitCommit string     `json:"git_commit,omitempty"`

	// GatherWarnings surfaces non-fatal failures encountered while
	// collecting the descriptive payload (a stats query failed, a
	// numeric column failed to parse). Distinct from the user-defined
	// @warning directive list above.
	GatherWarnings []string `json:"gather_warnings,omitempty"`
}

// ColumnInfo represents a column with its metadata.
//
//lintcheck:nojsonversion nested under ModelInfo; the wrapping payload's SchemaVersion versions this field too.
type ColumnInfo struct {
	Name        string `json:"name"`
	Type        string `json:"type"`
	Nullable    string `json:"nullable,omitempty"`
	Constraint  string `json:"constraint,omitempty"`
	Description string `json:"description,omitempty"`
}

// RunInfo represents a single run.
//
//lintcheck:nojsonversion nested under ModelInfo.RecentRuns; versioned via the wrapping payload.
type RunInfo struct {
	Timestamp string `json:"timestamp"`
	Duration  int64  `json:"duration_ms"`
	Rows      int64  `json:"rows"`
	Success   bool   `json:"success"`
}

// StepInfo represents an execution step.
//
//lintcheck:nojsonversion nested under ModelInfo.Steps; versioned via the wrapping payload.
type StepInfo struct {
	Name       string `json:"name"`
	DurationMs int64  `json:"duration_ms"`
	Status     string `json:"status"`
}

// runDescribe executes the describe command for a model.
func runDescribe(cfg *config.Config, target string) error {
	sess, err := duckdb.NewSession("")
	if err != nil {
		return fmt.Errorf("create session: %w", err)
	}
	defer closeSessionOrLog(sess)

	if err := sess.InitWithCatalog(cfg.ConfigPath); err != nil {
		return fmt.Errorf("init session: %w", err)
	}

	// Gather all model info
	info, err := gatherModelInfo(sess, cfg, target)
	if err != nil {
		return fmt.Errorf("gather model info: %w", err)
	}

	// In --json mode, emit the structured info to stdout. The box rendering
	// still runs but its output is routed to stderr by the output package.
	if output.JSONEnabled {
		output.EmitJSON(info)
		return nil
	}

	// Print the formatted output
	printModelBox(info)

	// Show suggested commands
	printSuggestedCommands(target)

	return nil
}

// printSuggestedCommands prints suggested follow-up commands.
func printSuggestedCommands(target string) {
	output.Println()
	output.Println("Commands:")
	output.Fprintf("  ondatrasql run %s          # Run this model\n", target)
	output.Fprintf("  ondatrasql sandbox %s      # Run in sandbox (diff + impact)\n", target)
	output.Fprintf("  ondatrasql edit %s         # Edit in $EDITOR\n", target)
	output.Fprintf("  ondatrasql lineage %s      # Show column lineage\n", target)
}

// gatherModelInfo collects all information about a model.
func gatherModelInfo(sess *duckdb.Session, cfg *config.Config, target string) (*ModelInfo, error) {
	info := &ModelInfo{
		SchemaVersion: describeModelSchemaVersion,
		Target:        target,
	}

	// Parse schema from target
	parts := strings.SplitN(target, ".", 2)
	if len(parts) == 2 {
		info.Schema = parts[0]
	}

	// Get commit info for basic model info
	commitInfo, err := backfill.GetModelCommitInfo(sess, target)
	if err != nil {
		return nil, fmt.Errorf("get commit info: %w", err)
	}
	if commitInfo == nil {
		return nil, fmt.Errorf("model %q not found", target)
	}

	info.Kind = commitInfo.Kind
	if info.Kind == "" {
		info.Kind = "table"
	}
	info.Materialization = commitInfo.RunType
	info.SourceFile = commitInfo.SourceFile

	// Execution profile from last run
	info.DuckDBVer = commitInfo.DuckDBVersion
	info.GitCommit = commitInfo.GitCommit
	for _, s := range commitInfo.Steps {
		info.Steps = append(info.Steps, StepInfo{
			Name:       s.Name,
			DurationMs: s.DurationMs,
			Status:     s.Status,
		})
	}

	// Read and parse source file for definition and quality rules.
	// Surface ParseModel failures via BlueprintError so machine-readable
	// callers know the live source no longer parses (rather than silently
	// emitting an empty Definition / Constraints / Audits / Warnings set).
	if info.SourceFile != "" {
		sourceFilePath := filepath.Join(cfg.ProjectDir, info.SourceFile)
		model, parseErr := parser.ParseModel(sourceFilePath, cfg.ProjectDir)
		if parseErr != nil {
			info.BlueprintError = fmt.Sprintf("source file %s no longer parses: %v", info.SourceFile, parseErr)
		} else {
			info.Definition = model.SQL
			info.ScriptType = model.ScriptType
			info.Constraints = model.Constraints
			info.Audits = model.Audits
			info.Warnings = model.Warnings
			// Cross-link to blueprint via AST walk (M2). Uses the same
			// session that's already attached to the catalog — only
			// json_serialize_sql is needed, no catalog reads.
			info.Blueprint, info.BlueprintError = detectModelBlueprint(sess, cfg.ProjectDir, model.SQL)
		}
	}

	// Get statistics from snapshots
	statsQuery := fmt.Sprintf(`
		SELECT
			COUNT(*) as total_runs,
			COALESCE(AVG(CAST(commit_extra_info->>'duration_ms' AS DOUBLE)), 0) as avg_duration,
			COALESCE(SUM(CAST(commit_extra_info->>'rows_affected' AS BIGINT)), 0) as total_rows,
			MIN(strftime(snapshot_time, '%%Y-%%m-%%d')) as first_run,
			MAX(strftime(snapshot_time, '%%Y-%%m-%%d %%H:%%M')) as last_run,
			100 as success_rate
		FROM snapshots()
		WHERE commit_extra_info->>'model' = '%s'
	`, duckdb.EscapeSQL(target))

	rows, err := sess.QueryRowsMap(statsQuery)
	if err != nil {
		info.GatherWarnings = append(info.GatherWarnings, fmt.Sprintf("stats query: %v", err))
	} else if len(rows) > 0 {
		row := rows[0]
		if v, perr := strconv.Atoi(row["total_runs"]); perr == nil {
			info.TotalRuns = v
		} else if row["total_runs"] != "" {
			info.GatherWarnings = append(info.GatherWarnings, fmt.Sprintf("parse total_runs: %v", perr))
		}
		if v, perr := strconv.ParseFloat(row["avg_duration"], 64); perr == nil {
			info.AvgDuration = int64(v)
		} else if row["avg_duration"] != "" {
			info.GatherWarnings = append(info.GatherWarnings, fmt.Sprintf("parse avg_duration: %v", perr))
		}
		if v, perr := strconv.ParseInt(row["total_rows"], 10, 64); perr == nil {
			info.TotalRows = v
		} else if row["total_rows"] != "" {
			info.GatherWarnings = append(info.GatherWarnings, fmt.Sprintf("parse total_rows: %v", perr))
		}
		info.FirstRun = row["first_run"]
		info.LastRun = row["last_run"]
		if v, perr := strconv.Atoi(row["success_rate"]); perr == nil {
			info.SuccessRate = v
		} else if row["success_rate"] != "" {
			info.GatherWarnings = append(info.GatherWarnings, fmt.Sprintf("parse success_rate: %v", perr))
		}
	}

	// Get table size
	sizeQuery := fmt.Sprintf(`
		SELECT
			COALESCE(pg_size_pretty(SUM(estimated_size)::BIGINT), 'N/A') as size
		FROM duckdb_tables()
		WHERE schema_name || '.' || table_name = '%s'
	`, duckdb.EscapeSQL(target))
	sizeRows, err := sess.QueryRowsMap(sizeQuery)
	if err != nil {
		info.GatherWarnings = append(info.GatherWarnings, fmt.Sprintf("table size: %v", err))
	} else if len(sizeRows) > 0 {
		info.TableSize = sizeRows[0]["size"]
	}
	if info.TableSize == "" {
		info.TableSize = "N/A"
	}

	// Get table comment from catalog
	if len(parts) == 2 {
		commentQuery := fmt.Sprintf(`
			SELECT COALESCE(comment, '') FROM duckdb_tables()
			WHERE schema_name = '%s' AND table_name = '%s'
		`, duckdb.EscapeSQL(parts[0]), duckdb.EscapeSQL(parts[1]))
		if commentVal, err := sess.QueryValue(commentQuery); err == nil {
			info.Description = commentVal
		}
	}

	// Get recent runs
	recentQuery := fmt.Sprintf(`
		SELECT
			strftime(snapshot_time, '%%Y-%%m-%%d %%H:%%M') as timestamp,
			CAST(commit_extra_info->>'duration_ms' AS INTEGER) as duration,
			CAST(commit_extra_info->>'rows_affected' AS BIGINT) as rows
		FROM snapshots()
		WHERE commit_extra_info->>'model' = '%s'
		ORDER BY snapshot_id DESC
		LIMIT 3
	`, duckdb.EscapeSQL(target))
	recentRows, err := sess.QueryRowsMap(recentQuery)
	if err != nil {
		info.GatherWarnings = append(info.GatherWarnings, fmt.Sprintf("recent runs: %v", err))
	} else {
		for _, row := range recentRows {
			dur, derr := strconv.ParseInt(row["duration"], 10, 64)
			if derr != nil && row["duration"] != "" {
				info.GatherWarnings = append(info.GatherWarnings, fmt.Sprintf("parse recent duration: %v", derr))
			}
			rowCount, rerr := strconv.ParseInt(row["rows"], 10, 64)
			if rerr != nil && row["rows"] != "" {
				info.GatherWarnings = append(info.GatherWarnings, fmt.Sprintf("parse recent rows: %v", rerr))
			}
			info.RecentRuns = append(info.RecentRuns, RunInfo{
				Timestamp: row["timestamp"],
				Duration:  dur,
				Rows:      rowCount,
				Success:   true, // We only store successful runs currently
			})
		}
	}

	// Get dependencies from commit info
	info.Dependencies = commitInfo.Depends

	// Get downstream models
	downstream, err := backfill.GetDownstreamModels(sess, target)
	if err != nil {
		info.GatherWarnings = append(info.GatherWarnings, fmt.Sprintf("downstream models: %v", err))
	} else {
		info.Downstream = downstream
	}

	// Get columns with comments
	if len(parts) == 2 {
		colQuery := fmt.Sprintf(`
			SELECT
				c.column_name,
				c.data_type,
				CASE WHEN c.is_nullable = 'YES' THEN 'NULL' ELSE 'NOT NULL' END as nullable,
				COALESCE(dc.comment, '') as comment
			FROM information_schema.columns c
			LEFT JOIN duckdb_columns() dc
				ON dc.schema_name = c.table_schema
				AND dc.table_name = c.table_name
				AND dc.column_name = c.column_name
			WHERE c.table_schema = '%s' AND c.table_name = '%s'
			ORDER BY c.ordinal_position
		`, duckdb.EscapeSQL(parts[0]), duckdb.EscapeSQL(parts[1]))
		colRows, err := sess.QueryRowsMap(colQuery)
		if err == nil {
			for _, row := range colRows {
				info.Columns = append(info.Columns, ColumnInfo{
					Name:        row["column_name"],
					Type:        row["data_type"],
					Nullable:    row["nullable"],
					Description: row["comment"],
				})
			}
		}
	}

	return info, nil
}

// printModelBox prints the model info in a nice box format.
func printModelBox(info *ModelInfo) {
	// Title
	printTopBorder()
	printCenteredLine(info.Target)
	if info.Description != "" {
		desc := info.Description
		if displayWidth(desc) > boxWidth-4 {
			runes := []rune(desc)
			for i := len(runes) - 1; i > 0; i-- {
				desc = string(runes[:i]) + "..."
				if displayWidth(desc) <= boxWidth-4 {
					break
				}
			}
		}
		printCenteredLine(desc)
	}
	printSectionBorder("")

	// Basic info section
	printEmptyLine()
	printTwoColumns("Kind", info.Kind, "Schema", info.Schema)
	printTwoColumns("Materialization", info.Materialization, "File", shortenPath(info.SourceFile))
	printTwoColumns("Created", info.FirstRun, "Updated", shortenTime(info.LastRun))
	printEmptyLine()

	// Statistics section
	printSectionBorder("Statistics")
	printEmptyLine()
	printTwoColumns("Total Runs", fmt.Sprintf("%d", info.TotalRuns), "Avg Duration", fmt.Sprintf("%dms", info.AvgDuration))
	printTwoColumns("Total Rows", formatNumber(info.TotalRows), "Table Size", info.TableSize)
	printTwoColumns("Success Rate", fmt.Sprintf("%d%%", info.SuccessRate), "Last Run", shortenTime(info.LastRun))
	printEmptyLine()

	// Recent Runs section
	if len(info.RecentRuns) > 0 {
		printSectionBorder("Recent Runs")
		printEmptyLine()
		for _, run := range info.RecentRuns {
			status := "ok"
			if !run.Success {
				status = "FAIL"
			}
			line := fmt.Sprintf("[%s] %s    %dms    %s rows", status, run.Timestamp, run.Duration, formatNumber(run.Rows))
			printPaddedLine(line)
		}
		printEmptyLine()
	}

	// Execution Profile section (from last run)
	if len(info.Steps) > 0 {
		printSectionBorder("Execution Profile (last run)")
		printEmptyLine()

		// Show version info
		versionLine := ""
		if info.DuckDBVer != "" {
			versionLine += "DuckDB " + info.DuckDBVer
		}
		if info.GitCommit != "" {
			if versionLine != "" {
				versionLine += "  |  "
			}
			versionLine += "git:" + info.GitCommit
		}
		if versionLine != "" {
			printPaddedLine(versionLine)
			printEmptyLine()
		}

		// Show step breakdown as a lined table
		var totalMs int64
		for _, s := range info.Steps {
			totalMs += s.DurationMs
		}

		for _, s := range info.Steps {
			// Calculate percentage and bar width
			pct := 0.0
			if totalMs > 0 {
				pct = float64(s.DurationMs) / float64(totalMs) * 100
			}
			barWidth := int(pct / 100 * 20) // Max 20 chars for bar
			if barWidth < 1 && s.DurationMs > 0 {
				barWidth = 1
			}
			bar := strings.Repeat("█", barWidth)

			// Format: step_name          ████ 12ms (25%)
			stepLine := fmt.Sprintf("%-22s %s %dms", s.Name, bar, s.DurationMs)
			if pct > 0 {
				stepLine += fmt.Sprintf(" (%.0f%%)", pct)
			}
			printPaddedLine(stepLine)
		}
		printEmptyLine()
	}

	// Dependencies and Downstream section (side by side)
	printDualSectionBorder("Dependencies", "Downstream")
	printDualEmptyLine()
	maxRows := max(len(info.Dependencies), len(info.Downstream))
	if maxRows == 0 {
		printDualColumn("(none)", "(none)")
	} else {
		for i := 0; i < maxRows; i++ {
			left := ""
			right := ""
			if i < len(info.Dependencies) {
				left = "> " + info.Dependencies[i]
			}
			if i < len(info.Downstream) {
				right = "< " + info.Downstream[i]
			}
			printDualColumn(left, right)
		}
	}
	printDualEmptyLine()

	// Determine next section to close dual section properly
	hasRules := len(info.Constraints) > 0 || len(info.Audits) > 0 || len(info.Warnings) > 0
	hasDefinition := info.Definition != ""

	// Data Quality Rules section
	if hasRules {
		printDualToSectionBorder("Data Quality Rules")
		printEmptyLine()
		for _, c := range info.Constraints {
			printPaddedLine("[BLOCK] " + c)
		}
		for _, a := range info.Audits {
			printPaddedLine("[AUDIT] " + a)
		}
		for _, w := range info.Warnings {
			printPaddedLine("[WARN]  " + w)
		}
		printEmptyLine()
	}

	// Definition section
	if hasDefinition {
		defType := "SQL"
		if info.ScriptType == parser.ScriptTypeStarlark {
			defType = "Starlark"
		}
		if hasRules {
			printSectionBorder("Definition (" + defType + ")")
		} else {
			printDualToSectionBorder("Definition (" + defType + ")")
		}
		printEmptyLine()
		// Print first few lines of definition
		lines := strings.Split(info.Definition, "\n")
		maxLines := 8
		for i, line := range lines {
			if i >= maxLines {
				printPaddedLine("... (" + fmt.Sprintf("%d", len(lines)-maxLines) + " more lines)")
				break
			}
			// Trim and truncate long lines
			line = strings.TrimRight(line, " \t\r")
			runes := []rune(line)
			if len(runes) > boxWidth-6 {
				line = string(runes[:boxWidth-9]) + "..."
			}
			printPaddedLine(line)
		}
		printEmptyLine()
	}

	// Columns section
	hasColumns := len(info.Columns) > 0
	if hasColumns {
		if hasRules || hasDefinition {
			printSectionBorder("Columns")
		} else {
			printDualToSectionBorder("Columns")
		}
		printEmptyLine()
		for _, col := range info.Columns {
			extra := col.Constraint
			if col.Description != "" {
				if extra != "" {
					extra += " "
				}
				extra += col.Description
			}
			line := fmt.Sprintf("%-14s %-12s %-10s %s", col.Name, col.Type, col.Nullable, extra)
			printPaddedLine(line)
		}
		printEmptyLine()
	}

	// If nothing closed the dual section, close it now
	if !hasRules && !hasDefinition && !hasColumns {
		printDualToSectionBorder("")
	}

	if len(info.GatherWarnings) > 0 {
		printSectionBorder("Gather Warnings")
		printEmptyLine()
		for _, w := range info.GatherWarnings {
			printPaddedLine("  " + w)
		}
		printEmptyLine()
	}

	// Bottom border
	printBottomBorder()
}

// Helper functions for box drawing.
//
// All box helpers are no-ops in --json mode (output.JSONEnabled). This keeps
// JSON consumers from getting box-drawing characters mixed into their stdout
// stream — Bug S10. The JSON line emission paths run independently and
// produce machine-readable output only.

func printTopBorder() {
	if output.JSONEnabled {
		return
	}
	output.Println(topLeft + strings.Repeat(horizontal, boxWidth) + topRight)
}

func printBottomBorder() {
	if output.JSONEnabled {
		return
	}
	output.Println(bottomLeft + strings.Repeat(horizontal, boxWidth) + bottomRight)
}

func printSectionBorder(title string) {
	if output.JSONEnabled {
		return
	}
	if title == "" {
		output.Println(leftT + strings.Repeat(horizontal, boxWidth) + rightT)
	} else {
		titlePart := horizontal + horizontal + horizontal + " " + title + " "
		remaining := boxWidth - runeLen(titlePart)
		output.Println(leftT + titlePart + strings.Repeat(horizontal, remaining) + rightT)
	}
}

func runeLen(s string) int {
	return len([]rune(s))
}

// displayWidth returns the display width of a string in terminal columns.
// For ASCII text, this is simply the rune count.
func displayWidth(s string) int {
	return len([]rune(s))
}

func printDualSectionBorder(left, right string) {
	if output.JSONEnabled {
		return
	}
	// Total inner width is boxWidth, minus 1 for middle separator = 63
	// Split: left=31, right=32
	leftHalf := (boxWidth - 1) / 2  // 31
	rightHalf := boxWidth - 1 - leftHalf // 32
	leftTitle := horizontal + horizontal + horizontal + " " + left + " "
	leftRemaining := leftHalf - runeLen(leftTitle)
	if leftRemaining < 0 {
		leftRemaining = 0
	}
	rightTitle := horizontal + horizontal + horizontal + " " + right + " "
	rightRemaining := rightHalf - runeLen(rightTitle)
	if rightRemaining < 0 {
		rightRemaining = 0
	}
	output.Println(leftT + leftTitle + strings.Repeat(horizontal, leftRemaining) + topT + rightTitle + strings.Repeat(horizontal, rightRemaining) + rightT)
}

func printCenteredLine(text string) {
	if output.JSONEnabled {
		return
	}
	textWidth := displayWidth(text)
	padding := (boxWidth - textWidth) / 2
	if padding < 0 {
		padding = 0
	}
	rightPadding := boxWidth - textWidth - padding
	if rightPadding < 0 {
		rightPadding = 0
	}
	output.Println(vertical + strings.Repeat(" ", padding) + text + strings.Repeat(" ", rightPadding) + vertical)
}

func printEmptyLine() {
	if output.JSONEnabled {
		return
	}
	output.Println(vertical + strings.Repeat(" ", boxWidth) + vertical)
}

func printPaddedLine(text string) {
	if output.JSONEnabled {
		return
	}
	textWidth := displayWidth(text)
	if textWidth > boxWidth-4 {
		// Truncate by runes but recalculate width
		runes := []rune(text)
		for i := len(runes) - 1; i > 0; i-- {
			text = string(runes[:i]) + "..."
			textWidth = displayWidth(text)
			if textWidth <= boxWidth-4 {
				break
			}
		}
	}
	padding := boxWidth - textWidth - 2 // 2 for leading spaces
	if padding < 0 {
		padding = 0
	}
	output.Println(vertical + "  " + text + strings.Repeat(" ", padding) + vertical)
}

func printTwoColumns(label1, value1, label2, value2 string) {
	col1 := fmt.Sprintf("%-16s %s", label1, value1)
	col2 := fmt.Sprintf("%-14s %s", label2, value2)

	// Truncate if needed
	halfWidth := boxWidth/2 - 2
	col1Width := displayWidth(col1)
	col2Width := displayWidth(col2)

	if col1Width > halfWidth {
		runes := []rune(col1)
		for i := len(runes) - 1; i > 0; i-- {
			col1 = string(runes[:i]) + "..."
			col1Width = displayWidth(col1)
			if col1Width <= halfWidth {
				break
			}
		}
	}
	if col2Width > halfWidth {
		runes := []rune(col2)
		for i := len(runes) - 1; i > 0; i-- {
			col2 = string(runes[:i]) + "..."
			col2Width = displayWidth(col2)
			if col2Width <= halfWidth {
				break
			}
		}
	}

	// Pad columns
	col1 = col1 + strings.Repeat(" ", halfWidth-col1Width)
	col2 = col2 + strings.Repeat(" ", halfWidth-col2Width)

	output.Println(vertical + "  " + col1 + col2 + "  " + vertical)
}

func printDualColumn(left, right string) {
	leftHalf := (boxWidth - 1) / 2  // 31
	rightHalf := boxWidth - 1 - leftHalf // 32

	leftWidth := displayWidth(left)
	rightWidth := displayWidth(right)

	if leftWidth > leftHalf-4 {
		runes := []rune(left)
		for i := len(runes) - 1; i > 0; i-- {
			left = string(runes[:i]) + "..."
			leftWidth = displayWidth(left)
			if leftWidth <= leftHalf-4 {
				break
			}
		}
	}
	if rightWidth > rightHalf-4 {
		runes := []rune(right)
		for i := len(runes) - 1; i > 0; i-- {
			right = string(runes[:i]) + "..."
			rightWidth = displayWidth(right)
			if rightWidth <= rightHalf-4 {
				break
			}
		}
	}

	leftPadded := "  " + left + strings.Repeat(" ", leftHalf-leftWidth-2)
	rightPadded := "  " + right + strings.Repeat(" ", rightHalf-rightWidth-2)

	output.Println(vertical + leftPadded + vertical + rightPadded + vertical)
}

func printDualEmptyLine() {
	leftHalf := (boxWidth - 1) / 2  // 31
	rightHalf := boxWidth - 1 - leftHalf // 32
	output.Println(vertical + strings.Repeat(" ", leftHalf) + vertical + strings.Repeat(" ", rightHalf) + vertical)
}

func printDualToSectionBorder(title string) {
	leftHalf := (boxWidth - 1) / 2  // 31
	rightHalf := boxWidth - 1 - leftHalf // 32
	if title == "" {
		// Just close the dual section
		output.Println(leftT + strings.Repeat(horizontal, leftHalf) + bottomT + strings.Repeat(horizontal, rightHalf) + rightT)
	} else {
		// Title on left, ╩ in middle, rest of line on right
		titlePart := horizontal + horizontal + horizontal + " " + title + " "
		titleLen := runeLen(titlePart)
		// Fill between title and center ╩
		fillToCenter := leftHalf - titleLen
		if fillToCenter < 0 {
			fillToCenter = 0
		}
		output.Println(leftT + titlePart + strings.Repeat(horizontal, fillToCenter) + bottomT + strings.Repeat(horizontal, rightHalf) + rightT)
	}
}

// Utility functions

func shortenPath(path string) string {
	if path == "" {
		return "N/A"
	}
	// Just show the filename
	return filepath.Base(path)
}

func shortenTime(timestamp string) string {
	if timestamp == "" {
		return "N/A"
	}
	// If it's a full datetime, just show HH:MM
	if len(timestamp) > 10 {
		parts := strings.Split(timestamp, " ")
		if len(parts) >= 2 {
			return parts[1]
		}
	}
	return timestamp
}

func formatNumber(n int64) string {
	if n < 1000 {
		return fmt.Sprintf("%d", n)
	}
	if n < 1000000 {
		return fmt.Sprintf("%.1fK", float64(n)/1000)
	}
	return fmt.Sprintf("%.1fM", float64(n)/1000000)
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// detectModelBlueprint walks the model SQL's AST for a TABLE_FUNCTION node
// matching a registered blueprint and returns its name.
//
// Returns (name, "") when detection succeeded — name is empty if no
// blueprint is referenced. Returns ("", reason) when detection couldn't
// run or couldn't be verified; callers should surface the reason rather
// than silently dropping the cross-link. Reasons include:
//   - registry empty due to parse errors in lib/
//   - AST serialization failed for the model SQL
//   - the model's TABLE_FUNCTION call references a blueprint that's in
//     the project's lib/ tree but failed to parse (so libcall.Detect
//     filtered the call out as "unknown")
//
// AST-based detection (no string fallback) — false positives from lib
// names appearing in string literals or comments are not possible.
func detectModelBlueprint(sess *duckdb.Session, projectDir, sql string) (string, string) {
	reg, scanErrs := libregistry.ScanLenient(projectDir)
	if reg.Empty() && len(scanErrs) > 0 {
		return "", fmt.Sprintf("blueprint registry empty due to parse errors in lib/ (%d files failed)", len(scanErrs))
	}

	// Always serialize SQL → AST first. Walking the AST gives us the
	// full set of TABLE_FUNCTION names, which we need to detect the
	// "model uses a broken blueprint" case below.
	if sess == nil || sql == "" {
		return "", ""
	}
	astJSON, err := lineage.GetAST(sess, sql)
	if err != nil {
		return "", fmt.Sprintf("AST serialization failed: %v", err)
	}
	ast, err := duckast.Parse(astJSON)
	if err != nil {
		return "", fmt.Sprintf("AST parse failed: %v", err)
	}

	// Resolve registered TABLE_FUNCTION calls.
	calls := libcall.Detect(ast, reg)
	if len(calls) > 0 {
		return calls[0].FuncName, ""
	}

	// No registered call matched. Check whether the AST has a
	// TABLE_FUNCTION whose name corresponds to a blueprint that
	// ScanLenient failed to parse — that's a verification gap, not a
	// "no blueprint used" outcome.
	if reg.Empty() {
		return "", ""
	}
	for _, name := range libcall.AllTableFunctionNames(ast) {
		if _, broken := scanErrs[name+".star"]; broken {
			return "", fmt.Sprintf("model uses blueprint %q which failed to parse — cross-link unverified", name)
		}
	}
	return "", ""
}
