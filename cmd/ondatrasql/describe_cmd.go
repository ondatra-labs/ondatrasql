// OndatraSQL - You don't need a data stack anymore
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
	"github.com/ondatra-labs/ondatrasql/internal/duckdb"
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

// ModelInfo holds all information about a model for display.
type ModelInfo struct {
	// Basic info
	Target          string
	Kind            string
	Schema          string
	Materialization string
	SourceFile      string
	IsScript        bool
	ScriptType      parser.ScriptType

	// Statistics
	TotalRuns    int
	AvgDuration  int64
	TotalRows    int64
	TableSize    string
	SuccessRate  int
	LastRun      string
	FirstRun     string

	// Dependencies
	Dependencies []string
	Downstream   []string

	// Columns
	Columns []ColumnInfo

	// Recent runs
	RecentRuns []RunInfo

	// Definition (SQL or Starlark code)
	Definition string

	// Description is the table-level comment from @description directive.
	Description string

	// Data Quality Rules
	Constraints []string
	Audits      []string
	Warnings    []string

	// Execution profile (from last run)
	Steps     []StepInfo
	DuckDBVer string
	GitCommit string
}

// ColumnInfo represents a column with its metadata.
type ColumnInfo struct {
	Name        string
	Type        string
	Nullable    string
	Constraint  string
	Description string
}

// RunInfo represents a single run.
type RunInfo struct {
	Timestamp  string
	Duration   int64
	Rows       int64
	Success    bool
}

// StepInfo represents an execution step.
type StepInfo struct {
	Name       string
	DurationMs int64
	Status     string
}

// runDescribe executes the describe command for a model.
func runDescribe(cfg *config.Config, target string) error {
	sess, err := duckdb.NewSession("")
	if err != nil {
		return fmt.Errorf("create session: %w", err)
	}
	defer sess.Close()

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
	output.Fprintf("  ondatrasql %s              # Run this model\n", target)
	output.Fprintf("  ondatrasql --sandbox %s    # Run in sandbox (diff + impact)\n", target)
	output.Fprintf("  ondatrasql edit %s         # Edit in $EDITOR\n", target)
	output.Fprintf("  ondatrasql lineage %s      # Show column lineage\n", target)
}

// gatherModelInfo collects all information about a model.
func gatherModelInfo(sess *duckdb.Session, cfg *config.Config, target string) (*ModelInfo, error) {
	info := &ModelInfo{
		Target: target,
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

	// Read and parse source file for definition and quality rules
	if info.SourceFile != "" {
		sourceFilePath := filepath.Join(cfg.ProjectDir, info.SourceFile)
		if model, parseErr := parser.ParseModel(sourceFilePath, cfg.ProjectDir); parseErr == nil {
			info.Definition = model.SQL
			info.IsScript = model.IsScript
			info.ScriptType = model.ScriptType
			info.Constraints = model.Constraints
			info.Audits = model.Audits
			info.Warnings = model.Warnings
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
		FROM %s.snapshots()
		WHERE commit_extra_info->>'model' = '%s'
	`, sess.CatalogAlias(), duckdb.EscapeSQL(target))

	rows, err := sess.QueryRowsMap(statsQuery)
	if err == nil && len(rows) > 0 {
		row := rows[0]
		info.TotalRuns, _ = strconv.Atoi(row["total_runs"])
		avgDur, _ := strconv.ParseFloat(row["avg_duration"], 64)
		info.AvgDuration = int64(avgDur)
		info.TotalRows, _ = strconv.ParseInt(row["total_rows"], 10, 64)
		info.FirstRun = row["first_run"]
		info.LastRun = row["last_run"]
		info.SuccessRate, _ = strconv.Atoi(row["success_rate"])
	}

	// Get table size
	sizeQuery := fmt.Sprintf(`
		SELECT
			COALESCE(pg_size_pretty(SUM(estimated_size)::BIGINT), 'N/A') as size
		FROM duckdb_tables()
		WHERE schema_name || '.' || table_name = '%s'
	`, duckdb.EscapeSQL(target))
	sizeRows, err := sess.QueryRowsMap(sizeQuery)
	if err == nil && len(sizeRows) > 0 {
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
		FROM %s.snapshots()
		WHERE commit_extra_info->>'model' = '%s'
		ORDER BY snapshot_id DESC
		LIMIT 3
	`, sess.CatalogAlias(), duckdb.EscapeSQL(target))
	recentRows, err := sess.QueryRowsMap(recentQuery)
	if err == nil {
		for _, row := range recentRows {
			dur, _ := strconv.ParseInt(row["duration"], 10, 64)
			rowCount, _ := strconv.ParseInt(row["rows"], 10, 64)
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
	if err == nil {
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
			if len(line) > boxWidth-6 {
				line = line[:boxWidth-9] + "..."
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
