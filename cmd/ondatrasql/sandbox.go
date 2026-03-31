// OndatraSQL - A data pipeline framework for DuckDB + DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package main

import (
	"fmt"
	"strings"

	"github.com/ondatra-labs/ondatrasql/internal/duckdb"
	"github.com/ondatra-labs/ondatrasql/internal/execute"
	"github.com/ondatra-labs/ondatrasql/internal/parser"
	sql "github.com/ondatra-labs/ondatrasql/internal/sql"
)

// quoteTarget quotes a schema.table target for safe use in SQL queries.
func quoteTarget(target string) string {
	parts := strings.SplitN(target, ".", 2)
	if len(parts) == 2 {
		return duckdb.QuoteIdentifier(parts[0]) + "." + duckdb.QuoteIdentifier(parts[1])
	}
	return duckdb.QuoteIdentifier(target)
}

// printSandboxResult prints a compact result line for DAG sandbox runs.
func printSandboxResult(result *execute.Result, target string, err error) {
	if result == nil {
		// Model failed before producing a result
		if err != nil {
			printPaddedLine(fmt.Sprintf("[FAIL] %s", target))
			errMsg := err.Error()
			if len(errMsg) > 50 {
				errMsg = errMsg[:47] + "..."
			}
			printPaddedLine(fmt.Sprintf("       %s", errMsg))
		}
		return
	}

	status := "OK"
	if len(result.Errors) > 0 {
		status = "FAIL"
	}

	// Compact format: [OK] staging.customers (123ms)
	printPaddedLine(fmt.Sprintf("[%s] %s (%s)", status, result.Target, result.Duration.Round(1e6)))

	if len(result.Errors) > 0 {
		printPaddedLine(fmt.Sprintf("       %s", truncate(result.Errors[0], 50)))
	}
}

// showDagSandboxSummary shows a summary of all changes across all models.
func showDagSandboxSummary(sess *duckdb.Session, models []*parser.Model, failedTargets map[string]string) {
	printSectionBorder("Summary")
	printEmptyLine()

	var unchanged, changed, warnings int
	failed := len(failedTargets)
	var changedModels []string

	modelKinds := make(map[string]string) // target -> kind
	for _, m := range models {
		modelKinds[m.Target] = m.Kind

		// Skip failed models
		if _, isFailed := failedTargets[m.Target]; isFailed {
			continue
		}

		// Check if table exists in sandbox via information_schema (avoids
		// error-type guessing from SELECT COUNT(*) failures).
		parts := strings.SplitN(m.Target, ".", 2)
		if len(parts) != 2 {
			unchanged++
			continue
		}
		existsVal, existsErr := sess.QueryValue(fmt.Sprintf(
			"SELECT COUNT(*) FROM information_schema.tables WHERE table_catalog = 'sandbox' AND table_schema = '%s' AND table_name = '%s'",
			duckdb.EscapeSQL(parts[0]), duckdb.EscapeSQL(parts[1])))
		if existsErr != nil {
			// Metadata query itself failed — warn instead of silently skipping.
			printPaddedLine(fmt.Sprintf("  [WARN] %s: cannot check sandbox table: %s", m.Target, truncate(existsErr.Error(), 40)))
			warnings++
			continue
		}
		if existsVal == "0" {
			// Table doesn't exist in sandbox — model was skipped (no changes),
			// prod table is visible via search_path. Count as unchanged.
			unchanged++
			continue
		}

		sandboxCount, sandboxErr := sess.QueryValue(fmt.Sprintf("SELECT COUNT(*) FROM sandbox.%s", quoteTarget(m.Target)))
		if sandboxErr != nil {
			printPaddedLine(fmt.Sprintf("  [WARN] %s: sandbox query error: %s", m.Target, truncate(sandboxErr.Error(), 40)))
			warnings++
			continue
		}

		// Check if table exists in prod
		prodCount, prodErr := sess.QueryValue(fmt.Sprintf("SELECT COUNT(*) FROM %s.%s", sess.ProdAlias(), quoteTarget(m.Target)))
		if prodErr != nil {
			// New table (doesn't exist in prod, but exists in sandbox)
			changed++
			changedModels = append(changedModels, m.Target)
			continue
		}

		var prod, sandbox int64
		fmt.Sscanf(prodCount, "%d", &prod)
		fmt.Sscanf(sandboxCount, "%d", &sandbox)

		if prod != sandbox {
			changed++
			changedModels = append(changedModels, m.Target)
		} else {
			// Check for actual row differences even if count is same
			// SCD2 models: exclude metadata columns from diff
			diffTemplate := "queries/sandbox_diff_count.sql"
			if m.Kind == "scd2" {
				diffTemplate = "queries/sandbox_diff_count_scd2.sql"
			}
			addedSQL := sql.MustFormat(diffTemplate, "sandbox", sess.ProdAlias(), m.Target)
			added, _ := sess.QueryValue(addedSQL)
			var addedCount int64
			fmt.Sscanf(added, "%d", &addedCount)

			if addedCount > 0 {
				changed++
				changedModels = append(changedModels, m.Target)
			} else {
				unchanged++
			}
		}
	}

	printPaddedLine(fmt.Sprintf("Unchanged: %d models", unchanged))
	printPaddedLine(fmt.Sprintf("Changed:   %d models", changed))
	if failed > 0 {
		printPaddedLine(fmt.Sprintf("Failed:    %d models", failed))
	}
	if warnings > 0 {
		printPaddedLine(fmt.Sprintf("Warnings:  %d models (query errors)", warnings))
	}

	// Show details for changed models
	if len(changedModels) > 0 {
		printEmptyLine()
		printSectionBorder("Changes")
		printEmptyLine()

		for _, target := range changedModels {
			showDagModelDiff(sess, target, modelKinds[target])
		}
	}

	// Show details for failed models
	if len(failedTargets) > 0 {
		printEmptyLine()
		printSectionBorder("Failed")
		printEmptyLine()

		for target, errMsg := range failedTargets {
			printPaddedLine(target)
			// Truncate long error messages
			if len(errMsg) > 55 {
				errMsg = errMsg[:52] + "..."
			}
			printPaddedLine(fmt.Sprintf("  %s", errMsg))
			printEmptyLine()
		}
	}
}

// showDagModelDiff shows diff for a single model in DAG summary.
// Uses same format as showSandboxDiff for consistency.
func showDagModelDiff(sess *duckdb.Session, target, kind string) {
	printPaddedLine(target)

	// Check if new table
	prodCount, prodErr := sess.QueryValue(fmt.Sprintf("SELECT COUNT(*) FROM %s.%s", sess.ProdAlias(), quoteTarget(target)))
	if prodErr != nil {
		printPaddedLine("  (new table)")
		// Show column count for new tables
		parts := strings.Split(target, ".")
		tableName := target
		if len(parts) == 2 {
			tableName = parts[1]
		}
		sandboxCount, _ := sess.QueryValue(fmt.Sprintf("SELECT COUNT(*) FROM sandbox.%s", quoteTarget(target)))
		colCount, err := sess.QueryValue(fmt.Sprintf(`
			SELECT COUNT(*) FROM information_schema.columns
			WHERE table_catalog = 'sandbox' AND table_name = '%s'
		`, duckdb.EscapeSQL(tableName)))
		if err == nil && sandboxCount != "" {
			printPaddedLine(fmt.Sprintf("  Rows: %s, Columns: %s", sandboxCount, colCount))
		}
		printEmptyLine()
		return
	}

	sandboxCount, _ := sess.QueryValue(fmt.Sprintf("SELECT COUNT(*) FROM sandbox.%s", quoteTarget(target)))

	var prod, sandbox int64
	fmt.Sscanf(prodCount, "%d", &prod)
	fmt.Sscanf(sandboxCount, "%d", &sandbox)

	// Check and show schema changes FIRST (schema evolution)
	parts := strings.Split(target, ".")
	tableName := target
	if len(parts) == 2 {
		tableName = parts[1]
	}

	schemaSQL := sql.MustFormat("queries/sandbox_schema_diff.sql", "sandbox", sess.ProdAlias(), tableName)
	result, err := sess.Query(schemaSQL)
	if err == nil && result != "" {
		lines := strings.Split(strings.TrimSpace(result), "\n")
		if len(lines) > 1 {
			var added, removed, changed []string
			for _, line := range lines[1:] {
				cols := strings.Split(line, "\t")
				if len(cols) < 4 {
					continue
				}
				colName := strings.TrimSpace(cols[0])
				prodType := strings.TrimSpace(cols[1])
				sandboxType := strings.TrimSpace(cols[2])
				changeType := strings.TrimSpace(cols[3])
				switch changeType {
				case "added":
					added = append(added, fmt.Sprintf("%s (%s)", colName, sandboxType))
				case "removed":
					removed = append(removed, fmt.Sprintf("%s (%s)", colName, prodType))
				case "type_changed":
					changed = append(changed, fmt.Sprintf("%s: %s→%s", colName, prodType, sandboxType))
				}
			}
			if len(added) > 0 || len(removed) > 0 || len(changed) > 0 {
				printPaddedLine("  SCHEMA EVOLUTION:")
				if len(added) > 0 {
					printPaddedLine(fmt.Sprintf("    + Added: %s", strings.Join(added, ", ")))
				}
				if len(removed) > 0 {
					printPaddedLine(fmt.Sprintf("    - Removed: %s", strings.Join(removed, ", ")))
				}
				if len(changed) > 0 {
					printPaddedLine(fmt.Sprintf("    ~ Changed: %s", strings.Join(changed, ", ")))
				}
			}
		}
	}

	// Show row diff
	diff := sandbox - prod
	var pct float64
	if prod > 0 {
		pct = float64(diff) / float64(prod) * 100
	}

	if diff == 0 {
		printPaddedLine(fmt.Sprintf("  Rows: %d (no change)", prod))
	} else if diff > 0 {
		printPaddedLine(fmt.Sprintf("  Rows: %d → %d (+%d, +%.1f%%)", prod, sandbox, diff, pct))
	} else {
		printPaddedLine(fmt.Sprintf("  Rows: %d → %d (%d, %.1f%%)", prod, sandbox, diff, pct))
	}

	// Show added/removed row counts (content changes)
	// SCD2 models: exclude metadata columns from diff
	diffTemplate := "queries/sandbox_diff_count.sql"
	sampleTemplate := "queries/sandbox_sample.sql"
	if kind == "scd2" {
		diffTemplate = "queries/sandbox_diff_count_scd2.sql"
		sampleTemplate = "queries/sandbox_sample_scd2.sql"
	}
	addedSQL := sql.MustFormat(diffTemplate, "sandbox", sess.ProdAlias(), target)
	removedSQL := sql.MustFormat(diffTemplate, sess.ProdAlias(), "sandbox", target)
	added, _ := sess.QueryValue(addedSQL)
	removed, _ := sess.QueryValue(removedSQL)
	var addedCount, removedCount int64
	fmt.Sscanf(added, "%d", &addedCount)
	fmt.Sscanf(removed, "%d", &removedCount)

	if addedCount > 0 || removedCount > 0 {
		if addedCount > 0 {
			printPaddedLine(fmt.Sprintf("    + Added: %d rows", addedCount))
		}
		if removedCount > 0 {
			printPaddedLine(fmt.Sprintf("    - Removed: %d rows", removedCount))
		}

		// Show sample of changes (max 2 each)
		if addedCount > 0 {
			sampleSQL := sql.MustFormat(sampleTemplate, "sandbox", sess.ProdAlias(), target, "2")
			sampleResult, err := sess.Query(sampleSQL)
			if err == nil && sampleResult != "" {
				lines := strings.Split(sampleResult, "\n")
				if len(lines) > 1 {
					printPaddedLine("    Sample added:")
					for _, line := range lines[1:min(3, len(lines))] {
						if line != "" {
							printPaddedLine(fmt.Sprintf("      + %s", truncate(line, 48)))
						}
					}
				}
			}
		}
		if removedCount > 0 {
			sampleSQL := sql.MustFormat(sampleTemplate, sess.ProdAlias(), "sandbox", target, "2")
			sampleResult, err := sess.Query(sampleSQL)
			if err == nil && sampleResult != "" {
				lines := strings.Split(sampleResult, "\n")
				if len(lines) > 1 {
					printPaddedLine("    Sample removed:")
					for _, line := range lines[1:min(3, len(lines))] {
						if line != "" {
							printPaddedLine(fmt.Sprintf("      - %s", truncate(line, 48)))
						}
					}
				}
			}
		}
	}

	printEmptyLine()
}

// showSandboxDiff shows automatic diff after running a model in sandbox mode.
// This compares the sandbox output vs prod (using dynamic catalog aliases).
// For SCD2 models, metadata columns are excluded from the diff.
func showSandboxDiff(sess *duckdb.Session, target, kind string) {
	printSectionBorder("Diff vs Prod")
	printEmptyLine()

	// Check if table exists in prod
	prodCount, prodErr := sess.QueryValue(fmt.Sprintf("SELECT COUNT(*) FROM %s.%s", sess.ProdAlias(), quoteTarget(target)))
	if prodErr != nil {
		// Table is new, doesn't exist in prod
		printPaddedLine("(new table)")
		showNewTableSchema(sess, target)
		printEmptyLine()
		return
	}

	// Show schema changes first
	showSchemaDiff(sess, target)

	// Get sandbox count
	sandboxCount, sandboxErr := sess.QueryValue(fmt.Sprintf("SELECT COUNT(*) FROM sandbox.%s", quoteTarget(target)))
	if sandboxErr != nil {
		printEmptyLine()
		return
	}

	// Parse counts
	var prod, sandbox int64
	fmt.Sscanf(prodCount, "%d", &prod)
	fmt.Sscanf(sandboxCount, "%d", &sandbox)

	// Calculate diff
	diff := sandbox - prod
	var pct float64
	if prod > 0 {
		pct = float64(diff) / float64(prod) * 100
	}

	// Show row count comparison
	if diff == 0 {
		printPaddedLine(fmt.Sprintf("Rows: %d (no change)", prod))
	} else if diff > 0 {
		printPaddedLine(fmt.Sprintf("Rows: %d → %d (+%d, +%.1f%%)", prod, sandbox, diff, pct))
	} else {
		printPaddedLine(fmt.Sprintf("Rows: %d → %d (%d, %.1f%%)", prod, sandbox, diff, pct))
	}

	// Get added/removed/changed counts
	// SCD2 models: exclude metadata columns from diff
	diffTemplate := "queries/sandbox_diff_count.sql"
	if kind == "scd2" {
		diffTemplate = "queries/sandbox_diff_count_scd2.sql"
	}

	addedSQL := sql.MustFormat(diffTemplate, "sandbox", sess.ProdAlias(), target)
	removedSQL := sql.MustFormat(diffTemplate, sess.ProdAlias(), "sandbox", target)

	added, _ := sess.QueryValue(addedSQL)
	removed, _ := sess.QueryValue(removedSQL)

	var addedCount, removedCount int64
	fmt.Sscanf(added, "%d", &addedCount)
	fmt.Sscanf(removed, "%d", &removedCount)

	if addedCount > 0 || removedCount > 0 {
		if addedCount > 0 {
			printPaddedLine(fmt.Sprintf("  + Added: %d rows", addedCount))
		}
		if removedCount > 0 {
			printPaddedLine(fmt.Sprintf("  - Removed: %d rows", removedCount))
		}

		// Show sample of differences
		printEmptyLine()
		showSampleDiff(sess, target, kind, addedCount, removedCount)
	}
	printEmptyLine()
}

// showSampleDiff shows sample rows that differ between sandbox and prod.
func showSampleDiff(sess *duckdb.Session, target, kind string, addedCount, removedCount int64) {
	sampleTemplate := "queries/sandbox_sample.sql"
	if kind == "scd2" {
		sampleTemplate = "queries/sandbox_sample_scd2.sql"
	}

	// Show added rows (in sandbox but not in prod)
	if addedCount > 0 {
		addedSQL := sql.MustFormat(sampleTemplate, "sandbox", sess.ProdAlias(), target, "2")
		result, err := sess.Query(addedSQL)
		if err == nil && result != "" {
			lines := strings.Split(result, "\n")
			if len(lines) > 1 {
				printPaddedLine("Sample added:")
				for _, line := range lines[:min(3, len(lines))] {
					if line != "" {
						printPaddedLine("  + " + truncate(line, 54))
					}
				}
			}
		}
	}

	// Show removed rows (in prod but not in sandbox)
	if removedCount > 0 {
		removedSQL := sql.MustFormat(sampleTemplate, sess.ProdAlias(), "sandbox", target, "2")
		result, err := sess.Query(removedSQL)
		if err == nil && result != "" {
			lines := strings.Split(result, "\n")
			if len(lines) > 1 {
				printPaddedLine("Sample removed:")
				for _, line := range lines[:min(3, len(lines))] {
					if line != "" {
						printPaddedLine("  - " + truncate(line, 54))
					}
				}
			}
		}
	}
}

func truncate(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen-3] + "..."
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// showSchemaDiff shows schema changes between sandbox and prod.
func showSchemaDiff(sess *duckdb.Session, target string) {
	// Parse schema.table format
	parts := strings.Split(target, ".")
	tableName := target
	if len(parts) == 2 {
		tableName = parts[1]
	}

	schemaSQL := sql.MustFormat("queries/sandbox_schema_diff.sql", "sandbox", sess.ProdAlias(), tableName)
	result, err := sess.Query(schemaSQL)
	if err != nil || result == "" {
		return
	}

	lines := strings.Split(strings.TrimSpace(result), "\n")
	if len(lines) <= 1 {
		return // No header or no changes
	}

	hasChanges := false
	var added, removed, changed []string

	for _, line := range lines[1:] { // Skip header
		cols := strings.Split(line, "\t")
		if len(cols) < 4 {
			continue
		}
		colName := strings.TrimSpace(cols[0])
		prodType := strings.TrimSpace(cols[1])
		sandboxType := strings.TrimSpace(cols[2])
		changeType := strings.TrimSpace(cols[3])

		switch changeType {
		case "added":
			added = append(added, fmt.Sprintf("%s (%s)", colName, sandboxType))
			hasChanges = true
		case "removed":
			removed = append(removed, fmt.Sprintf("%s (%s)", colName, prodType))
			hasChanges = true
		case "type_changed":
			changed = append(changed, fmt.Sprintf("%s: %s → %s", colName, prodType, sandboxType))
			hasChanges = true
		}
	}

	if !hasChanges {
		return
	}

	printPaddedLine("SCHEMA EVOLUTION:")
	if len(added) > 0 {
		printPaddedLine(fmt.Sprintf("  + Added: %s", strings.Join(added, ", ")))
	}
	if len(removed) > 0 {
		printPaddedLine(fmt.Sprintf("  - Removed: %s", strings.Join(removed, ", ")))
	}
	if len(changed) > 0 {
		printPaddedLine(fmt.Sprintf("  ~ Changed: %s", strings.Join(changed, ", ")))
	}
	printEmptyLine()
}

// showNewTableSchema shows the schema of a new table in sandbox.
func showNewTableSchema(sess *duckdb.Session, target string) {
	// Parse schema.table format
	parts := strings.Split(target, ".")
	tableName := target
	if len(parts) == 2 {
		tableName = parts[1]
	}

	// Get columns from sandbox
	colSQL := fmt.Sprintf(`
		SELECT column_name, data_type
		FROM information_schema.columns
		WHERE table_catalog = 'sandbox' AND table_name = '%s'
		ORDER BY ordinal_position
	`, duckdb.EscapeSQL(tableName))

	result, err := sess.Query(colSQL)
	if err != nil || result == "" {
		return
	}

	lines := strings.Split(strings.TrimSpace(result), "\n")
	if len(lines) <= 1 {
		return
	}

	printPaddedLine("Columns:")
	for _, line := range lines[1:] { // Skip header
		cols := strings.Split(line, "\t")
		if len(cols) >= 2 {
			colName := strings.TrimSpace(cols[0])
			colType := strings.TrimSpace(cols[1])
			printPaddedLine(fmt.Sprintf("  • %s (%s)", colName, colType))
		}
	}
}
