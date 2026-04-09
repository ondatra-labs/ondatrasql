// OndatraSQL - You don't need a data stack anymore
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

// schemasDiffer compares the column lists of `target` between the sandbox
// catalog and the prod catalog and returns true if they differ in any way
// (added, removed, renamed, or type-changed columns). Filtering excludes
// kind-specific metadata columns like _content_hash and SCD2 valid_from/to
// so the diff reflects user-meaningful schema changes only.
//
// Bug S6 fix: showDagSandboxSummary used to compare only row counts, so a
// pure schema change (column added with identical row count) was reported
// as "unchanged".
func schemasDiffer(sess *duckdb.Session, target string) (bool, error) {
	parts := strings.SplitN(target, ".", 2)
	if len(parts) != 2 {
		return false, fmt.Errorf("invalid target %q (expected schema.table)", target)
	}
	schema, table := parts[0], parts[1]

	colsForCatalog := func(catalog string) (map[string]string, error) {
		q := fmt.Sprintf(`
			SELECT column_name, data_type
			FROM information_schema.columns
			WHERE table_catalog = '%s'
			  AND table_schema = '%s'
			  AND table_name = '%s'
			  AND column_name NOT IN ('_content_hash', 'valid_from_snapshot', 'valid_to_snapshot', 'is_current')
			ORDER BY ordinal_position`,
			duckdb.EscapeSQL(catalog), duckdb.EscapeSQL(schema), duckdb.EscapeSQL(table))
		rows, err := sess.QueryRowsMap(q)
		if err != nil {
			return nil, err
		}
		cols := make(map[string]string, len(rows))
		for _, r := range rows {
			cols[r["column_name"]] = r["data_type"]
		}
		return cols, nil
	}

	sandboxCols, err := colsForCatalog(sess.CatalogAlias())
	if err != nil {
		return false, fmt.Errorf("query sandbox columns: %w", err)
	}
	prodCols, err := colsForCatalog(sess.ProdAlias())
	if err != nil {
		return false, fmt.Errorf("query prod columns: %w", err)
	}

	if len(sandboxCols) != len(prodCols) {
		return true, nil
	}
	for name, sandboxType := range sandboxCols {
		prodType, ok := prodCols[name]
		if !ok || prodType != sandboxType {
			return true, nil
		}
	}
	return false, nil
}

// tableExistsIn checks via information_schema whether a schema.table exists
// in the given catalog. Returns (exists, error). The caller MUST handle the
// error explicitly — collapsing it to a bool would just move the silent
// "any failure means missing" mis-classification one level deeper, which is
// the bug this helper was originally written to avoid.
func tableExistsIn(sess *duckdb.Session, catalog, target string) (bool, error) {
	parts := strings.SplitN(target, ".", 2)
	if len(parts) != 2 {
		return false, fmt.Errorf("invalid target %q (expected schema.table)", target)
	}
	val, err := sess.QueryValue(fmt.Sprintf(
		"SELECT COUNT(*) FROM information_schema.tables "+
			"WHERE table_catalog = '%s' AND table_schema = '%s' AND table_name = '%s'",
		duckdb.EscapeSQL(catalog), duckdb.EscapeSQL(parts[0]), duckdb.EscapeSQL(parts[1])))
	if err != nil {
		return false, err
	}
	return val != "0" && val != "", nil
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
		// error-type guessing from SELECT COUNT(*) failures). A real
		// metadata-query failure is surfaced as a warning rather than
		// silently classified as "missing".
		existsInSandbox, existsErr := tableExistsIn(sess, sess.CatalogAlias(), m.Target)
		if existsErr != nil {
			printPaddedLine(fmt.Sprintf("  [WARN] %s: sandbox metadata error: %s", m.Target, truncate(existsErr.Error(), 40)))
			warnings++
			continue
		}
		if !existsInSandbox {
			// Table doesn't exist in sandbox — model was skipped (no changes),
			// prod table is visible via search_path. Count as unchanged.
			unchanged++
			continue
		}

		sandboxCount, sandboxErr := sess.QueryValue(fmt.Sprintf("SELECT COUNT(*) FROM %s.%s", sess.CatalogAlias(), quoteTarget(m.Target)))
		if sandboxErr != nil {
			printPaddedLine(fmt.Sprintf("  [WARN] %s: sandbox query error: %s", m.Target, truncate(sandboxErr.Error(), 40)))
			warnings++
			continue
		}

		// Check if table exists in prod via information_schema, not via
		// catching errors from COUNT(*). Real metadata-query failures are
		// warnings, not silent "new table" reclassification.
		existsInProd, prodExistsErr := tableExistsIn(sess, sess.ProdAlias(), m.Target)
		if prodExistsErr != nil {
			printPaddedLine(fmt.Sprintf("  [WARN] %s: prod metadata error: %s", m.Target, truncate(prodExistsErr.Error(), 40)))
			warnings++
			continue
		}
		if !existsInProd {
			// New table (doesn't exist in prod, but exists in sandbox)
			changed++
			changedModels = append(changedModels, m.Target)
			continue
		}
		prodCount, prodErr := sess.QueryValue(fmt.Sprintf("SELECT COUNT(*) FROM %s.%s", sess.ProdAlias(), quoteTarget(m.Target)))
		if prodErr != nil {
			printPaddedLine(fmt.Sprintf("  [WARN] %s: prod query error: %s", m.Target, truncate(prodErr.Error(), 40)))
			warnings++
			continue
		}

		var prod, sandbox int64
		fmt.Sscanf(prodCount, "%d", &prod)
		fmt.Sscanf(sandboxCount, "%d", &sandbox)

		// v0.12.1 (Bug S6 fix): in addition to the row-count and row-content
		// check, compare schemas. A column add/drop/rename without a row-count
		// change used to be reported as "unchanged" — exactly what users care
		// about validating in sandbox.
		schemaDiffers, schemaErr := schemasDiffer(sess, m.Target)
		if schemaErr != nil {
			printPaddedLine(fmt.Sprintf("  [WARN] %s: schema check error: %s", m.Target, truncate(schemaErr.Error(), 40)))
			warnings++
			continue
		}

		if prod != sandbox || schemaDiffers {
			changed++
			changedModels = append(changedModels, m.Target)
		} else {
			// Check for actual row differences even if count is same
			// SCD2 models: exclude metadata columns from diff
			diffTemplate := "queries/sandbox_diff_count.sql"
			if m.Kind == "scd2" {
				diffTemplate = "queries/sandbox_diff_count_scd2.sql"
			}
			addedSQL := sql.MustFormat(diffTemplate, sess.CatalogAlias(), sess.ProdAlias(), m.Target)
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

	// Check if new table — via information_schema, not by catching COUNT
	// errors. Surface a metadata-query failure as a warning, not as a
	// silent "new table" reclassification.
	existsInProd, existsErr := tableExistsIn(sess, sess.ProdAlias(), target)
	if existsErr != nil {
		printPaddedLine(fmt.Sprintf("  [WARN] prod metadata error: %s", truncate(existsErr.Error(), 50)))
		printEmptyLine()
		return
	}
	if !existsInProd {
		printPaddedLine("  (new table)")
		// Show column count for new tables. Schema must be in the WHERE
		// clause to avoid mixing columns from same-named tables across
		// schemas (e.g. raw.orders vs staging.orders).
		schemaName, tableName := "", target
		if parts := strings.SplitN(target, ".", 2); len(parts) == 2 {
			schemaName = parts[0]
			tableName = parts[1]
		}
		sandboxCount, _ := sess.QueryValue(fmt.Sprintf("SELECT COUNT(*) FROM %s.%s", sess.CatalogAlias(), quoteTarget(target)))
		colCount, err := sess.QueryValue(fmt.Sprintf(`
			SELECT COUNT(*) FROM information_schema.columns
			WHERE table_catalog = '%s' AND table_schema = '%s' AND table_name = '%s'
		`, duckdb.EscapeSQL(sess.CatalogAlias()), duckdb.EscapeSQL(schemaName), duckdb.EscapeSQL(tableName)))
		if err == nil && sandboxCount != "" {
			printPaddedLine(fmt.Sprintf("  Rows: %s, Columns: %s", sandboxCount, colCount))
		}
		printEmptyLine()
		return
	}

	prodCount, _ := sess.QueryValue(fmt.Sprintf("SELECT COUNT(*) FROM %s.%s", sess.ProdAlias(), quoteTarget(target)))
	sandboxCount, _ := sess.QueryValue(fmt.Sprintf("SELECT COUNT(*) FROM %s.%s", sess.CatalogAlias(), quoteTarget(target)))

	var prod, sandbox int64
	fmt.Sscanf(prodCount, "%d", &prod)
	fmt.Sscanf(sandboxCount, "%d", &sandbox)

	// Check and show schema changes FIRST (schema evolution).
	// Pass the full schema.table target so the SQL filters by both
	// catalog AND schema, avoiding cross-schema name collisions.
	schemaSQL := sql.MustFormat("queries/sandbox_schema_diff.sql", sess.CatalogAlias(), sess.ProdAlias(), target)
	// Use QueryRowsMap so the rows are properly parsed — the previous
	// code split sess.Query's CSV output on '\t' and silently dropped
	// every row, hiding all schema-evolution rendering in DAG summary.
	if rows, err := sess.QueryRowsMap(schemaSQL); err == nil && len(rows) > 0 {
		var added, removed, changed []string
		for _, row := range rows {
			colName := row["column_name"]
			prodType := row["prod_type"]
			sandboxType := row["sandbox_type"]
			changeType := row["change_type"]
			if colName == "" {
				continue
			}
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
	addedSQL := sql.MustFormat(diffTemplate, sess.CatalogAlias(), sess.ProdAlias(), target)
	removedSQL := sql.MustFormat(diffTemplate, sess.ProdAlias(), sess.CatalogAlias(), target)
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
			sampleSQL := sql.MustFormat(sampleTemplate, sess.CatalogAlias(), sess.ProdAlias(), target, "2")
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
			sampleSQL := sql.MustFormat(sampleTemplate, sess.ProdAlias(), sess.CatalogAlias(), target, "2")
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

	// Check via information_schema, not by catching COUNT errors —
	// otherwise any prod query failure gets misclassified as "new table".
	// A real metadata-query failure surfaces as a warning instead.
	existsInProd, existsErr := tableExistsIn(sess, sess.ProdAlias(), target)
	if existsErr != nil {
		printPaddedLine(fmt.Sprintf("[WARN] prod metadata error: %s", truncate(existsErr.Error(), 50)))
		printEmptyLine()
		return
	}
	if !existsInProd {
		printPaddedLine("(new table)")
		showNewTableSchema(sess, target)
		printEmptyLine()
		return
	}
	prodCount, _ := sess.QueryValue(fmt.Sprintf("SELECT COUNT(*) FROM %s.%s", sess.ProdAlias(), quoteTarget(target)))

	// Show schema changes first
	showSchemaDiff(sess, target)

	// Get sandbox count
	sandboxCount, sandboxErr := sess.QueryValue(fmt.Sprintf("SELECT COUNT(*) FROM %s.%s", sess.CatalogAlias(), quoteTarget(target)))
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

	addedSQL := sql.MustFormat(diffTemplate, sess.CatalogAlias(), sess.ProdAlias(), target)
	removedSQL := sql.MustFormat(diffTemplate, sess.ProdAlias(), sess.CatalogAlias(), target)

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

	// Show added rows (in sandbox but not in prod). The Query result
	// is CSV-shaped with the column header on the first line — slice
	// from index 1 to skip it, otherwise the header gets rendered as
	// a "+ doubled,msg" sample row, which is wrong and confusing.
	if addedCount > 0 {
		addedSQL := sql.MustFormat(sampleTemplate, sess.CatalogAlias(), sess.ProdAlias(), target, "2")
		result, err := sess.Query(addedSQL)
		if err == nil && result != "" {
			lines := strings.Split(result, "\n")
			if len(lines) > 1 {
				printPaddedLine("Sample added:")
				for _, line := range lines[1:min(3, len(lines))] {
					if line != "" {
						printPaddedLine("  + " + truncate(line, 54))
					}
				}
			}
		}
	}

	// Show removed rows (in prod but not in sandbox) — same header skip.
	if removedCount > 0 {
		removedSQL := sql.MustFormat(sampleTemplate, sess.ProdAlias(), sess.CatalogAlias(), target, "2")
		result, err := sess.Query(removedSQL)
		if err == nil && result != "" {
			lines := strings.Split(result, "\n")
			if len(lines) > 1 {
				printPaddedLine("Sample removed:")
				for _, line := range lines[1:min(3, len(lines))] {
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
	// Pass the full schema.table target — the SQL filters by both
	// catalog AND schema to avoid cross-schema name collisions.
	schemaSQL := sql.MustFormat("queries/sandbox_schema_diff.sql", sess.CatalogAlias(), sess.ProdAlias(), target)
	// Use QueryRowsMap so the rows are properly parsed — the previous code
	// split sess.Query's CSV output on '\t' (which CSV doesn't use), so
	// every row got dropped silently and schema evolution rendering was
	// effectively dead code.
	rows, err := sess.QueryRowsMap(schemaSQL)
	if err != nil || len(rows) == 0 {
		return
	}

	hasChanges := false
	var added, removed, changed []string

	for _, row := range rows {
		colName := row["column_name"]
		prodType := row["prod_type"]
		sandboxType := row["sandbox_type"]
		changeType := row["change_type"]
		if colName == "" {
			continue
		}

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
	// Parse schema.table format. Schema is REQUIRED in the WHERE clause —
	// otherwise raw.orders and staging.orders would mix columns into one
	// listing because they share the same table_name in the same catalog.
	schemaName, tableName := "", target
	if parts := strings.SplitN(target, ".", 2); len(parts) == 2 {
		schemaName = parts[0]
		tableName = parts[1]
	}

	// Get columns from sandbox (filtered by schema to disambiguate
	// same-named tables across schemas). Uses QueryRowsMap so we get
	// properly-parsed rows — sess.Query produces CSV but the previous
	// code split the result on '\t' and silently dropped every row.
	colSQL := fmt.Sprintf(`
		SELECT column_name, data_type
		FROM information_schema.columns
		WHERE table_catalog = '%s' AND table_schema = '%s' AND table_name = '%s'
		ORDER BY ordinal_position
	`, duckdb.EscapeSQL(sess.CatalogAlias()), duckdb.EscapeSQL(schemaName), duckdb.EscapeSQL(tableName))

	rows, err := sess.QueryRowsMap(colSQL)
	if err != nil || len(rows) == 0 {
		return
	}

	printPaddedLine("Columns:")
	for _, row := range rows {
		colName := row["column_name"]
		colType := row["data_type"]
		if colName != "" {
			printPaddedLine(fmt.Sprintf("  • %s (%s)", colName, colType))
		}
	}
}
