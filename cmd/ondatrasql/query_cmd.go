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

// runHistory shows run history with optional model filter and limit.
// Usage: ondatrasql history [model] [--limit N]
func runHistory(cfg *config.Config, args []string) error {
	// Parse args
	var model string
	limit := 50 // Default limit

	for i := 0; i < len(args); i++ {
		a := args[i]
		switch {
		case a == "--limit" || a == "-l":
			if i+1 >= len(args) {
				return &invocationErr{fmt.Errorf("%s requires a positive integer value", a)}
			}
			n, err := strconv.Atoi(args[i+1])
			if err != nil {
				return &invocationErr{fmt.Errorf("invalid limit: %s", args[i+1])}
			}
			if n <= 0 {
				return &invocationErr{fmt.Errorf("--limit must be a positive integer (got %d)", n)}
			}
			limit = n
			i++
		case strings.HasPrefix(a, "--limit="):
			n, err := strconv.Atoi(strings.TrimPrefix(a, "--limit="))
			if err != nil {
				return &invocationErr{fmt.Errorf("invalid limit: %s", a)}
			}
			if n <= 0 {
				return &invocationErr{fmt.Errorf("--limit must be a positive integer (got %d)", n)}
			}
			limit = n
		case strings.HasPrefix(a, "-"):
			return &invocationErr{fmt.Errorf("unknown flag: %s", a)}
		default:
			if model != "" {
				return &invocationErr{fmt.Errorf("unexpected argument: %s (model already set to %q)", a, model)}
			}
			model = a
		}
	}

	sess, err := duckdb.NewSession("")
	if err != nil {
		return fmt.Errorf("create session: %w", err)
	}
	defer closeSessionOrLog(sess)

	if err := sess.InitWithCatalog(cfg.ConfigPath); err != nil {
		return fmt.Errorf("init session: %w", err)
	}

	sqlQuery, err := sql.LoadQuery("history")
	if err != nil {
		return fmt.Errorf("load query: %w", err)
	}

	// Add model filter if specified
	if model != "" {
		sqlQuery = strings.Replace(sqlQuery,
			"ORDER BY snapshot_id DESC",
			fmt.Sprintf("AND LOWER(commit_extra_info->>'model') = LOWER('%s')\nORDER BY snapshot_id DESC", duckdb.EscapeSQL(model)),
			1)
	}

	// Replace limit
	sqlQuery = strings.Replace(sqlQuery, "LIMIT 20", fmt.Sprintf("LIMIT %d", limit), 1)

	// Execute query
	rows, err := sess.QueryRowsMap(sqlQuery)
	if err != nil {
		return fmt.Errorf("query history: %w", err)
	}

	// In --json mode, emit the row list to stdout. The box rendering still
	// runs but its output is routed to stderr by the output package.
	//
	// Transform rows from the SQL query's presentation-only aliases
	// ("Run ID", "ms", "Time") into stable snake_case keys so JSON
	// consumers don't break when SQL aliases change for human display.
	// Numeric fields (id, rows, duration_ms) emit as JSON numbers,
	// not strings, so consumers don't need to parse them.
	if output.JSONEnabled {
		jsonRows := make([]map[string]any, len(rows))
		// Always emit as []. Always-present empty array beats absent-key
		// shape drift: typed clients can decode unconditionally instead
		// of branching on `if "warnings" in env`. (R6 finding.)
		warnings := []string{}
		for i, row := range rows {
			// Parse failures here indicate the snapshot history table
			// has non-numeric data in fields the query forces to integer
			// shape — surface as a warning rather than silently coercing
			// to 0 (Codex round 5 finding).
			id, idErr := strconv.ParseInt(row["ID"], 10, 64)
			if idErr != nil && row["ID"] != "" {
				warnings = append(warnings, fmt.Sprintf("row %d: parse id %q: %v", i, row["ID"], idErr))
			}
			rowsCount, rowsErr := strconv.ParseInt(row["Rows"], 10, 64)
			if rowsErr != nil && row["Rows"] != "" {
				warnings = append(warnings, fmt.Sprintf("row %d: parse rows %q: %v", i, row["Rows"], rowsErr))
			}
			duration, durErr := strconv.ParseInt(row["ms"], 10, 64)
			if durErr != nil && row["ms"] != "" {
				warnings = append(warnings, fmt.Sprintf("row %d: parse duration_ms %q: %v", i, row["ms"], durErr))
			}
			jsonRows[i] = map[string]any{
				"id":          id,
				"time":        row["Time"],
				"model":       row["Model"],
				"kind":        row["Kind"],
				"run_type":    row["Type"],
				"rows":        rowsCount,
				"duration_ms": duration,
				"run_id":      row["Run ID"],
			}
		}
		envelope := map[string]any{
			"schema_version": 1,
			"model":          model,
			"limit":          limit,
			"runs":           jsonRows,
			"warnings":       warnings,
		}
		output.EmitJSON(envelope)
		return nil
	}

	// Print box format
	printHistoryBox(rows, model, limit)
	return nil
}

// History box width (wider than standard 64)
const historyBoxWidth = 122

// printHistoryBox prints history in box format.
func printHistoryBox(rows []map[string]string, model string, limit int) {
	title := "Run History"
	if model != "" {
		title = fmt.Sprintf("History: %s", model)
	}

	// Top border
	output.Println(topLeft + strings.Repeat(horizontal, historyBoxWidth) + topRight)

	// Centered title
	titleWidth := len([]rune(title))
	padding := (historyBoxWidth - titleWidth) / 2
	rightPad := historyBoxWidth - titleWidth - padding
	output.Println(vertical + strings.Repeat(" ", padding) + title + strings.Repeat(" ", rightPad) + vertical)

	// Section border
	output.Println(leftT + strings.Repeat(horizontal, historyBoxWidth) + rightT)

	// Empty line
	output.Println(vertical + strings.Repeat(" ", historyBoxWidth) + vertical)

	if len(rows) == 0 {
		line := "  No runs found"
		output.Println(vertical + line + strings.Repeat(" ", historyBoxWidth-len([]rune(line))) + vertical)
		output.Println(vertical + strings.Repeat(" ", historyBoxWidth) + vertical)
		output.Println(bottomLeft + strings.Repeat(horizontal, historyBoxWidth) + bottomRight)
		return
	}

	// Header
	header := fmt.Sprintf("  %4s  %-19s  %-36s  %-6s  %-8s  %5s  %5s  %-22s", "ID", "Time", "Model", "Kind", "Type", "Rows", "ms", "Run ID")
	output.Println(vertical + header + strings.Repeat(" ", historyBoxWidth-len([]rune(header))) + vertical)

	// Empty line after header
	output.Println(vertical + strings.Repeat(" ", historyBoxWidth) + vertical)

	for _, row := range rows {
		// Truncate model name if needed
		modelName := row["Model"]
		if len(modelName) > 36 {
			modelName = modelName[:33] + "..."
		}

		kind := row["Kind"]
		if kind == "" {
			kind = "-"
		}

		runType := row["Type"]
		if runType == "" {
			runType = "-"
		}

		rowsVal := row["Rows"]
		if rowsVal == "" {
			rowsVal = "-"
		}

		msVal := row["ms"]
		if msVal == "" {
			msVal = "-"
		}

		runID := row["Run ID"]
		if len(runID) > 22 {
			runID = runID[:22]
		}

		line := fmt.Sprintf("  %4s  %-19s  %-36s  %-6s  %-8s  %5s  %5s  %-22s", row["ID"], row["Time"], modelName, kind, runType, rowsVal, msVal, runID)
		// Ensure line fits in box
		lineRunes := []rune(line)
		if len(lineRunes) > historyBoxWidth {
			line = string(lineRunes[:historyBoxWidth-3]) + "..."
			lineRunes = []rune(line)
		}
		padding := historyBoxWidth - len(lineRunes)
		if padding < 0 {
			padding = 0
		}
		output.Println(vertical + line + strings.Repeat(" ", padding) + vertical)
	}

	// Empty line and bottom
	output.Println(vertical + strings.Repeat(" ", historyBoxWidth) + vertical)
	output.Println(bottomLeft + strings.Repeat(horizontal, historyBoxWidth) + bottomRight)

	if len(rows) == limit {
		output.Fprintf("\nShowing %d rows (use --limit N to show more)\n", limit)
	}
}

// runQueryTable queries a model table with optional limit.
// Usage: ondatrasql query <schema.table> [--limit N] [--format csv|json|markdown]
func runQueryTable(cfg *config.Config, args []string) error {
	if len(args) < 1 {
		return &invocationErr{fmt.Errorf("usage: ondatrasql query <schema.table> [--limit N] [--format csv|json|markdown]")}
	}

	target := args[0]
	parts := strings.SplitN(target, ".", 2)
	if len(parts) != 2 {
		return &invocationErr{fmt.Errorf("invalid target format, expected schema.table (e.g., raw.customers)")}
	}
	schema, table := parts[0], parts[1]

	// Parse flags
	limit := 0
	format := "markdown"
	for i := 1; i < len(args); i++ {
		a := args[i]
		switch {
		case a == "--limit" || a == "-l":
			if i+1 >= len(args) {
				return &invocationErr{fmt.Errorf("%s requires a positive integer value", a)}
			}
			n, err := strconv.Atoi(args[i+1])
			if err != nil {
				return &invocationErr{fmt.Errorf("invalid limit: %s", args[i+1])}
			}
			if n <= 0 {
				return &invocationErr{fmt.Errorf("--limit must be a positive integer (got %d)", n)}
			}
			limit = n
			i++
		case strings.HasPrefix(a, "--limit="):
			n, err := strconv.Atoi(strings.TrimPrefix(a, "--limit="))
			if err != nil {
				return &invocationErr{fmt.Errorf("invalid limit: %s", a)}
			}
			if n <= 0 {
				return &invocationErr{fmt.Errorf("--limit must be a positive integer (got %d)", n)}
			}
			limit = n
		case a == "--format" || a == "-f":
			if i+1 >= len(args) {
				return &invocationErr{fmt.Errorf("%s requires a value (csv|json|markdown)", a)}
			}
			format = args[i+1]
			i++
		case strings.HasPrefix(a, "--format="):
			format = strings.TrimPrefix(a, "--format=")
		case strings.HasPrefix(a, "-"):
			return &invocationErr{fmt.Errorf("unknown flag: %s", a)}
		default:
			return &invocationErr{fmt.Errorf("unexpected argument: %s", a)}
		}
	}
	switch format {
	case "csv", "json", "markdown", "md":
	default:
		return &invocationErr{fmt.Errorf("invalid --format value %q (want csv|json|markdown)", format)}
	}

	sess, err := duckdb.NewSession("")
	if err != nil {
		return fmt.Errorf("create session: %w", err)
	}
	defer closeSessionOrLog(sess)

	if err := sess.InitWithCatalog(cfg.ConfigPath); err != nil {
		return fmt.Errorf("init session: %w", err)
	}

	// Build query - session already in DuckLake context after InitWithCatalog
	var query string
	quotedTable := duckdb.QuoteIdentifier(schema) + "." + duckdb.QuoteIdentifier(table)
	if limit > 0 {
		query = fmt.Sprintf("SELECT * FROM %s LIMIT %d", quotedTable, limit)
	} else {
		query = fmt.Sprintf("SELECT * FROM %s", quotedTable)
	}

	if output.JSONEnabled {
		rows, err := sess.QueryRowsAny(query)
		if err != nil {
			return fmt.Errorf("query: %w", err)
		}
		jsonRows := make([]map[string]any, len(rows))
		for i, row := range rows {
			out := make(map[string]any, len(row))
			for k, v := range row {
				out[k] = duckdb.JSONValue(v)
			}
			jsonRows[i] = out
		}
		// warnings:[] for shape parity with history/run/stats envelopes.
		// query has no current warning sources, but the field is part
		// of the contract so typed clients can decode unconditionally
		// across all read commands.
		output.EmitJSON(map[string]any{
			"schema_version": 1,
			"table":          target,
			"limit":          limit,
			"rows":           jsonRows,
			"warnings":       []string{},
		})
		return nil
	}

	return sess.QueryPrint(query, format)
}

// runSQL executes a read-only SQL query and prints the results.
// Usage: ondatrasql sql "SELECT ..."
//
// Only read-only statements are accepted: SELECT (including WITH ... SELECT,
// FROM-first, VALUES), EXPLAIN, DESCRIBE, SHOW, SUMMARIZE, and table function
// calls invoked via FROM (e.g. read_csv, glob, lake.snapshots). DDL/DML
// statements (CREATE/DROP/ALTER/INSERT/UPDATE/DELETE), CALL procedures
// (e.g. ducklake_merge_adjacent_files), and PRAGMA are rejected — PRAGMA
// because DuckDB uses the same statement type for read pragmas (PRAGMA
// show_tables) and mutating ones (PRAGMA threads=1, PRAGMA enable_profiling),
// and we can't allow one without allowing the other. Use SHOW/DESCRIBE/
// SUMMARIZE for read-only introspection and models in models/ for data
// mutations. See allowedSQLStmtTypes in internal/duckdb/session.go.
func runSQL(cfg *config.Config, query string, format string) error {
	sess, err := duckdb.NewSession("")
	if err != nil {
		return fmt.Errorf("create session: %w", err)
	}
	defer closeSessionOrLog(sess)

	if err := sess.InitWithCatalog(cfg.ConfigPath); err != nil {
		return fmt.Errorf("init session: %w", err)
	}

	if err := sess.EnsureReadOnly(query); err != nil {
		return err
	}

	if output.JSONEnabled {
		rows, err := sess.QueryRowsAny(query)
		if err != nil {
			return fmt.Errorf("sql: %w", err)
		}
		jsonRows := make([]map[string]any, len(rows))
		for i, row := range rows {
			out := make(map[string]any, len(row))
			for k, v := range row {
				out[k] = duckdb.JSONValue(v)
			}
			jsonRows[i] = out
		}
		// See `query` envelope above — warnings:[] is part of the
		// stable contract even when there are no warning sources.
		output.EmitJSON(map[string]any{
			"schema_version": 1,
			"query":          query,
			"rows":           jsonRows,
			"warnings":       []string{},
		})
		return nil
	}

	return sess.QueryPrint(query, format)
}
