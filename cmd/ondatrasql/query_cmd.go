// OndatraSQL - You don't need a data stack anymore
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
		switch args[i] {
		case "--limit", "-l":
			if i+1 < len(args) {
				n, err := strconv.Atoi(args[i+1])
				if err != nil {
					return fmt.Errorf("invalid limit: %s", args[i+1])
				}
				if n <= 0 {
					return fmt.Errorf("--limit must be a positive integer (got %d)", n)
				}
				limit = n
				i++
			}
		default:
			if !strings.HasPrefix(args[i], "-") && model == "" {
				model = args[i]
			}
		}
	}

	sess, err := duckdb.NewSession("")
	if err != nil {
		return fmt.Errorf("create session: %w", err)
	}
	defer sess.Close()

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
	if output.JSONEnabled {
		output.EmitJSON(map[string]any{
			"model": model,
			"limit": limit,
			"runs":  rows,
		})
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
	padding := (historyBoxWidth - len(title)) / 2
	rightPad := historyBoxWidth - len(title) - padding
	output.Println(vertical + strings.Repeat(" ", padding) + title + strings.Repeat(" ", rightPad) + vertical)

	// Section border
	output.Println(leftT + strings.Repeat(horizontal, historyBoxWidth) + rightT)

	// Empty line
	output.Println(vertical + strings.Repeat(" ", historyBoxWidth) + vertical)

	if len(rows) == 0 {
		line := "  No runs found"
		output.Println(vertical + line + strings.Repeat(" ", historyBoxWidth-len(line)) + vertical)
		output.Println(vertical + strings.Repeat(" ", historyBoxWidth) + vertical)
		output.Println(bottomLeft + strings.Repeat(horizontal, historyBoxWidth) + bottomRight)
		return
	}

	// Header
	header := fmt.Sprintf("  %4s  %-19s  %-36s  %-6s  %-8s  %5s  %5s  %-22s", "ID", "Time", "Model", "Kind", "Type", "Rows", "ms", "Run ID")
	output.Println(vertical + header + strings.Repeat(" ", historyBoxWidth-len(header)) + vertical)

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
		if len(line) > historyBoxWidth {
			line = line[:historyBoxWidth-3] + "..."
		}
		padding := historyBoxWidth - len(line)
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
		return fmt.Errorf("usage: ondatrasql query <schema.table> [--limit N] [--format csv|json|markdown]")
	}

	target := args[0]
	parts := strings.SplitN(target, ".", 2)
	if len(parts) != 2 {
		return fmt.Errorf("invalid target format, expected schema.table (e.g., raw.customers)")
	}
	schema, table := parts[0], parts[1]

	// Parse flags
	limit := 0
	format := "markdown"
	for i := 1; i < len(args); i++ {
		switch args[i] {
		case "--limit", "-l":
			if i+1 < len(args) {
				n, err := strconv.Atoi(args[i+1])
				if err != nil {
					return fmt.Errorf("invalid limit: %s", args[i+1])
				}
				if n <= 0 {
					return fmt.Errorf("--limit must be a positive integer (got %d)", n)
				}
				limit = n
				i++
			}
		case "--format", "-f":
			if i+1 < len(args) {
				format = args[i+1]
				i++
			}
		}
	}

	sess, err := duckdb.NewSession("")
	if err != nil {
		return fmt.Errorf("create session: %w", err)
	}
	defer sess.Close()

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
	defer sess.Close()

	if err := sess.InitWithCatalog(cfg.ConfigPath); err != nil {
		return fmt.Errorf("init session: %w", err)
	}

	if err := sess.EnsureReadOnly(query); err != nil {
		return err
	}

	return sess.QueryPrint(query, format)
}
