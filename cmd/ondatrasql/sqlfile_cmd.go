// OndatraSQL - You don't need a data stack anymore
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/ondatra-labs/ondatrasql/internal/config"
	"github.com/ondatra-labs/ondatrasql/internal/duckdb"
	"github.com/ondatra-labs/ondatrasql/internal/output"
)

// runSQLFile executes a SQL file from the sql/ directory.
// In sandbox mode, it runs against a copy of the catalog to preview effects.
func runSQLFile(cfg *config.Config, sqlFile string, sandboxMode bool) error {
	// Read the SQL file
	content, err := os.ReadFile(sqlFile)
	if err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("SQL file not found: %s", sqlFile)
		}
		return fmt.Errorf("read SQL file: %w", err)
	}

	sqlContent := string(content)
	if strings.TrimSpace(sqlContent) == "" {
		return fmt.Errorf("SQL file is empty: %s", sqlFile)
	}

	// sql/ files run real catalog/maintenance operations against DuckLake
	// metadata. Sandbox mode attaches the prod metadata catalog as READ_ONLY,
	// so any metadata-writing function fails partway through with a cryptic
	// transaction error. Sandbox previews don't make sense for these scripts
	// anyway — reject upfront with a clear message. (Bug 2)
	cmdName := strings.TrimSuffix(filepath.Base(sqlFile), ".sql")
	if sandboxMode {
		return fmt.Errorf(
			"%q cannot run in sandbox mode: sql/ scripts execute real catalog "+
				"operations against DuckLake metadata (run without 'sandbox' to "+
				"execute on prod)", cmdName)
	}

	// Allocate a unique per-pid sandbox directory.
	var sandboxDir string
	if sandboxMode {
		var err error
		sandboxDir, err = createSandbox(cfg)
		if err != nil {
			return fmt.Errorf("create sandbox: %w", err)
		}
		defer os.RemoveAll(sandboxDir)
	}

	// Create session
	sess, err := duckdb.NewSession("")
	if err != nil {
		return fmt.Errorf("create session: %w", err)
	}
	defer sess.Close()

	// Initialize with catalog
	if sandboxMode {
		sandboxCatalog := filepath.Join(sandboxDir, "sandbox.sqlite")
		if err := sess.InitSandbox(cfg.ConfigPath, cfg.Catalog.ConnStr, cfg.Catalog.DataPath, sandboxCatalog, cfg.Catalog.Alias); err != nil {
			return fmt.Errorf("init sandbox session: %w", err)
		}
	} else {
		if err := sess.InitWithCatalog(cfg.ConfigPath); err != nil {
			return fmt.Errorf("init session: %w", err)
		}
	}

	// Print header
	printTopBorder()
	if sandboxMode {
		printCenteredLine(fmt.Sprintf("SANDBOX: %s", cmdName))
	} else {
		printCenteredLine(strings.Title(cmdName))
	}
	printSectionBorder("")
	printEmptyLine()

	if sandboxMode {
		printPaddedLine("Running against sandbox copy (prod unchanged)")
	}
	printEmptyLine()

	// Execute the SQL
	printSectionBorder("Operations")
	printEmptyLine()

	// Split into statements and execute each
	statements := splitStatements(sqlContent)
	for _, stmt := range statements {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" || isOnlyComments(stmt) {
			continue
		}

		// Show what we're executing (truncated)
		displayStmt := truncateSQL(stmt, 55)
		printPaddedLine(displayStmt)

		// Execute
		result, err := sess.Query(stmt)
		if err != nil {
			printPaddedLine(fmt.Sprintf("  ERROR: %s", truncateStr(err.Error(), 50)))
			printEmptyLine()
			printBottomBorder()
			// Cleanup sandbox on error
			if sandboxMode {
				sess.Close()
				os.RemoveAll(sandboxDir)
			}
			return fmt.Errorf("%s failed: %w", cmdName, err)
		}

		// Show result if any
		if result != "" && !strings.HasPrefix(strings.TrimSpace(result), "count") {
			lines := strings.Split(strings.TrimSpace(result), "\n")
			for _, line := range lines[:min(5, len(lines))] {
				if line != "" {
					printPaddedLine(fmt.Sprintf("  %s", truncateStr(line, 54)))
				}
			}
			if len(lines) > 5 {
				printPaddedLine(fmt.Sprintf("  ... and %d more lines", len(lines)-5))
			}
		}
		printPaddedLine("  OK")
		printEmptyLine()
	}

	printBottomBorder()

	// Cleanup sandbox
	if sandboxMode {
		sess.Close()
		os.RemoveAll(sandboxDir)
		output.Println()
		output.Fprintf("Sandbox cleaned up. Run 'ondatrasql %s' to execute on prod.\n", cmdName)
	}

	return nil
}

// splitStatements splits SQL into individual statements.
// Handles semicolons but ignores those inside strings.
func splitStatements(sql string) []string {
	var statements []string
	var current strings.Builder
	inString := false
	stringChar := rune(0)

	for _, r := range sql {
		if inString {
			current.WriteRune(r)
			if r == stringChar {
				inString = false
			}
		} else {
			if r == '\'' || r == '"' {
				inString = true
				stringChar = r
				current.WriteRune(r)
			} else if r == ';' {
				stmt := strings.TrimSpace(current.String())
				if stmt != "" {
					statements = append(statements, stmt)
				}
				current.Reset()
			} else {
				current.WriteRune(r)
			}
		}
	}

	// Don't forget the last statement if no trailing semicolon
	stmt := strings.TrimSpace(current.String())
	if stmt != "" {
		statements = append(statements, stmt)
	}

	return statements
}

// truncateSQL truncates SQL for display, handling multi-line.
func truncateSQL(sql string, maxLen int) string {
	// Replace newlines with spaces for display
	sql = strings.ReplaceAll(sql, "\n", " ")
	sql = strings.ReplaceAll(sql, "\t", " ")
	// Collapse multiple spaces
	for strings.Contains(sql, "  ") {
		sql = strings.ReplaceAll(sql, "  ", " ")
	}
	sql = strings.TrimSpace(sql)

	if len(sql) <= maxLen {
		return sql
	}
	return sql[:maxLen-3] + "..."
}

// isOnlyComments checks if SQL contains only comments.
func isOnlyComments(sql string) bool {
	for _, line := range strings.Split(sql, "\n") {
		line = strings.TrimSpace(line)
		if line != "" && !strings.HasPrefix(line, "--") {
			return false
		}
	}
	return true
}
