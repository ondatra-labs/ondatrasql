// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
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
		defer func() { _ = os.RemoveAll(sandboxDir) }() // ignored: best-effort temp-dir cleanup
	}

	// Create session
	sess, err := duckdb.NewSession("")
	if err != nil {
		return fmt.Errorf("create session: %w", err)
	}
	defer closeSessionOrLog(sess)

	// Initialize with catalog
	if sandboxMode {
		if n := duckdb.SandboxPgActiveConnections(cfg.Catalog.ConnStr); n > 0 {
			fmt.Fprintf(os.Stderr, "warning: sandbox will terminate %d active connection(s) to postgres catalog %q\n", n, cfg.Catalog.Alias)
			if !output.JSONEnabled {
				fmt.Fprintf(os.Stderr, "Continue? [y/N] ")
				var answer string
				_, _ = fmt.Scanln(&answer) // empty input falls through to safety default below
				if answer != "y" && answer != "Y" {
					return fmt.Errorf("sandbox cancelled by user")
				}
			}
		}
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
		printCenteredLine(titleCase(cmdName))
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

		// Strip leading comments for display
		displayStmt := stripLeadingComments(stmt)
		if displayStmt == "" {
			continue
		}
		printPaddedLine(truncateSQL(displayStmt, 55))

		// Execute
		result, err := sess.Query(stmt)
		if err != nil {
			printPaddedLine(fmt.Sprintf("  ERROR: %s", truncateStr(err.Error(), 50)))
			printEmptyLine()
			printBottomBorder()
			// Cleanup sandbox on error — outer return already propagates
			// the real failure, so cleanup-call errors aren't actionable.
			if sandboxMode {
				_ = sess.Close()
				_ = os.RemoveAll(sandboxDir)
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

	// Cleanup sandbox — best-effort; the success path doesn't have an
	// alternative channel to surface cleanup errors on.
	if sandboxMode {
		_ = sess.Close()
		_ = os.RemoveAll(sandboxDir)
		output.Println()
		output.Fprintf("Sandbox cleaned up. Run 'ondatrasql %s' to execute on prod.\n", cmdName)
	}

	return nil
}

// splitStatements splits SQL into individual statements.
// Handles semicolons but ignores those inside strings.
// titleCase upper-cases the first ASCII letter of each whitespace-
// separated word. ASCII-only is fine here because the input is one of
// the fixed sql/<cmd>.sql command names (flush, merge, expire, ...)
// that we control. strings.Title is deprecated in Go 1.18+ and the
// recommended replacement (golang.org/x/text/cases) is overkill for
// this single ASCII-only call site.
func titleCase(s string) string {
	if s == "" {
		return s
	}
	out := []byte(s)
	upper := true
	for i, b := range out {
		switch {
		case b == ' ' || b == '\t' || b == '_' || b == '-':
			upper = true
		case upper && b >= 'a' && b <= 'z':
			out[i] = b - ('a' - 'A')
			upper = false
		default:
			upper = false
		}
	}
	return string(out)
}

func splitStatements(sql string) []string {
	var statements []string
	var current strings.Builder
	inString := false
	stringChar := byte(0)
	inLineComment := false
	inBlockComment := false

	for i := 0; i < len(sql); i++ {
		ch := sql[i]

		// Block comment
		if inBlockComment {
			current.WriteByte(ch)
			if ch == '*' && i+1 < len(sql) && sql[i+1] == '/' {
				current.WriteByte(sql[i+1])
				i++
				inBlockComment = false
			}
			continue
		}

		// Line comment
		if inLineComment {
			current.WriteByte(ch)
			if ch == '\n' {
				inLineComment = false
			}
			continue
		}

		// String/identifier literal
		if inString {
			current.WriteByte(ch)
			if ch == stringChar {
				if i+1 < len(sql) && sql[i+1] == stringChar {
					current.WriteByte(sql[i+1])
					i++ // escaped quote
				} else {
					inString = false
				}
			}
			continue
		}

		// Start of line comment
		if ch == '-' && i+1 < len(sql) && sql[i+1] == '-' {
			inLineComment = true
			current.WriteByte(ch)
			continue
		}

		// Start of block comment
		if ch == '/' && i+1 < len(sql) && sql[i+1] == '*' {
			inBlockComment = true
			current.WriteByte(ch)
			current.WriteByte(sql[i+1])
			i++
			continue
		}

		// String or double-quoted identifier
		if ch == '\'' || ch == '"' {
			inString = true
			stringChar = ch
			current.WriteByte(ch)
		} else if ch == ';' {
			stmt := strings.TrimSpace(current.String())
			if stmt != "" {
				statements = append(statements, stmt)
			}
			current.Reset()
		} else {
			current.WriteByte(ch)
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

// stripLeadingComments removes leading comment lines from SQL for display.
func stripLeadingComments(sql string) string {
	lines := strings.Split(sql, "\n")
	for len(lines) > 0 {
		line := strings.TrimSpace(lines[0])
		if line == "" || strings.HasPrefix(line, "--") {
			lines = lines[1:]
		} else {
			break
		}
	}
	return strings.TrimSpace(strings.Join(lines, "\n"))
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
