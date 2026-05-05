// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package script

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	duckdb "github.com/duckdb/duckdb-go/v2"
	dbsess "github.com/ondatra-labs/ondatrasql/internal/duckdb"
)

// rowCollector is the interface for accumulating rows from save() calls.
type rowCollector interface {
	add(row map[string]interface{}) error
	count() int
}

// saveCollector accumulates data from save() calls in memory.
type saveCollector struct {
	target  string
	sess    *dbsess.Session
	data    []map[string]interface{}
	columns []string
}

// add adds a row to the collector. Empty rows (0 columns) are rejected.
func (c *saveCollector) add(row map[string]interface{}) error {
	if len(row) == 0 {
		return fmt.Errorf("save.row: empty dict (no columns)")
	}
	// Track all unique columns across all rows (not just first row)
	// This prevents data loss when later rows have extra columns
	seen := make(map[string]bool)
	for _, col := range c.columns {
		seen[col] = true
	}
	for k := range row {
		if !seen[k] {
			c.columns = append(c.columns, k)
		}
	}
	// Sort for consistent ordering (Go maps are unordered)
	sort.Strings(c.columns)
	c.data = append(c.data, row)
	return nil
}

// count returns the number of rows collected so far.
func (c *saveCollector) count() int {
	return len(c.data)
}

// createTempTable creates a temp table from collected data using the DuckDB
// Appender API for efficient bulk loading without SQL string building.
func (c *saveCollector) createTempTable() (string, error) {
	if len(c.data) == 0 {
		return "", nil
	}

	if c.sess == nil {
		return "", fmt.Errorf("no database session available")
	}

	tmpTable := "tmp_" + sanitize(c.target)

	// IF EXISTS makes "table doesn't exist" not an error; a real
	// failure here (catalog locked, transaction aborted) would also
	// break the CREATE that follows, so propagate it.
	if err := c.sess.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", tmpTable)); err != nil {
		return "", fmt.Errorf("drop existing temp table %s: %w", tmpTable, err)
	}

	// Infer column types from data and create the table
	colTypes := c.inferTypes()
	var b strings.Builder
	b.WriteString(fmt.Sprintf("CREATE TEMP TABLE %s (", tmpTable))
	for i, col := range c.columns {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(quoteIdentifier(col))
		b.WriteByte(' ')
		b.WriteString(colTypes[col])
	}
	b.WriteByte(')')
	if err := c.sess.Exec(b.String()); err != nil {
		return "", fmt.Errorf("create temp table: %w", err)
	}

	// Use Appender API for bulk loading
	err := c.sess.RawConn(func(raw any) error {
		driverConn, ok := raw.(driver.Conn)
		if !ok {
			return fmt.Errorf("unexpected connection type: %T", raw)
		}

		appender, err := duckdb.NewAppenderFromConn(driverConn, "", tmpTable)
		if err != nil {
			return fmt.Errorf("create appender: %w", err)
		}
		defer func() { _ = appender.Close() }() // appender flushed by caller; close error not actionable here

		for _, row := range c.data {
			vals := make([]driver.Value, len(c.columns))
			for j, col := range c.columns {
				v := row[col]
				// Fix JSON round-trip: float64 that are whole numbers → int64
				// for BIGINT columns (json.Unmarshal converts all numbers to float64).
				// Refuse to silently truncate fractional values — if the type was
				// inferred as BIGINT from earlier whole-number rows, a later
				// fractional value means the inference was wrong and the caller
				// needs to know rather than getting silently rounded data.
				if f, ok := v.(float64); ok && colTypes[col] == "BIGINT" {
					if f != float64(int64(f)) {
						return fmt.Errorf(
							"column %q: fractional value %v cannot be stored as BIGINT (column type was inferred from earlier whole-number rows; mix integer and decimal values explicitly or cast in the source)",
							col, f)
					}
					v = int64(f)
				}
				// Convert slices/maps to JSON strings for VARCHAR columns.
				// DuckDB Appender can't handle Go slices/maps directly.
				switch v.(type) {
				case []any, map[string]any:
					if b, err := json.Marshal(v); err == nil {
						v = string(b)
					} else {
						v = fmt.Sprintf("%v", v)
					}
				}
				vals[j] = v // nil maps to NULL
			}
			if err := appender.AppendRow(vals...); err != nil {
				return fmt.Errorf("append row: %w", err)
			}
		}
		return nil
	})
	if err != nil {
		return "", err
	}

	return tmpTable, nil
}

// inferTypes scans collected data to determine DuckDB column types.
func (c *saveCollector) inferTypes() map[string]string {
	types := make(map[string]string)
	for _, col := range c.columns {
		types[col] = "VARCHAR" // default
	}

	// Scan rows to find first non-nil value per column
	for _, row := range c.data {
		allResolved := true
		for _, col := range c.columns {
			if types[col] != "VARCHAR" || row[col] == nil {
				if row[col] == nil {
					allResolved = false
				}
				continue
			}
			switch v := row[col].(type) {
			case int, int64:
				types[col] = "BIGINT"
			case float64:
				// JSON unmarshal converts all numbers to float64.
				// Detect integers to preserve BIGINT type.
				if v == float64(int64(v)) {
					types[col] = "BIGINT"
					row[col] = int64(v) // convert back for Appender
				} else {
					types[col] = "DOUBLE"
				}
			case bool:
				types[col] = "BOOLEAN"
			default:
				types[col] = "VARCHAR"
			}
		}
		if allResolved {
			break
		}
	}
	return types
}

// sanitize converts a target name to a safe identifier.
func sanitize(s string) string {
	result := ""
	for _, c := range s {
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_' {
			result += string(c)
		} else {
			result += "_"
		}
	}
	return result
}

// quoteIdentifier quotes a SQL identifier for safe use in queries.
// Handles special characters, spaces, reserved words, etc.
func quoteIdentifier(s string) string {
	escaped := ""
	for _, c := range s {
		if c == '"' {
			escaped += `""`
		} else {
			escaped += string(c)
		}
	}
	return `"` + escaped + `"`
}
