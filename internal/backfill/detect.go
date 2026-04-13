// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package backfill

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/ondatra-labs/ondatrasql/internal/duckdb"
	"github.com/ondatra-labs/ondatrasql/internal/lineage"
)

// escapeSQL escapes single quotes for SQL string literals.
func escapeSQL(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}

// Column represents a column in a table schema.
type Column struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

// StepInfo represents a single execution step with timing.
type StepInfo struct {
	Name       string `json:"name"`
	DurationMs int64  `json:"duration_ms"`
	Status     string `json:"status"` // "ok", "skip", "error"
}

// CommitInfo contains metadata from a DuckLake commit.
type CommitInfo struct {
	// Core identification
	Model         string                  `json:"model"`
	SQLHash       string                  `json:"sql_hash"`
	SchemaHash    string                  `json:"schema_hash,omitempty"`
	Columns       []Column                `json:"columns,omitempty"`
	ColumnLineage []lineage.ColumnLineage `json:"column_lineage,omitempty"`
	RunType       string                  `json:"run_type"`
	RowsAffected  int64                   `json:"rows_affected"`
	DagRunID      string                  `json:"dag_run_id,omitempty"`
	Depends       []string                `json:"depends"`

	Kind          string `json:"kind,omitempty"`           // table/append/merge/scd2/partition
	SourceFile    string `json:"source_file,omitempty"`    // models/staging/orders.sql
	StartTime     string `json:"start_time,omitempty"`     // ISO8601 timestamp
	EndTime       string `json:"end_time,omitempty"`       // ISO8601 timestamp
	DurationMs    int64  `json:"duration_ms,omitempty"`    // Execution time in milliseconds
	DuckDBVersion string `json:"duckdb_version,omitempty"` // e.g. "1.1.3"

	// Execution trace
	Steps []StepInfo `json:"steps,omitempty"` // Individual execution steps with timing
	Error string     `json:"error,omitempty"` // Error message if failed

	// Git source control fields (Phase 4)
	GitCommit  string `json:"git_commit,omitempty"`  // Current commit SHA
	GitBranch  string `json:"git_branch,omitempty"`  // Current branch name
	GitRepoURL string `json:"git_repo_url,omitempty"` // Remote repository URL
}

// CaptureSchema queries the schema of a table and returns columns with types.
// Supports: table, schema.table, and catalog.schema.table formats.
// Note: Model targets are always 2 parts (schema.table) after path flattening.
// Uses DuckDB macros (ondatra_get_columns*) loaded at session startup.
func CaptureSchema(sess *duckdb.Session, table string) ([]Column, error) {
	// Split into parts and build query based on format
	parts := strings.Split(table, ".")
	var query string

	switch len(parts) {
	case 1:
		// Just table name (temp tables)
		query = fmt.Sprintf("SELECT * FROM ondatra_get_columns_simple('%s')", escapeSQL(parts[0]))
	case 2:
		// schema.table (model targets after path flattening)
		query = fmt.Sprintf("SELECT * FROM ondatra_get_columns('%s', '%s')", escapeSQL(parts[0]), escapeSQL(parts[1]))
	default:
		return nil, fmt.Errorf("invalid table name %q: max 2 parts (schema.table) supported", table)
	}

	rows, err := sess.QueryRows(query)
	if err != nil {
		return nil, fmt.Errorf("query schema: %w", err)
	}

	var columns []Column
	for _, row := range rows {
		// Each row is "column_name|data_type" (pipe-separated)
		// Types are already normalized by the SQL macro ondatra_normalize_type()
		parts := strings.Split(row, "|")
		if len(parts) >= 2 {
			columns = append(columns, Column{
				Name: parts[0],
				Type: parts[1],
			})
		}
	}

	return columns, nil
}

// ComputeSchemaHash creates a deterministic hash of the schema.
func ComputeSchemaHash(columns []Column) string {
	if len(columns) == 0 {
		return ""
	}

	// Create sorted copy to ensure deterministic ordering
	sorted := make([]Column, len(columns))
	copy(sorted, columns)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].Name < sorted[j].Name
	})

	// Build canonical string representation
	var parts []string
	for _, col := range sorted {
		parts = append(parts, fmt.Sprintf("%s:%s", col.Name, col.Type))
	}

	// Hash it
	h := sha256.Sum256([]byte(strings.Join(parts, ",")))
	return hex.EncodeToString(h[:16]) // First 16 bytes = 32 hex chars
}

// SchemaChangeType represents the type of schema change detected.
type SchemaChangeType string

const (
	SchemaChangeNone        SchemaChangeType = "none"        // No schema changes
	SchemaChangeAdditive    SchemaChangeType = "additive"    // Only new columns added
	SchemaChangeDestructive SchemaChangeType = "destructive" // Columns dropped or types narrowed
	SchemaChangeTypeChange  SchemaChangeType = "type_change" // Type widened (promotable)
)

// TypeChange represents a column type change.
type TypeChange struct {
	Column  string
	OldType string
	NewType string
}

// ColumnRename represents a column rename detected via lineage analysis.
type ColumnRename struct {
	OldName string // Previous column name
	NewName string // New column name
	Source  string // Source reference (table.column) that links them
}

// SchemaChange describes the differences between two schemas.
type SchemaChange struct {
	Type        SchemaChangeType
	Added       []Column       // New columns
	Dropped     []Column       // Removed columns
	TypeChanged []TypeChange   // Changed types
	Renamed     []ColumnRename // Renamed columns (detected via lineage)
}

// ClassifySchemaChange compares old and new schemas to classify the change.
// If sess is provided, uses DuckDB's can_cast_implicitly() for type promotion checks.
// If sess is nil, falls back to Go-based type promotion logic.
func ClassifySchemaChange(old, new []Column, sess *duckdb.Session) SchemaChange {
	result := SchemaChange{Type: SchemaChangeNone}

	// Build maps for comparison
	oldMap := make(map[string]string)
	for _, col := range old {
		oldMap[col.Name] = col.Type
	}

	newMap := make(map[string]string)
	for _, col := range new {
		newMap[col.Name] = col.Type
	}

	// Find added columns (in new but not in old)
	for _, col := range new {
		if _, exists := oldMap[col.Name]; !exists {
			result.Added = append(result.Added, col)
		}
	}

	// Find dropped columns (in old but not in new)
	for _, col := range old {
		if _, exists := newMap[col.Name]; !exists {
			result.Dropped = append(result.Dropped, col)
		}
	}

	// Find type changes (in both but different types)
	for _, col := range new {
		if oldType, exists := oldMap[col.Name]; exists && oldType != col.Type {
			result.TypeChanged = append(result.TypeChanged, TypeChange{
				Column:  col.Name,
				OldType: oldType,
				NewType: col.Type,
			})
		}
	}

	// Classify the change type
	result.Type = classifyChangeType(result, sess)

	return result
}

// classifyChangeType determines the overall change type from the differences.
// If sess is provided, uses SQL-based type promotion check.
func classifyChangeType(change SchemaChange, sess *duckdb.Session) SchemaChangeType {
	hasAdded := len(change.Added) > 0
	hasDropped := len(change.Dropped) > 0
	hasTypeChanges := len(change.TypeChanged) > 0
	hasRenames := len(change.Renamed) > 0

	// No changes
	if !hasAdded && !hasDropped && !hasTypeChanges && !hasRenames {
		return SchemaChangeNone
	}

	// Dropped columns are always destructive
	if hasDropped {
		return SchemaChangeDestructive
	}

	// Check type changes - some are promotable, some are destructive
	if hasTypeChanges {
		if !areAllTypesPromotable(change.TypeChanged, sess) {
			return SchemaChangeDestructive
		}
		// All type changes are promotable
		if hasAdded || hasRenames {
			return SchemaChangeAdditive // Mixed: additive + promotable type changes + renames
		}
		return SchemaChangeTypeChange
	}

	// Only added columns and/or renames (both are additive operations)
	return SchemaChangeAdditive
}

// areAllTypesPromotable checks if all type changes can be done without data loss.
// Uses batched SQL query with ondatra_batch_type_check macro for efficiency.
func areAllTypesPromotable(changes []TypeChange, sess *duckdb.Session) bool {
	// Build comma-separated pairs: "old1,new1,old2,new2,..."
	var pairs []string
	for _, tc := range changes {
		pairs = append(pairs, escapeSQL(tc.OldType), escapeSQL(tc.NewType))
	}
	pairsStr := strings.Join(pairs, ",")

	query := fmt.Sprintf("SELECT ondatra_batch_type_check('%s')", pairsStr)
	result, err := sess.QueryValue(query)
	if err != nil {
		return false // Query failed, assume not promotable
	}
	return result == "true"
}

// GetPreviousSnapshot returns the snapshot ID of the latest commit for THIS
// model. Call this BEFORE materialize — the returned ID becomes the "previous"
// snapshot after materialize creates a new one. Used for audits/rollback
// comparison. Returns 0 if the model has never been committed, so audits that
// compare against a previous version skip cleanly on the first run.
//
// In sandbox mode, snapshot history must come from the prod catalog — sandbox
// has no committed history of its own. The audit SQL on the receiving side
// then time-travels against the same prod-prefixed reference (see runner.go's
// `historicalTable` plumbing) so the snapshot ID is valid where it's used.
// (Review finding 1)
func GetPreviousSnapshot(sess *duckdb.Session, target string) (int64, error) {
	snapshotCatalog := sess.CatalogAlias()
	if sess.ProdAlias() != "" {
		snapshotCatalog = sess.ProdAlias()
	}
	query := fmt.Sprintf(
		`SELECT COALESCE((SELECT snapshot_id FROM %s.snapshots()
			WHERE LOWER(commit_extra_info->>'model') = LOWER('%s')
			ORDER BY snapshot_id DESC LIMIT 1), 0)`,
		snapshotCatalog, escapeSQL(target))
	result, err := sess.QueryValue(query)
	if err != nil {
		return 0, nil
	}

	if result == "" {
		return 0, nil
	}

	var id int64
	_, err = fmt.Sscanf(result, "%d", &id)
	if err != nil {
		return 0, nil
	}

	return id, nil
}

// GetPreviousSchema retrieves the schema from the most recent commit for a target.
// Returns nil if no previous schema exists or on any error.
// Uses ondatra_get_prev_columns macro loaded at session startup.
func GetPreviousSchema(sess *duckdb.Session, target string) ([]Column, error) {
	query := fmt.Sprintf("SELECT ondatra_get_prev_columns('%s')", escapeSQL(target))

	result, err := sess.QueryValue(query)
	if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}
	if result == "" || result == "null" || result == "[]" {
		return nil, nil // No previous schema
	}

	// The ->>' operator returns text, so columns array should be a JSON string like:
	// [{"name":"id","type":"INTEGER"},{"name":"name","type":"VARCHAR"}]

	// Parse the JSON array
	var columns []Column
	if err := json.Unmarshal([]byte(result), &columns); err != nil {
		return nil, fmt.Errorf("json parse failed for '%s': %w", result, err)
	}

	return columns, nil
}

// GetDownstreamModels returns all models that depend on the given target.
// This is a reverse dependency lookup using the stored 'depends' field in commit metadata.
// Uses ondatra_get_downstream TABLE macro loaded at session startup.
func GetDownstreamModels(sess *duckdb.Session, target string) ([]string, error) {
	query := fmt.Sprintf("SELECT * FROM ondatra_get_downstream('%s')", escapeSQL(target))
	return sess.QueryRows(query)
}

// GetModelCommitInfo retrieves the full CommitInfo for a model's latest commit.
// Uses ondatra_get_commit_info macro loaded at session startup.
func GetModelCommitInfo(sess *duckdb.Session, target string) (*CommitInfo, error) {
	query := fmt.Sprintf("SELECT ondatra_get_commit_info('%s')", escapeSQL(target))

	result, err := sess.QueryValue(query)
	if err != nil {
		return nil, err
	}

	if result == "" {
		return nil, nil
	}

	var info CommitInfo
	if err := json.Unmarshal([]byte(result), &info); err != nil {
		return nil, fmt.Errorf("parse commit info: %w", err)
	}

	return &info, nil
}

