// OndatraSQL - A data pipeline framework for DuckDB + DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package execute

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/ondatra-labs/ondatrasql/internal/backfill"
	"github.com/ondatra-labs/ondatrasql/internal/duckdb"
	"github.com/ondatra-labs/ondatrasql/internal/parser"
	"github.com/ondatra-labs/ondatrasql/internal/sql"
)

// buildCommentSQL generates COMMENT ON statements for table and column descriptions.
// Returns empty string if there are no descriptions to set.
// Target is unquoted (safe: validated by parser.ValidateIdentifier, consistent with SQL templates).
func buildCommentSQL(model *parser.Model) string {
	var parts []string

	if model.Description != "" {
		parts = append(parts, fmt.Sprintf("COMMENT ON TABLE %s IS '%s'",
			model.Target, escapeSQL(model.Description)))
	}

	// Sort column names for deterministic output
	colNames := make([]string, 0, len(model.ColumnDescriptions))
	for colName := range model.ColumnDescriptions {
		colNames = append(colNames, colName)
	}
	sort.Strings(colNames)

	for _, colName := range colNames {
		parts = append(parts, fmt.Sprintf("COMMENT ON COLUMN %s.%s IS '%s'",
			model.Target, duckdb.QuoteIdentifier(colName), escapeSQL(model.ColumnDescriptions[colName])))
	}

	return strings.Join(parts, ";\n")
}

// applyComments executes COMMENT ON statements for table and column descriptions.
// Called after the commit transaction — COMMENT ON is idempotent so safe to run separately.
func (r *Runner) applyComments(model *parser.Model) error {
	commentSQL := buildCommentSQL(model)
	if commentSQL == "" {
		return nil
	}
	return r.sess.Exec(commentSQL)
}

// ensureSchemaExists creates the schema if it doesn't exist.
// Target format is always "schema.table" (model targets have 2 parts after path flattening).
func (r *Runner) ensureSchemaExists(target string) error {
	schema := strings.Split(target, ".")[0]
	query := fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", duckdb.QuoteIdentifier(schema))
	return r.sess.Exec(query)
}

// quoteIdentifiers quotes each identifier and joins with ", ".
func quoteIdentifiers(names []string) string {
	quoted := make([]string, len(names))
	for i, name := range names {
		quoted[i] = duckdb.QuoteIdentifier(name)
	}
	return strings.Join(quoted, ", ")
}

// tableExistsCheck checks if a table exists using ondatra_table_exists macro.
// Only called with model targets which are always "schema.table" (2 parts).
func (r *Runner) tableExistsCheck(target string) (bool, error) {
	parts := strings.Split(target, ".")
	query := fmt.Sprintf("SELECT ondatra_table_exists('%s', '%s')", escapeSQL(parts[0]), escapeSQL(parts[1]))
	result, err := r.sess.QueryValue(query)
	if err != nil {
		return false, err
	}
	return result == "true", nil
}

// materialize creates the target table from the temp table.
// It runs the CREATE/INSERT and commit metadata in a single transaction.
// If schemaChange is provided, it applies schema evolution (ALTER TABLE) before INSERT.
// Optional extraPreSQL statements are prepended to the transaction (e.g. ack inserts).
func (r *Runner) materialize(model *parser.Model, tmpTable string, isBackfill bool, schemaChange *backfill.SchemaChange, sqlHash, runType string, result *Result, startTime time.Time, extraPreSQL ...string) (int64, error) {
	// Get row count from temp table first
	countSQL := fmt.Sprintf("SELECT COUNT(*) FROM %s", tmpTable)
	countResult, err := r.sess.QueryValue(countSQL)
	if err != nil {
		return 0, fmt.Errorf("count temp table: %w", err)
	}

	var count int64
	if _, err := fmt.Sscanf(countResult, "%d", &count); err != nil {
		return 0, fmt.Errorf("parse row count %q: %w", countResult, err)
	}

	// Ensure schema exists before creating table
	if err := r.ensureSchemaExists(model.Target); err != nil {
		return 0, fmt.Errorf("ensure schema: %w", err)
	}

	// Apply schema evolution if needed (before INSERT)
	// This adds new columns or widens types on the target table
	if schemaChange != nil && !isBackfill {
		if err := r.applySchemaEvolution(model.Target, *schemaChange); err != nil {
			return 0, fmt.Errorf("schema evolution: %w", err)
		}
	}

	// In sandbox mode, force backfill if target table doesn't exist in sandbox.
	// Incremental operations (INSERT INTO, MERGE) require the table to exist.
	if !isBackfill && model.Kind != "table" && r.sess.ProdAlias() != "" {
		if !r.tableExistsInCatalog(model.Target, r.sess.CatalogAlias()) {
			isBackfill = true
		}
	}

	// Build the main SQL statement
	var mainSQL string

	switch model.Kind {
	case "table":
		mainSQL = sql.MustFormat("execute/table.sql", model.Target, tmpTable)

	case "append":
		if isBackfill {
			mainSQL = sql.MustFormat("execute/table.sql", model.Target, tmpTable)
		} else {
			mainSQL = sql.MustFormat("execute/append.sql", model.Target, tmpTable)
		}

	case "merge":
		if isBackfill {
			mainSQL = sql.MustFormat("execute/table.sql", model.Target, tmpTable)
		} else {
			// Use MERGE INTO for upsert (DuckLake doesn't support INSERT OR REPLACE)
			if model.UniqueKey == "" {
				return 0, fmt.Errorf("merge kind requires unique_key directive")
			}
			// Build MERGE INTO statement
			// Get columns from temp table for UPDATE SET clause
			cols, err := r.getTableColumns(tmpTable)
			if err != nil {
				return 0, fmt.Errorf("get columns for merge: %w", err)
			}
			mainSQL = r.buildMergeSQL(model.Target, tmpTable, model.UniqueKey, cols)
		}

	case "scd2":
		if model.UniqueKey == "" {
			return 0, fmt.Errorf("scd2 kind requires unique_key directive")
		}
		// SCD2 handles its own transaction - return early
		return r.materializeSCD2(model, tmpTable, isBackfill, sqlHash, runType, result, startTime)

	case "partition":
		if model.UniqueKey == "" {
			return 0, fmt.Errorf("partition kind requires unique_key directive")
		}
		// Partition handles its own transaction - return early
		return r.materializePartition(model, tmpTable, isBackfill, sqlHash, runType, result, startTime)

	default:
		return 0, fmt.Errorf("unknown kind: %s", model.Kind)
	}

	// Capture schema and lineage metadata - with detailed sub-step tracing
	var stepStart time.Time

	// Sub-step: Capture schema from temp table (information_schema query)
	stepStart = time.Now()
	columns, err := backfill.CaptureSchema(r.sess, tmpTable)
	if err != nil {
		// Log warning but don't fail - schema tracking is optional
		columns = nil
	}
	r.trace(result, "meta.schema_capture", stepStart, "ok")

	// Sub-step: Compute schema hash (in-memory)
	stepStart = time.Now()
	schemaHash := backfill.ComputeSchemaHash(columns)
	r.trace(result, "meta.schema_hash", stepStart, "ok")

	// Sub-step: Extract column-level lineage and table dependencies (single AST query)
	stepStart = time.Now()
	colLineage, tableDeps, _ := r.extractLineage(model.SQL)
	r.trace(result, "meta.lineage_extract", stepStart, "ok")

	// Sub-step: JSON serialization (in-memory)
	// Build commit metadata JSON with dag_run_id, depends, schema, and lineage
	// Build commit metadata
	endTime := time.Now()

	// Convert trace steps to commit metadata format
	steps := ConvertTraceToSteps(result.Trace)

	info := backfill.CommitInfo{
		Model:         model.Target,
		SQLHash:       sqlHash,
		SchemaHash:    schemaHash,
		Columns:       columns,
		ColumnLineage: colLineage,
		RunType:       runType,
		RowsAffected:  count,
		DagRunID:      r.dagRunID,
		Depends:       tableDeps,
		// Execution metadata
		Kind:          model.Kind,
		SourceFile:    model.FilePath,
		StartTime:     startTime.UTC().Format(time.RFC3339),
		EndTime:       endTime.UTC().Format(time.RFC3339),
		DurationMs:    endTime.Sub(startTime).Milliseconds(),
		DuckDBVersion: r.sess.GetVersion(),
		// Execution trace
		Steps:      steps,

		// Git fields
		GitCommit:  r.gitInfo.Commit,
		GitBranch:  r.gitInfo.Branch,
		GitRepoURL: r.gitInfo.RepoURL,
	}
	jsonBytes, err := json.Marshal(info)
	if err != nil {
		return 0, fmt.Errorf("marshal commit metadata: %w", err)
	}
	extraInfo := string(jsonBytes)
	r.trace(result, "meta.json_serialize", stepStart, "ok")

	// Prepend extra SQL (e.g. ack inserts) and registry upsert to main SQL (within same transaction)
	var preParts []string
	for _, extra := range extraPreSQL {
		if extra != "" {
			preParts = append(preParts, extra)
		}
	}
	registrySQL := fmt.Sprintf("DELETE FROM _ondatra_registry WHERE target = '%s';\nINSERT INTO _ondatra_registry VALUES ('%s', '%s', current_timestamp)",
		escapeSQL(model.Target), escapeSQL(model.Target), escapeSQL(model.Kind))
	preParts = append(preParts, registrySQL)
	mainSQL = strings.Join(preParts, ";\n") + ";\n" + mainSQL

	// Execute everything in a single transaction
	// Escape extraInfo to prevent SQL injection from file paths/lineage with quotes
	txnSQL := sql.MustFormat("execute/commit.sql", mainSQL, model.Target, escapeSQL(extraInfo))

	stepStart = time.Now()
	if err := r.sess.Exec(txnSQL); err != nil {
		r.trace(result, "commit", stepStart, "error")
		return 0, err
	}
	r.trace(result, "commit", stepStart, "ok")

	// Apply COMMENT ON statements (best-effort, data already committed)
	r.applyComments(model)

	// Apply DuckLake storage hints (best-effort, on backfill or full table replacement)
	if isBackfill || model.Kind == "table" {
		r.applyStorageHints(model)
	}

	return count, nil
}

// applyStorageHints applies DuckLake native storage optimizations.
// These are idempotent ALTER TABLE operations that only affect future writes.
// - @sorted_by: ALTER TABLE SET SORTED BY (improves min/max statistics, faster queries)
// - @partitioned_by: ALTER TABLE SET PARTITIONED BY (auto-organizes files)
func (r *Runner) applyStorageHints(model *parser.Model) {
	if len(model.SortedBy) > 0 {
		cols := quoteIdentifiers(model.SortedBy)
		sortSQL := fmt.Sprintf("ALTER TABLE %s SET SORTED BY (%s)", model.Target, cols)
		r.sess.Exec(sortSQL)
	}

	if len(model.PartitionedBy) > 0 {
		cols := quoteIdentifiers(model.PartitionedBy)
		partSQL := fmt.Sprintf("ALTER TABLE %s SET PARTITIONED BY (%s)", model.Target, cols)
		r.sess.Exec(partSQL)
	}
}

// buildMergeSQL builds a MERGE INTO statement for upsert operations.
func (r *Runner) buildMergeSQL(target, source, uniqueKey string, cols []string) string {
	// Quote identifiers to handle reserved words, spaces, and mixed casing
	qk := duckdb.QuoteIdentifier(uniqueKey)

	// Build UPDATE SET clause for all columns except the unique key
	var setClauses []string
	for _, col := range cols {
		if col != uniqueKey {
			qc := duckdb.QuoteIdentifier(col)
			setClauses = append(setClauses, fmt.Sprintf("%s = source.%s", qc, qc))
		}
	}

	setClause := "SET " + strings.Join(setClauses, ", ")
	if len(setClauses) == 0 {
		// If only the unique key column exists, just update the key itself
		setClause = fmt.Sprintf("SET %s = source.%s", qk, qk)
	}

	return sql.MustFormat("execute/merge.sql", target, source, qk, qk, setClause)
}

// applySchemaEvolution applies additive schema changes (renames, new columns, type promotions) to the target table.
// This is called before materialization when we can evolve the schema instead of doing a full backfill.
func (r *Runner) applySchemaEvolution(target string, change backfill.SchemaChange) error {
	// Apply column renames first (before adding new columns, in case of name conflicts)
	for _, rename := range change.Renamed {
		oldName := duckdb.QuoteIdentifier(rename.OldName)
		newName := duckdb.QuoteIdentifier(rename.NewName)
		alterSQL := sql.MustFormat("schema/alter_rename_column.sql", target, oldName, newName)
		if err := r.sess.Exec(alterSQL); err != nil {
			return fmt.Errorf("rename column %s to %s: %w", rename.OldName, rename.NewName, err)
		}
	}

	// Apply additive changes (new columns)
	for _, col := range change.Added {
		qc := duckdb.QuoteIdentifier(col.Name)
		alterSQL := sql.MustFormat("schema/alter_add_column.sql", target, qc, col.Type)
		if err := r.sess.Exec(alterSQL); err != nil {
			return fmt.Errorf("add column %s: %w", col.Name, err)
		}
	}

	// Apply type promotions
	for _, tc := range change.TypeChanged {
		qc := duckdb.QuoteIdentifier(tc.Column)
		alterSQL := sql.MustFormat("schema/alter_column_type.sql", target, qc, tc.NewType)
		if err := r.sess.Exec(alterSQL); err != nil {
			return fmt.Errorf("alter column %s type: %w", tc.Column, err)
		}
	}

	return nil
}

// materializeSCD2 creates or updates an SCD2 table with history tracking.
// SCD2 tables have additional columns: valid_from_snapshot, valid_to_snapshot, is_current.
// On first run (isBackfill=true or table doesn't exist), creates the table with SCD2 columns.
// On subsequent runs, closes old versions and inserts new versions for changed/new rows.
func (r *Runner) materializeSCD2(model *parser.Model, tmpTable string, isBackfill bool, sqlHash, runType string, result *Result, startTime time.Time) (int64, error) {
	uk := duckdb.QuoteIdentifier(model.UniqueKey)
	ukRaw := model.UniqueKey

	// Ensure schema exists before creating table
	if err := r.ensureSchemaExists(model.Target); err != nil {
		return 0, fmt.Errorf("ensure schema: %w", err)
	}

	// Check if target table exists using macro
	tableExists, err := r.tableExistsCheck(model.Target)
	if err != nil {
		return 0, fmt.Errorf("check table exists: %w", err)
	}

	// Get current snapshot
	snapshotResult, err := r.sess.QueryValue("SELECT getvariable('curr_snapshot')::BIGINT")
	if err != nil {
		return 0, fmt.Errorf("get current snapshot: %w", err)
	}
	var currSnapshot int64
	if _, err := fmt.Sscanf(snapshotResult, "%d", &currSnapshot); err != nil {
		return 0, fmt.Errorf("parse snapshot %q: %w", snapshotResult, err)
	}

	// Get source columns (excluding SCD2 columns if they somehow got included)
	sourceCols, err := r.getTableColumns(tmpTable)
	if err != nil {
		return 0, fmt.Errorf("get source columns: %w", err)
	}
	// Filter out any SCD2 columns that might be in source
	var cleanCols []string
	for _, col := range sourceCols {
		if col != "valid_from_snapshot" && col != "valid_to_snapshot" && col != "is_current" {
			cleanCols = append(cleanCols, col)
		}
	}
	colList := quoteIdentifiers(cleanCols)

	var rowsAffected int64

	if !tableExists || isBackfill {
		// First run: Create table with SCD2 columns
		createSQL := sql.MustFormat("execute/scd2_init.sql", model.Target, colList, currSnapshot, tmpTable)

		// Count rows
		countSQL := fmt.Sprintf("SELECT COUNT(*) FROM %s", tmpTable)
		countResult, err := r.sess.QueryValue(countSQL)
		if err != nil {
			return 0, fmt.Errorf("count rows: %w", err)
		}
		if _, err := fmt.Sscanf(countResult, "%d", &rowsAffected); err != nil {
			return 0, fmt.Errorf("parse row count %q: %w", countResult, err)
		}

		// Build commit metadata
		stepStart := time.Now()
		columns, _ := backfill.CaptureSchema(r.sess, tmpTable)
		schemaHash := backfill.ComputeSchemaHash(columns)
		colLineage, tableDeps, _ := r.extractLineage(model.SQL)
		r.trace(result, "metadata", stepStart, "ok")

		endTime := time.Now()
		steps := ConvertTraceToSteps(result.Trace)
		info := backfill.CommitInfo{
			Model:         model.Target,
			SQLHash:       sqlHash,
			SchemaHash:    schemaHash,
			Columns:       columns,
			ColumnLineage: colLineage,
			RunType:       runType,
			RowsAffected:  rowsAffected,
			DagRunID:      r.dagRunID,
			Depends:       tableDeps,
			// Execution metadata
			Kind:          model.Kind,
			SourceFile:    model.FilePath,
			StartTime:     startTime.UTC().Format(time.RFC3339),
			EndTime:       endTime.UTC().Format(time.RFC3339),
			DurationMs:    endTime.Sub(startTime).Milliseconds(),
			DuckDBVersion: r.sess.GetVersion(),
			// Execution trace
			Steps:      steps,
	
			// Git fields
			GitCommit:  r.gitInfo.Commit,
			GitBranch:  r.gitInfo.Branch,
			GitRepoURL: r.gitInfo.RepoURL,
		}
		jsonBytes, err := json.Marshal(info)
		if err != nil {
			return 0, fmt.Errorf("marshal commit metadata: %w", err)
		}
		extraInfo := string(jsonBytes)

		// Execute in transaction
		// Escape extraInfo to prevent SQL injection from file paths/lineage with quotes
		txnSQL := sql.MustFormat("execute/commit.sql", createSQL, model.Target, escapeSQL(extraInfo))

		stepStart = time.Now()
		if err := r.sess.Exec(txnSQL); err != nil {
			r.trace(result, "commit", stepStart, "error")
			return 0, fmt.Errorf("create scd2 table: %w", err)
		}
		r.trace(result, "commit", stepStart, "ok")

		r.applyComments(model)
		r.applyStorageHints(model)

		return rowsAffected, nil
	}

	// Incremental run: Find changes and update history

	// Build comparison columns (exclude unique_key for change detection)
	var compareCols []string
	for _, col := range cleanCols {
		if col != ukRaw {
			compareCols = append(compareCols, col)
		}
	}

	// Build WHERE clause for detecting changes
	var changeConditions []string
	for _, col := range compareCols {
		qc := duckdb.QuoteIdentifier(col)
		changeConditions = append(changeConditions, fmt.Sprintf("(t.%s IS DISTINCT FROM c.%s)", qc, qc))
	}
	changeWhere := strings.Join(changeConditions, " OR ")
	if changeWhere == "" {
		changeWhere = "false" // No columns to compare
	}

	// Step 1+2: Create temp tables for changes and deletions in one round-trip
	detectSQL := sql.MustFormat("execute/scd2_detect.sql",
		model.Target, tmpTable, uk, uk, tmpTable, uk, uk, changeWhere,
		uk, model.Target, uk, uk, tmpTable)

	if err := r.sess.Exec(detectSQL); err != nil {
		return 0, fmt.Errorf("detect scd2 changes: %w", err)
	}

	// Count affected rows (new + changed)
	countSQL := "SELECT COUNT(*) FROM scd2_changes"
	countResult, err := r.sess.QueryValue(countSQL)
	if err != nil {
		r.sess.Exec("DROP TABLE IF EXISTS scd2_changes")
		r.sess.Exec("DROP TABLE IF EXISTS scd2_deleted")
		return 0, fmt.Errorf("count changes: %w", err)
	}
	if _, err := fmt.Sscanf(countResult, "%d", &rowsAffected); err != nil {
		return 0, fmt.Errorf("parse row count %q: %w", countResult, err)
	}

	// Build commit metadata
	stepStart := time.Now()
	columns, _ := backfill.CaptureSchema(r.sess, tmpTable)
	schemaHash := backfill.ComputeSchemaHash(columns)
	colLineage, tableDeps, _ := r.extractLineage(model.SQL)
	r.trace(result, "metadata", stepStart, "ok")

	endTime := time.Now()
	steps := ConvertTraceToSteps(result.Trace)
	info := backfill.CommitInfo{
		Model:         model.Target,
		SQLHash:       sqlHash,
		SchemaHash:    schemaHash,
		Columns:       columns,
		ColumnLineage: colLineage,
		RunType:       runType,
		RowsAffected:  rowsAffected,
		DagRunID:      r.dagRunID,
		Depends:       tableDeps,
		// Execution metadata
		Kind:          model.Kind,
		SourceFile:    model.FilePath,
		StartTime:     startTime.UTC().Format(time.RFC3339),
		EndTime:       endTime.UTC().Format(time.RFC3339),
		DurationMs:    endTime.Sub(startTime).Milliseconds(),
		DuckDBVersion: r.sess.GetVersion(),
		// Execution trace
		Steps:      steps,

		// Git fields
		GitCommit:  r.gitInfo.Commit,
		GitBranch:  r.gitInfo.Branch,
		GitRepoURL: r.gitInfo.RepoURL,
	}
	jsonBytes, err := json.Marshal(info)
	if err != nil {
		return 0, fmt.Errorf("marshal commit metadata: %w", err)
	}
	extraInfo := string(jsonBytes)

	// Step 3: Close old versions (changed + deleted)
	// Step 4: Insert new versions (new + changed)
	// Escape extraInfo to prevent SQL injection from file paths/lineage with quotes
	txnSQL := sql.MustFormat("execute/scd2_update.sql",
		model.Target, currSnapshot-1, uk, uk,
		model.Target, currSnapshot-1, uk, uk,
		model.Target, colList, colList, currSnapshot,
		model.Target, escapeSQL(extraInfo))

	stepStart = time.Now()
	if err := r.sess.Exec(txnSQL); err != nil {
		r.trace(result, "commit", stepStart, "error")
		r.sess.Exec("DROP TABLE IF EXISTS scd2_changes")
		r.sess.Exec("DROP TABLE IF EXISTS scd2_deleted")
		return 0, fmt.Errorf("update scd2 table: %w", err)
	}
	r.trace(result, "commit", stepStart, "ok")

	r.applyComments(model)

	// Cleanup temp tables
	r.sess.Exec("DROP TABLE IF EXISTS scd2_changes")
	r.sess.Exec("DROP TABLE IF EXISTS scd2_deleted")

	return rowsAffected, nil
}

// materializePartition creates or updates a partition table.
// Partition tables replace entire partitions (DELETE + INSERT) instead of row-by-row upserts.
// On first run (isBackfill=true or table doesn't exist), creates the table.
// On subsequent runs, deletes matching partitions and inserts new data.
func (r *Runner) materializePartition(model *parser.Model, tmpTable string, isBackfill bool, sqlHash, runType string, result *Result, startTime time.Time) (int64, error) {
	// Split UniqueKey on comma for partition column list
	var partitionColList []string
	for _, col := range strings.Split(model.UniqueKey, ",") {
		col = strings.TrimSpace(col)
		if col != "" {
			partitionColList = append(partitionColList, col)
		}
	}
	partitionCols := quoteIdentifiers(partitionColList)

	// Ensure schema exists before creating table
	if err := r.ensureSchemaExists(model.Target); err != nil {
		return 0, fmt.Errorf("ensure schema: %w", err)
	}

	// Check if target table exists using macro
	tableExists, err := r.tableExistsCheck(model.Target)
	if err != nil {
		return 0, fmt.Errorf("check table exists: %w", err)
	}

	// Count rows in temp table
	countSQL := fmt.Sprintf("SELECT COUNT(*) FROM %s", tmpTable)
	countResult, err := r.sess.QueryValue(countSQL)
	if err != nil {
		return 0, fmt.Errorf("count rows: %w", err)
	}
	var rowsAffected int64
	if _, err := fmt.Sscanf(countResult, "%d", &rowsAffected); err != nil {
		return 0, fmt.Errorf("parse row count %q: %w", countResult, err)
	}

	// Build commit metadata
	stepStart := time.Now()
	columns, _ := backfill.CaptureSchema(r.sess, tmpTable)
	schemaHash := backfill.ComputeSchemaHash(columns)
	colLineage, tableDeps, _ := r.extractLineage(model.SQL)
	r.trace(result, "metadata", stepStart, "ok")

	endTime := time.Now()
	steps := ConvertTraceToSteps(result.Trace)
	info := backfill.CommitInfo{
		Model:         model.Target,
		SQLHash:       sqlHash,
		SchemaHash:    schemaHash,
		Columns:       columns,
		ColumnLineage: colLineage,
		RunType:       runType,
		RowsAffected:  rowsAffected,
		DagRunID:      r.dagRunID,
		Depends:       tableDeps,
		// Execution metadata
		Kind:          model.Kind,
		SourceFile:    model.FilePath,
		StartTime:     startTime.UTC().Format(time.RFC3339),
		EndTime:       endTime.UTC().Format(time.RFC3339),
		DurationMs:    endTime.Sub(startTime).Milliseconds(),
		DuckDBVersion: r.sess.GetVersion(),
		// Execution trace
		Steps:      steps,

		// Git fields
		GitCommit:  r.gitInfo.Commit,
		GitBranch:  r.gitInfo.Branch,
		GitRepoURL: r.gitInfo.RepoURL,
	}
	jsonBytes, err := json.Marshal(info)
	if err != nil {
		return 0, fmt.Errorf("marshal commit metadata: %w", err)
	}
	extraInfo := string(jsonBytes)

	if !tableExists || isBackfill {
		// First run: Create table from temp table
		createSQL := sql.MustFormat("execute/table.sql", model.Target, tmpTable)

		// Escape extraInfo to prevent SQL injection from file paths/lineage with quotes
		txnSQL := sql.MustFormat("execute/commit.sql", createSQL, model.Target, escapeSQL(extraInfo))

		stepStart = time.Now()
		if err := r.sess.Exec(txnSQL); err != nil {
			r.trace(result, "commit", stepStart, "error")
			return 0, fmt.Errorf("create partition table: %w", err)
		}
		r.trace(result, "commit", stepStart, "ok")

		r.applyComments(model)
		r.applyStorageHints(model)

		return rowsAffected, nil
	}

	// Incremental run: Delete matching partitions, then insert new data
	// Step 1: Get distinct partition values from temp table
	// Step 2: Delete rows in target where partition values match
	// Step 3: Insert new rows from temp table

	// Escape extraInfo to prevent SQL injection from file paths/lineage with quotes
	txnSQL := sql.MustFormat("execute/partition_delete.sql",
		model.Target, partitionCols, partitionCols, tmpTable,
		model.Target, tmpTable,
		model.Target, escapeSQL(extraInfo))

	stepStart = time.Now()
	if err := r.sess.Exec(txnSQL); err != nil {
		r.trace(result, "commit", stepStart, "error")
		return 0, fmt.Errorf("update partition table: %w", err)
	}
	r.trace(result, "commit", stepStart, "ok")

	r.applyComments(model)

	return rowsAffected, nil
}
