// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package execute

import (
	"encoding/json"
	"fmt"
	"os"
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
// It runs the CREATE/INSERT, audits, and commit metadata in a single
// transaction. If schemaChange is provided, schema evolution (ALTER
// TABLE) is included in the same transaction so a failing audit rolls
// it back together with the data write.
//
// auditSQL is the pre-rendered transactional audit check produced by
// validation.DispatchAuditsTransactional — empty string when the model
// has no audits. The caller is responsible for handling parse errors
// before calling materialize, so this function trusts auditSQL is
// well-formed (or empty).
//
// Optional extraPreSQL statements are prepended to the transaction
// (e.g. ack inserts for Starlark scripts).
func (r *Runner) materialize(model *parser.Model, tmpTable string, isBackfill bool, schemaChange *backfill.SchemaChange, auditSQL, sqlHash, runType string, result *Result, startTime time.Time, extraPreSQL ...string) (int64, error) {
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

	// Apply schema evolution if needed (before any materialization).
	//
	// For SCD2/tracked/partition the ALTER must be applied immediately so
	// Schema evolution (ALTER TABLE) is folded into the materialize
	// transaction for ALL kinds. DuckLake supports DDL inside explicit
	// transactions, so a failing audit rolls back the schema change too.
	// For scd2/tracked/partition, the ALTER runs inside the transaction
	// before change-detection queries so they see the new columns.
	//
	// On backfill with type changes (e.g. unique_key type changed), the
	// ALTER is still needed before TRUNCATE+INSERT so the target schema
	// matches the new data types.
	var schemaEvolutionSQL string
	if schemaChange != nil {
		schemaEvolutionSQL = r.buildSchemaEvolutionSQL(model.Target, *schemaChange)
	}

	// In sandbox mode, force backfill if target table doesn't exist in sandbox.
	// TRUNCATE + INSERT and incremental operations require the table to exist.
	// A transient information_schema error here used to be silently treated as
	// "table missing" and forced an unintended backfill on tables that actually
	// had data. Now we surface the error instead.
	if !isBackfill && r.sess.ProdAlias() != "" {
		exists, existsErr := r.tableExistsInCatalog(model.Target, r.sess.CatalogAlias())
		if existsErr != nil {
			return 0, existsErr
		}
		if !exists {
			isBackfill = true
		}
	}

	// Check if target table already exists. If it does, never use CREATE OR REPLACE —
	// it breaks DuckLake's snapshot chain. Use TRUNCATE + INSERT BY NAME instead.
	targetExists, _ := r.tableExistsCheck(model.Target)

	// Build the main SQL statement
	var mainSQL string

	switch model.Kind {
	case "table":
		if !targetExists {
			mainSQL = sql.MustFormat("execute/table.sql", model.Target, tmpTable)
		} else {
			mainSQL = fmt.Sprintf("TRUNCATE %s;\nINSERT INTO %s BY NAME SELECT * FROM %s",
				model.Target, model.Target, tmpTable)
		}

	case "append":
		if !targetExists {
			mainSQL = sql.MustFormat("execute/table.sql", model.Target, tmpTable)
		} else if isBackfill {
			mainSQL = fmt.Sprintf("TRUNCATE %s;\nINSERT INTO %s BY NAME SELECT * FROM %s",
				model.Target, model.Target, tmpTable)
		} else {
			mainSQL = sql.MustFormat("execute/append.sql", model.Target, tmpTable)
		}

	case "merge":
		if model.UniqueKey == "" {
			return 0, fmt.Errorf("merge kind requires unique_key directive")
		}
		// Validate that the unique_key columns actually exist in the model
		// output (Bug 16) and don't contain NULL (Bug 22). Both checks run
		// on every merge — including backfill — so issues fail loudly on
		// the FIRST run instead of cryptically on the second.
		if err := r.ensureColumnsExist(tmpTable, "@unique_key", model.UniqueKey); err != nil {
			return 0, err
		}
		if err := r.ensureUniqueKeyNotNull(tmpTable, model.UniqueKey); err != nil {
			return 0, err
		}
		if model.Incremental != "" {
			if err := r.ensureColumnsExist(tmpTable, "@incremental", model.Incremental); err != nil {
				return 0, err
			}
		}
		if !targetExists {
			mainSQL = sql.MustFormat("execute/table.sql", model.Target, tmpTable)
		} else if isBackfill {
			mainSQL = fmt.Sprintf("TRUNCATE %s;\nINSERT INTO %s BY NAME SELECT * FROM %s",
				model.Target, model.Target, tmpTable)
		} else {
			// Use MERGE INTO for upsert (DuckLake doesn't support INSERT OR REPLACE)
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
		// Bug 16 + 22: validate unique_key columns exist and contain no NULLs.
		// SCD2 uses unique_key as the row identity for change detection.
		if err := r.ensureColumnsExist(tmpTable, "@unique_key", model.UniqueKey); err != nil {
			return 0, err
		}
		if err := r.ensureUniqueKeyNotNull(tmpTable, model.UniqueKey); err != nil {
			return 0, err
		}
		// SCD2 handles its own transaction - return early
		return r.materializeSCD2(model, tmpTable, isBackfill, schemaEvolutionSQL, auditSQL, sqlHash, runType, result, startTime, extraPreSQL...)

	case "tracked":
		if model.GroupKey == "" {
			return 0, fmt.Errorf("tracked kind requires @group_key directive")
		}
		// Validate group_key column exists and contains no NULLs.
		if err := r.ensureColumnsExist(tmpTable, "@group_key", model.GroupKey); err != nil {
			return 0, err
		}
		if err := r.ensureUniqueKeyNotNull(tmpTable, model.GroupKey); err != nil {
			return 0, err
		}
		// Tracked handles its own transaction - return early
		return r.materializeTracked(model, tmpTable, isBackfill, schemaEvolutionSQL, auditSQL, sqlHash, runType, result, startTime, extraPreSQL...)

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

	// Build the materialize transaction body. Order:
	//   1. schema evolution (ALTER TABLE …)        ← inside the txn
	//   2. extraPreSQL (e.g. Starlark ack inserts)
	//   3. registry upsert
	//   4. mainSQL (TRUNCATE/INSERT/MERGE)
	// Audits get inserted between (4) and set_commit_message via the
	// commit.sql template's pre-commit-checks slot, so a failing audit
	// raises error() and rolls back everything from (1) through (4).
	var preParts []string
	if schemaEvolutionSQL != "" {
		preParts = append(preParts, schemaEvolutionSQL)
	}
	for _, extra := range extraPreSQL {
		if extra != "" {
			preParts = append(preParts, extra)
		}
	}
	registrySQL := fmt.Sprintf("DELETE FROM _ondatra_registry WHERE target = '%s';\nINSERT INTO _ondatra_registry VALUES ('%s', '%s', current_timestamp)",
		escapeSQL(model.Target), escapeSQL(model.Target), escapeSQL(model.Kind))
	preParts = append(preParts, registrySQL)
	mainSQL = strings.Join(preParts, ";\n") + ";\n" + mainSQL

	// Execute everything in a single transaction. The commit.sql template's
	// pre-commit-checks slot folds in the audit error() wrapper so a failing
	// audit aborts the whole BEGIN/COMMIT atomically.
	// Escape extraInfo to prevent SQL injection from file paths/lineage with quotes.
	txnSQL := sql.MustFormat("execute/commit.sql", mainSQL, auditSQL, model.Target, escapeSQL(extraInfo))

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
		if err := r.sess.Exec(sortSQL); err != nil {
			fmt.Fprintf(os.Stderr, "  WARN: sorted_by on %s: %v\n", model.Target, err)
		}
	}

	if len(model.PartitionedBy) > 0 {
		// Partition expressions may be transforms like bucket(16, col) — don't quote those
		parts := make([]string, len(model.PartitionedBy))
		for i, p := range model.PartitionedBy {
			if strings.Contains(p, "(") {
				parts[i] = p // transform expression, pass through as-is
			} else {
				parts[i] = duckdb.QuoteIdentifier(p)
			}
		}
		partSQL := fmt.Sprintf("ALTER TABLE %s SET PARTITIONED BY (%s)", model.Target, strings.Join(parts, ", "))
		if err := r.sess.Exec(partSQL); err != nil {
			fmt.Fprintf(os.Stderr, "  WARN: partitioned_by on %s: %v\n", model.Target, err)
		}
	}
}

// buildMergeSQL builds a MERGE INTO statement for upsert operations.
func (r *Runner) buildMergeSQL(target, source, uniqueKey string, cols []string) string {
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

	// Add new columns BEFORE dropping old ones. DuckDB rejects DROP on the
	// last remaining column ("Cannot drop column: table only has one column
	// remaining"). Adding first ensures the table always has at least one
	// column when drops execute. INSERT BY NAME matches by name not position,
	// so the temporary extra columns are harmless.
	for _, col := range change.Added {
		qc := duckdb.QuoteIdentifier(col.Name)
		alterSQL := sql.MustFormat("schema/alter_add_column.sql", target, qc, col.Type)
		if err := r.sess.Exec(alterSQL); err != nil {
			return fmt.Errorf("add column %s: %w", col.Name, err)
		}
	}

	// Drop removed columns (safe now — new columns were added first)
	for _, col := range change.Dropped {
		qc := duckdb.QuoteIdentifier(col.Name)
		dropSQL := fmt.Sprintf("ALTER TABLE %s DROP COLUMN %s", target, qc)
		if err := r.sess.Exec(dropSQL); err != nil {
			return fmt.Errorf("drop column %s: %w", col.Name, err)
		}
	}

	// Apply type changes. Try ALTER SET DATA TYPE first (works for promotable
	// changes like INTEGER → BIGINT). If DuckLake rejects it, fall back to
	// DROP + ADD which always works. DuckLake's ALTER TYPE rules are stricter
	// than our ondatra_is_type_promotable macro — for example INTEGER → DOUBLE
	// is considered promotable by the macro but rejected by DuckLake. The
	// fallback handles all such edge cases. The column moves to the end of
	// the schema, but INSERT BY NAME matches by name not position.
	for _, tc := range change.TypeChanged {
		qc := duckdb.QuoteIdentifier(tc.Column)
		alterSQL := sql.MustFormat("schema/alter_column_type.sql", target, qc, tc.NewType)
		if err := r.sess.Exec(alterSQL); err != nil {
			dropSQL := fmt.Sprintf("ALTER TABLE %s DROP COLUMN %s", target, qc)
			if dropErr := r.sess.Exec(dropSQL); dropErr != nil {
				return fmt.Errorf("change column %s type from %s to %s: ALTER failed (%w), DROP fallback also failed: %w", tc.Column, tc.OldType, tc.NewType, err, dropErr)
			}
			addSQL := sql.MustFormat("schema/alter_add_column.sql", target, qc, tc.NewType)
			if addErr := r.sess.Exec(addSQL); addErr != nil {
				return fmt.Errorf("change column %s type from %s to %s: ALTER failed (%w), ADD fallback also failed: %w", tc.Column, tc.OldType, tc.NewType, err, addErr)
			}
		}
	}

	return nil
}

// buildSchemaEvolutionSQL builds ALTER TABLE statements for schema evolution without executing them.
// Returns a single SQL string with all ALTER statements separated by semicolons.
func (r *Runner) buildSchemaEvolutionSQL(target string, change backfill.SchemaChange) string {
	var stmts []string

	for _, rename := range change.Renamed {
		oldName := duckdb.QuoteIdentifier(rename.OldName)
		newName := duckdb.QuoteIdentifier(rename.NewName)
		stmts = append(stmts, sql.MustFormat("schema/alter_rename_column.sql", target, oldName, newName))
	}
	// Add before drop — prevents "Cannot drop column: table only has one column remaining"
	for _, col := range change.Added {
		qc := duckdb.QuoteIdentifier(col.Name)
		stmts = append(stmts, sql.MustFormat("schema/alter_add_column.sql", target, qc, col.Type))
	}
	for _, col := range change.Dropped {
		qc := duckdb.QuoteIdentifier(col.Name)
		stmts = append(stmts, fmt.Sprintf("ALTER TABLE %s DROP COLUMN %s", target, qc))
	}
	// Type changes: always use DROP + ADD instead of ALTER SET DATA TYPE.
	// DuckLake's ALTER TYPE rules are stricter than OndatraSQL's promotability
	// check (e.g. INTEGER → DOUBLE is considered promotable by our macro but
	// rejected by DuckLake). DROP + ADD always works. The column moves to the
	// end of the schema, but INSERT BY NAME matches by name not position.
	for _, tc := range change.TypeChanged {
		qc := duckdb.QuoteIdentifier(tc.Column)
		stmts = append(stmts, fmt.Sprintf("ALTER TABLE %s DROP COLUMN %s", target, qc))
		stmts = append(stmts, sql.MustFormat("schema/alter_add_column.sql", target, qc, tc.NewType))
	}

	return strings.Join(stmts, ";\n")
}

// materializeSCD2 creates or updates an SCD2 table with history tracking.
// SCD2 tables have additional columns: valid_from_snapshot, valid_to_snapshot, is_current.
// On first run (isBackfill=true or table doesn't exist), creates the table with SCD2 columns.
// On subsequent runs, closes old versions and inserts new versions for changed/new rows.
func (r *Runner) materializeSCD2(model *parser.Model, tmpTable string, isBackfill bool, schemaEvolutionSQL, auditSQL, sqlHash, runType string, result *Result, startTime time.Time, extraPreSQL ...string) (int64, error) {
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

	if !tableExists {
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
		txnSQL := sql.MustFormat("execute/commit.sql", createSQL, auditSQL, model.Target, escapeSQL(extraInfo))

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

	if isBackfill {
		// SQL changed but table exists: TRUNCATE + INSERT preserves snapshot chain.
		// SCD2 history resets (all rows become current with new valid_from_snapshot).
		countSQL := fmt.Sprintf("SELECT COUNT(*) FROM %s", tmpTable)
		countResult, err := r.sess.QueryValue(countSQL)
		if err != nil {
			return 0, fmt.Errorf("count rows: %w", err)
		}
		fmt.Sscanf(countResult, "%d", &rowsAffected)

		stepStart := time.Now()
		columns, _ := backfill.CaptureSchema(r.sess, tmpTable)
		schemaHash := backfill.ComputeSchemaHash(columns)
		colLineage, tableDeps, _ := r.extractLineage(model.SQL)
		r.trace(result, "metadata", stepStart, "ok")

		endTime := time.Now()
		steps := ConvertTraceToSteps(result.Trace)
		info := backfill.CommitInfo{
			Model: model.Target, SQLHash: sqlHash, SchemaHash: schemaHash,
			Columns: columns, ColumnLineage: colLineage, RunType: runType,
			RowsAffected: rowsAffected, DagRunID: r.dagRunID, Depends: tableDeps,
			Kind: model.Kind, SourceFile: model.FilePath,
			StartTime: startTime.UTC().Format(time.RFC3339),
			EndTime: endTime.UTC().Format(time.RFC3339),
			DurationMs: endTime.Sub(startTime).Milliseconds(),
			DuckDBVersion: r.sess.GetVersion(), Steps: steps,
			GitCommit: r.gitInfo.Commit, GitBranch: r.gitInfo.Branch, GitRepoURL: r.gitInfo.RepoURL,
		}
		jsonBytes, _ := json.Marshal(info)

		mainSQL := fmt.Sprintf(
			"TRUNCATE %s;\nINSERT INTO %s BY NAME SELECT %s, %d::BIGINT AS valid_from_snapshot, CAST(NULL AS BIGINT) AS valid_to_snapshot, true AS is_current FROM %s",
			model.Target, model.Target, colList, currSnapshot, tmpTable)
		if schemaEvolutionSQL != "" {
			mainSQL = schemaEvolutionSQL + ";\n" + mainSQL
		}
		txnSQL := sql.MustFormat("execute/commit.sql", mainSQL, auditSQL, model.Target, escapeSQL(string(jsonBytes)))

		stepStart = time.Now()
		if err := r.sess.Exec(txnSQL); err != nil {
			r.trace(result, "commit", stepStart, "error")
			return 0, fmt.Errorf("backfill scd2 table: %w", err)
		}
		r.trace(result, "commit", stepStart, "ok")

		r.applyComments(model)
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

	// All steps run inside one transaction so ALTER + change-detection +
	// DML + audit are atomic. DuckDB sees pending DDL within the same txn.
	detectSQL := sql.MustFormat("execute/scd2_detect.sql",
		model.Target, tmpTable, uk, uk, tmpTable, uk, uk, changeWhere,
		uk, model.Target, uk, uk, tmpTable)

	// Step 1: BEGIN transaction + schema evolution + change detection
	stepStart := time.Now()
	if err := r.sess.Exec("BEGIN"); err != nil {
		return 0, fmt.Errorf("begin scd2 transaction: %w", err)
	}
	rollbackOnErr := func(err error, msg string) (int64, error) {
		r.trace(result, "commit", stepStart, "error")
		r.sess.Exec("ROLLBACK")
		r.sess.Exec("DROP TABLE IF EXISTS scd2_changes")
		r.sess.Exec("DROP TABLE IF EXISTS scd2_deleted")
		return 0, fmt.Errorf("%s: %w", msg, err)
	}

	if schemaEvolutionSQL != "" {
		if err := r.sess.Exec(schemaEvolutionSQL); err != nil {
			return rollbackOnErr(err, "schema evolution")
		}
	}

	// Execute extraPreSQL (e.g. Badger ack records) inside the transaction
	for _, extra := range extraPreSQL {
		if err := r.sess.Exec(extra); err != nil {
			return rollbackOnErr(err, "extra pre-sql")
		}
	}

	if err := r.sess.Exec(detectSQL); err != nil {
		return rollbackOnErr(err, "detect scd2 changes")
	}

	// Step 2: Count affected rows (inside txn)
	countResult, err := r.sess.QueryValue("SELECT COUNT(*) FROM scd2_changes")
	if err != nil {
		return rollbackOnErr(err, "count changes")
	}
	if _, err := fmt.Sscanf(countResult, "%d", &rowsAffected); err != nil {
		return 0, fmt.Errorf("parse row count %q: %w", countResult, err)
	}

	// Step 3: Build commit metadata (inside txn — temp tables visible)
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
		Kind:          model.Kind,
		SourceFile:    model.FilePath,
		StartTime:     startTime.UTC().Format(time.RFC3339),
		EndTime:       endTime.UTC().Format(time.RFC3339),
		DurationMs:    endTime.Sub(startTime).Milliseconds(),
		DuckDBVersion: r.sess.GetVersion(),
		Steps:         steps,
		GitCommit:     r.gitInfo.Commit,
		GitBranch:     r.gitInfo.Branch,
		GitRepoURL:    r.gitInfo.RepoURL,
	}
	jsonBytes, err := json.Marshal(info)
	if err != nil {
		return rollbackOnErr(err, "marshal commit metadata")
	}
	extraInfo := string(jsonBytes)

	// Step 4: Registry + DML + audit + commit message (all inside the txn)
	registrySQL := fmt.Sprintf("DELETE FROM _ondatra_registry WHERE target = '%s';\nINSERT INTO _ondatra_registry VALUES ('%s', 'scd2', current_timestamp)",
		escapeSQL(model.Target), escapeSQL(model.Target))
	if err := r.sess.Exec(registrySQL); err != nil {
		return rollbackOnErr(err, "registry upsert")
	}

	// Close old versions for changed rows
	closeChangedSQL := fmt.Sprintf("UPDATE %s SET valid_to_snapshot = %d, is_current = false WHERE is_current IS true AND %s IN (SELECT %s FROM scd2_changes WHERE _change_type = 'changed')",
		model.Target, currSnapshot-1, uk, uk)
	if err := r.sess.Exec(closeChangedSQL); err != nil {
		return rollbackOnErr(err, "close changed versions")
	}

	// Close versions for deleted rows
	closeDeletedSQL := fmt.Sprintf("UPDATE %s SET valid_to_snapshot = %d, is_current = false WHERE is_current IS true AND %s IN (SELECT %s FROM scd2_deleted)",
		model.Target, currSnapshot-1, uk, uk)
	if err := r.sess.Exec(closeDeletedSQL); err != nil {
		return rollbackOnErr(err, "close deleted versions")
	}

	// Insert new versions
	insertSQL := fmt.Sprintf("INSERT INTO %s (%s, valid_from_snapshot, valid_to_snapshot, is_current) SELECT %s, %d, NULL, true FROM scd2_changes",
		model.Target, colList, colList, currSnapshot)
	if err := r.sess.Exec(insertSQL); err != nil {
		return rollbackOnErr(err, "insert new versions")
	}

	// Audit (raises error() and aborts if any fail)
	if auditSQL != "" {
		if err := r.sess.Exec(auditSQL); err != nil {
			return rollbackOnErr(err, "audit failed")
		}
	}

	// Commit with metadata
	commitSQL := fmt.Sprintf("CALL set_commit_message('ondatrasql', 'Pipeline run: %s', extra_info => '%s');\nCOMMIT",
		model.Target, escapeSQL(extraInfo))
	if err := r.sess.Exec(commitSQL); err != nil {
		return rollbackOnErr(err, "commit scd2")
	}
	r.trace(result, "commit", stepStart, "ok")

	r.applyComments(model)

	// Cleanup temp tables
	r.sess.Exec("DROP TABLE IF EXISTS scd2_changes")
	r.sess.Exec("DROP TABLE IF EXISTS scd2_deleted")

	return rowsAffected, nil
}


// materializeTracked creates or updates a tracked table with content-hash change detection.
// Groups rows by group_key and computes an md5 hash of all non-key columns per group.
// On incremental runs, only groups with changed or new hashes are replaced (DELETE + INSERT).
// The _content_hash column is added automatically, like SCD2's valid_from_snapshot.
func (r *Runner) materializeTracked(model *parser.Model, tmpTable string, isBackfill bool, schemaEvolutionSQL, auditSQL, sqlHash, runType string, result *Result, startTime time.Time, extraPreSQL ...string) (int64, error) {
	// Parse composite group_key: "region, year" → ["region", "year"]
	var keyParts []string
	for _, part := range strings.Split(model.GroupKey, ",") {
		keyParts = append(keyParts, strings.TrimSpace(part))
	}
	// Build quoted key column list: "region", "year"
	var quotedKeys []string
	for _, k := range keyParts {
		quotedKeys = append(quotedKeys, duckdb.QuoteIdentifier(k))
	}
	uk := strings.Join(quotedKeys, ", ")

	// Build JOIN condition: s."region" = g."region" AND s."year" = g."year"
	var joinParts []string
	for _, k := range keyParts {
		qk := duckdb.QuoteIdentifier(k)
		joinParts = append(joinParts, fmt.Sprintf("s.%s = g.%s", qk, qk))
	}
	joinCond := strings.Join(joinParts, " AND ")

	// Build key column set for exclusion from hash
	keyCols := make(map[string]bool)
	for _, k := range keyParts {
		keyCols[k] = true
	}

	// Ensure schema exists
	if err := r.ensureSchemaExists(model.Target); err != nil {
		return 0, fmt.Errorf("ensure schema: %w", err)
	}

	// Check if target table exists
	tableExists, err := r.tableExistsCheck(model.Target)
	if err != nil {
		return 0, fmt.Errorf("check table exists: %w", err)
	}

	// Get source columns, filter out _content_hash if present in source
	allCols, err := r.getTableColumns(tmpTable)
	if err != nil {
		return 0, fmt.Errorf("get columns: %w", err)
	}
	var cleanCols []string
	for _, col := range allCols {
		if col != "_content_hash" {
			cleanCols = append(cleanCols, col)
		}
	}

	// Build hash columns (all except group_key columns), sorted alphabetically
	// so that reordering columns in the SELECT does not change the hash.
	var hashCols []string
	for _, col := range cleanCols {
		if !keyCols[col] {
			hashCols = append(hashCols, col)
		}
	}
	sort.Strings(hashCols)

	// Build deterministic content-hash expression for the group:
	//
	//   sum(hash(row(col1, col2, ...)))::VARCHAR
	//
	// MSet-Add-Hash (Clarke et al, MIT 2003): sum of per-row hashes is
	// commutative + associative + multiset-collision-resistant. Properties:
	//   - order-independent across rows (sum is commutative)
	//   - multiset-safe: duplicate rows do not cancel (vs bit_xor) and
	//     N copies of the same row contribute N * hash, not 0
	//   - NULL-distinct from zero/empty: hash(row(x, NULL)) != hash(row(x, 0))
	//     because row() preserves typed NULL natively (concat_ws would have
	//     silently dropped NULL — that was the bug in the previous form)
	//   - precision-safe: hash() returns UBIGINT, sum(UBIGINT) returns
	//     HUGEINT (not DOUBLE), so the 64-bit row hashes accumulate into
	//     a 128-bit group hash without precision loss
	//   - heterogeneous columns supported: row() takes any types, unlike
	//     a list literal which requires homogeneous element types
	//   - O(1) memory per group: no string accumulation
	//
	// hashCols was sorted above, so the row's column order is fixed
	// regardless of SELECT column order.
	var hashAggExpr string
	if len(hashCols) == 0 {
		hashAggExpr = "0::HUGEINT"
	} else {
		var rowParts []string
		for _, col := range hashCols {
			rowParts = append(rowParts, duckdb.QuoteIdentifier(col))
		}
		hashAggExpr = fmt.Sprintf("sum(hash(row(%s)))::VARCHAR", strings.Join(rowParts, ", "))
	}

	// Build qualified column list for SELECT (s."col1", s."col2", ...)
	var qualifiedCols []string
	for _, col := range cleanCols {
		qualifiedCols = append(qualifiedCols, "s."+duckdb.QuoteIdentifier(col))
	}
	qualifiedColList := strings.Join(qualifiedCols, ", ")

	// Create temp table with _content_hash joined per row
	hashTmpSQL := fmt.Sprintf(`CREATE TEMP TABLE tracked_hashed AS
WITH group_hash AS (
    SELECT %s, %s AS _content_hash FROM %s GROUP BY %s
)
SELECT %s, g._content_hash
FROM %s s JOIN group_hash g ON %s`,
		uk, hashAggExpr, tmpTable, uk,
		qualifiedColList, tmpTable, joinCond)

	if err := r.sess.Exec(hashTmpSQL); err != nil {
		return 0, fmt.Errorf("compute content hash: %w", err)
	}
	defer r.sess.Exec("DROP TABLE IF EXISTS tracked_hashed")

	// Count rows
	countResult, err := r.sess.QueryValue("SELECT COUNT(*) FROM tracked_hashed")
	if err != nil {
		return 0, fmt.Errorf("count rows: %w", err)
	}
	var totalRows int64
	fmt.Sscanf(countResult, "%d", &totalRows)

	// Build commit metadata
	stepStart := time.Now()
	columns, _ := backfill.CaptureSchema(r.sess, "tracked_hashed")
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
		DagRunID:      r.dagRunID,
		Depends:       tableDeps,
		Kind:          model.Kind,
		SourceFile:    model.FilePath,
		StartTime:     startTime.UTC().Format(time.RFC3339),
		EndTime:       endTime.UTC().Format(time.RFC3339),
		DurationMs:    endTime.Sub(startTime).Milliseconds(),
		DuckDBVersion: r.sess.GetVersion(),
		Steps:         steps,
		GitCommit:     r.gitInfo.Commit,
		GitBranch:     r.gitInfo.Branch,
		GitRepoURL:    r.gitInfo.RepoURL,
	}

	if !tableExists {
		// First run: CREATE table from hashed temp table
		info.RowsAffected = totalRows
		jsonBytes, _ := json.Marshal(info)

		createSQL := sql.MustFormat("execute/table.sql", model.Target, "tracked_hashed")
		txnSQL := sql.MustFormat("execute/commit.sql", createSQL, auditSQL, model.Target, escapeSQL(string(jsonBytes)))

		stepStart = time.Now()
		if err := r.sess.Exec(txnSQL); err != nil {
			r.trace(result, "commit", stepStart, "error")
			return 0, fmt.Errorf("create tracked table: %w", err)
		}
		r.trace(result, "commit", stepStart, "ok")

		r.applyComments(model)
		r.applyStorageHints(model)

		return totalRows, nil
	}

	if isBackfill {
		// SQL changed but table exists: TRUNCATE + INSERT preserves snapshot chain
		info.RowsAffected = totalRows
		jsonBytes, _ := json.Marshal(info)

		mainSQL := fmt.Sprintf("TRUNCATE %s;\nINSERT INTO %s BY NAME SELECT * FROM tracked_hashed",
			model.Target, model.Target)
		if schemaEvolutionSQL != "" {
			mainSQL = schemaEvolutionSQL + ";\n" + mainSQL
		}
		for _, extra := range extraPreSQL {
			mainSQL = extra + ";\n" + mainSQL
		}
		txnSQL := sql.MustFormat("execute/commit.sql", mainSQL, auditSQL, model.Target, escapeSQL(string(jsonBytes)))

		stepStart = time.Now()
		if err := r.sess.Exec(txnSQL); err != nil {
			r.trace(result, "commit", stepStart, "error")
			return 0, fmt.Errorf("backfill tracked table: %w", err)
		}
		r.trace(result, "commit", stepStart, "ok")

		r.applyComments(model)
		return totalRows, nil
	}

	// Incremental: find changed/new/deleted groups
	// Build prefixed key lists and JOIN conditions
	var nPrefixed []string // n."region", n."year"
	var oPrefixed []string // o."region", o."year"
	var nojoin []string    // n.key = o.key AND ...
	var lojoin []string    // o.key = h.key AND ...
	var nullcheck []string // o.key IS NULL
	for _, k := range keyParts {
		qk := duckdb.QuoteIdentifier(k)
		nPrefixed = append(nPrefixed, "n."+qk)
		oPrefixed = append(oPrefixed, "o."+qk)
		nojoin = append(nojoin, fmt.Sprintf("n.%s = o.%s", qk, qk))
		lojoin = append(lojoin, fmt.Sprintf("o.%s = h.%s", qk, qk))
		nullcheck = append(nullcheck, fmt.Sprintf("o.%s IS NULL", qk))
	}
	nuk := strings.Join(nPrefixed, ", ")
	ouk := strings.Join(oPrefixed, ", ")
	detectSQL := fmt.Sprintf(`CREATE TEMP TABLE tracked_changes AS
SELECT %[1]s, 'upsert' AS _action FROM (
    SELECT DISTINCT %[3]s, _content_hash FROM tracked_hashed
) n
LEFT JOIN (
    SELECT DISTINCT %[3]s, _content_hash FROM %[4]s
) o ON %[5]s AND n._content_hash = o._content_hash
WHERE %[6]s
UNION ALL
SELECT %[2]s, 'delete' AS _action FROM (
    SELECT DISTINCT %[3]s FROM %[4]s
) o
LEFT JOIN (
    SELECT DISTINCT %[3]s FROM tracked_hashed
) h ON %[7]s
WHERE h.%[8]s IS NULL`,
		nuk, ouk, uk, model.Target,
		strings.Join(nojoin, " AND "),
		strings.Join(nullcheck, " AND "),
		strings.Join(lojoin, " AND "),
		quotedKeys[0])

	if err := r.sess.Exec(detectSQL); err != nil {
		return 0, fmt.Errorf("detect tracked changes: %w", err)
	}
	defer r.sess.Exec("DROP TABLE IF EXISTS tracked_changes")

	// Count changed groups
	changeCount, err := r.sess.QueryValue("SELECT COUNT(*) FROM tracked_changes")
	if err != nil {
		return 0, fmt.Errorf("count changes: %w", err)
	}
	var changedGroups int64
	fmt.Sscanf(changeCount, "%d", &changedGroups)

	if changedGroups == 0 {
		// Nothing changed — commit with 0 rows.
		// Registry upsert ensures DuckLake creates a snapshot (so schema metadata is committed).
		info.RowsAffected = 0
		jsonBytes, _ := json.Marshal(info)
		noChangeSQL := fmt.Sprintf("DELETE FROM _ondatra_registry WHERE target = '%s';\nINSERT INTO _ondatra_registry VALUES ('%s', '%s', current_timestamp)",
			escapeSQL(model.Target), escapeSQL(model.Target), escapeSQL(model.Kind))
		if schemaEvolutionSQL != "" {
			noChangeSQL = schemaEvolutionSQL + ";\n" + noChangeSQL
		}
		for _, extra := range extraPreSQL {
			noChangeSQL = extra + ";\n" + noChangeSQL
		}
		txnSQL := sql.MustFormat("execute/commit.sql", noChangeSQL, auditSQL, model.Target, escapeSQL(string(jsonBytes)))
		stepStart = time.Now()
		if err := r.sess.Exec(txnSQL); err != nil {
			r.trace(result, "commit", stepStart, "error")
			r.sess.Exec("ROLLBACK")
			return 0, fmt.Errorf("commit tracked (no changes): %w", err)
		}
		r.trace(result, "commit", stepStart, "ok")
		return 0, nil
	}

	// Count rows being inserted (upsert groups only, not deletes)
	// Build JOIN for composite key matching
	var countJoin []string
	var delJoin []string
	var insJoin []string
	for _, k := range keyParts {
		qk := duckdb.QuoteIdentifier(k)
		countJoin = append(countJoin, fmt.Sprintf("h.%s = c.%s", qk, qk))
		delJoin = append(delJoin, fmt.Sprintf("t.%s = c.%s", qk, qk))
		insJoin = append(insJoin, fmt.Sprintf("h.%s = c.%s", qk, qk))
	}
	rowCountSQL := fmt.Sprintf("SELECT COUNT(*) FROM tracked_hashed h JOIN tracked_changes c ON %s WHERE c._action = 'upsert'",
		strings.Join(countJoin, " AND "))
	rowCountResult, _ := r.sess.QueryValue(rowCountSQL)
	var rowsAffected int64
	fmt.Sscanf(rowCountResult, "%d", &rowsAffected)

	info.RowsAffected = rowsAffected
	jsonBytes, _ := json.Marshal(info)

	// DELETE old rows for all changed/deleted groups, INSERT new rows for upsert groups only
	mainSQL := fmt.Sprintf(`DELETE FROM %[1]s t USING tracked_changes c WHERE %[2]s;
INSERT INTO %[1]s BY NAME SELECT h.* FROM tracked_hashed h JOIN tracked_changes c ON %[3]s WHERE c._action = 'upsert'`,
		model.Target,
		strings.Join(delJoin, " AND "),
		strings.Join(insJoin, " AND "))
	if schemaEvolutionSQL != "" {
		mainSQL = schemaEvolutionSQL + ";\n" + mainSQL
	}
	for _, extra := range extraPreSQL {
		mainSQL = extra + ";\n" + mainSQL
	}

	txnSQL := sql.MustFormat("execute/commit.sql", mainSQL, auditSQL, model.Target, escapeSQL(string(jsonBytes)))

	stepStart = time.Now()
	if err := r.sess.Exec(txnSQL); err != nil {
		r.trace(result, "commit", stepStart, "error")
		return 0, fmt.Errorf("update tracked table: %w", err)
	}
	r.trace(result, "commit", stepStart, "ok")

	r.applyComments(model)

	return rowsAffected, nil
}
