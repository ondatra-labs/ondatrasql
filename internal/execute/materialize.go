// OndatraSQL - You don't need a data stack anymore
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
// It runs the CREATE/INSERT, audits, and commit metadata in a single
// transaction. If schemaChange is provided, schema evolution (ALTER
// TABLE) is included in the same transaction so a failing audit rolls
// it back together with the data write.
//
// auditSQL is the pre-rendered transactional audit check produced by
// validation.AuditsToTransactionalSQL — empty string when the model
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
	// their change-detection queries (which compare temp-table columns
	// against the target table) see the updated target schema before they
	// build the diff SQL. Each of those code paths runs its own audit
	// inside its own commit transaction, so the early-apply ALTER is
	// not part of the audit-rollback envelope for those kinds.
	//
	// For table/append/merge the ALTER is folded into the materialize
	// transaction (prepended to mainSQL below), so a failing audit also
	// rolls back the schema change. DuckLake supports DDL inside an
	// explicit transaction:
	//   https://ducklake.select/docs/stable/duckdb/advanced_features/transactions
	var schemaEvolutionSQL string
	if schemaChange != nil && !isBackfill {
		schemaEvolutionSQL = r.buildSchemaEvolutionSQL(model.Target, *schemaChange)
		if model.Kind == "scd2" || model.Kind == "tracked" || model.Kind == "partition" {
			if err := r.sess.Exec(schemaEvolutionSQL); err != nil {
				return 0, fmt.Errorf("schema evolution: %w", err)
			}
			schemaEvolutionSQL = "" // already applied; do not duplicate
		}
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
		return r.materializeSCD2(model, tmpTable, isBackfill, auditSQL, sqlHash, runType, result, startTime)

	case "partition":
		if model.UniqueKey == "" {
			return 0, fmt.Errorf("partition kind requires unique_key directive")
		}
		// Bug 16 + 22: validate unique_key columns exist and contain no NULLs.
		// Partition uses unique_key as the partition identifier.
		if err := r.ensureColumnsExist(tmpTable, "@unique_key", model.UniqueKey); err != nil {
			return 0, err
		}
		if err := r.ensureUniqueKeyNotNull(tmpTable, model.UniqueKey); err != nil {
			return 0, err
		}
		// Partition handles its own transaction - return early
		return r.materializePartition(model, tmpTable, isBackfill, auditSQL, sqlHash, runType, result, startTime)

	case "tracked":
		if model.UniqueKey == "" {
			return 0, fmt.Errorf("tracked kind requires unique_key directive")
		}
		// Bug 16 + 22: validate unique_key columns exist and contain no NULLs.
		// Tracked uses unique_key as the row identity for content-hash dedup.
		if err := r.ensureColumnsExist(tmpTable, "@unique_key", model.UniqueKey); err != nil {
			return 0, err
		}
		if err := r.ensureUniqueKeyNotNull(tmpTable, model.UniqueKey); err != nil {
			return 0, err
		}
		// Tracked handles its own transaction - return early
		return r.materializeTracked(model, tmpTable, isBackfill, auditSQL, sqlHash, runType, result, startTime)

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

// reverseSchemaEvolution undoes schema changes applied by applySchemaEvolution.
// Called during rollback to restore the table schema to match the previous snapshot
// before inserting historical data. Best-effort: errors are logged but not fatal,
// since a partially reversed schema is better than aborting the rollback entirely.
func (r *Runner) reverseSchemaEvolution(target string, change backfill.SchemaChange) {
	// Reverse type promotions (old type)
	for _, tc := range change.TypeChanged {
		qc := duckdb.QuoteIdentifier(tc.Column)
		alterSQL := sql.MustFormat("schema/alter_column_type.sql", target, qc, tc.OldType)
		r.sess.Exec(alterSQL) // best-effort
	}

	// Re-add dropped columns
	for _, col := range change.Dropped {
		qc := duckdb.QuoteIdentifier(col.Name)
		alterSQL := sql.MustFormat("schema/alter_add_column.sql", target, qc, col.Type)
		r.sess.Exec(alterSQL) // best-effort
	}

	// Drop added columns
	for _, col := range change.Added {
		qc := duckdb.QuoteIdentifier(col.Name)
		dropSQL := fmt.Sprintf("ALTER TABLE %s DROP COLUMN %s", target, qc)
		r.sess.Exec(dropSQL) // best-effort
	}

	// Reverse renames
	for _, rename := range change.Renamed {
		oldName := duckdb.QuoteIdentifier(rename.OldName)
		newName := duckdb.QuoteIdentifier(rename.NewName)
		// Reverse: rename newName back to oldName
		alterSQL := sql.MustFormat("schema/alter_rename_column.sql", target, newName, oldName)
		r.sess.Exec(alterSQL) // best-effort
	}
}

// materializeSCD2 creates or updates an SCD2 table with history tracking.
// SCD2 tables have additional columns: valid_from_snapshot, valid_to_snapshot, is_current.
// On first run (isBackfill=true or table doesn't exist), creates the table with SCD2 columns.
// On subsequent runs, closes old versions and inserts new versions for changed/new rows.
func (r *Runner) materializeSCD2(model *parser.Model, tmpTable string, isBackfill bool, auditSQL, sqlHash, runType string, result *Result, startTime time.Time) (int64, error) {
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
	// auditSQL is folded in just before set_commit_message so a failing
	// audit aborts the SCD2 update atomically (same guarantee as the
	// generic commit.sql template).
	// Escape extraInfo to prevent SQL injection from file paths/lineage with quotes.
	txnSQL := sql.MustFormat("execute/scd2_update.sql",
		escapeSQL(model.Target), escapeSQL(model.Target),
		model.Target, currSnapshot-1, uk, uk,
		model.Target, currSnapshot-1, uk, uk,
		model.Target, colList, colList, currSnapshot,
		auditSQL, model.Target, escapeSQL(extraInfo))

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
func (r *Runner) materializePartition(model *parser.Model, tmpTable string, isBackfill bool, auditSQL, sqlHash, runType string, result *Result, startTime time.Time) (int64, error) {
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

	if !tableExists {
		// First run: Create table from temp table
		createSQL := sql.MustFormat("execute/table.sql", model.Target, tmpTable)

		// Escape extraInfo to prevent SQL injection from file paths/lineage with quotes
		txnSQL := sql.MustFormat("execute/commit.sql", createSQL, auditSQL, model.Target, escapeSQL(extraInfo))

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

	if isBackfill {
		// SQL changed but table exists: TRUNCATE + INSERT preserves snapshot chain
		mainSQL := fmt.Sprintf("TRUNCATE %s;\nINSERT INTO %s BY NAME SELECT * FROM %s",
			model.Target, model.Target, tmpTable)
		txnSQL := sql.MustFormat("execute/commit.sql", mainSQL, auditSQL, model.Target, escapeSQL(extraInfo))

		stepStart = time.Now()
		if err := r.sess.Exec(txnSQL); err != nil {
			r.trace(result, "commit", stepStart, "error")
			return 0, fmt.Errorf("backfill partition table: %w", err)
		}
		r.trace(result, "commit", stepStart, "ok")

		r.applyComments(model)
		return rowsAffected, nil
	}

	// Incremental run: Delete matching partitions, then insert new data
	registrySQL := fmt.Sprintf("DELETE FROM _ondatra_registry WHERE target = '%s';\nINSERT INTO _ondatra_registry VALUES ('%s', '%s', current_timestamp)",
		escapeSQL(model.Target), escapeSQL(model.Target), escapeSQL(model.Kind))

	// Escape extraInfo to prevent SQL injection from file paths/lineage with quotes.
	// auditSQL is folded into partition_delete.sql so a failing audit aborts
	// the DELETE+INSERT atomically (same guarantee as commit.sql for the other kinds).
	txnSQL := registrySQL + ";\n" + sql.MustFormat("execute/partition_delete.sql",
		model.Target, partitionCols, partitionCols, tmpTable,
		model.Target, tmpTable,
		auditSQL, model.Target, escapeSQL(extraInfo))

	stepStart = time.Now()
	if err := r.sess.Exec(txnSQL); err != nil {
		r.trace(result, "commit", stepStart, "error")
		return 0, fmt.Errorf("update partition table: %w", err)
	}
	r.trace(result, "commit", stepStart, "ok")

	r.applyComments(model)

	return rowsAffected, nil
}

// materializeTracked creates or updates a tracked table with content-hash change detection.
// Groups rows by unique_key and computes an md5 hash of all non-key columns per group.
// On incremental runs, only groups with changed or new hashes are replaced (DELETE + INSERT).
// The _content_hash column is added automatically, like SCD2's valid_from_snapshot.
func (r *Runner) materializeTracked(model *parser.Model, tmpTable string, isBackfill bool, auditSQL, sqlHash, runType string, result *Result, startTime time.Time) (int64, error) {
	uk := duckdb.QuoteIdentifier(model.UniqueKey)

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

	// Build hash columns (all except unique_key)
	var hashCols []string
	for _, col := range cleanCols {
		if col != model.UniqueKey {
			hashCols = append(hashCols, col)
		}
	}

	// Build deterministic hash expression:
	// md5(string_agg(concat_ws('|', COALESCE(col1::VARCHAR,''), ...), '|' ORDER BY col1, col2, ...))
	var hashAggExpr string
	if len(hashCols) == 0 {
		// Only unique_key column — no content to hash, use constant
		hashAggExpr = "md5('')"
	} else {
		var concatParts []string
		for _, col := range hashCols {
			concatParts = append(concatParts, fmt.Sprintf("COALESCE(%s::VARCHAR, '')", duckdb.QuoteIdentifier(col)))
		}
		concatExpr := fmt.Sprintf("concat_ws('|', %s)", strings.Join(concatParts, ", "))

		var orderParts []string
		for _, col := range hashCols {
			orderParts = append(orderParts, duckdb.QuoteIdentifier(col))
		}
		orderExpr := strings.Join(orderParts, ", ")

		hashAggExpr = fmt.Sprintf("md5(string_agg(%s, '|' ORDER BY %s))", concatExpr, orderExpr)
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
FROM %s s JOIN group_hash g ON s.%s = g.%s`,
		uk, hashAggExpr, tmpTable, uk,
		qualifiedColList, tmpTable, uk, uk)

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
	// - LEFT JOIN detects new and changed groups (in source but not matching target)
	// - UNION detects deleted groups (in target but not in source)
	detectSQL := fmt.Sprintf(`CREATE TEMP TABLE tracked_changes AS
SELECT n.%[1]s, 'upsert' AS _action FROM (
    SELECT DISTINCT %[1]s, _content_hash FROM tracked_hashed
) n
LEFT JOIN (
    SELECT DISTINCT %[1]s, _content_hash FROM %[2]s
) o ON n.%[1]s = o.%[1]s AND n._content_hash = o._content_hash
WHERE o.%[1]s IS NULL
UNION ALL
SELECT o.%[1]s, 'delete' AS _action FROM (
    SELECT DISTINCT %[1]s FROM %[2]s
) o
WHERE o.%[1]s NOT IN (SELECT DISTINCT %[1]s FROM tracked_hashed)`, uk, model.Target)

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
		registrySQL := fmt.Sprintf("DELETE FROM _ondatra_registry WHERE target = '%s';\nINSERT INTO _ondatra_registry VALUES ('%s', '%s', current_timestamp)",
			escapeSQL(model.Target), escapeSQL(model.Target), escapeSQL(model.Kind))
		txnSQL := sql.MustFormat("execute/commit.sql", registrySQL, auditSQL, model.Target, escapeSQL(string(jsonBytes)))
		stepStart = time.Now()
		r.sess.Exec(txnSQL)
		r.trace(result, "commit", stepStart, "ok")
		return 0, nil
	}

	// Count rows being inserted (upsert groups only, not deletes)
	rowCountSQL := fmt.Sprintf("SELECT COUNT(*) FROM tracked_hashed WHERE %s IN (SELECT %s FROM tracked_changes WHERE _action = 'upsert')", uk, uk)
	rowCountResult, _ := r.sess.QueryValue(rowCountSQL)
	var rowsAffected int64
	fmt.Sscanf(rowCountResult, "%d", &rowsAffected)

	info.RowsAffected = rowsAffected
	jsonBytes, _ := json.Marshal(info)

	// DELETE old rows for all changed/deleted groups, INSERT new rows for upsert groups only
	mainSQL := fmt.Sprintf(`DELETE FROM %[1]s WHERE %[2]s IN (SELECT %[2]s FROM tracked_changes);
INSERT INTO %[1]s BY NAME SELECT * FROM tracked_hashed WHERE %[2]s IN (SELECT %[2]s FROM tracked_changes WHERE _action = 'upsert')`,
		model.Target, uk)

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
