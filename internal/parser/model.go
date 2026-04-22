// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

// Package parser handles SQL model file parsing.
// It extracts directives (@kind, @constraint, @audit, @warning, etc.) from SQL comments.
package parser

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

// ScriptType indicates the type of script for a model.
type ScriptType string

const (
	ScriptTypeNone     ScriptType = ""         // SQL model (not a script)
	ScriptTypeStarlark ScriptType = "starlark" // Starlark script
)

// IsModelFile returns true if the file has a valid model extension.
func IsModelFile(path string) bool {
	ext := filepath.Ext(path)
	return ext == ".sql"
}

// ColumnDef represents a column definition for @kind: events models.
type ColumnDef struct {
	Name    string
	Type    string
	NotNull bool
}

// Model represents a parsed SQL model file with its directives.
type Model struct {
	// Target is the fully qualified table name (schema.table).
	Target string

	// Kind is the materialization type: table, append, merge, scd2, partition, or tracked.
	Kind string

	// ScriptType indicates the type of script (starlark) or empty for SQL.
	ScriptType ScriptType

	// UniqueKey is the column used for merge/scd2/tracked operations.
	UniqueKey string

	// PartitionedBy lists columns for DuckLake native storage partitioning.
	// Set via @partitioned_by directive. Applied as ALTER TABLE SET PARTITIONED BY.
	PartitionedBy []string

	// Incremental is the column used for cursor-based incremental loading.
	// When set, the system tracks MAX(Incremental) to fetch only new data.
	Incremental string

	// IncrementalInitial is the starting value for first run.
	// Default: "1970-01-01T00:00:00Z" for timestamps.
	IncrementalInitial string

	// SQL is the model query without directive comments.
	// If ScriptType is set, this contains Starlark script code.
	SQL string

	// Constraints are validation rules checked before INSERT.
	Constraints []string

	// Audits are validation rules checked after INSERT (rollback on failure).
	Audits []string

	// Warnings are soft validation rules that only log warnings (no rollback).
	Warnings []string

	// Extensions lists DuckDB extensions to load before execution.
	// Format: "name" or "name FROM repository" (e.g., "httpfs", "infera FROM community")
	Extensions []string

	// Description is the table-level comment stored via COMMENT ON TABLE.
	Description string

	// ColumnDescriptions maps column names to their descriptions.
	// Set via @column directives: @column: name = description
	ColumnDescriptions map[string]string

	// ColumnTags maps column names to their tags (e.g. PII, mask, mask_email).
	// Tags are single-word tokens after | in @column directives.
	// Used for masking: tag names map to DuckDB macros applied during materialization.
	ColumnTags map[string][]string

	// SortedBy lists columns for DuckLake native sorted table optimization.
	// Set via @sorted_by directive. Applied as ALTER TABLE SET SORTED BY after materialization.
	SortedBy []string

	// Expose marks the model for OData serving via `ondatrasql odata <port>`.
	Expose bool

	// ExposeKey is the optional primary key column for OData EntityType Key.
	// Set via @expose <column>. If empty, all columns are used as composite key.
	ExposeKey string

	// Columns holds column definitions for @kind: events models.
	// Parsed from the model body (DDL-style column definitions instead of SQL).
	Columns []ColumnDef

	// Source is the Starlark function name for lib/ blueprints (e.g. "gam_report").
	// The function is loaded from lib/<source>.star via load().
	Source string

	// SourceConfig holds config key-value pairs passed to the source function (lib/ blueprints).
	SourceConfig map[string]any

	// Sink is the name of the push function in lib/<sink>.star for outbound sync.
	// Set via @sink directive. When set, runtime pushes delta rows to external API after materialization.
	Sink string

	// FilePath is the original file path (empty for database-stored models).
	FilePath string
}

// splitStatements splits SQL on semicolons, respecting string literals and
// SQL comments. Returns non-empty trimmed statements.
func splitStatements(sql string) []string {
	var stmts []string
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

		// Start of line comment
		if !inString && ch == '-' && i+1 < len(sql) && sql[i+1] == '-' {
			inLineComment = true
			current.WriteByte(ch)
			continue
		}

		// Start of block comment
		if !inString && ch == '/' && i+1 < len(sql) && sql[i+1] == '*' {
			inBlockComment = true
			current.WriteByte(ch)
			current.WriteByte(sql[i+1])
			i++
			continue
		}

		// String/identifier literal handling (single and double quotes)
		if !inString && (ch == '\'' || ch == '"') {
			inString = true
			stringChar = ch
			current.WriteByte(ch)
		} else if inString && ch == stringChar {
			current.WriteByte(ch)
			if i+1 < len(sql) && sql[i+1] == stringChar {
				current.WriteByte(sql[i+1])
				i++ // escaped quote
			} else {
				inString = false
			}
		} else if ch == ';' && !inString {
			s := strings.TrimSpace(current.String())
			if s != "" {
				stmts = append(stmts, s)
			}
			current.Reset()
		} else {
			current.WriteByte(ch)
		}
	}

	s := strings.TrimSpace(current.String())
	if s != "" {
		stmts = append(stmts, s)
	}

	return stmts
}

// c matches comment prefixes: -- (SQL) or // (Starlark) or # (Starlark)
const c = `(?:--|//|#)`

var (
	kindRe               = regexp.MustCompile(`^` + c + `\s*@kind:\s*(.+)$`)
	scriptRe             = regexp.MustCompile(`^` + c + `\s*@script\s*$`)
	uniqueKeyRe          = regexp.MustCompile(`^` + c + `\s*@unique_key:\s*(.+)$`)
	partitionedByRe      = regexp.MustCompile(`^` + c + `\s*@partitioned_by:\s*(.+)$`)
	incrementalRe        = regexp.MustCompile(`^` + c + `\s*@incremental:\s*(.+)$`)
	incrementalInitialRe = regexp.MustCompile(`^` + c + `\s*@incremental_initial:\s*(.+)$`)
	constraintRe         = regexp.MustCompile(`^` + c + `\s*@constraint:\s*(.+)$`)
	auditRe              = regexp.MustCompile(`^` + c + `\s*@audit:\s*(.+)$`)
	warningRe            = regexp.MustCompile(`^` + c + `\s*@warning:\s*(.+)$`)
	extensionRe          = regexp.MustCompile(`^` + c + `\s*@extension:\s*(.+)$`)
	descriptionRe        = regexp.MustCompile(`^` + c + `\s*@description:\s*(.+)$`)
	columnRe             = regexp.MustCompile(`^` + c + `\s*@column:\s*(.+)$`)
	sortedByRe           = regexp.MustCompile(`^` + c + `\s*@sorted_by:\s*(.+)$`)
	exposeRe                = regexp.MustCompile(`^` + c + `\s*@expose(?:\s+(.+))?$`)
	sinkRe                  = regexp.MustCompile(`^` + c + `\s*@sink:\s*(.*)$`)
	sinkDetectDeletesRe     = regexp.MustCompile(`^` + c + `\s*@sink_detect_deletes:\s*(.+)$`)
	sinkDeleteThresholdRe   = regexp.MustCompile(`^` + c + `\s*@sink_delete_threshold:\s*(.+)$`)
	commentRe = regexp.MustCompile(`^(?:--|//|#)`)
)

// ParseModel reads a SQL file and extracts all directives.
// The target name is derived from the file path relative to projectDir/models/.
// Returns an error if the file cannot be read or has invalid directives.
func ParseModel(path, projectDir string) (*Model, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open model file: %w", err)
	}
	defer file.Close()

	// Get absolute path for internal use
	absPath, err := filepath.Abs(path)
	if err != nil {
		return nil, fmt.Errorf("get absolute path: %w", err)
	}

	// Calculate relative path from project root (for git URLs and portability)
	relFilePath := absPath
	if projectDir != "" {
		if rel, err := filepath.Rel(projectDir, absPath); err == nil && !strings.HasPrefix(rel, "..") {
			relFilePath = rel
		}
	}

	model := &Model{
		FilePath: relFilePath,
		Kind:     "table", // default
	}

	ext := filepath.Ext(absPath)

	// Calculate target from path
	// DuckDB only supports schema.table (2 parts), so we flatten deeper paths:
	//   models/orders.sql           -> main.orders
	//   models/staging/orders.sql   -> staging.orders
	//   models/raw/api/orders.sql   -> raw.api__orders (__ separates folder levels)
	modelsDir := filepath.Join(projectDir, "models")
	relPath, err := filepath.Rel(modelsDir, absPath)
	if err != nil || strings.HasPrefix(relPath, "..") {
		// File is outside models/ - use tmp schema
		name := strings.TrimSuffix(filepath.Base(absPath), ext)
		// Sanitize dots in filename to underscores (e.g. adhoc.v2.sql → tmp.adhoc_v2)
		name = strings.ReplaceAll(name, ".", "_")
		model.Target = "tmp." + name
	} else {
		relPath = strings.TrimSuffix(relPath, ext)
		parts := strings.Split(relPath, string(filepath.Separator))

		// Validate path components
		for _, part := range parts {
			if err := ValidatePathSegment(part); err != nil {
				return nil, fmt.Errorf("invalid model path: %w", err)
			}
			if strings.Contains(part, "__") {
				return nil, fmt.Errorf("path component %q contains reserved pattern '__' (used as folder separator in table names)", part)
			}
		}

		var schema, table string
		switch len(parts) {
		case 1:
			// models/orders.sql -> main.orders
			schema = "main"
			table = parts[0]
		default:
			// models/staging/orders.sql -> staging.orders
			// models/raw/api/orders.sql -> raw.api__orders
			schema = parts[0]
			table = strings.Join(parts[1:], "__")
		}
		model.Target = schema + "." + table
	}

	// Parse file
	// Use Scanner.Buffer() to handle long SQL lines (default is 64KB)
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 1024*1024), 10*1024*1024) // 1MB initial, 10MB max
	var sqlLines []string
	inHeader := true

	for scanner.Scan() {
		line := scanner.Text()
		trimmed := strings.TrimSpace(line)

		switch {
		case kindRe.MatchString(trimmed):
			matches := kindRe.FindStringSubmatch(trimmed)
			model.Kind = strings.TrimSpace(matches[1])

		case scriptRe.MatchString(trimmed):
			return nil, fmt.Errorf("the @script directive was removed. Starlark is now used only in lib/ functions for API ingestion and outbound sync. Remove this directive")

		case uniqueKeyRe.MatchString(trimmed):
			matches := uniqueKeyRe.FindStringSubmatch(trimmed)
			model.UniqueKey = strings.TrimSpace(matches[1])

		case partitionedByRe.MatchString(trimmed):
			matches := partitionedByRe.FindStringSubmatch(trimmed)
			// Parse comma-separated partition expressions (paren-aware to support bucket(N, col))
			cols := splitParenAware(matches[1])
			for _, col := range cols {
				col = strings.TrimSpace(col)
				if col != "" {
					model.PartitionedBy = append(model.PartitionedBy, col)
				}
			}

		case incrementalRe.MatchString(trimmed):
			matches := incrementalRe.FindStringSubmatch(trimmed)
			model.Incremental = strings.TrimSpace(matches[1])

		case incrementalInitialRe.MatchString(trimmed):
			matches := incrementalInitialRe.FindStringSubmatch(trimmed)
			val := strings.TrimSpace(matches[1])
			val = strings.Trim(val, `"'`)
			model.IncrementalInitial = val

		case constraintRe.MatchString(trimmed):
			matches := constraintRe.FindStringSubmatch(trimmed)
			model.Constraints = append(model.Constraints, strings.TrimSpace(matches[1]))

		case auditRe.MatchString(trimmed):
			matches := auditRe.FindStringSubmatch(trimmed)
			model.Audits = append(model.Audits, strings.TrimSpace(matches[1]))

		case warningRe.MatchString(trimmed):
			matches := warningRe.FindStringSubmatch(trimmed)
			model.Warnings = append(model.Warnings, strings.TrimSpace(matches[1]))

		case extensionRe.MatchString(trimmed):
			matches := extensionRe.FindStringSubmatch(trimmed)
			model.Extensions = append(model.Extensions, strings.TrimSpace(matches[1]))

		case sortedByRe.MatchString(trimmed):
			matches := sortedByRe.FindStringSubmatch(trimmed)
			cols := strings.Split(matches[1], ",")
			for _, col := range cols {
				col = strings.TrimSpace(col)
				if col != "" {
					model.SortedBy = append(model.SortedBy, col)
				}
			}

		case exposeRe.MatchString(trimmed):
			model.Expose = true
			if matches := exposeRe.FindStringSubmatch(trimmed); matches[1] != "" {
				model.ExposeKey = strings.TrimSpace(matches[1])
			}

		case sinkRe.MatchString(trimmed):
			matches := sinkRe.FindStringSubmatch(trimmed)
			model.Sink = strings.TrimSpace(matches[1])
			if model.Sink == "" {
				return nil, fmt.Errorf("@sink requires a lib function name (e.g. @sink: hubspot_push)")
			}

		case sinkDetectDeletesRe.MatchString(trimmed):
			return nil, fmt.Errorf("@sink_detect_deletes was removed — delete detection is now automatic for merge and tracked models with @sink. Remove this directive")

		case sinkDeleteThresholdRe.MatchString(trimmed):
			return nil, fmt.Errorf("@sink_delete_threshold was removed — delete threshold is no longer supported. Remove this directive")

		case descriptionRe.MatchString(trimmed):
			matches := descriptionRe.FindStringSubmatch(trimmed)
			model.Description = strings.TrimSpace(matches[1])

		case columnRe.MatchString(trimmed):
			matches := columnRe.FindStringSubmatch(trimmed)
			raw := strings.TrimSpace(matches[1])
			// Parse "name = description | tag1 | tag2"
			// Tags are single-word tokens after |. Used for masking macros.
			// Multi-word text after | is preserved as description.
			if colName, colDesc, ok := strings.Cut(raw, "="); ok {
				colName = strings.TrimSpace(colName)
				colDesc = strings.TrimSpace(colDesc)
				colDesc, tags := extractColumnTags(colDesc)
				if colName != "" && colDesc != "" {
					if model.ColumnDescriptions == nil {
						model.ColumnDescriptions = make(map[string]string)
					}
					model.ColumnDescriptions[colName] = colDesc
				}
				if colName != "" && len(tags) > 0 {
					if model.ColumnTags == nil {
						model.ColumnTags = make(map[string][]string)
					}
					model.ColumnTags[colName] = tags
				}
			}

		case commentRe.MatchString(trimmed) && inHeader:
			// Header comment - ignore

		default:
			// SQL code
			inHeader = false
			sqlLines = append(sqlLines, line)
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("read model file: %w", err)
	}

	fullSQL := strings.TrimSpace(strings.Join(sqlLines, "\n"))

	// Models must be a single SQL statement. Multi-statement files are rejected.
	statements := splitStatements(fullSQL)
	switch len(statements) {
	case 0:
		// handled below by empty SQL check
	case 1:
		model.SQL = statements[0]
	default:
		return nil, fmt.Errorf("model contains %d SQL statements (separated by ;), but only single-statement models are supported. Remove extra statements or split into separate models", len(statements))
	}

	// For events kind, parse body as column definitions instead of SQL
	if model.Kind == "events" {
		cols, err := parseColumnDefs(model.SQL)
		if err != nil {
			return nil, fmt.Errorf("parse column definitions: %w", err)
		}
		model.Columns = cols
	}

	// Validate
	if err := validateModel(model); err != nil {
		return nil, err
	}

	return model, nil
}

// columnTagRe matches a single-word tag (letters, digits, underscores).
// Hyphens are not allowed — they produce invalid SQL (interpreted as subtraction).
var columnTagRe = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_]*$`)

// extractColumnTags extracts trailing pipe-separated tags from a column description.
// Tags must be single-word tokens (e.g. "PII", "mask_email").
// Multi-word text after | is kept as part of the description.
// Returns the cleaned description and the list of tags.
// Examples:
//
//	"Social security number | PII"           → "Social security number", ["PII"]
//	"Revenue | Cost ratio"                   → "Revenue | Cost ratio", []
//	"Email address | PII | mask_email"       → "Email address", ["PII", "mask_email"]
func extractColumnTags(desc string) (string, []string) {
	var tags []string
	for {
		idx := strings.LastIndex(desc, "|")
		if idx < 0 {
			break
		}
		after := strings.TrimSpace(desc[idx+1:])
		if columnTagRe.MatchString(after) {
			tags = append(tags, after)
			desc = strings.TrimSpace(desc[:idx])
		} else {
			break
		}
	}
	// Reverse tags (we extracted right-to-left)
	for i, j := 0, len(tags)-1; i < j; i, j = i+1, j-1 {
		tags[i], tags[j] = tags[j], tags[i]
	}
	return desc, tags
}

// validateModel checks that the model has valid values.
func validateModel(m *Model) error {
	// Validate kind
	switch m.Kind {
	case "table", "append", "merge", "scd2", "partition", "events", "tracked":
		// OK
	case "view":
		return fmt.Errorf("the 'view' kind was removed in v0.14.0. Use '@kind: table' instead — DuckLake makes table storage essentially free, and you gain snapshots, sandbox, lineage, validation, and CDC")
	default:
		return fmt.Errorf("invalid kind %q: must be table, append, merge, scd2, partition, events, or tracked", m.Kind)
	}

	// SQL models must have a non-empty body. Scripts and events kind have
	// their own body semantics and are exempt. (Bug 11)
	if m.ScriptType == ScriptTypeNone && m.Kind != "events" && strings.TrimSpace(m.SQL) == "" {
		return fmt.Errorf("model %s has no SQL body — write a SELECT statement after the directives", m.Target)
	}

	// Validate target
	if err := ValidateIdentifier(m.Target); err != nil {
		return fmt.Errorf("invalid target: %w", err)
	}

	// @expose is only allowed on materialized SQL models
	if m.Expose {
		if m.ScriptType != "" {
			return fmt.Errorf("@expose is only supported for SQL models (not scripts)")
		}
		switch m.Kind {
		case "table", "merge", "scd2", "append", "partition", "tracked":
			// OK — materialized with fixed schema
		default:
			return fmt.Errorf("@expose is not supported for %s kind (only table, merge, scd2, append, partition, tracked)", m.Kind)
		}
		if m.ExposeKey != "" {
			if err := ValidateColumnName(m.ExposeKey); err != nil {
				return fmt.Errorf("invalid @expose key: %w", err)
			}
		}
	}

	// Events models have restricted directives — only @description is allowed
	if m.Kind == "events" {
		if len(m.Columns) == 0 {
			return fmt.Errorf("events model must define at least 1 column")
		}
		if m.UniqueKey != "" {
			return fmt.Errorf("@unique_key is not supported for events")
		}
		if len(m.PartitionedBy) > 0 {
			return fmt.Errorf("@partitioned_by is not supported for events")
		}
		if m.Incremental != "" {
			return fmt.Errorf("@incremental is not supported for events")
		}
		if len(m.SortedBy) > 0 {
			return fmt.Errorf("@sorted_by is not supported for events")
		}
		if len(m.Constraints) > 0 {
			return fmt.Errorf("@constraint is not supported for events")
		}
		if len(m.Audits) > 0 {
			return fmt.Errorf("@audit is not supported for events")
		}
		if len(m.Warnings) > 0 {
			return fmt.Errorf("@warning is not supported for events")
		}
		if len(m.ColumnDescriptions) > 0 {
			return fmt.Errorf("@column is not supported for events (use column definitions in the body)")
		}
		if len(m.ColumnTags) > 0 {
			return fmt.Errorf("@column tags are not supported for events")
		}
		if len(m.Extensions) > 0 {
			return fmt.Errorf("@extension is not supported for events")
		}
	}

	// Validate @sink directives
	if m.Sink != "" && m.Kind == "events" {
		return fmt.Errorf("@sink is not supported for events kind (events has its own ingest pipeline)")
	}

	// Validate unique_key if present
	if m.UniqueKey != "" {
		if strings.Contains(m.UniqueKey, ",") {
			if m.Kind != "partition" {
				return fmt.Errorf("only a single unique_key column is supported for %s kind", m.Kind)
			}
			// Validate each column individually, reject empty segments
			var validCols int
			for _, col := range strings.Split(m.UniqueKey, ",") {
				col = strings.TrimSpace(col)
				if col == "" {
					return fmt.Errorf("invalid unique_key %q: empty column name (trailing or double comma)", m.UniqueKey)
				}
				if err := ValidateColumnName(col); err != nil {
					return fmt.Errorf("invalid unique_key: %w", err)
				}
				validCols++
			}
			if validCols == 0 {
				return fmt.Errorf("invalid unique_key %q: no valid columns", m.UniqueKey)
			}
		} else {
			if err := ValidateColumnName(m.UniqueKey); err != nil {
				return fmt.Errorf("invalid unique_key: %w", err)
			}
		}
	}

	// Partition kind requires unique_key
	if m.Kind == "partition" && m.UniqueKey == "" {
		return fmt.Errorf("partition kind requires @unique_key directive")
	}

	// Tracked kind requires unique_key (single column only)
	if m.Kind == "tracked" && m.UniqueKey == "" {
		return fmt.Errorf("tracked kind requires @unique_key directive")
	}

	// @partitioned_by is a storage hint — not supported on partition kind
	// (partition kind uses @unique_key for DELETE+INSERT, not DuckLake file layout)
	if m.Kind == "partition" && len(m.PartitionedBy) > 0 {
		return fmt.Errorf("@partitioned_by is not supported for partition kind (use @unique_key for partition columns)")
	}

	// Validate partitioned_by expressions (column names or transforms like bucket(N, col))
	for _, col := range m.PartitionedBy {
		if err := ValidatePartitionExpr(col); err != nil {
			return fmt.Errorf("invalid partitioned_by expression: %w", err)
		}
	}

	// Validate incremental column if present (column name, no dots allowed)
	if m.Incremental != "" {
		if err := ValidateColumnName(m.Incremental); err != nil {
			return fmt.Errorf("invalid incremental column: %w", err)
		}
	}

	// Validate @column column names (column names, no dots allowed)
	for colName := range m.ColumnDescriptions {
		if err := ValidateColumnName(colName); err != nil {
			return fmt.Errorf("invalid @column name: %w", err)
		}
	}

	// Validate @column tag column names (may exist without descriptions)
	for colName := range m.ColumnTags {
		if err := ValidateColumnName(colName); err != nil {
			return fmt.Errorf("invalid @column name: %w", err)
		}
	}

	// Validate sorted_by columns if present (column names, no dots allowed)
	for _, col := range m.SortedBy {
		if err := ValidateColumnName(col); err != nil {
			return fmt.Errorf("invalid sorted_by column: %w", err)
		}
	}

	return nil
}

// ValidateIdentifier checks that a SQL identifier is safe.
// Only allows letters, numbers, underscores, and dots.
// Each dot-separated part must start with a letter or underscore.
// Use this for qualified names like schema.table.
func ValidateIdentifier(s string) error {
	if s == "" {
		return fmt.Errorf("identifier cannot be empty")
	}

	validRe := regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*(\.[a-zA-Z_][a-zA-Z0-9_]*)*$`)
	if !validRe.MatchString(s) {
		return fmt.Errorf("invalid identifier %q: only letters, numbers, underscores, and dots allowed", s)
	}

	return nil
}

// ValidatePathSegment checks that a model path segment (directory or filename) is safe for SQL.
// Only allows ASCII letters, numbers, and underscores. Path segments become
// part of the resulting schema/table identifier (e.g. models/raw/orders.sql →
// raw.orders), and OndatraSQL deliberately keeps identifiers ASCII to avoid
// quoting/escape edge cases across DuckDB and DuckLake catalogs. Non-ASCII
// names (åäö, 注文, etc.) must be transliterated by hand. (Bug 24)
func ValidatePathSegment(s string) error {
	if s == "" {
		return fmt.Errorf("path segment cannot be empty")
	}

	validRe := regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)
	if !validRe.MatchString(s) {
		return fmt.Errorf(
			"invalid path segment %q: model paths become SQL identifiers, so only "+
				"ASCII letters, numbers, and underscores are allowed (rename the file/directory — "+
				"e.g. 'låneansökan' → 'laneansokan')",
			s)
	}

	return nil
}

// ValidateColumnName checks that a column name is safe.
// Only allows letters, numbers, and underscores (no dots).
// Use this for single column names like @unique_key, @partitioned_by, @incremental.
func ValidateColumnName(s string) error {
	if s == "" {
		return fmt.Errorf("column name cannot be empty")
	}

	validRe := regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)
	if !validRe.MatchString(s) {
		return fmt.Errorf("invalid column name %q: only letters, numbers, and underscores allowed (no dots)", s)
	}

	// Reject reserved internal names used by sink protocol
	if strings.HasPrefix(s, "__ondatra_") {
		return fmt.Errorf("column name %q is reserved (prefix __ondatra_ is used internally)", s)
	}

	return nil
}

// ValidatePartitionExpr checks that a partition expression is safe.
// Allows column names (e.g. "region") or known DuckLake transform functions
// (year, month, day, hour, bucket).
func ValidatePartitionExpr(s string) error {
	if s == "" {
		return fmt.Errorf("partition expression cannot be empty")
	}
	// Known DuckLake partition transforms (case-insensitive — DuckDB treats function names as case-insensitive)
	knownTransforms := regexp.MustCompile(`(?i)^(year|month|day|hour|bucket)\(.+\)$`)
	if knownTransforms.MatchString(s) {
		return nil
	}
	// Unknown transform function — reject
	unknownTransform := regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*\(.+\)$`)
	if unknownTransform.MatchString(s) {
		return fmt.Errorf("unsupported partition transform %q: supported transforms are year(), month(), day(), hour(), bucket()", s)
	}
	// Plain column name
	return ValidateColumnName(s)
}

// splitParenAware splits a string on commas, but ignores commas inside parentheses.
func splitParenAware(s string) []string {
	var parts []string
	depth := 0
	start := 0
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '(':
			depth++
		case ')':
			if depth > 0 {
				depth--
			}
		case ',':
			if depth == 0 {
				parts = append(parts, s[start:i])
				start = i + 1
			}
		}
	}
	parts = append(parts, s[start:])
	return parts
}

// parseColumnDefs parses DDL-style column definitions from an events model body.
// Format: "col_name TYPE [NOT NULL]," — one per line, trailing commas optional.
// Example:
//
//	event_name VARCHAR NOT NULL,
//	page_url VARCHAR,
//	received_at TIMESTAMPTZ
func parseColumnDefs(body string) ([]ColumnDef, error) {
	var cols []ColumnDef

	for _, line := range strings.Split(body, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "--") || strings.HasPrefix(line, "//") || strings.HasPrefix(line, "#") {
			continue
		}

		// Strip trailing comma
		line = strings.TrimRight(line, ",")
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		parts := strings.Fields(line)
		if len(parts) < 2 {
			return nil, fmt.Errorf("invalid column definition: %q (expected: name TYPE [NOT NULL])", line)
		}

		// Parse: name TYPE [NOT NULL]
		// Supports multi-word types like DOUBLE PRECISION, TIMESTAMP WITH TIME ZONE.
		// Rejects inline constraints (PRIMARY KEY, DEFAULT, CHECK, UNIQUE, REFERENCES).
		notNullIdx := -1
		for i := 1; i < len(parts)-1; i++ {
			if strings.EqualFold(parts[i], "NOT") && strings.EqualFold(parts[i+1], "NULL") {
				notNullIdx = i
				break
			}
		}

		var typeParts []string
		col := ColumnDef{Name: parts[0]}
		var trailingIdx int // first token after type+NOT NULL
		if notNullIdx > 0 {
			typeParts = parts[1:notNullIdx]
			col.NotNull = true
			trailingIdx = notNullIdx + 2
		} else {
			typeParts = parts[1:]
			trailingIdx = len(parts)
		}
		col.Type = strings.Join(typeParts, " ")

		// Reject trailing tokens — events columns only support name TYPE [NOT NULL]
		if trailingIdx < len(parts) {
			return nil, fmt.Errorf("invalid column definition: %q (only name TYPE [NOT NULL] is supported, found extra: %s)",
				line, strings.Join(parts[trailingIdx:], " "))
		}

		if err := ValidateColumnName(col.Name); err != nil {
			return nil, fmt.Errorf("column %q: %w", col.Name, err)
		}

		cols = append(cols, col)
	}

	return cols, nil
}
