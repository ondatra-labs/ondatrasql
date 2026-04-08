// OndatraSQL - You don't need a data stack anymore
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

// Package lineage extracts column-level lineage from SQL using DuckDB's AST.
package lineage

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/ondatra-labs/ondatrasql/internal/duckdb"
)

// TransformationType describes how a column was transformed.
type TransformationType string

const (
	TransformIdentity    TransformationType = "IDENTITY"    // Direct column copy
	TransformAggregation TransformationType = "AGGREGATION" // Aggregate function (SUM, COUNT, etc)
	TransformArithmetic  TransformationType = "ARITHMETIC"  // Math operations (+, -, *, /)
	TransformConditional TransformationType = "CONDITIONAL" // CASE/WHEN logic
	TransformCast        TransformationType = "CAST"        // Type conversion
	TransformFunction    TransformationType = "FUNCTION"    // Other function calls
)

// SourceColumn represents a source column with its transformation type.
type SourceColumn struct {
	Table          string             `json:"table"`                    // Source table (e.g., "staging.orders")
	Column         string             `json:"column"`                   // Source column name (e.g., "amount")
	Transformation TransformationType `json:"transformation"`           // How it was transformed
	FunctionName   string             `json:"function,omitempty"`       // Function name if applicable
}

// ColumnLineage represents the lineage of a single output column.
type ColumnLineage struct {
	Column  string         `json:"column"`  // Output column name
	Sources []SourceColumn `json:"sources"` // Source columns with table/column/transformation
}

// AST structures for parsing DuckDB's json_serialize_sql output
type ast struct {
	Statements []statement `json:"statements"`
}

type statement struct {
	Node node `json:"node"`
}

type node struct {
	CTEMap      cteMap       `json:"cte_map"`
	SelectList  []expression `json:"select_list"`
	FromTable   fromTable    `json:"from_table"`
	WhereClause *expression  `json:"where_clause"`
	Having      *expression  `json:"having"`
	Qualify     *expression  `json:"qualify"`
	// Set operations: UNION / UNION ALL / INTERSECT / EXCEPT.
	// DuckDB represents these as SET_OPERATION_NODE with `left` and `right`
	// sub-nodes (each a SELECT_NODE) and `setop_type` describing the kind.
	// Without these fields the AST silently unmarshals to an empty node and
	// both column lineage and DAG dependency extraction lose all sources.
	Left      *node  `json:"left"`
	Right     *node  `json:"right"`
	SetOpType string `json:"setop_type"`
}

type cteMap struct {
	Map []cteEntry `json:"map"`
}

type cteEntry struct {
	Key   string   `json:"key"`
	Value cteValue `json:"value"`
}

type cteValue struct {
	Query statement `json:"query"`
}

type fromTable struct {
	TableName  string     `json:"table_name"`
	SchemaName string     `json:"schema_name"`
	Alias      string     `json:"alias"`
	Left       *fromTable `json:"left"`
	Right      *fromTable `json:"right"`
	Subquery   *statement `json:"subquery"` // For sub-selects in FROM: FROM (SELECT ...) AS sub
}

type expression struct {
	Class        string       `json:"class"`
	Type         string       `json:"type"`
	Alias        string       `json:"alias"`
	ColumnNames  []string     `json:"column_names"`
	Children     []expression `json:"children"`
	FunctionName string       `json:"function_name"`
	IsOperator   bool         `json:"is_operator"`
	CaseChecks   []caseCheck  `json:"case_checks"`
	Subquery     *statement   `json:"subquery"`  // For SUBQUERY expressions
	Child        *expression  `json:"child"`     // For comparison child
}

type caseCheck struct {
	WhenExpr expression `json:"when_expr"`
	ThenExpr expression `json:"then_expr"`
}

// Extractor holds state for recursive CTE resolution.
type Extractor struct {
	cteNodes map[string]node                      // CTE name -> Node
	resolved map[string]map[string][]SourceColumn // CTE name -> col -> final sources
}

// NewExtractor creates an extractor from a parsed AST.
func newExtractor(a ast) *Extractor {
	e := &Extractor{
		cteNodes: make(map[string]node),
		resolved: make(map[string]map[string][]SourceColumn),
	}
	if len(a.Statements) > 0 {
		for _, cte := range a.Statements[0].Node.CTEMap.Map {
			e.cteNodes[cte.Key] = cte.Value.Query.Node
		}
	}
	return e
}

// resolveCTE recursively resolves a CTE's columns to their ultimate source tables.
func (e *Extractor) resolveCTE(cteName string) map[string][]SourceColumn {
	// Return cached if already resolved
	if cols, ok := e.resolved[cteName]; ok {
		return cols
	}

	n, ok := e.cteNodes[cteName]
	if !ok {
		return nil
	}

	// Mark as being resolved (prevent infinite recursion)
	e.resolved[cteName] = make(map[string][]SourceColumn)

	// Set operation CTE (UNION / UNION ALL / INTERSECT / EXCEPT — common
	// pattern, also how RECURSIVE CTEs are represented). The SET_OPERATION
	// node has empty SelectList; column names come from LEFT, sources are
	// the positional merge of LEFT + RIGHT. Without this branch, set-op
	// CTEs would lose all column lineage and consumers downstream would
	// see references back to the CTE name itself instead of the real source.
	if n.Left != nil || n.Right != nil {
		merged := e.resolveSetOpCols(n)
		for k, v := range merged {
			e.resolved[cteName][k] = v
		}
		return e.resolved[cteName]
	}

	aliasInfo := collectAliases(n.FromTable)

	for _, expr := range n.SelectList {
		outName := getOutputName(expr)
		sources := e.traceExprWithType(expr, aliasInfo)
		e.resolved[cteName][outName] = sources
	}

	return e.resolved[cteName]
}

// resolveSetOpCols resolves the column lineage for a SET_OPERATION_NODE.
// The result map mirrors the LEFT side's output names, with sources merged
// positionally from both sides.
func (e *Extractor) resolveSetOpCols(n node) map[string][]SourceColumn {
	leftCols := e.resolveSelectNodeCols(n.Left)
	rightCols := e.resolveSelectNodeCols(n.Right)

	// Output column names come from LEFT (DuckDB convention).
	type orderedCol struct {
		name    string
		sources []SourceColumn
	}
	var ordered []orderedCol
	if n.Left != nil {
		for _, expr := range nestedSelectList(*n.Left) {
			outName := getOutputName(expr)
			ordered = append(ordered, orderedCol{name: outName, sources: leftCols[outName]})
		}
	}
	// Append right side's sources positionally.
	if n.Right != nil {
		rightNames := nestedSelectList(*n.Right)
		for i, expr := range rightNames {
			if i >= len(ordered) {
				break
			}
			rightOut := getOutputName(expr)
			ordered[i].sources = append(ordered[i].sources, rightCols[rightOut]...)
		}
	}

	result := make(map[string][]SourceColumn, len(ordered))
	for _, oc := range ordered {
		result[oc.name] = oc.sources
	}
	return result
}

// resolveSelectNodeCols traces every output column of a SELECT-shaped node
// (or recurses for nested set-op nodes) and returns name → sources.
func (e *Extractor) resolveSelectNodeCols(n *node) map[string][]SourceColumn {
	if n == nil {
		return nil
	}
	if n.Left != nil || n.Right != nil {
		return e.resolveSetOpCols(*n)
	}
	result := make(map[string][]SourceColumn)
	aliases := collectAliases(n.FromTable)
	for _, expr := range n.SelectList {
		outName := getOutputName(expr)
		result[outName] = e.traceExprWithType(expr, aliases)
	}
	return result
}

// nestedSelectList returns the LEFT-most SELECT_NODE's select list for a
// possibly-nested set-op node. Used to derive positional column order from
// arbitrarily nested UNIONs.
func nestedSelectList(n node) []expression {
	if n.Left != nil {
		return nestedSelectList(*n.Left)
	}
	return n.SelectList
}

// traceExprWithType traces an expression and returns detailed source info with transformation types.
func (e *Extractor) traceExprWithType(expr expression, info aliasInfo) []SourceColumn {
	var sources []SourceColumn

	switch expr.Class {
	case "COLUMN_REF":
		if len(expr.ColumnNames) >= 1 {
			var table, col string
			var tableAlias string
			if len(expr.ColumnNames) == 2 {
				tableAlias = expr.ColumnNames[0]
				col = expr.ColumnNames[1]
				table = info.aliases[tableAlias]
			} else {
				col = expr.ColumnNames[0]
				// For unqualified columns, use the primary table (first in FROM clause)
				// This provides deterministic behavior instead of random map iteration
				table = info.primaryTable
			}

			// Subquery alias: resolve via the subquery's own select list.
			// Has to be checked BEFORE the table-name path because subqueries
			// don't have a table name. (Review finding 2)
			if tableAlias != "" {
				if sub, isSubquery := info.subqueries[tableAlias]; isSubquery {
					sources = append(sources, e.resolveSubqueryColumn(sub, col)...)
					return sources
				}
			}
			// Unqualified column with no primary table but a primary
			// subquery in FROM: resolve via the subquery's select list.
			// (Review finding 2 — `SELECT col FROM (SELECT a AS col FROM t) sub`.)
			if tableAlias == "" && table == "" && info.primarySubquery != nil {
				sources = append(sources, e.resolveSubqueryColumn(info.primarySubquery, col)...)
				return sources
			}

			if table != "" {
				// Check if table is a CTE - resolve recursively
				if _, isCTE := e.cteNodes[table]; isCTE {
					cteCols := e.resolveCTE(table)
					if deeper, ok := cteCols[col]; ok {
						// CTE sources already have Table/Column populated
						sources = append(sources, deeper...)
					} else {
						sources = append(sources, SourceColumn{
							Table:          table,
							Column:         col,
							Transformation: TransformIdentity,
						})
					}
				} else {
					sources = append(sources, SourceColumn{
						Table:          table,
						Column:         col,
						Transformation: TransformIdentity,
					})
				}
			}
		}

	case "FUNCTION":
		// Determine transformation type based on function
		transType := classifyFunction(expr.FunctionName, expr.IsOperator)

		// Trace through function arguments
		for _, child := range expr.Children {
			childSources := e.traceExprWithType(child, info)
			for _, cs := range childSources {
				sources = append(sources, SourceColumn{
					Table:          cs.Table,
					Column:         cs.Column,
					Transformation: transType,
					FunctionName:   expr.FunctionName,
				})
			}
		}

	case "WINDOW":
		// Window function: SUM(amount) OVER (...). The aggregate function name
		// is on the WINDOW node itself; the aggregated expression is in children.
		// (Bug 23) Treat the same as a regular FUNCTION node so the column is
		// linked back to its source with a transformation tag (e.g. [SUM]).
		transType := classifyFunction(expr.FunctionName, expr.IsOperator)
		for _, child := range expr.Children {
			childSources := e.traceExprWithType(child, info)
			for _, cs := range childSources {
				sources = append(sources, SourceColumn{
					Table:          cs.Table,
					Column:         cs.Column,
					Transformation: transType,
					FunctionName:   expr.FunctionName,
				})
			}
		}

	case "CASE":
		// Trace through CASE checks (when/then expressions)
		for _, check := range expr.CaseChecks {
			// Trace the THEN expression (this is the actual value)
			thenSources := e.traceExprWithType(check.ThenExpr, info)
			for _, ts := range thenSources {
				sources = append(sources, SourceColumn{
					Table:          ts.Table,
					Column:         ts.Column,
					Transformation: TransformConditional,
				})
			}
			// Also trace WHEN conditions as they affect the logic
			whenSources := e.traceExprWithType(check.WhenExpr, info)
			for _, ws := range whenSources {
				sources = append(sources, SourceColumn{
					Table:          ws.Table,
					Column:         ws.Column,
					Transformation: TransformConditional,
				})
			}
		}
		// Trace through children for else clause
		for _, child := range expr.Children {
			childSources := e.traceExprWithType(child, info)
			for _, cs := range childSources {
				sources = append(sources, SourceColumn{
					Table:          cs.Table,
					Column:         cs.Column,
					Transformation: TransformConditional,
				})
			}
		}

	case "OPERATOR_CAST":
		// Type cast
		for _, child := range expr.Children {
			childSources := e.traceExprWithType(child, info)
			for _, cs := range childSources {
				sources = append(sources, SourceColumn{
					Table:          cs.Table,
					Column:         cs.Column,
					Transformation: TransformCast,
				})
			}
		}

	case "COMPARISON":
		// Comparison operators (used in CASE WHEN conditions)
		for _, child := range expr.Children {
			sources = append(sources, e.traceExprWithType(child, info)...)
		}
	}

	return sources
}

// classifyFunction determines the transformation type based on function name.
func classifyFunction(funcName string, isOperator bool) TransformationType {
	funcName = strings.ToUpper(funcName)

	// Arithmetic operators
	if isOperator {
		switch funcName {
		case "+", "-", "*", "/", "%", "^":
			return TransformArithmetic
		}
	}

	// Aggregate functions
	switch funcName {
	case "SUM", "COUNT", "AVG", "MIN", "MAX", "FIRST", "LAST",
		"STDDEV", "STDDEV_POP", "STDDEV_SAMP", "VARIANCE", "VAR_POP", "VAR_SAMP",
		"STRING_AGG", "ARRAY_AGG", "LIST", "LISTAGG", "GROUP_CONCAT":
		return TransformAggregation
	}

	// Default to generic function transform
	return TransformFunction
}

// extractMainQuery extracts lineage from the main query (after CTE resolution).
func (e *Extractor) extractMainQuery(n node) []ColumnLineage {
	// Set operation node (UNION / UNION ALL / INTERSECT / EXCEPT): the
	// output schema is determined by the LEFT side, but each output column
	// has sources from BOTH sides (positionally aligned). Recurse into
	// both sub-nodes and merge the matching column lineages.
	if n.Left != nil || n.Right != nil {
		var leftCols, rightCols []ColumnLineage
		if n.Left != nil {
			leftCols = e.extractMainQuery(*n.Left)
		}
		if n.Right != nil {
			rightCols = e.extractMainQuery(*n.Right)
		}
		var merged []ColumnLineage
		for i, lc := range leftCols {
			combined := ColumnLineage{
				Column:  lc.Column,
				Sources: append([]SourceColumn{}, lc.Sources...),
			}
			if i < len(rightCols) {
				combined.Sources = append(combined.Sources, rightCols[i].Sources...)
			}
			merged = append(merged, combined)
		}
		return merged
	}

	var result []ColumnLineage
	aliases := collectAliases(n.FromTable)

	for _, expr := range n.SelectList {
		outName := getOutputName(expr)
		sources := e.traceExprWithType(expr, aliases)

		result = append(result, ColumnLineage{
			Column:  outName,
			Sources: sources,
		})
	}
	return result
}

// aliasInfo contains table alias mapping and the primary table
type aliasInfo struct {
	aliases         map[string]string     // alias -> table name
	subqueries      map[string]*statement // alias -> subquery statement (FROM (SELECT ...) sub)
	primaryTable    string                // first table in FROM clause (for unqualified columns)
	primarySubquery *statement            // first source in FROM if it's a subquery (for unqualified columns)
}

// collectAliases builds a map of table aliases to fully qualified table names from a FROM clause.
// Also returns the primary table (first in FROM clause) for resolving unqualified columns.
// Subquery aliases (FROM (SELECT ...) AS sub) are recorded separately so column lineage
// can resolve them via the subquery's own select list. (Review finding 2)
func collectAliases(ft fromTable) aliasInfo {
	info := aliasInfo{
		aliases:    make(map[string]string),
		subqueries: make(map[string]*statement),
	}
	first := true
	var collect func(fromTable)
	collect = func(f fromTable) {
		if f.TableName != "" {
			// Build fully qualified table name (schema.table if schema exists)
			qualifiedName := f.TableName
			if f.SchemaName != "" {
				qualifiedName = f.SchemaName + "." + f.TableName
			}

			alias := f.Alias
			if alias == "" {
				alias = f.TableName
			}
			info.aliases[alias] = qualifiedName
			// Track the first (leftmost) table as primary
			if first {
				info.primaryTable = qualifiedName
				first = false
			}
		}
		// Subquery in FROM: register the alias so COLUMN_REFs like
		// `sub.x` can be resolved via the subquery's own select list.
		if f.Subquery != nil && f.Alias != "" {
			info.subqueries[f.Alias] = f.Subquery
			if first {
				// First source is a subquery — record it as the primary
				// subquery so unqualified COLUMN_REFs (e.g. just `col`
				// in `SELECT col FROM (SELECT a AS col FROM t) sub`)
				// can still be resolved via the subquery's select list.
				info.primarySubquery = f.Subquery
				first = false
			}
		}
		// Process left side first (it's the primary table in JOINs)
		if f.Left != nil {
			collect(*f.Left)
		}
		if f.Right != nil {
			collect(*f.Right)
		}
	}
	collect(ft)
	return info
}

// resolveSubqueryColumn finds the lineage of a column inside a FROM-subquery.
// Walks the subquery's select list, picks the matching output, and traces it
// against the subquery's own FROM/aliases. Recursive — handles nested
// subqueries AND set-operation subqueries (UNION/INTERSECT/EXCEPT inside the
// subquery), where the top node has no SelectList and column lineage lives
// in Left/Right sub-nodes.
func (e *Extractor) resolveSubqueryColumn(sub *statement, col string) []SourceColumn {
	if sub == nil {
		return nil
	}
	// Set-op subquery: delegate to resolveSelectNodeCols which knows how
	// to merge Left/Right positionally. The top node's empty SelectList
	// would otherwise hide every column. (Review finding)
	if sub.Node.Left != nil || sub.Node.Right != nil {
		cols := e.resolveSelectNodeCols(&sub.Node)
		return cols[col]
	}
	subAliases := collectAliases(sub.Node.FromTable)
	for _, expr := range sub.Node.SelectList {
		if getOutputName(expr) == col {
			return e.traceExprWithType(expr, subAliases)
		}
	}
	return nil
}

// getOutputName extracts the output column name from an expression.
func getOutputName(expr expression) string {
	if expr.Alias != "" {
		return expr.Alias
	}
	if len(expr.ColumnNames) > 0 {
		return expr.ColumnNames[len(expr.ColumnNames)-1]
	}
	return "?"
}

// ExtractFromAST extracts column-level lineage from a pre-parsed AST JSON.
// Use GetAST to fetch the AST first, then share it with ExtractTablesFromAST.
func ExtractFromAST(astJSON string) ([]ColumnLineage, error) {
	var a ast
	if err := json.Unmarshal([]byte(astJSON), &a); err != nil {
		return nil, fmt.Errorf("failed to parse AST: %w", err)
	}

	if len(a.Statements) == 0 {
		return nil, fmt.Errorf("no statements in AST")
	}

	extractor := newExtractor(a)
	lineages := extractor.extractMainQuery(a.Statements[0].Node)

	return lineages, nil
}

// GetAST fetches the parsed AST JSON from DuckDB for a SQL query.
// The result can be passed to ExtractFromAST and ExtractTablesFromAST
// to avoid duplicate queries when both column lineage and tables are needed.
func GetAST(sess *duckdb.Session, sql string) (string, error) {
	// Escape single quotes for the SQL string
	escaped := strings.ReplaceAll(sql, "'", "''")

	// Use DuckDB's json_serialize_sql to get the AST
	// Wrap with REPLACE to remove newlines (CSV output breaks on multi-line values)
	// Use alias 'ast' to prevent the embedded SQL from appearing in the column header
	query := fmt.Sprintf("SELECT REPLACE(REPLACE(CAST(json_serialize_sql('%s') AS VARCHAR), chr(10), ''), chr(13), '') AS ast", escaped)

	astJSON, err := sess.QueryValue(query)
	if err != nil {
		return "", fmt.Errorf("failed to serialize SQL: %w", err)
	}

	if astJSON == "" {
		return "", fmt.Errorf("empty AST returned")
	}

	return astJSON, nil
}

// Extract extracts column-level lineage from a SQL query using DuckDB's AST parser.
// Returns nil if lineage cannot be extracted (parsing errors, etc.).
// Note: If you also need table references, use GetAST + ExtractFromAST + ExtractTablesFromAST
// to avoid duplicate queries.
func Extract(sess *duckdb.Session, sql string) ([]ColumnLineage, error) {
	astJSON, err := GetAST(sess, sql)
	if err != nil {
		return nil, err
	}
	return ExtractFromAST(astJSON)
}

// TableRef represents a table reference with its role in the query.
type TableRef struct {
	Table       string // Full table name (schema.table)
	Alias       string // Alias used in query
	IsFirstFrom bool   // True if this is the first table in FROM clause (primary source)
	IsJoin      bool   // True if this table is from a JOIN clause
}

// ExtractTables extracts all table references from a SQL query using DuckDB's AST parser.
// Returns tables in order: first FROM table, then JOIN tables.
// CTEs are NOT included in the result - only physical tables.
// Note: If you also need column lineage, use GetAST + ExtractFromAST + ExtractTablesFromAST
// to avoid duplicate queries.
func ExtractTables(sess *duckdb.Session, sql string) ([]TableRef, error) {
	astJSON, err := GetAST(sess, sql)
	if err != nil {
		return nil, err
	}
	return ExtractTablesFromAST(astJSON)
}

// ExtractTablesFromAST extracts table references from a pre-parsed AST JSON.
func ExtractTablesFromAST(astJSON string) ([]TableRef, error) {
	var a ast
	if err := json.Unmarshal([]byte(astJSON), &a); err != nil {
		return nil, fmt.Errorf("failed to parse AST: %w", err)
	}

	if len(a.Statements) == 0 {
		return nil, fmt.Errorf("no statements in AST")
	}

	// Build set of CTE names to exclude
	cteNames := make(map[string]bool)
	for _, cte := range a.Statements[0].Node.CTEMap.Map {
		cteNames[cte.Key] = true
	}

	// Extract tables from entire node (FROM, WHERE, subqueries, CTEs)
	var tables []TableRef
	extractTablesFromNode(a.Statements[0].Node, cteNames, &tables, true)

	return tables, nil
}

// extractTablesFromNode extracts tables from an entire SELECT node including subqueries.
func extractTablesFromNode(n node, cteNames map[string]bool, tables *[]TableRef, isFirst bool) {
	// Extract from CTEs first
	for _, cte := range n.CTEMap.Map {
		extractTablesFromNode(cte.Value.Query.Node, cteNames, tables, false)
	}

	// Set operation node (UNION / UNION ALL / INTERSECT / EXCEPT): walk
	// both sides. The first table on the left side keeps `isFirst=true`;
	// the right side becomes "joins" so the first-from logic stays sane.
	if n.Left != nil {
		extractTablesFromNode(*n.Left, cteNames, tables, isFirst)
	}
	if n.Right != nil {
		extractTablesFromNode(*n.Right, cteNames, tables, false)
	}

	// Extract from FROM clause
	extractTablesFromFrom(n.FromTable, cteNames, tables, isFirst)

	// Extract from WHERE clause (may contain subqueries)
	if n.WhereClause != nil {
		extractTablesFromExpr(*n.WhereClause, cteNames, tables)
	}

	// Extract from HAVING clause
	if n.Having != nil {
		extractTablesFromExpr(*n.Having, cteNames, tables)
	}

	// Extract from QUALIFY clause
	if n.Qualify != nil {
		extractTablesFromExpr(*n.Qualify, cteNames, tables)
	}

	// Extract from SELECT list (scalar subqueries)
	for _, expr := range n.SelectList {
		extractTablesFromExpr(expr, cteNames, tables)
	}
}

// extractTablesFromExpr extracts tables from expressions (handles subqueries).
func extractTablesFromExpr(expr expression, cteNames map[string]bool, tables *[]TableRef) {
	// Handle subquery expressions
	if expr.Subquery != nil {
		extractTablesFromNode(expr.Subquery.Node, cteNames, tables, false)
	}

	// Handle child expression (e.g., left side of IN comparison)
	if expr.Child != nil {
		extractTablesFromExpr(*expr.Child, cteNames, tables)
	}

	// Recurse into children
	for _, child := range expr.Children {
		extractTablesFromExpr(child, cteNames, tables)
	}

	// Handle CASE expressions
	for _, cc := range expr.CaseChecks {
		extractTablesFromExpr(cc.WhenExpr, cteNames, tables)
		extractTablesFromExpr(cc.ThenExpr, cteNames, tables)
	}
}

// extractTablesFromFrom recursively extracts table references from a FROM clause.
func extractTablesFromFrom(ft fromTable, cteNames map[string]bool, tables *[]TableRef, isFirst bool) {
	if ft.TableName != "" {
		// Skip CTEs - we only want physical tables
		if !cteNames[ft.TableName] {
			// Build full table name including schema if present
			fullName := ft.TableName
			if ft.SchemaName != "" {
				fullName = ft.SchemaName + "." + ft.TableName
			}
			ref := TableRef{
				Table:       fullName,
				Alias:       ft.Alias,
				IsFirstFrom: isFirst && len(*tables) == 0,
				IsJoin:      !isFirst || len(*tables) > 0,
			}
			*tables = append(*tables, ref)
		}
	}

	// Process LEFT side of JOIN first (maintains FROM order)
	if ft.Left != nil {
		extractTablesFromFrom(*ft.Left, cteNames, tables, true)
	}

	// Then RIGHT side (these are the JOIN tables)
	if ft.Right != nil {
		extractTablesFromFrom(*ft.Right, cteNames, tables, false)
	}

	// Recurse into sub-select FROM clauses: FROM (SELECT ... FROM upstream) AS sub
	// (Bug 31) The DuckDB AST represents these with type=SUBQUERY and a nested query.
	// Without this recursion, dependencies inside derived tables are silently dropped
	// from the DAG, causing the runner to skip dependent models when upstream changes.
	if ft.Subquery != nil {
		extractTablesFromNode(ft.Subquery.Node, cteNames, tables, isFirst)
	}
}

// GetAllTables returns all table names from a SQL query (for DAG dependency detection).
func GetAllTables(sess *duckdb.Session, sql string) ([]string, error) {
	tables, err := ExtractTables(sess, sql)
	if err != nil {
		return nil, err
	}
	return GetAllTablesFromRefs(tables), nil
}

// GetAllTablesFromAST returns all table names from a pre-fetched AST JSON.
// Use with GetAST to share the AST with ExtractFromAST.
func GetAllTablesFromAST(astJSON string) ([]string, error) {
	tables, err := ExtractTablesFromAST(astJSON)
	if err != nil {
		return nil, err
	}
	return GetAllTablesFromRefs(tables), nil
}

// GetAllTablesFromRefs converts TableRef slice to unique table name strings.
func GetAllTablesFromRefs(tables []TableRef) []string {
	var result []string
	seen := make(map[string]bool)
	for _, t := range tables {
		if !seen[t.Table] {
			result = append(result, t.Table)
			seen[t.Table] = true
		}
	}
	return result
}

// ExtractAll extracts both column lineage and table dependencies in a single query.
// More efficient than calling Extract and GetAllTables separately.
func ExtractAll(sess *duckdb.Session, sql string) ([]ColumnLineage, []string, error) {
	astJSON, err := GetAST(sess, sql)
	if err != nil {
		return nil, nil, err
	}

	colLineage, err := ExtractFromAST(astJSON)
	if err != nil {
		return nil, nil, err
	}

	tableDeps, err := GetAllTablesFromAST(astJSON)
	if err != nil {
		return colLineage, nil, err
	}

	return colLineage, tableDeps, nil
}

// GetCDCTables determines which tables need CDC based on column lineage.
// Tables with AGGREGATION transformations need CDC (new rows affect aggregates).
// Tables with only IDENTITY/FUNCTION are dimension lookups (full scan needed).
func GetCDCTables(lineage []ColumnLineage) map[string]bool {
	cdcTables := make(map[string]bool)

	for _, col := range lineage {
		for _, src := range col.Sources {
			if src.Transformation == TransformAggregation {
				// This table contributes to an aggregation - needs CDC
				cdcTables[src.Table] = true
			}
		}
	}

	return cdcTables
}
