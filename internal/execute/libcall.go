// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package execute

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/ondatra-labs/ondatrasql/internal/duckast"
	"github.com/ondatra-labs/ondatrasql/internal/libregistry"
)

// LibCall represents a detected lib function call in a SQL model's FROM clause.
type LibCall struct {
	FuncName  string           // lib function name (e.g. "gam_fetch")
	CallIndex int              // unique index per call (0, 1, 2...) for dedup
	Lib       *libregistry.LibFunc // registry entry
	ArgNodes  []*duckast.Node  // raw AST arg expressions
	ASTNode   *duckast.Node    // the TABLE_FUNCTION AST node (for identity matching in rewrite)
	TempTable string           // set after execution, used for AST rewrite
}

// detectLibCalls walks the AST and finds TABLE_FUNCTION nodes that match
// registered lib functions. Returns a list of calls to execute.
func detectLibCalls(ast *duckast.AST, reg *libregistry.Registry) []LibCall {
	if ast == nil || reg == nil || reg.Empty() {
		return nil
	}

	var calls []LibCall
	callIndex := 0
	ast.Walk(func(n *duckast.Node) bool {
		if !n.IsTableFunction() {
			return true
		}
		name := n.TableFunctionName()
		if name == "" {
			return true
		}
		lf := reg.Get(name)
		if lf == nil || lf.IsSink {
			return true // not a lib function or is a sink
		}
		calls = append(calls, LibCall{
			FuncName:  name,
			CallIndex: callIndex,
			Lib:       lf,
			ArgNodes:  n.TableFunctionArgs(),
			ASTNode:   n,
		})
		callIndex++
		return true
	})

	return calls
}

// detectLibCallsFromSQL detects lib function calls by scanning raw SQL text.
// Used as fallback when AST-based detection fails (DuckDB expands registered
// macros in json_serialize_sql, hiding TABLE_FUNCTION nodes from the AST).
//
// NOTE: This string-based scan may produce false positives if a lib function
// name appears inside a string literal (e.g. 'call gam_fetch(...)'). This is
// acceptable because AST-based detection is the primary path; this fallback
// only activates when the AST is unavailable, and false positives result in
// a benign "lib function returned no data" rather than incorrect results.
func detectLibCallsFromSQL(sql string, reg *libregistry.Registry) []LibCall {
	if reg == nil || reg.Empty() {
		return nil
	}

	// Strip string literals and comments to avoid false positives.
	// Replace content inside '...' and "..." with spaces (preserving offsets
	// so error messages stay meaningful, though we don't use offsets here).
	cleaned := stripStringsAndComments(sql)
	cleanedLower := strings.ToLower(cleaned)

	var calls []LibCall
	callIndex := 0
	for _, lf := range reg.TableFuncs() {
		pattern := strings.ToLower(lf.Name) + "("
		search := cleanedLower
		for {
			idx := strings.Index(search, pattern)
			if idx < 0 {
				break
			}
			calls = append(calls, LibCall{
				FuncName:  lf.Name,
				CallIndex: callIndex,
				Lib:       lf,
			})
			callIndex++
			search = search[idx+len(pattern):]
		}
	}
	return calls
}

// stripStringsAndComments replaces the content of SQL string literals
// and comments with spaces, so substring searches don't match inside them.
func stripStringsAndComments(sql string) string {
	out := []byte(sql)
	i := 0
	for i < len(out) {
		// Line comment
		if i+1 < len(out) && out[i] == '-' && out[i+1] == '-' {
			for i < len(out) && out[i] != '\n' {
				out[i] = ' '
				i++
			}
			continue
		}
		// Block comment
		if i+1 < len(out) && out[i] == '/' && out[i+1] == '*' {
			out[i] = ' '
			out[i+1] = ' '
			i += 2
			for i < len(out) {
				if i+1 < len(out) && out[i] == '*' && out[i+1] == '/' {
					out[i] = ' '
					out[i+1] = ' '
					i += 2
					break
				}
				out[i] = ' '
				i++
			}
			continue
		}
		// String literal
		if out[i] == '\'' || out[i] == '"' {
			quote := out[i]
			i++ // skip opening quote
			for i < len(out) {
				if out[i] == quote {
					if i+1 < len(out) && out[i+1] == quote {
						out[i] = ' '
						out[i+1] = ' '
						i += 2 // escaped quote
					} else {
						i++ // closing quote
						break
					}
				} else {
					out[i] = ' '
					i++
				}
			}
			continue
		}
		i++
	}
	return string(out)
}

// evaluateArgs evaluates each argument expression from a TABLE_FUNCTION AST node
// by serializing it to SQL and executing SELECT <expr>. This handles string literals,
// glob('*.pdf'), getvariable('x'), env-expanded '${VAR}', and any DuckDB expression.
func (r *Runner) evaluateArgs(call *LibCall) ([]any, error) {
	args := make([]any, len(call.ArgNodes))
	for i, argNode := range call.ArgNodes {
		val, err := r.evaluateSingleArg(argNode)
		if err != nil {
			return nil, fmt.Errorf("arg %d of %s: %w", i, call.FuncName, err)
		}
		args[i] = val
	}
	return args, nil
}

// evaluateSingleArg serializes an AST expression node back to SQL and evaluates it.
func (r *Runner) evaluateSingleArg(node *duckast.Node) (any, error) {
	if node == nil || node.IsNil() {
		return nil, nil
	}

	// For simple string constants, extract directly from AST (avoid round-trip)
	if node.Class() == "CONSTANT" {
		raw := node.Raw()
		if valMap, ok := raw["value"].(map[string]any); ok {
			if v, exists := valMap["value"]; exists {
				return v, nil
			}
		}
	}

	// For complex expressions (glob, getvariable, etc.), serialize and evaluate via DuckDB
	// Wrap the expression in a minimal SELECT to get the AST shape json_deserialize_sql expects
	exprAST := buildSelectExprAST(node)
	b, err := json.Marshal(exprAST)
	if err != nil {
		return nil, fmt.Errorf("serialize arg: %w", err)
	}
	serialized := string(b)

	sql, err := r.deserializeAST(serialized)
	if err != nil {
		return nil, fmt.Errorf("deserialize arg: %w", err)
	}

	// sql is "SELECT <expr>" — execute it
	result, err := r.sess.QueryValue(sql)
	if err != nil {
		return nil, fmt.Errorf("evaluate: %w", err)
	}
	return result, nil
}

// buildSelectExprAST wraps an expression node in a minimal SELECT AST
// so it can be deserialized via json_deserialize_sql.
func buildSelectExprAST(exprNode *duckast.Node) map[string]any {
	return map[string]any{
		"error": false,
		"statements": []any{
			map[string]any{
				"node": map[string]any{
					"type":      "SELECT_NODE",
					"modifiers": []any{},
					"cte_map":   map[string]any{"map": []any{}},
					"select_list": []any{
						exprNode.Raw(),
					},
					"from_table":          nil,
					"where_clause":        nil,
					"group_expressions":   []any{},
					"group_sets":          []any{},
					"aggregate_handling":  "STANDARD_HANDLING",
					"having":             nil,
					"sample":             nil,
					"qualify":            nil,
				},
				"named_param_map": []any{},
			},
		},
	}
}

// matchColumns extracts all column references from the query (SELECT, WHERE,
// JOIN ON, GROUP BY, ORDER BY, HAVING) and matches them against the lib
// function's TABLE.columns. For category-based libs (GAM), splits into
// dimensions and metrics. For dynamic_columns, passes all through.
func matchColumns(ast *duckast.AST, lib *libregistry.LibFunc) (dims, mets, cols []string, err error) {
	if ast == nil {
		return nil, nil, nil, nil
	}

	// Extract column names from entire query (not just SELECT)
	selectCols := extractAllColumnNames(ast)
	if len(selectCols) == 0 {
		return nil, nil, nil, nil
	}

	// Dynamic columns: pass all through with type info from AST
	if lib.DynamicColumns {
		return nil, nil, selectCols, nil
	}

	// Build lookup from TABLE.columns
	colMap := make(map[string]*libregistry.Column, len(lib.Columns))
	for i := range lib.Columns {
		colMap[strings.ToUpper(lib.Columns[i].Name)] = &lib.Columns[i]
	}

	for _, name := range selectCols {
		upper := strings.ToUpper(name)
		col, ok := colMap[upper]
		if !ok {
			// Column not in TABLE.columns — could be from a JOINed table, skip
			continue
		}
		cols = append(cols, col.Name)
		switch col.Category {
		case "dimension":
			dims = append(dims, col.Name)
		case "metric":
			mets = append(mets, col.Name)
		}
	}

	return dims, mets, cols, nil
}

// duckDBToJSONSchemaType maps DuckDB type names to JSON Schema types.
// Used to pass schema-ready type info to fetch() so blueprints don't
// need their own type mapping.
var duckDBToJSONSchemaType = map[string]string{
	"DECIMAL":  "number",
	"DOUBLE":   "number",
	"FLOAT":    "number",
	"REAL":     "number",
	"INTEGER":  "integer",
	"BIGINT":   "integer",
	"SMALLINT": "integer",
	"TINYINT":  "integer",
	"HUGEINT":  "integer",
	"BOOLEAN":  "boolean",
	"JSON":     "array",
}

// extractTypedSelectColumns extracts column names and JSON Schema types from the SELECT list.
// Returns []map[string]any where each entry has "name" and "type" keys.
// Types are JSON Schema types (string, number, integer, boolean), not DuckDB types.
func extractTypedSelectColumns(ast *duckast.AST) []any {
	if ast == nil {
		return nil
	}
	stmts := ast.Statements()
	if len(stmts) == 0 {
		return nil
	}
	// Statements() already unwraps the "node" field
	selectNode := stmts[0]
	selectList := selectNode.SelectList()
	if len(selectList) == 0 {
		return nil
	}

	var result []any
	for _, item := range selectList {
		var name, colType string

		if item.Class() == "CAST" {
			// SELECT col::TYPE AS alias
			castType := item.Field("cast_type")
			if castType != nil {
				colType = castType.String("id")
				// User-defined types (JSON, etc.) have id=UNBOUND with name in type_info
				if colType == "UNBOUND" {
					if typeInfo := castType.Field("type_info"); typeInfo != nil {
						if name := typeInfo.String("name"); name != "" {
							colType = name
						}
					}
				}
			}
			child := item.Field("child")
			if child != nil && child.Class() == "COLUMN_REF" {
				names := child.ColumnNames()
				if len(names) > 0 {
					name = names[len(names)-1]
				}
			}
			// Use alias if present (SELECT total::DECIMAL AS amount)
			if alias := item.Alias(); alias != "" {
				name = alias
			}
		} else if item.Class() == "COLUMN_REF" {
			names := item.ColumnNames()
			if len(names) > 0 {
				name = names[len(names)-1]
			}
			colType = "VARCHAR"
		} else {
			// Expression (CASE, function, etc.) — use alias
			if alias := item.Alias(); alias != "" {
				name = alias
			}
			colType = "VARCHAR"
		}

		if name != "" {
			// Map DuckDB type to JSON Schema type
			jsonType := "string"
			if mapped, ok := duckDBToJSONSchemaType[colType]; ok {
				jsonType = mapped
			}
			result = append(result, map[string]any{"name": name, "type": jsonType})
		}
	}
	return result
}

// extractAllColumnNames walks the entire AST and collects all COLUMN_REF names.
// This captures columns used in SELECT, WHERE, JOIN ON, GROUP BY, ORDER BY,
// HAVING, and QUALIFY -- not just the SELECT list.
func extractAllColumnNames(ast *duckast.AST) []string {
	seen := make(map[string]bool)
	var names []string

	ast.Walk(func(n *duckast.Node) bool {
		if n.Class() == "COLUMN_REF" {
			colNames := n.ColumnNames()
			if len(colNames) > 0 {
				name := colNames[len(colNames)-1]
				if !seen[name] {
					seen[name] = true
					names = append(names, name)
				}
			}
		}
		return true
	})

	return names
}

// extractSelectColumnNames pulls column reference names from the SELECT list.
// Kept for backwards compatibility but matchColumns now uses extractAllColumnNames.
func extractSelectColumnNames(ast *duckast.AST) []string {
	stmts := ast.Statements()
	if len(stmts) == 0 {
		return nil
	}

	var names []string
	for _, expr := range stmts[0].SelectList() {
		// Direct column reference: SELECT col_name
		if expr.Class() == "COLUMN_REF" {
			colNames := expr.ColumnNames()
			if len(colNames) > 0 {
				names = append(names, colNames[len(colNames)-1]) // last part is the column name
			}
			continue
		}
		// Cast expression: SELECT col_name::TYPE — unwrap to find the column ref
		// DuckDB represents :: casts as class=CAST with a child column ref
		if expr.Class() == "CAST" || (expr.Class() == "OPERATOR" && expr.FunctionName() == "cast") {
			child := expr.Field("child")
			if child.IsNil() {
				// Try children array (older DuckDB versions)
				children := expr.Children()
				if len(children) > 0 {
					child = children[0]
				}
			}
			if !child.IsNil() && child.Class() == "COLUMN_REF" {
				colNames := child.ColumnNames()
				if len(colNames) > 0 {
					names = append(names, colNames[len(colNames)-1])
				}
			}
		}
	}
	return names
}

// buildLibCallKwargs builds the kwargs map for calling a Starlark lib function.
func buildLibCallKwargs(lib *libregistry.LibFunc, args []any, dims, mets, cols []string, typedCols []any) map[string]any {
	kwargs := make(map[string]any)

	// Positional args mapped to TABLE.args names
	for i, argName := range lib.Args {
		if i < len(args) {
			kwargs[argName] = args[i]
		}
	}

	// Column selections (convert []string to []interface{} for goToStarlark)
	if len(dims) > 0 {
		kwargs["dimensions"] = toInterfaceSlice(dims)
	}
	if len(mets) > 0 {
		kwargs["metrics"] = toInterfaceSlice(mets)
	}
	if len(cols) > 0 {
		// For dynamic_columns, pass typed column info if available
		if lib.DynamicColumns && typedCols != nil {
			kwargs["columns"] = typedCols
		} else {
			kwargs["columns"] = toInterfaceSlice(cols)
		}
	}

	return kwargs
}

// toInterfaceSlice converts []string to []interface{} for goToStarlark compatibility.
func toInterfaceSlice(ss []string) []interface{} {
	result := make([]interface{}, len(ss))
	for i, s := range ss {
		result[i] = s
	}
	return result
}

// rewriteLibCalls replaces TABLE_FUNCTION nodes with BASE_TABLE references
// to the temp tables created by lib function execution.
// Uses AST node identity (pointer) to match each call uniquely, avoiding
// collisions when the same lib function is used multiple times in one query.
func rewriteLibCalls(ast *duckast.AST, calls []LibCall) {
	// Build lookup: AST node pointer → temp table name
	nodeToTemp := make(map[*duckast.Node]string, len(calls))
	for i := range calls {
		if calls[i].ASTNode != nil {
			nodeToTemp[calls[i].ASTNode] = calls[i].TempTable
		}
	}

	ast.ReplaceTableFunctions(
		func(n *duckast.Node) bool {
			_, ok := nodeToTemp[n]
			return ok
		},
		func(n *duckast.Node) map[string]any {
			return map[string]any{
				"type":              "BASE_TABLE",
				"alias":            n.Alias(),
				"sample":           nil,
				"query_location":   0,
				"catalog_name":     "",
				"schema_name":      "",
				"table_name":       nodeToTemp[n],
				"column_name_alias": []any{},
			}
		},
	)
}

// findMatchingParen finds the position after the closing paren, starting from
// pos (which should be just after the opening paren). Respects:
//   - single-quoted strings ('...' with '' escapes)
//   - double-quoted identifiers ("...")
//   - line comments (-- ...)
//   - block comments (/* ... */)
func findMatchingParen(sql string, pos int) int {
	depth := 1
	i := pos
	for i < len(sql) && depth > 0 {
		ch := sql[i]
		switch {
		case ch == '\'':
			// Skip single-quoted string (handles '' escapes)
			i++
			for i < len(sql) {
				if sql[i] == '\'' {
					if i+1 < len(sql) && sql[i+1] == '\'' {
						i += 2
						continue
					}
					break
				}
				i++
			}
		case ch == '"':
			// Skip double-quoted identifier
			i++
			for i < len(sql) {
				if sql[i] == '"' {
					break
				}
				i++
			}
		case ch == '-' && i+1 < len(sql) && sql[i+1] == '-':
			// Skip line comment
			i += 2
			for i < len(sql) && sql[i] != '\n' {
				i++
			}
		case ch == '/' && i+1 < len(sql) && sql[i+1] == '*':
			// Skip block comment
			i += 2
			for i+1 < len(sql) {
				if sql[i] == '*' && sql[i+1] == '/' {
					i++
					break
				}
				i++
			}
		case ch == '(':
			depth++
		case ch == ')':
			depth--
		}
		i++
	}
	return i
}

// rewriteLibCallsByString replaces lib function calls in SQL using string
// matching. Used for API dict libs where macros hide the TABLE_FUNCTION
// node from the AST. Searches stripped SQL (no strings/comments) to find
// correct occurrence positions, then applies rewrites to the original SQL.
func rewriteLibCallsByString(execSQL string, calls []LibCall) string {
	// Strip strings and comments for accurate occurrence matching.
	// Positions in stripped SQL correspond to positions in original SQL
	// because stripStringsAndComments replaces content with spaces.
	stripped := strings.ToLower(stripStringsAndComments(execSQL))

	type rewriteEntry struct {
		start, end int
		tempTable  string
	}
	var rewrites []rewriteEntry

	funcOccurrence := make(map[string]int)
	for _, c := range calls {
		if c.TempTable == "" {
			continue
		}
		pattern := strings.ToLower(c.FuncName) + "("
		targetOccurrence := funcOccurrence[c.FuncName]
		funcOccurrence[c.FuncName]++

		searchFrom := 0
		occurrence := 0
		for searchFrom < len(stripped) {
			idx := strings.Index(stripped[searchFrom:], pattern)
			if idx < 0 {
				break
			}
			absIdx := searchFrom + idx
			if occurrence == targetOccurrence {
				// Find matching close paren in original SQL,
				// skipping parentheses inside string literals.
				parenStart := absIdx + len(pattern)
				end := findMatchingParen(execSQL, parenStart)
				rewrites = append(rewrites, rewriteEntry{absIdx, end, c.TempTable})
				break
			}
			occurrence++
			searchFrom = absIdx + len(pattern)
		}
	}

	// Apply in reverse order to preserve offsets
	sort.Slice(rewrites, func(i, j int) bool {
		return rewrites[i].start > rewrites[j].start
	})
	for _, rw := range rewrites {
		execSQL = execSQL[:rw.start] + rw.tempTable + execSQL[rw.end:]
	}
	return execSQL
}
