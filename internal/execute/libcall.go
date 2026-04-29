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
	"github.com/ondatra-labs/ondatrasql/internal/script"
)

// LibCall represents a detected lib function call in a SQL model's FROM clause.
type LibCall struct {
	FuncName     string               // lib function name (e.g. "gam_fetch")
	CallIndex    int                  // unique index per call (0, 1, 2...) for dedup
	Lib          *libregistry.LibFunc // registry entry
	ArgNodes     []*duckast.Node      // raw AST arg expressions
	ASTNode      *duckast.Node        // the TABLE_FUNCTION AST node (for identity matching in rewrite)
	TempTable    string               // set after execution, used for AST rewrite
	ScriptResult *script.Result       // set after execution, for Badger claim lifecycle
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
		// Dollar-quoted string ($tag$...$tag$ or $$...$$)
		if out[i] == '$' {
			// Extract the tag: everything from $ to next $ (inclusive)
			tagEnd := i + 1
			for tagEnd < len(out) && out[tagEnd] != '$' {
				c := out[tagEnd]
				// Tags are alphanumeric + underscore only
				if !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_') {
					break
				}
				tagEnd++
			}
			if tagEnd < len(out) && out[tagEnd] == '$' {
				tag := string(out[i : tagEnd+1]) // e.g. "$$" or "$foo$"
				// Blank the opening tag
				for j := i; j <= tagEnd; j++ {
					out[j] = ' '
				}
				i = tagEnd + 1
				// Scan for closing tag
				for i < len(out) {
					if i+len(tag)-1 < len(out) && string(out[i:i+len(tag)]) == tag {
						for j := 0; j < len(tag); j++ {
							out[i+j] = ' '
						}
						i += len(tag)
						break
					}
					out[i] = ' '
					i++
				}
				continue
			}
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

// matchColumns extracts column references for a specific lib call and
// validates against supported_columns if declared. For dynamic-column
// libs, it returns only columns referenced against this lib's table
// alias (input-shape), not the full output projection.
func matchColumns(ast *duckast.AST, lib *libregistry.LibFunc, alias string, singleSource bool) ([]string, error) {
	if ast == nil {
		return nil, nil
	}

	if lib.DynamicColumns {
		// Extract columns referenced against this specific lib call
		cols := extractColumnsForLib(ast, alias, singleSource)
		if len(cols) == 0 {
			// Fallback: use all column names from AST
			cols = extractAllColumnNames(ast)
		}

		// Validate against supported_columns whitelist if declared.
		if len(lib.SupportedColumns) > 0 {
			allowed := make(map[string]bool, len(lib.SupportedColumns))
			for _, c := range lib.SupportedColumns {
				allowed[strings.ToUpper(c)] = true
			}
			for _, name := range cols {
				if name != "" && !allowed[strings.ToUpper(name)] {
					return nil, fmt.Errorf("column %q is not in supported_columns for %s()", name, lib.Name)
				}
			}
		}
		return cols, nil
	}

	selectCols := extractAllColumnNames(ast)

	// Match against declared columns
	colMap := make(map[string]*libregistry.Column, len(lib.Columns))
	for i := range lib.Columns {
		colMap[strings.ToUpper(lib.Columns[i].Name)] = &lib.Columns[i]
	}

	var cols []string
	for _, name := range selectCols {
		upper := strings.ToUpper(name)
		col, ok := colMap[upper]
		if !ok {
			continue
		}
		cols = append(cols, col.Name)
	}

	return cols, nil
}

// normalizeType maps a DuckDB type name to a normalized type representation.
// Returns a map with "type" and optional extra fields (precision, scale, tz, etc.).
func normalizeType(duckdbType string) map[string]any {
	upper := strings.ToUpper(strings.TrimSpace(duckdbType))

	// DECIMAL(p,s) / NUMERIC(p,s)
	if strings.HasPrefix(upper, "DECIMAL") || strings.HasPrefix(upper, "NUMERIC") {
		result := map[string]any{"type": "decimal", "json_schema_type": "number"}
		if idx := strings.Index(upper, "("); idx >= 0 {
			endIdx := strings.Index(upper, ")")
			if endIdx < 0 {
				endIdx = len(upper)
			}
			inner := upper[idx+1 : endIdx]
			parts := strings.Split(inner, ",")
			if len(parts) >= 1 {
				result["precision"] = strings.TrimSpace(parts[0])
			}
			if len(parts) >= 2 {
				result["scale"] = strings.TrimSpace(parts[1])
			}
		} else {
			result["precision"] = "18"
			result["scale"] = "3"
		}
		return result
	}

	// Timestamp variants
	switch upper {
	case "TIMESTAMPTZ", "TIMESTAMP WITH TIME ZONE":
		return map[string]any{"type": "timestamp", "json_schema_type": "string", "tz": true, "precision": "us"}
	case "TIMESTAMP_NS":
		return map[string]any{"type": "timestamp", "json_schema_type": "string", "tz": false, "precision": "ns"}
	case "TIMESTAMP_MS":
		return map[string]any{"type": "timestamp", "json_schema_type": "string", "tz": false, "precision": "ms"}
	case "TIMESTAMP_S":
		return map[string]any{"type": "timestamp", "json_schema_type": "string", "tz": false, "precision": "s"}
	case "TIMESTAMP", "DATETIME", "TIMESTAMP_US":
		return map[string]any{"type": "timestamp", "json_schema_type": "string", "tz": false, "precision": "us"}
	}

	// Simple type mapping
	simpleMap := map[string]string{
		// Integers
		"TINYINT": "integer", "INT1": "integer",
		"SMALLINT": "integer", "INT2": "integer", "INT16": "integer", "SHORT": "integer",
		"INTEGER": "integer", "INT": "integer", "INT4": "integer", "INT32": "integer", "SIGNED": "integer",
		"BIGINT": "integer", "INT8": "integer", "INT64": "integer", "LONG": "integer",
		"HUGEINT": "integer", "INT128": "integer",
		"UTINYINT":  "integer", "UINT8": "integer",
		"USMALLINT": "integer", "UINT16": "integer",
		"UINTEGER":  "integer", "UINT32": "integer",
		"UBIGINT":   "integer", "UINT64": "integer",
		"UHUGEINT":  "integer", "UINT128": "integer",
		// Floats
		"FLOAT": "float", "FLOAT4": "float", "REAL": "float",
		"DOUBLE": "float", "FLOAT8": "float",
		// Boolean
		"BOOLEAN": "boolean", "BOOL": "boolean", "LOGICAL": "boolean",
		// Temporal
		"DATE":                "date",
		"TIME":                "time",
		"TIMETZ":              "time",
		"TIME WITH TIME ZONE": "time",
		"TIME_NS":             "time",
		"INTERVAL":            "interval",
		// Text
		"VARCHAR": "string", "CHAR": "string", "BPCHAR": "string",
		"TEXT": "string", "STRING": "string", "NVARCHAR": "string",
		// Structured
		"JSON": "json",
		"UUID": "uuid",
		"BLOB": "blob", "BYTEA": "blob", "BINARY": "blob", "VARBINARY": "blob",
		"BIT": "bit", "BITSTRING": "bit",
		// Composite (simple fallback — recursive extraction via normalizeTypeFromAST)
		"LIST": "list", "ARRAY": "list",
		"MAP":     "map",
		"STRUCT":  "struct", "ROW": "struct",
		"UNION":   "union",
		"VARIANT": "variant",
		// Bignum / decimal aliases
		"BIGNUM": "decimal", "DEC": "decimal",
		// Integer aliases
		"INTEGRAL": "integer", "OID": "integer",
		// Enum
		"ENUM": "string",
	}

	if mapped, ok := simpleMap[upper]; ok {
		return map[string]any{"type": mapped, "json_schema_type": toJSONSchemaType(mapped)}
	}

	// Unrecognized — default to string
	return map[string]any{"type": "string", "json_schema_type": "string"}
}

// toJSONSchemaType maps a normalized type to its JSON Schema equivalent.
// JSON Schema has: string, number, integer, boolean, array, object, null.
func toJSONSchemaType(normalized string) string {
	switch normalized {
	case "integer":
		return "integer"
	case "float", "decimal":
		return "number"
	case "boolean":
		return "boolean"
	case "json", "list":
		return "array"
	case "map", "struct":
		return "object"
	default:
		return "string"
	}
}

// normalizeTypeFromAST extracts a normalized type from a DuckDB cast_type AST node.
// Handles composite types recursively: LIST element, STRUCT fields, MAP key/value.
func normalizeTypeFromAST(castType *duckast.Node) map[string]any {
	if castType == nil {
		return map[string]any{"type": "string", "json_schema_type": "string"}
	}

	id := castType.String("id")
	typeInfo := castType.Field("type_info")

	switch id {
	case "LIST", "ARRAY":
		result := map[string]any{"type": "list", "json_schema_type": "array"}
		if typeInfo != nil {
			childType := typeInfo.Field("child_type")
			if childType != nil {
				result["element"] = normalizeTypeFromAST(childType)
			}
		}
		return result

	case "STRUCT":
		result := map[string]any{"type": "struct", "json_schema_type": "object"}
		if typeInfo != nil {
			childTypes := typeInfo.FieldList("child_types")
			var fields []any
			for _, ct := range childTypes {
				fieldName := ct.String("first")
				fieldType := ct.Field("second")
				field := normalizeTypeFromAST(fieldType)
				field["name"] = fieldName
				fields = append(fields, field)
			}
			if len(fields) > 0 {
				result["fields"] = fields
			}
		}
		return result

	case "MAP":
		// DuckDB internally represents MAP as LIST(STRUCT(key, value))
		result := map[string]any{"type": "map", "json_schema_type": "object"}
		if typeInfo != nil {
			childType := typeInfo.Field("child_type")
			if childType != nil {
				structInfo := childType.Field("type_info")
				if structInfo != nil {
					childTypes := structInfo.FieldList("child_types")
					for _, ct := range childTypes {
						name := ct.String("first")
						typeNode := ct.Field("second")
						if name == "key" {
							result["key"] = normalizeTypeFromAST(typeNode)
						} else if name == "value" {
							result["value"] = normalizeTypeFromAST(typeNode)
						}
					}
				}
			}
		}
		return result

	case "UNION":
		result := map[string]any{"type": "union", "json_schema_type": "string"}
		if typeInfo != nil {
			childTypes := typeInfo.FieldList("child_types")
			var members []any
			for _, ct := range childTypes {
				memberName := ct.String("first")
				memberType := ct.Field("second")
				member := normalizeTypeFromAST(memberType)
				member["name"] = memberName
				members = append(members, member)
			}
			if len(members) > 0 {
				result["members"] = members
			}
		}
		return result

	case "DECIMAL", "NUMERIC":
		result := map[string]any{"type": "decimal", "json_schema_type": "number"}
		if typeInfo != nil {
			if w := typeInfo.String("width"); w != "" {
				result["precision"] = w
			}
			if s := typeInfo.String("scale"); s != "" {
				result["scale"] = s
			}
		}
		if result["precision"] == nil {
			result["precision"] = "18"
			result["scale"] = "3"
		}
		return result

	case "UNBOUND":
		// User-defined types (JSON, etc.)
		if typeInfo != nil {
			if name := typeInfo.String("name"); name != "" {
				return normalizeType(name)
			}
		}
		return map[string]any{"type": "string", "json_schema_type": "string"}

	default:
		return normalizeType(id)
	}
}

// extractTypedSelectColumns extracts column names and normalized types from the SELECT list.
// Returns []map[string]any where each entry has "name", "type", and optional extra fields.
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
		var name string

		var normalized map[string]any

		if item.Class() == "CAST" {
			// SELECT col::TYPE AS alias — extract type recursively from AST
			castType := item.Field("cast_type")
			normalized = normalizeTypeFromAST(castType)
			child := item.Field("child")
			if child != nil && child.Class() == "COLUMN_REF" {
				names := child.ColumnNames()
				if len(names) > 0 {
					name = names[len(names)-1]
				}
			}
			if alias := item.Alias(); alias != "" {
				name = alias
			}
		} else if item.Class() == "COLUMN_REF" {
			names := item.ColumnNames()
			if len(names) > 0 {
				name = names[len(names)-1]
			}
			normalized = map[string]any{"type": "string"}
		} else {
			if alias := item.Alias(); alias != "" {
				name = alias
			}
			normalized = map[string]any{"type": "string"}
		}

		if name != "" {
			normalized["name"] = name
			result = append(result, normalized)
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

// extractColumnsForLib walks the entire AST and collects column names
// referenced against a specific lib call's table alias. This includes
// SELECT, WHERE, JOIN ON, GROUP BY, ORDER BY, HAVING — the full set
// of input columns the query expects from the lib, not just the output
// projection.
//
// For qualified refs like u.user_id, the first part is matched against
// the alias. For unqualified refs (e.g. user_id), they are included
// only if singleSource is true (the query has exactly one lib source,
// so all unqualified refs belong to it).
func extractColumnsForLib(ast *duckast.AST, alias string, singleSource bool) []string {
	if ast == nil {
		return nil
	}
	seen := make(map[string]bool)
	var names []string

	ast.Walk(func(n *duckast.Node) bool {
		if n.Class() != "COLUMN_REF" {
			return true
		}
		parts := n.ColumnNames()
		if len(parts) == 0 {
			return true
		}

		var colName string
		if len(parts) >= 2 {
			// Qualified: alias.column — match on alias
			if alias != "" && strings.EqualFold(parts[0], alias) {
				colName = parts[1]
			}
		} else if singleSource {
			// Unqualified column, single lib source — belongs to this lib
			colName = parts[0]
		}

		if colName != "" && !seen[colName] {
			seen[colName] = true
			names = append(names, colName)
		}
		return true
	})

	return names
}


// validateStrictLibSchema enforces the strict-schema contract on lib-backed
// models: SQL is the complete schema source. Every output column must be
// explicitly selected, explicitly cast to its final DuckDB type, and
// explicitly aliased. The runtime never infers the output schema from data,
// from the target table, or from regular-table catalog metadata.
//
// Rules — applied to every projection in every SELECT in the model
// (top-level, CTEs, set-op branches, subqueries):
//   - SELECT * is rejected.
//   - Outermost node must be CAST. Bare COLUMN_REF, computed expressions,
//     literals, function calls — all rejected unless wrapped in a cast.
//   - The projection must carry an explicit alias (`AS name`). Implicit
//     names from underlying COLUMN_REFs do not count.
//   - Two projections in the same SELECT cannot share the same output name.
//
// Walking every SELECT_NODE (not only the top-level one) is what closes
// the bypass via CTE / UNION — without that, an inner `SELECT *` could
// hide behind a properly-typed outer projection.
//
// The rules apply uniformly: a column from a regular table joined with a
// lib still needs a cast and alias. The contract is the same whether the
// lib declares its columns statically or dynamically — for any lib-backed
// model, SQL is the schema authority.
//
// Returns nil only when there are no lib calls in the model (pure SQL
// transforms are not subject to this contract).
func validateStrictLibSchema(ast *duckast.AST, libCalls []LibCall) error {
	if ast == nil || len(libCalls) == 0 {
		return nil
	}

	var validationErr error
	ast.Walk(func(n *duckast.Node) bool {
		if validationErr != nil {
			return false // stop walking
		}
		if !n.IsSelectNode() {
			return true
		}
		if err := validateSelectProjections(n); err != nil {
			validationErr = err
			return false
		}
		return true
	})
	return validationErr
}

// validateSelectProjections applies the strict-schema rules to a single
// SELECT_NODE's select list. Used by validateStrictLibSchema for every
// SELECT_NODE in the AST (including those inside CTEs, set-op branches,
// and subqueries).
func validateSelectProjections(selectNode *duckast.Node) error {
	seenNames := make(map[string]bool)
	for _, item := range selectNode.SelectList() {
		// SELECT * is unconditionally rejected.
		if item.Class() == "STAR" {
			return fmt.Errorf(
				"SELECT * is not allowed in a lib-backed model; project each output column with an explicit cast and alias (e.g. col::TYPE AS col)",
			)
		}

		// Outermost node must be CAST.
		if item.Class() != "CAST" {
			name := projectionDisplayName(item)
			return fmt.Errorf(
				"projection %q is not cast; lib-backed models require every output column to be wrapped in an explicit cast (e.g. (%s)::TYPE AS alias)",
				name, name,
			)
		}

		// CAST must carry an explicit alias.
		alias := item.Alias()
		if alias == "" {
			name := projectionDisplayName(item)
			return fmt.Errorf(
				"cast projection %q has no explicit alias; lib-backed models require every output column to be aliased (e.g. %s::TYPE AS alias)",
				name, name,
			)
		}

		// Duplicate alias check — output schema must be unambiguous.
		key := strings.ToLower(alias)
		if seenNames[key] {
			return fmt.Errorf(
				"duplicate output column name %q; each projection must produce a uniquely-named column",
				alias,
			)
		}
		seenNames[key] = true
	}
	return nil
}

// projectionDisplayName returns a short string suitable for naming a
// projection in an error message. CAST → underlying column / "<expression>";
// COLUMN_REF → the column name; everything else → "<expression>".
func projectionDisplayName(item *duckast.Node) string {
	if item == nil {
		return "<expression>"
	}
	switch item.Class() {
	case "CAST":
		child := item.Field("child")
		if child != nil && child.Class() == "COLUMN_REF" {
			names := child.ColumnNames()
			if len(names) > 0 {
				return names[len(names)-1]
			}
		}
		return "<expression>"
	case "COLUMN_REF":
		names := item.ColumnNames()
		if len(names) > 0 {
			return names[len(names)-1]
		}
		return "<expression>"
	default:
		return "<expression>"
	}
}

// allLibsReturnedNoChange reports whether every lib call in this run
// returned 0 rows AND declared `empty_result == "no_change"` (the default).
// Used by tracked materialize to decide whether to preserve the target
// or proceed with delete-on-missing semantics. If any lib returned rows
// or any lib explicitly set `delete_missing`, this is false.
func allLibsReturnedNoChange(libCalls []LibCall) bool {
	if len(libCalls) == 0 {
		return false
	}
	for i := range libCalls {
		sr := libCalls[i].ScriptResult
		if sr == nil {
			return false
		}
		if sr.RowCount > 0 {
			return false
		}
		if sr.EmptyResult != script.EmptyNoChange {
			return false
		}
	}
	return true
}

// colShape is a single column's name + DuckDB type, used for building
// 0-row stub temp tables that need to match the model's declared schema.
type colShape struct {
	name    string
	sqlType string // DuckDB DDL type, e.g. "VARCHAR", "DECIMAL(18,3)", "JSON"
}

// extractColShapeForLib produces the column shape that a 0-row stub for
// this lib call must satisfy, derived entirely from the model's SQL.
//
// The model's SELECT is the schema authority. This walks the projection
// for typed information (explicit casts) and merges with all-AST refs
// (JOIN ON, WHERE, etc) so the stub also covers input-only columns the
// query needs but doesn't project.
//
// Rules:
//   - Projection columns with explicit cast (`col::TYPE` or `col::TYPE AS alias`)
//     get that DuckDB type
//   - Other projection columns (bare COLUMN_REF, computed expressions) get VARCHAR
//   - Input-only refs (in WHERE/JOIN but not projected) get VARCHAR
//   - Result is sorted by column name for deterministic ordering
//
// Returns empty if the SQL doesn't reference any columns belonging to this
// lib (e.g. `SELECT *` against a single lib source — falls back to caller).
func extractColShapeForLib(ast *duckast.AST, alias string, singleSource bool) []colShape {
	if ast == nil {
		return nil
	}

	// Step 1: typed projection — walk SELECT list, capture (name → sqlType)
	// for projections that belong to THIS lib alias. Multi-lib queries must
	// not leak another lib's projection types into this stub. Bare
	// COLUMN_REF and other forms get VARCHAR.
	projTypes := make(map[string]string)
	stmts := ast.Statements()
	if len(stmts) > 0 {
		for _, item := range stmts[0].SelectList() {
			name, sqlType, sourceAlias := projectionNameTypeAndAlias(item)
			if name == "" {
				continue
			}
			// Filter by alias the same way extractColumnsForLib does for refs:
			//   - qualified projection: only keep when its alias matches
			//   - unqualified projection: keep only in single-source mode
			if sourceAlias != "" {
				if alias == "" || !strings.EqualFold(sourceAlias, alias) {
					continue
				}
			} else if !singleSource {
				continue
			}
			projTypes[name] = sqlType
		}
	}

	// Step 2: all refs for this lib (projection + JOIN ON + WHERE + ...)
	refCols := extractColumnsForLib(ast, alias, singleSource)
	if len(refCols) == 0 && len(projTypes) == 0 {
		return nil
	}

	// Step 3: union — refs determine which columns appear, projTypes
	// supplies types where known, VARCHAR otherwise.
	seen := make(map[string]bool)
	var shape []colShape
	for _, name := range refCols {
		if seen[name] {
			continue
		}
		seen[name] = true
		t, ok := projTypes[name]
		if !ok {
			t = "VARCHAR"
		}
		shape = append(shape, colShape{name: name, sqlType: t})
	}
	// Projection-only columns (typed projection but not referenced as input —
	// rare but possible with `SELECT 'literal' AS col FROM lib(...)`).
	// Only valid in single-source mode: in multi-lib queries we cannot
	// attribute an unqualified projection to a specific lib without the
	// risk of leaking another lib's columns into this stub.
	if singleSource {
		for name, t := range projTypes {
			if seen[name] {
				continue
			}
			shape = append(shape, colShape{name: name, sqlType: t})
		}
	}

	sort.Slice(shape, func(i, j int) bool { return shape[i].name < shape[j].name })
	return shape
}

// projectionNameTypeAndAlias extracts (column-name, DuckDB-DDL-type, source-alias)
// from a single SELECT-list AST node. The source-alias is the table alias
// the underlying COLUMN_REF qualifies to (e.g. "a" for `a.id::BIGINT`), or
// "" for unqualified refs and non-COLUMN_REF expressions. Returns "" name
// for unrecognised forms.
//
// CAST nodes carry an `id` field on their cast_type child holding the DuckDB
// type name ("DECIMAL", "VARCHAR", "JSON", etc). Composite types (DECIMAL with
// precision/scale, LIST<X>, STRUCT) are reconstructed best-effort from
// cast_type's `type_info`.
func projectionNameTypeAndAlias(item *duckast.Node) (string, string, string) {
	if item == nil {
		return "", "", ""
	}

	switch item.Class() {
	case "CAST":
		castType := item.Field("cast_type")
		sqlType := sqlTypeFromCastNode(castType)
		var sourceAlias, underlyingName string
		child := item.Field("child")
		if child != nil && child.Class() == "COLUMN_REF" {
			names := child.ColumnNames()
			if len(names) >= 2 {
				sourceAlias = names[0]
				underlyingName = names[len(names)-1]
			} else if len(names) == 1 {
				underlyingName = names[0]
			}
		}
		// Name preference: alias > underlying column name
		if alias := item.Alias(); alias != "" {
			return alias, sqlType, sourceAlias
		}
		if underlyingName != "" {
			return underlyingName, sqlType, sourceAlias
		}
		return "", sqlType, sourceAlias

	case "COLUMN_REF":
		names := item.ColumnNames()
		if len(names) == 0 {
			return "", "VARCHAR", ""
		}
		var sourceAlias string
		if len(names) >= 2 {
			sourceAlias = names[0]
		}
		name := names[len(names)-1]
		if alias := item.Alias(); alias != "" {
			name = alias
		}
		return name, "VARCHAR", sourceAlias

	default:
		// Computed expression, function call, literal — VARCHAR is the safe
		// default; explicit cast in the SELECT recovers precision. We can't
		// attribute these to a specific lib alias.
		if alias := item.Alias(); alias != "" {
			return alias, "VARCHAR", ""
		}
		return "", "VARCHAR", ""
	}
}

// sqlTypeFromCastNode reconstructs a DuckDB DDL type string from a cast_type
// AST node. Returns "VARCHAR" if the node is missing or unrecognised.
func sqlTypeFromCastNode(castType *duckast.Node) string {
	if castType == nil {
		return "VARCHAR"
	}
	id := castType.String("id")
	if id == "" {
		return "VARCHAR"
	}

	// DECIMAL / NUMERIC carry precision + scale in type_info.
	if id == "DECIMAL" || id == "NUMERIC" {
		typeInfo := castType.Field("type_info")
		if typeInfo != nil {
			width := typeInfo.String("width")
			scale := typeInfo.String("scale")
			if width != "" && scale != "" {
				return fmt.Sprintf("DECIMAL(%s,%s)", width, scale)
			}
			if width != "" {
				return fmt.Sprintf("DECIMAL(%s,0)", width)
			}
		}
		return "DECIMAL"
	}

	// UNBOUND is DuckDB's encoding for user-defined types (JSON, UUID,
	// extension types). The actual type name lives on type_info.name.
	if id == "UNBOUND" {
		typeInfo := castType.Field("type_info")
		if typeInfo != nil {
			if name := typeInfo.String("name"); name != "" {
				return name
			}
		}
		return "VARCHAR"
	}

	// Composite + parameterized types — DuckDB needs the full shape
	// (`STRUCT(a INT, b VARCHAR)`, `UNION(...)`, `ENUM(...)`) which we
	// can't reconstruct from the stub-side AST. The stub holds 0 rows;
	// VARCHAR is the safest column type that won't fail CREATE and won't
	// constrain anything (the real data flows through the rewrite path,
	// not the stub).
	switch id {
	case "LIST", "ARRAY":
		return "VARCHAR[]" // simplification: stub doesn't need true element type
	case "STRUCT":
		return "VARCHAR"
	case "MAP":
		return "VARCHAR"
	case "UNION":
		return "VARCHAR"
	case "ENUM":
		return "VARCHAR"
	}

	// Most DuckDB types accept their id verbatim as DDL: numeric (TINYINT,
	// SMALLINT, INTEGER, BIGINT, HUGEINT, UTINYINT..UHUGEINT, FLOAT, DOUBLE,
	// BIGNUM, VARINT), string (VARCHAR), boolean (BOOLEAN), temporal (DATE,
	// TIME, TIMETZ, TIMESTAMP, TIMESTAMPTZ, INTERVAL), binary (BLOB, BIT,
	// UUID), JSON.
	return id
}

// buildLibCallKwargs builds the kwargs map for calling a Starlark lib function.
func buildLibCallKwargs(lib *libregistry.LibFunc, args []any, cols []string, typedCols []any) map[string]any {
	kwargs := make(map[string]any)

	// Args mapped to API.fetch.args names — all as kwargs
	for i, argName := range lib.Args {
		if i < len(args) {
			kwargs[argName] = args[i]
		}
	}

	// Column selections — filtered to only columns belonging to this lib
	if len(cols) > 0 {
		if lib.DynamicColumns && typedCols != nil {
			// Filter typedCols to only include columns in cols
			colSet := make(map[string]bool, len(cols))
			for _, c := range cols {
				colSet[strings.ToUpper(c)] = true
			}
			var filtered []any
			for _, tc := range typedCols {
				m, ok := tc.(map[string]any)
				if !ok {
					continue
				}
				name, _ := m["name"].(string)
				if colSet[strings.ToUpper(name)] {
					filtered = append(filtered, tc)
				}
			}
			if len(filtered) > 0 {
				kwargs["columns"] = filtered
			} else {
				kwargs["columns"] = toInterfaceSlice(cols)
			}
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
		case ch == '$':
			// Skip dollar-quoted string ($tag$...$tag$ or $$...$$)
			tagEnd := i + 1
			for tagEnd < len(sql) && sql[tagEnd] != '$' {
				c := sql[tagEnd]
				if !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_') {
					break
				}
				tagEnd++
			}
			if tagEnd < len(sql) && sql[tagEnd] == '$' {
				tag := sql[i : tagEnd+1]
				i = tagEnd + 1
				for i+len(tag)-1 < len(sql) {
					if sql[i:i+len(tag)] == tag {
						i += len(tag) - 1
						break
					}
					i++
				}
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
		// Always increment occurrence counter even for empty TempTable,
		// so subsequent calls with data target the correct SQL occurrence.
		targetOccurrence := funcOccurrence[c.FuncName]
		funcOccurrence[c.FuncName]++
		if c.TempTable == "" {
			continue
		}
		pattern := strings.ToLower(c.FuncName) + "("

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
