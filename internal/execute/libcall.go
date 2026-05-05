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
	"github.com/ondatra-labs/ondatrasql/internal/libcall"
	"github.com/ondatra-labs/ondatrasql/internal/libregistry"
	"github.com/ondatra-labs/ondatrasql/internal/script"
)

// LibCall is a detected lib function call augmented with runtime state.
//
// The static-analysis view (FuncName, CallIndex, Lib, ArgNodes, ASTNode)
// lives in libcall.Call and is shared with the validate/describe paths.
// Runtime-only fields (TempTable, ScriptResult) live here because they
// are populated during materialize/state-store claim lifecycle.
type LibCall struct {
	libcall.Call
	TempTable    string         // set after execution, used for AST rewrite
	ScriptResult *script.Result // set after execution, for state-store claim lifecycle
}

// detectLibCalls walks the AST for TABLE_FUNCTION nodes matching registered
// lib functions and returns runtime-augmentable LibCall values. Thin
// wrapper around libcall.Detect — the actual AST walk lives in the
// libcall package so it can be reused by validate/describe without
// pulling in the runtime.
func detectLibCalls(ast *duckast.AST, reg *libregistry.Registry) []LibCall {
	calls := libcall.Detect(ast, reg)
	if len(calls) == 0 {
		return nil
	}
	out := make([]LibCall, len(calls))
	for i, c := range calls {
		out[i] = LibCall{Call: c}
	}
	return out
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
//
// This fallback is bound to runtime: validate/describe paths must use
// libcall.Detect or libcall.DetectInSQL, which never fall back to strings.
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
			calls = append(calls, LibCall{Call: libcall.Call{
				FuncName:  lf.Name,
				CallIndex: callIndex,
				Lib:       lf,
			}})
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

// canonicalDuckType maps a DuckDB type name (and its many aliases) to its
// canonical DuckDB-native syntax. The canonical name is what surfaces in
// the column dict's `type` field for primitive types; blueprints can pass
// it back into DuckDB (json_transform, CAST, etc.) without translation.
//
// Information-preserving: integer width (TINYINT/SMALLINT/INTEGER/BIGINT/
// HUGEINT and unsigned variants), float precision (FLOAT vs DOUBLE), and
// timestamp variants are kept distinct. The previous design collapsed all
// of these to "integer" / "float" / "timestamp" + side-channel fields,
// losing width/precision in the column dict.
func canonicalDuckType(duckdbType string) string {
	upper := strings.ToUpper(strings.TrimSpace(duckdbType))

	// DECIMAL(p,s) / NUMERIC(p,s) — DECIMAL is canonical, precision/scale stay in the syntax
	if strings.HasPrefix(upper, "DECIMAL") || strings.HasPrefix(upper, "NUMERIC") {
		if idx := strings.Index(upper, "("); idx >= 0 {
			endIdx := strings.Index(upper, ")")
			if endIdx < 0 {
				endIdx = len(upper)
			}
			inner := upper[idx+1 : endIdx]
			parts := strings.Split(inner, ",")
			precision := strings.TrimSpace(parts[0])
			if precision == "" {
				return "DECIMAL(18,3)"
			}
			if len(parts) >= 2 {
				return "DECIMAL(" + precision + "," + strings.TrimSpace(parts[1]) + ")"
			}
			return "DECIMAL(" + precision + ",0)"
		}
		// Bare DECIMAL — DuckDB defaults; expose as DECIMAL(18,3) explicitly.
		return "DECIMAL(18,3)"
	}

	// Aliases → canonical DuckDB syntax. Width/precision distinctions are
	// preserved (no collapse to "integer" / "float").
	canonical := map[string]string{
		// Signed integers
		"TINYINT": "TINYINT", "INT1": "TINYINT",
		"SMALLINT": "SMALLINT", "INT2": "SMALLINT", "INT16": "SMALLINT", "SHORT": "SMALLINT",
		"INTEGER": "INTEGER", "INT": "INTEGER", "INT4": "INTEGER", "INT32": "INTEGER", "SIGNED": "INTEGER",
		"BIGINT": "BIGINT", "INT8": "BIGINT", "INT64": "BIGINT", "LONG": "BIGINT",
		"HUGEINT": "HUGEINT", "INT128": "HUGEINT",
		// Unsigned integers
		"UTINYINT": "UTINYINT", "UINT8": "UTINYINT",
		"USMALLINT": "USMALLINT", "UINT16": "USMALLINT",
		"UINTEGER": "UINTEGER", "UINT32": "UINTEGER",
		"UBIGINT": "UBIGINT", "UINT64": "UBIGINT",
		"UHUGEINT": "UHUGEINT", "UINT128": "UHUGEINT",
		// Floats
		"FLOAT": "FLOAT", "FLOAT4": "FLOAT", "REAL": "FLOAT",
		"DOUBLE": "DOUBLE", "FLOAT8": "DOUBLE",
		// Boolean
		"BOOLEAN": "BOOLEAN", "BOOL": "BOOLEAN", "LOGICAL": "BOOLEAN",
		// Temporal — variants kept distinct (DuckDB-native names)
		"DATE":                            "DATE",
		"TIME":                            "TIME",
		"TIMETZ":                          "TIMETZ",
		"TIME WITH TIME ZONE":             "TIMETZ",
		"TIME_NS":                         "TIME_NS",
		"TIMESTAMP":                       "TIMESTAMP",
		"DATETIME":                        "TIMESTAMP",
		"TIMESTAMP_US":                    "TIMESTAMP",
		"TIMESTAMPTZ":                     "TIMESTAMPTZ",
		"TIMESTAMP WITH TIME ZONE":        "TIMESTAMPTZ",
		"TIMESTAMP_NS":                    "TIMESTAMP_NS",
		"TIMESTAMP_MS":                    "TIMESTAMP_MS",
		"TIMESTAMP_S":                     "TIMESTAMP_S",
		"INTERVAL":                        "INTERVAL",
		// Text
		"VARCHAR": "VARCHAR", "CHAR": "VARCHAR", "BPCHAR": "VARCHAR",
		"TEXT": "VARCHAR", "STRING": "VARCHAR", "NVARCHAR": "VARCHAR",
		// Other primitives
		"JSON": "JSON",
		"UUID": "UUID",
		"BLOB": "BLOB", "BYTEA": "BLOB", "BINARY": "BLOB", "VARBINARY": "BLOB",
		"BIT": "BIT", "BITSTRING": "BIT",
		"ENUM": "VARCHAR", // ENUM materialized values are strings
	}

	if c, ok := canonical[upper]; ok {
		return c
	}

	// Unrecognized → VARCHAR (catch-all primitive in DuckDB).
	return "VARCHAR"
}

// normalizeType maps a DuckDB type name to a DuckDB-native type value
// suitable for a column dict's `type` field. For primitives this is just
// the canonical type name as a string. Composite types (LIST, STRUCT,
// MAP, UNION) reach this path only when extracted by name without
// structure info — return the bare keyword string and let
// normalizeTypeFromAST handle the structured variant.
func normalizeType(duckdbType string) any {
	upper := strings.ToUpper(strings.TrimSpace(duckdbType))
	switch upper {
	case "LIST", "ARRAY":
		return "LIST"
	case "MAP":
		return "MAP"
	case "STRUCT", "ROW":
		return "STRUCT"
	case "UNION":
		return "UNION"
	case "VARIANT":
		return "VARIANT"
	}
	return canonicalDuckType(duckdbType)
}

// normalizeTypeFromAST extracts a DuckDB-native type from a cast_type AST
// node. The shape mirrors DuckDB's `json_structure` output:
//
//	primitive  → DuckDB-syntax string ("VARCHAR", "BIGINT", "DECIMAL(18,3)", "TIMESTAMPTZ")
//	LIST       → []any with one element: [<inner type>]
//	STRUCT     → map[string]any with field name → field type
//	MAP/UNION  → DuckDB-syntax string (e.g. "MAP(VARCHAR, BIGINT)", "UNION(name VARCHAR, ...)")
//
// Composite types nest naturally — LIST of STRUCT becomes
// `[{"f1": "VARCHAR", "f2": ["BIGINT"]}]`.
func normalizeTypeFromAST(castType *duckast.Node) any {
	if castType == nil {
		return "VARCHAR"
	}

	id := castType.String("id")
	typeInfo := castType.Field("type_info")

	switch id {
	case "LIST", "ARRAY":
		if typeInfo != nil {
			childType := typeInfo.Field("child_type")
			if childType != nil {
				return []any{normalizeTypeFromAST(childType)}
			}
		}
		return []any{"VARCHAR"}

	case "STRUCT":
		fields := map[string]any{}
		if typeInfo != nil {
			for _, ct := range typeInfo.FieldList("child_types") {
				fieldName := ct.String("first")
				fieldType := ct.Field("second")
				if fieldName != "" {
					fields[fieldName] = normalizeTypeFromAST(fieldType)
				}
			}
		}
		return fields

	case "MAP":
		// DuckDB stores MAP as LIST(STRUCT(key, value)). Reconstruct the
		// MAP(K, V) syntax string from the wrapped struct.
		key := "VARCHAR"
		value := "VARCHAR"
		if typeInfo != nil {
			if childType := typeInfo.Field("child_type"); childType != nil {
				if structInfo := childType.Field("type_info"); structInfo != nil {
					for _, ct := range structInfo.FieldList("child_types") {
						name := ct.String("first")
						typeNode := ct.Field("second")
						switch name {
						case "key":
							key = duckTypeToString(normalizeTypeFromAST(typeNode))
						case "value":
							value = duckTypeToString(normalizeTypeFromAST(typeNode))
						}
					}
				}
			}
		}
		return "MAP(" + key + ", " + value + ")"

	case "UNION":
		var members []string
		if typeInfo != nil {
			for _, ct := range typeInfo.FieldList("child_types") {
				memberName := ct.String("first")
				memberType := ct.Field("second")
				memberStr := duckTypeToString(normalizeTypeFromAST(memberType))
				if memberName != "" {
					members = append(members, memberName+" "+memberStr)
				} else {
					members = append(members, memberStr)
				}
			}
		}
		return "UNION(" + strings.Join(members, ", ") + ")"

	case "DECIMAL", "NUMERIC":
		precision := "18"
		scale := "3"
		if typeInfo != nil {
			if w := typeInfo.String("width"); w != "" {
				precision = w
			}
			if s := typeInfo.String("scale"); s != "" {
				scale = s
			}
		}
		return "DECIMAL(" + precision + "," + scale + ")"

	case "UNBOUND":
		// User-defined types (JSON, etc.) — name lives in type_info.
		if typeInfo != nil {
			if name := typeInfo.String("name"); name != "" {
				return normalizeType(name)
			}
		}
		return "VARCHAR"

	default:
		return normalizeType(id)
	}
}

// duckTypeToString flattens a possibly-composite type back to a single
// DuckDB-syntax string. Used to inline LIST/STRUCT into MAP/UNION's
// string syntax (which doesn't have a dict/list shape of its own).
func duckTypeToString(t any) string {
	switch v := t.(type) {
	case string:
		return v
	case []any:
		if len(v) == 0 {
			return "LIST(VARCHAR)"
		}
		return "LIST(" + duckTypeToString(v[0]) + ")"
	case map[string]any:
		// STRUCT — sort keys for deterministic output.
		keys := make([]string, 0, len(v))
		for k := range v {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		parts := make([]string, 0, len(keys))
		for _, k := range keys {
			parts = append(parts, k+" "+duckTypeToString(v[k]))
		}
		return "STRUCT(" + strings.Join(parts, ", ") + ")"
	}
	return "VARCHAR"
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
		var typeValue any

		if item.Class() == "CAST" {
			// `SELECT col::TYPE AS alias` — type comes from the cast,
			// `name` comes from the cast's child COLUMN_REF (the API
			// field name the blueprint queries by and uses as row key).
			// The SQL alias is the materialized column name in DuckLake;
			// it is intentionally NOT mirrored here so that blueprints
			// can rename columns at projection time without changing
			// what they fetch.
			castType := item.Field("cast_type")
			typeValue = normalizeTypeFromAST(castType)
			child := item.Field("child")
			if child != nil && child.Class() == "COLUMN_REF" {
				names := child.ColumnNames()
				if len(names) > 0 {
					name = names[len(names)-1]
				}
			}
		} else if item.Class() == "COLUMN_REF" {
			names := item.ColumnNames()
			if len(names) > 0 {
				name = names[len(names)-1]
			}
			typeValue = "VARCHAR"
		} else {
			if alias := item.Alias(); alias != "" {
				name = alias
			}
			typeValue = "VARCHAR"
		}

		if name != "" {
			result = append(result, map[string]any{
				"name": name,
				"type": typeValue,
			})
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


// validateStrictLibSchema enforces the strict-schema contract on `@fetch`
// models: SQL is the complete schema source. Every output column must be
// explicitly selected, explicitly cast to its final DuckDB type, and
// explicitly aliased. The runtime never infers the output schema from
// data, from the target table, or from regular-table catalog metadata.
//
// Top-level rules (the `@fetch` model is exactly one SELECT against one
// lib call):
//   - FROM must be a single TABLE_FUNCTION (no JOIN, BASE_TABLE, or SUBQUERY)
//   - No WHERE, GROUP BY, DISTINCT, ORDER BY
//   - No top-level UNION/INTERSECT/EXCEPT
//
// Per-SELECT rules — applied to every SELECT_NODE in the AST (top-level,
// CTEs, set-op branches, subqueries) so an inner `SELECT *` can't hide
// behind a properly-typed outer projection:
//   - SELECT * is rejected
//   - Outermost node of every projection must be CAST
//   - CAST argument must be a bare or qualified COLUMN_REF
//   - Every projection must carry an explicit `AS alias`
//   - Two projections in the same SELECT cannot share an alias
//
// The caller (runner.go) gates this on `model.Fetch` — the relationship
// rule above ensures `@fetch` implies at least one lib call, so the
// validator never runs without a lib in FROM.
func validateStrictLibSchema(ast *duckast.AST) error {
	if ast == nil {
		return nil
	}

	// Top-level FROM-clause and modifier rules — only meaningful at the
	// statement root. Inner SELECTs (CTEs, subqueries, set-op branches)
	// are caught by the per-projection walk below.
	stmts := ast.Statements()
	if len(stmts) > 0 {
		root := stmts[0]
		if err := validateFetchTopLevel(root); err != nil {
			return err
		}
	}

	var validationErr error
	ast.Walk(func(n *duckast.Node) bool {
		if validationErr != nil {
			return false // stop walking
		}
		if !n.IsSelectNode() {
			return true
		}
		if err := validateSelectProjections(n, true /* fetchMode */); err != nil {
			validationErr = err
			return false
		}
		return true
	})
	return validationErr
}

// validateFetchTopLevel enforces the @fetch rules that only apply to the
// root statement: from_table must be a single TABLE_FUNCTION (no JOIN, no
// regular table, no subquery), no WHERE, no GROUP BY, no DISTINCT, no
// ORDER BY. Inner CTEs/subqueries are unreachable under these rules
// because the from_table is constrained to TABLE_FUNCTION.
func validateFetchTopLevel(root *duckast.Node) error {
	if root.IsNil() {
		return nil
	}
	if root.IsSetOpNode() {
		return fmt.Errorf(
			"@fetch models cannot use UNION/INTERSECT/EXCEPT — a @fetch model is exactly one SELECT against one lib call",
		)
	}
	if !root.IsSelectNode() {
		return nil
	}

	// FROM clause: must be exactly one TABLE_FUNCTION.
	from := root.FromTable()
	if !from.IsNil() {
		switch {
		case from.IsJoin():
			return fmt.Errorf(
				"@fetch models must have exactly one lib call in FROM — JOIN is not allowed; materialize the lib first, then JOIN in a downstream model",
			)
		case from.NodeType() == "BASE_TABLE":
			return fmt.Errorf(
				"@fetch models must have exactly one lib call in FROM — `FROM <table>` is not allowed; lib functions live in lib/*.star and are called as `FROM lib_name(...)`",
			)
		case from.NodeType() == "SUBQUERY":
			return fmt.Errorf(
				"@fetch models must have exactly one lib call in FROM — subqueries in FROM are not allowed; move the subquery to a downstream model",
			)
		case from.NodeType() == "EMPTY":
			return fmt.Errorf(
				"@fetch models require a `FROM lib_name(...)` — the SELECT has no FROM clause",
			)
		}
		// TABLE_FUNCTION is the only allowed shape; others fall through
		// to the per-projection walk which will catch them via cast/alias
		// checks. We don't enumerate every from-table type here — new
		// DuckDB shapes should not silently slip through.
	}

	// WHERE clause — incremental cursor is directive-driven, filtering
	// belongs in lib args or downstream models.
	if !root.WhereClause().IsNil() {
		return fmt.Errorf(
			"@fetch models cannot use WHERE — incremental cursor is directive-driven (@incremental), filtering belongs in lib args or downstream models",
		)
	}

	// GROUP BY — fetch is a passthrough projection of API rows.
	if len(root.FieldList("group_expressions")) > 0 {
		return fmt.Errorf(
			"@fetch models cannot use GROUP BY — push aggregation to a downstream model that reads from this @fetch table",
		)
	}

	// DISTINCT and ORDER BY — appear as modifiers on the SELECT_NODE.
	for _, mod := range root.Modifiers() {
		switch mod.NodeType() {
		case "DISTINCT_MODIFIER":
			return fmt.Errorf(
				"@fetch models cannot use DISTINCT — push deduplication to a downstream model",
			)
		case "ORDER_MODIFIER":
			return fmt.Errorf(
				"@fetch models cannot use ORDER BY — ordering belongs in downstream models",
			)
		}
		// LIMIT_MODIFIER is rejected by the cross-cutting
		// validateNoLimitOffset check that runs before this validator.
	}

	return nil
}

// validateSelectProjections applies the strict-schema rules to a single
// SELECT_NODE's select list. Used by validateStrictLibSchema for every
// SELECT_NODE in the AST (including those inside CTEs, set-op branches,
// and subqueries).
//
// `fetchMode == true` enables the strict-fetch additional rule that the
// CAST argument must be a bare or qualified COLUMN_REF. This is the
// invariant that lets blueprints reason about the API shape from the
// model's projection list — derived expressions, function calls, and
// arithmetic make the projection ambiguous as a row-key source.
//
// `fetchMode == false` is the @push variant: projections still need
// CAST + alias, but the cast child can be any expression (`CONCAT(a,b)`,
// `CASE`, arithmetic, function call). For @push, SQL is the shape
// authority — derived projections are legitimate shape construction.
func validateSelectProjections(selectNode *duckast.Node, fetchMode bool) error {
	directive := "@push"
	if fetchMode {
		directive = "@fetch"
	}
	seenNames := make(map[string]bool)
	for _, item := range selectNode.SelectList() {
		// SELECT * is unconditionally rejected.
		if item.Class() == "STAR" {
			return fmt.Errorf(
				"SELECT * is not allowed in a %s model; project each output column with an explicit cast and alias (e.g. col::TYPE AS col)",
				directive,
			)
		}

		// Outermost node must be CAST.
		if item.Class() != "CAST" {
			name := projectionDisplayName(item)
			return fmt.Errorf(
				"projection %q is not cast; %s models require every output column to be wrapped in an explicit cast (e.g. (%s)::TYPE AS alias)",
				name, directive, name,
			)
		}

		// CAST must carry an explicit alias.
		alias := item.Alias()
		if alias == "" {
			name := projectionDisplayName(item)
			return fmt.Errorf(
				"cast projection %q has no explicit alias; %s models require every output column to be aliased (e.g. %s::TYPE AS alias)",
				name, directive, name,
			)
		}

		// In fetch mode, the CAST argument must be a bare or qualified
		// COLUMN_REF. Function calls, arithmetic, CASE expressions etc.
		// would make the projection an opaque transformation of API data —
		// the @fetch contract is "raw projection of API fields with type
		// declared by SQL". Push it to a downstream model.
		if fetchMode {
			child := item.Field("child")
			if child == nil || child.Class() != "COLUMN_REF" {
				return fmt.Errorf(
					"@fetch projection %q must be `<col>::TYPE AS alias` — derived expressions, function calls, and arithmetic are not allowed in a @fetch model. Push the transformation to a downstream model that reads from this @fetch table",
					alias,
				)
			}
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
