// OndatraSQL - You don't need a data stack anymore
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package execute

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
)

// applySmartCDC applies CDC to specified tables using AST node manipulation.
// Tables in cdcTables get time-travel CDC via EXCEPT subquery.
//
// Implementation uses DuckDB's json_serialize_sql/json_deserialize_sql for
// correct-by-construction SQL rewriting. Table references inside string
// literals are never touched because the AST separates identifiers from values.
func (r *Runner) applySmartCDC(astJSON, kind string, cdcTables []string, snapshotID int64) (string, error) {
	if len(cdcTables) == 0 {
		return r.deserializeAST(astJSON)
	}

	// Validate kind supports CDC
	switch kind {
	case "append", "merge", "scd2", "partition":
		// OK
	default:
		return r.deserializeAST(astJSON)
	}

	// Parse AST JSON (UseNumber to preserve uint64 query_location values)
	root, err := parseASTJSON(astJSON)
	if err != nil {
		return "", fmt.Errorf("parse AST JSON: %w", err)
	}

	// Build lookup set for CDC tables (lowercase for case-insensitive matching)
	cdcSet := make(map[string]bool, len(cdcTables))
	for _, t := range cdcTables {
		cdcSet[strings.ToLower(t)] = true
	}

	// Determine prod catalog for sandbox mode
	catalog := ""
	if r.sess != nil && r.sess.ProdAlias() != "" {
		catalog = r.sess.ProdAlias()
	}

	// Walk AST and replace matching BASE_TABLE nodes with CDC subqueries
	walkAST(root, func(node map[string]any) map[string]any {
		nodeType, _ := node["type"].(string)
		if nodeType != "BASE_TABLE" {
			return nil
		}
		schema, _ := node["schema_name"].(string)
		table, _ := node["table_name"].(string)
		fullName := strings.ToLower(schema + "." + table)
		if !cdcSet[fullName] {
			return nil
		}
		alias, _ := node["alias"].(string)
		cat := catalog
		if existingCat, _ := node["catalog_name"].(string); existingCat != "" {
			cat = existingCat
		}
		return buildCDCSubquery(schema, table, cat, alias, snapshotID)
	})

	// Serialize back to JSON
	modified, err := json.Marshal(root)
	if err != nil {
		return "", fmt.Errorf("marshal modified AST: %w", err)
	}

	return r.deserializeAST(string(modified))
}

// applyEmptySmartCDC applies empty result to specified tables using AST node manipulation.
// Uses subquery replacement to return zero rows (WHERE false).
func (r *Runner) applyEmptySmartCDC(astJSON string, cdcTables []string) (string, error) {
	if len(cdcTables) == 0 {
		return r.deserializeAST(astJSON)
	}

	root, err := parseASTJSON(astJSON)
	if err != nil {
		return "", fmt.Errorf("parse AST JSON: %w", err)
	}

	cdcSet := make(map[string]bool, len(cdcTables))
	for _, t := range cdcTables {
		cdcSet[strings.ToLower(t)] = true
	}

	catalog := ""
	if r.sess != nil && r.sess.ProdAlias() != "" {
		catalog = r.sess.ProdAlias()
	}

	walkAST(root, func(node map[string]any) map[string]any {
		nodeType, _ := node["type"].(string)
		if nodeType != "BASE_TABLE" {
			return nil
		}
		schema, _ := node["schema_name"].(string)
		table, _ := node["table_name"].(string)
		fullName := strings.ToLower(schema + "." + table)
		if !cdcSet[fullName] {
			return nil
		}
		alias, _ := node["alias"].(string)
		cat := catalog
		if existingCat, _ := node["catalog_name"].(string); existingCat != "" {
			cat = existingCat
		}
		return buildEmptyCDCSubquery(schema, table, cat, alias)
	})

	modified, err := json.Marshal(root)
	if err != nil {
		return "", fmt.Errorf("marshal modified AST: %w", err)
	}

	return r.deserializeAST(string(modified))
}

// qualifyTablesInAST sets catalog_name on BASE_TABLE nodes matching the given tables.
// tablesToQualify maps lowercase "schema.table" to true.
func qualifyTablesInAST(root map[string]any, tablesToQualify map[string]bool, catalog string) {
	walkAST(root, func(node map[string]any) map[string]any {
		nodeType, _ := node["type"].(string)
		if nodeType != "BASE_TABLE" {
			return nil
		}
		schema, _ := node["schema_name"].(string)
		table, _ := node["table_name"].(string)
		fullName := strings.ToLower(schema + "." + table)
		if tablesToQualify[fullName] {
			node["catalog_name"] = catalog
		}
		return nil // mutate in place, no replacement
	})
}

// deserializeAST converts AST JSON back to SQL via DuckDB's json_deserialize_sql.
func (r *Runner) deserializeAST(astJSON string) (string, error) {
	escaped := strings.ReplaceAll(astJSON, "'", "''")
	query := fmt.Sprintf("SELECT json_deserialize_sql('%s')", escaped)
	result, err := r.sess.QueryValue(query)
	if err != nil {
		return "", fmt.Errorf("deserialize AST: %w", err)
	}
	return result, nil
}

// parseASTJSON parses JSON using UseNumber() to preserve large integers (e.g. query_location uint64).
// Standard json.Unmarshal converts numbers to float64, losing precision for values like 18446744073709551615.
func parseASTJSON(data string) (map[string]any, error) {
	dec := json.NewDecoder(bytes.NewReader([]byte(data)))
	dec.UseNumber()
	var root map[string]any
	if err := dec.Decode(&root); err != nil {
		return nil, err
	}
	return root, nil
}

// walkAST recursively walks a JSON tree (map[string]any / []any).
// The visitor is called on every map node. If it returns non-nil, the node
// is replaced; otherwise the walker recurses into children.
func walkAST(node any, visitor func(map[string]any) map[string]any) any {
	switch n := node.(type) {
	case map[string]any:
		if replacement := visitor(n); replacement != nil {
			return replacement
		}
		for k, v := range n {
			n[k] = walkAST(v, visitor)
		}
		return n
	case []any:
		for i, v := range n {
			n[i] = walkAST(v, visitor)
		}
		return n
	default:
		return node
	}
}

// buildCDCSubquery constructs a SUBQUERY AST node for CDC:
// (SELECT * FROM schema.table EXCEPT SELECT * FROM schema.table AT (VERSION => snapshotID))
func buildCDCSubquery(schema, table, catalog, alias string, snapshotID int64) map[string]any {
	baseTable := func(atClause any) map[string]any {
		return map[string]any{
			"type": "BASE_TABLE", "alias": "",
			"at_clause": atClause, "catalog_name": catalog,
			"column_name_alias": []any{},
			"query_location": 0, "sample": nil,
			"schema_name": schema, "table_name": table,
		}
	}

	selectStar := func(from map[string]any) map[string]any {
		return map[string]any{
			"type":               "SELECT_NODE",
			"aggregate_handling": "STANDARD_HANDLING",
			"cte_map":           map[string]any{"map": []any{}},
			"group_expressions": []any{},
			"group_sets":        []any{},
			"having":            nil,
			"modifiers":         []any{},
			"qualify":           nil,
			"sample":            nil,
			"where_clause":      nil,
			"select_list": []any{
				starExpr(),
			},
			"from_table": from,
		}
	}

	atClause := map[string]any{
		"unit": "VERSION",
		"expr": map[string]any{
			"alias": "", "class": "CONSTANT",
			"query_location": 0, "type": "VALUE_CONSTANT",
			"value": map[string]any{
				"is_null": false,
				"type":    map[string]any{"id": "BIGINT", "type_info": nil},
				"value":   snapshotID,
			},
		},
	}

	return map[string]any{
		"type":              "SUBQUERY",
		"alias":             alias,
		"column_name_alias": []any{},
		"sample":            nil,
		"query_location":    0,
		"subquery": map[string]any{
			"named_param_map": []any{},
			"node": map[string]any{
				"type":       "SET_OPERATION_NODE",
				"cte_map":    map[string]any{"map": []any{}},
				"setop_type": "EXCEPT",
				"setop_all":  false,
				"modifiers":  []any{},
				"left":       selectStar(baseTable(nil)),
				"right":      selectStar(baseTable(atClause)),
			},
		},
	}
}

// buildEmptyCDCSubquery constructs a SUBQUERY AST node that returns zero rows:
// (SELECT * FROM schema.table WHERE false)
func buildEmptyCDCSubquery(schema, table, catalog, alias string) map[string]any {
	return map[string]any{
		"type":              "SUBQUERY",
		"alias":             alias,
		"column_name_alias": []any{},
		"sample":            nil,
		"query_location":    0,
		"subquery": map[string]any{
			"named_param_map": []any{},
			"node": map[string]any{
				"type":               "SELECT_NODE",
				"aggregate_handling": "STANDARD_HANDLING",
				"cte_map":           map[string]any{"map": []any{}},
				"group_expressions": []any{},
				"group_sets":        []any{},
				"having":            nil,
				"modifiers":         []any{},
				"qualify":           nil,
				"sample":            nil,
				"select_list": []any{
					starExpr(),
				},
				"from_table": map[string]any{
					"type": "BASE_TABLE", "alias": "",
					"at_clause": nil, "catalog_name": catalog,
					"column_name_alias": []any{},
					"query_location": 0, "sample": nil,
					"schema_name": schema, "table_name": table,
				},
				"where_clause": whereFalseExpr(),
			},
		},
	}
}

// starExpr returns a SELECT * AST expression node.
func starExpr() map[string]any {
	return map[string]any{
		"alias": "", "class": "STAR", "columns": false,
		"exclude_list": []any{}, "expr": nil,
		"qualified_exclude_list": []any{},
		"query_location": 0, "relation_name": "",
		"rename_list": []any{}, "replace_list": []any{},
		"type": "STAR",
	}
}

// whereFalseExpr returns a WHERE false AST expression (CAST('f' AS BOOLEAN)).
func whereFalseExpr() map[string]any {
	return map[string]any{
		"alias":    "",
		"class":    "CAST",
		"type":     "OPERATOR_CAST",
		"try_cast": false,
		"cast_type": map[string]any{
			"id":        "BOOLEAN",
			"type_info": nil,
		},
		"query_location": 0,
		"child": map[string]any{
			"alias": "", "class": "CONSTANT",
			"query_location": 0, "type": "VALUE_CONSTANT",
			"value": map[string]any{
				"is_null": false,
				"type":    map[string]any{"id": "VARCHAR", "type_info": nil},
				"value":   "f",
			},
		},
	}
}

// quoteTableName quotes a table name for safe use in SQL.
// Handles both simple names and schema.table format.
//
// Examples:
//
//	orders         -> "orders"
//	staging.orders -> "staging"."orders"
//	catalog.schema.table -> "catalog"."schema"."table"
func quoteTableName(name string) string {
	parts := strings.Split(name, ".")
	quoted := make([]string, len(parts))
	for i, part := range parts {
		// Escape any existing quotes in the identifier
		escaped := strings.ReplaceAll(part, `"`, `""`)
		quoted[i] = `"` + escaped + `"`
	}
	return strings.Join(quoted, ".")
}
