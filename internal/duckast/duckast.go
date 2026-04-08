// OndatraSQL - You don't need a data stack anymore
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

// Package duckast wraps DuckDB's json_serialize_sql output as a typed
// view over a raw map. The underlying storage is map[string]any so the
// AST always preserves every field DuckDB emits — adding or removing a
// typed accessor never silently drops data, and DuckDB schema changes
// never silently break dependency extraction.
//
// This is the design pattern Tree-sitter, Babel, and Roslyn use: lazy
// typed accessors over a single source of truth (the raw map). Walkers
// iterate the map directly so they see every field, even ones that
// don't have explicit accessors. Mutations (e.g. CDC's BASE_TABLE
// rewrites) operate on the map and round-trip back to JSON cleanly.
//
// Both the lineage extractor and the CDC SQL rewriter use this package
// — eliminating the parallel AST implementations that previously caused
// silent dependency-extraction bugs every time DuckDB shipped a new
// AST shape.
package duckast

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"
)

// AST is a parsed DuckDB SQL statement tree.
type AST struct {
	raw map[string]any
}

// Parse parses DuckDB's `json_serialize_sql()` output. Uses
// json.Decoder.UseNumber so query_location values that exceed
// float64 precision (uint64 can be > 2^53) survive a round-trip.
func Parse(jsonStr string) (*AST, error) {
	if jsonStr == "" {
		return nil, fmt.Errorf("empty AST JSON")
	}
	dec := json.NewDecoder(bytes.NewReader([]byte(jsonStr)))
	dec.UseNumber()
	var root map[string]any
	if err := dec.Decode(&root); err != nil {
		return nil, fmt.Errorf("parse AST JSON: %w", err)
	}
	return &AST{raw: root}, nil
}

// Serialize re-marshals the AST back to JSON. Used by mutation callers
// (CDC) to send the modified tree to DuckDB's json_deserialize_sql.
func (a *AST) Serialize() (string, error) {
	b, err := json.Marshal(a.raw)
	if err != nil {
		return "", fmt.Errorf("marshal AST: %w", err)
	}
	return string(b), nil
}

// Raw returns the underlying map. Use sparingly — prefer typed accessors.
func (a *AST) Raw() map[string]any { return a.raw }

// Statements returns the top-level statement nodes (one per SQL
// statement in the input). For most queries this is a single node.
func (a *AST) Statements() []*Node {
	if a == nil {
		return nil
	}
	stmts, ok := a.raw["statements"].([]any)
	if !ok {
		return nil
	}
	out := make([]*Node, 0, len(stmts))
	for _, s := range stmts {
		sm, ok := s.(map[string]any)
		if !ok {
			continue
		}
		nodeMap, ok := sm["node"].(map[string]any)
		if !ok {
			continue
		}
		out = append(out, &Node{raw: nodeMap})
	}
	return out
}

// Walk visits every sub-node in the AST. See Node.Walk for semantics.
func (a *AST) Walk(visit func(*Node) bool) {
	if a == nil {
		return
	}
	walkValue(a.raw, visit)
}

// ReplaceBaseTables walks the tree and substitutes BASE_TABLE nodes for
// which match() returns true with the result of build(). Used by CDC
// to rewrite raw table refs into time-travel SUBQUERY wrappers.
func (a *AST) ReplaceBaseTables(match func(*Node) bool, build func(*Node) map[string]any) {
	if a == nil {
		return
	}
	replaceInValue(a.raw, match, build)
}

// ----------------------------------------------------------------------
// Node — a single AST node, backed by the underlying map.
// ----------------------------------------------------------------------

// Node is a single position in the AST. Methods are pure shortcuts that
// read from the underlying map; the map is the source of truth.
type Node struct {
	raw map[string]any
}

// NewNode wraps a raw map as a Node. Useful for callers that already
// have a parsed map (e.g. when interoperating with the existing CDC
// json.UseNumber path).
func NewNode(raw map[string]any) *Node { return &Node{raw: raw} }

// Raw returns the underlying map. Use for fields not covered by typed
// accessors, or for in-place mutation.
func (n *Node) Raw() map[string]any {
	if n == nil {
		return nil
	}
	return n.raw
}

// IsNil reports whether this Node is empty (used for nil-safe chaining
// like `node.Field("x").Field("y")` without panicking on missing fields).
func (n *Node) IsNil() bool { return n == nil || n.raw == nil }

// ----- Generic accessors -----

// String returns the value of a string-typed field, or "" if missing.
func (n *Node) String(key string) string {
	if n.IsNil() {
		return ""
	}
	s, _ := n.raw[key].(string)
	return s
}

// Bool returns the value of a bool-typed field.
func (n *Node) Bool(key string) bool {
	if n.IsNil() {
		return false
	}
	b, _ := n.raw[key].(bool)
	return b
}

// Field returns a child node for `key`, or nil if absent.
func (n *Node) Field(key string) *Node {
	if n.IsNil() {
		return nil
	}
	sub, ok := n.raw[key].(map[string]any)
	if !ok {
		return nil
	}
	return &Node{raw: sub}
}

// FieldList returns a list of child nodes for `key`. Items that aren't
// maps are skipped.
func (n *Node) FieldList(key string) []*Node {
	if n.IsNil() {
		return nil
	}
	arr, ok := n.raw[key].([]any)
	if !ok {
		return nil
	}
	out := make([]*Node, 0, len(arr))
	for _, item := range arr {
		if m, ok := item.(map[string]any); ok {
			out = append(out, &Node{raw: m})
		}
	}
	return out
}

// StringList returns a list of strings for `key`.
func (n *Node) StringList(key string) []string {
	if n.IsNil() {
		return nil
	}
	arr, ok := n.raw[key].([]any)
	if !ok {
		return nil
	}
	out := make([]string, 0, len(arr))
	for _, item := range arr {
		if s, ok := item.(string); ok {
			out = append(out, s)
		}
	}
	return out
}

// ----- Common DuckDB AST shape helpers -----

// NodeType returns the `type` discriminator field
// (SELECT_NODE, SET_OPERATION_NODE, BASE_TABLE, JOIN, SUBQUERY, ...).
func (n *Node) NodeType() string { return n.String("type") }

// Class returns the `class` field used by expression nodes
// (COLUMN_REF, FUNCTION, COMPARISON, CASE, CAST, OPERATOR, SUBQUERY, ...).
func (n *Node) Class() string { return n.String("class") }

// Alias returns the alias used by tables and expressions.
func (n *Node) Alias() string { return n.String("alias") }

// IsSelectNode reports whether this node is a regular SELECT body.
func (n *Node) IsSelectNode() bool { return n.NodeType() == "SELECT_NODE" }

// IsSetOpNode reports whether this node is UNION/INTERSECT/EXCEPT.
func (n *Node) IsSetOpNode() bool { return n.NodeType() == "SET_OPERATION_NODE" }

// IsBaseTable reports whether this node is a physical table reference.
func (n *Node) IsBaseTable() bool { return n.NodeType() == "BASE_TABLE" }

// IsJoin reports whether this node is a join in the FROM clause.
func (n *Node) IsJoin() bool { return n.NodeType() == "JOIN" }

// ----- SELECT-shape accessors -----

// SelectList returns the projection expressions for SELECT_NODE.
func (n *Node) SelectList() []*Node { return n.FieldList("select_list") }

// FromTable returns the FROM clause root.
func (n *Node) FromTable() *Node { return n.Field("from_table") }

// WhereClause returns the WHERE expression, or nil.
func (n *Node) WhereClause() *Node { return n.Field("where_clause") }

// Having returns the HAVING expression, or nil.
func (n *Node) Having() *Node { return n.Field("having") }

// Qualify returns the QUALIFY expression, or nil.
func (n *Node) Qualify() *Node { return n.Field("qualify") }

// SetOpLeft returns the left side of a SET_OPERATION_NODE.
func (n *Node) SetOpLeft() *Node { return n.Field("left") }

// SetOpRight returns the right side of a SET_OPERATION_NODE.
func (n *Node) SetOpRight() *Node { return n.Field("right") }

// SetOpType returns the kind of set operation (UNION/INTERSECT/EXCEPT).
func (n *Node) SetOpType() string { return n.String("setop_type") }

// ----- CTE accessors -----

// CTE binds a name to its body node.
type CTE struct {
	Name string
	Node *Node
}

// CTEs returns the named WITH-clauses on this SELECT_NODE.
func (n *Node) CTEs() []CTE {
	cteMap := n.Field("cte_map")
	if cteMap.IsNil() {
		return nil
	}
	entries := cteMap.FieldList("map")
	out := make([]CTE, 0, len(entries))
	for _, entry := range entries {
		name, _ := entry.raw["key"].(string)
		value, _ := entry.raw["value"].(map[string]any)
		query, _ := value["query"].(map[string]any)
		body, _ := query["node"].(map[string]any)
		if body != nil {
			out = append(out, CTE{Name: name, Node: &Node{raw: body}})
		}
	}
	return out
}

// ----- Result modifier accessors (ORDER BY / LIMIT / OFFSET) -----

// Modifiers returns ORDER BY / LIMIT / OFFSET / DISTINCT modifiers.
func (n *Node) Modifiers() []*Node { return n.FieldList("modifiers") }

// ModifierType returns the discriminator for a modifier node
// (LIMIT_MODIFIER, OFFSET_MODIFIER, ORDER_MODIFIER, ...).
func (n *Node) ModifierType() string { return n.String("type") }

// LimitExpr returns the LIMIT expression on a LIMIT_MODIFIER.
func (n *Node) LimitExpr() *Node { return n.Field("limit") }

// OffsetExpr returns the OFFSET expression on an OFFSET_MODIFIER.
func (n *Node) OffsetExpr() *Node { return n.Field("offset") }

// OrderEntry pairs an ORDER BY expression with its sort modifiers.
type OrderEntry struct {
	Expression *Node
}

// Orders returns the ORDER BY entries on an ORDER_MODIFIER.
func (n *Node) Orders() []OrderEntry {
	arr := n.FieldList("orders")
	out := make([]OrderEntry, 0, len(arr))
	for _, e := range arr {
		out = append(out, OrderEntry{Expression: e.Field("expression")})
	}
	return out
}

// ----- FROM-clause accessors -----

// TableName returns the table_name on a BASE_TABLE node.
func (n *Node) TableName() string { return n.String("table_name") }

// SchemaName returns the schema_name on a BASE_TABLE node.
func (n *Node) SchemaName() string { return n.String("schema_name") }

// CatalogName returns the catalog_name on a BASE_TABLE node.
func (n *Node) CatalogName() string { return n.String("catalog_name") }

// FullTableName returns "schema.table" or just "table" if no schema.
func (n *Node) FullTableName() string {
	schema := n.SchemaName()
	table := n.TableName()
	if schema != "" {
		return schema + "." + table
	}
	return table
}

// JoinLeft returns the left side of a JOIN-typed from_table.
func (n *Node) JoinLeft() *Node { return n.Field("left") }

// JoinRight returns the right side of a JOIN-typed from_table.
func (n *Node) JoinRight() *Node { return n.Field("right") }

// JoinCondition returns the ON-expression of a JOIN. Can contain
// scalar subqueries; missing it from extraction silently drops
// dependencies (a class of bug we want this package to make impossible).
func (n *Node) JoinCondition() *Node { return n.Field("condition") }

// SubqueryNode returns the inner node of a SUBQUERY-typed from_table
// or expression. Handles the wrapping path subquery.node automatically.
func (n *Node) SubqueryNode() *Node {
	sub := n.Field("subquery")
	if sub.IsNil() {
		return nil
	}
	if inner := sub.Field("node"); !inner.IsNil() {
		return inner
	}
	return sub
}

// ----- Expression accessors -----

// ColumnNames returns the qualified parts of a COLUMN_REF
// (e.g. ["alias", "column"] or ["column"]).
func (n *Node) ColumnNames() []string { return n.StringList("column_names") }

// FunctionName returns the function name on a FUNCTION/WINDOW node.
func (n *Node) FunctionName() string { return n.String("function_name") }

// IsOperator reports whether a FUNCTION node is actually an operator
// (`+`, `-`, etc.) rather than a named function call.
func (n *Node) IsOperator() bool { return n.Bool("is_operator") }

// Children returns the child expressions (function args, etc.).
func (n *Node) Children() []*Node { return n.FieldList("children") }

// Child returns the single child expression for SUBQUERY-class nodes
// in IN-style comparisons (the LHS being tested).
func (n *Node) Child() *Node { return n.Field("child") }

// ExprLeft returns the left operand of a binary COMPARISON.
// DuckDB stores these as `left`/`right`, NOT `children`.
func (n *Node) ExprLeft() *Node { return n.Field("left") }

// ExprRight returns the right operand of a binary COMPARISON.
func (n *Node) ExprRight() *Node { return n.Field("right") }

// CaseCheck pairs a WHEN expression with its THEN result.
type CaseCheck struct {
	When *Node
	Then *Node
}

// CaseChecks returns the WHEN/THEN pairs of a CASE expression.
func (n *Node) CaseChecks() []CaseCheck {
	arr := n.FieldList("case_checks")
	out := make([]CaseCheck, 0, len(arr))
	for _, c := range arr {
		out = append(out, CaseCheck{
			When: c.Field("when_expr"),
			Then: c.Field("then_expr"),
		})
	}
	return out
}

// ----- Walker -----

// Walk visits this node and every reachable sub-node. Returns false
// from `visit` to skip the subtree (the visitor itself is still called
// for the parent node, but its children are not traversed).
//
// The walker iterates the underlying map[string]any so it discovers
// every field DuckDB emits — including fields the typed accessors in
// this package don't yet model. New AST shapes from DuckDB upgrades
// participate in dependency extraction without code changes.
func (n *Node) Walk(visit func(*Node) bool) {
	if n.IsNil() {
		return
	}
	walkNode(n, visit)
}

func walkNode(n *Node, visit func(*Node) bool) {
	if !visit(n) {
		return
	}
	// Iterate map keys in sorted order so the walk sequence is
	// deterministic across runs. Go randomizes map iteration, and
	// callers that depend on visit order (e.g. picking the "first"
	// match by some criterion) would otherwise get flaky behavior.
	keys := make([]string, 0, len(n.raw))
	for k := range n.raw {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		walkValue(n.raw[k], visit)
	}
}

func walkValue(v any, visit func(*Node) bool) {
	switch x := v.(type) {
	case map[string]any:
		walkNode(&Node{raw: x}, visit)
	case []any:
		for _, item := range x {
			walkValue(item, visit)
		}
	}
}

// ----- Mutation: ReplaceBaseTables (used by CDC) -----

func replaceInValue(v any, match func(*Node) bool, build func(*Node) map[string]any) any {
	switch x := v.(type) {
	case map[string]any:
		node := &Node{raw: x}
		if node.IsBaseTable() && match(node) {
			replacement := build(node)
			// Replace the map's contents in place so all references
			// (including the one held by the parent slice/map) see
			// the new shape.
			for k := range x {
				delete(x, k)
			}
			for k, v := range replacement {
				x[k] = v
			}
			return x
		}
		for k, child := range x {
			x[k] = replaceInValue(child, match, build)
		}
		return x
	case []any:
		for i, item := range x {
			x[i] = replaceInValue(item, match, build)
		}
		return x
	default:
		return v
	}
}
