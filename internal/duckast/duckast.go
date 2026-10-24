// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
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
	"strconv"
	"strings"
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
	if err := checkShape(root); err != nil {
		return nil, err
	}
	return &AST{raw: root}, nil
}

// checkShape rejects a tree that is not in the shape DuckDB 2.0 emits, so
// that a reader never meets one. The accessors read only the 2.0 fields; on
// a 1.5 tree they would find no function arguments, no set-operation arms
// and no CTE bodies, and every consumer would carry on with nothing. A
// malformed function argument is rejected here too: skipping it would shift
// every later positional argument.
func checkShape(v any) error {
	switch x := v.(type) {
	case map[string]any:
		if err := checkNodeShape(x); err != nil {
			return err
		}
		for _, child := range x {
			if err := checkShape(child); err != nil {
				return err
			}
		}
	case []any:
		for _, item := range x {
			if err := checkShape(item); err != nil {
				return err
			}
		}
	}
	return nil
}

func checkNodeShape(m map[string]any) error {
	not2 := func(what string) error {
		return fmt.Errorf("AST is not in the DuckDB 2.0 json_serialize_sql shape: %s", what)
	}
	class, _ := m["class"].(string)
	switch class {
	case "FUNCTION", "WINDOW":
		args, ok := m["arguments"].([]any)
		if !ok {
			return not2(class + " node without an arguments list")
		}
		for i, item := range args {
			entry, ok := item.(map[string]any)
			if !ok {
				return not2(fmt.Sprintf("%s argument %d is not an object", class, i))
			}
			if _, ok := entry["expression"].(map[string]any); !ok {
				return not2(fmt.Sprintf("%s argument %d has no expression", class, i))
			}
			if name, present := entry["name"]; present {
				if _, ok := name.(string); !ok {
					return not2(fmt.Sprintf("%s argument %d has a non-string name", class, i))
				}
			}
		}
	case "CONSTANT":
		if _, ok := m["literal"].(map[string]any); !ok {
			return not2("CONSTANT node without a literal")
		}
	case "CAST":
		if _, ok := m["type_expr"].(map[string]any); !ok {
			return not2("CAST node without a type_expr")
		}
	}
	if m["type"] == "SET_OPERATION_NODE" && (m["left"] != nil || m["right"] != nil) {
		return not2("SET_OPERATION_NODE with left/right arms instead of children")
	}
	if cteMap, ok := m["cte_map"].(map[string]any); ok {
		entries, _ := cteMap["map"].([]any)
		for _, e := range entries {
			entry, _ := e.(map[string]any)
			value, _ := entry["value"].(map[string]any)
			if _, ok := value["query_node"].(map[string]any); !ok {
				return not2(fmt.Sprintf("CTE %v without a query_node", entry["key"]))
			}
		}
	}
	return nil
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
	replaceInValue(a.raw, func(n *Node) bool { return n.IsBaseTable() }, match, build)
}

// ReplaceTableFunctions walks the tree and substitutes TABLE_FUNCTION nodes
// for which match() returns true with the result of build(). Used to replace
// lib function calls (FROM gam_fetch(...)) with temp table references.
func (a *AST) ReplaceTableFunctions(match func(*Node) bool, build func(*Node) map[string]any) {
	if a == nil {
		return
	}
	replaceInValue(a.raw, func(n *Node) bool { return n.IsTableFunction() }, match, build)
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

// IsTableFunction reports whether this node is a table function call in FROM
// (e.g. FROM read_csv(...), FROM gam_fetch(...)).
func (n *Node) IsTableFunction() bool { return n.NodeType() == "TABLE_FUNCTION" }

// TableFunctionName returns the function name for a TABLE_FUNCTION node.
// The name is nested inside function.function_name.
func (n *Node) TableFunctionName() string {
	fn := n.Field("function")
	if fn.IsNil() {
		return ""
	}
	return fn.FunctionName()
}

// TableFunctionArgs returns the arguments of a TABLE_FUNCTION in order, with
// the name of each named argument (`f(limit => 5)`).
func (n *Node) TableFunctionArgs() ([]Argument, error) {
	fn := n.Field("function")
	if fn.IsNil() {
		return nil, nil
	}
	return fn.Arguments()
}

// Argument is one argument of a function call. Name is set for a named
// argument (`f(x := 1)`) and empty for a positional one.
type Argument struct {
	Name string
	Expr *Node
}

// Arguments returns the arguments of a FUNCTION or WINDOW node in order,
// each with its name if it was passed by name. DuckDB 2.0 lists them under
// `arguments` as {name, expression} pairs. A malformed entry is an error
// rather than skipped, since skipping it would shift every later argument;
// Parse already rejects one, so this only fires on a hand-built node.
func (n *Node) Arguments() ([]Argument, error) {
	if n.IsNil() {
		return nil, nil
	}
	arr, _ := n.raw["arguments"].([]any)
	out := make([]Argument, 0, len(arr))
	for i, item := range arr {
		m, _ := item.(map[string]any)
		expr, _ := m["expression"].(map[string]any)
		if expr == nil {
			return nil, fmt.Errorf("argument %d of %s has no expression", i, n.FunctionName())
		}
		name, _ := m["name"].(string)
		out = append(out, Argument{Name: name, Expr: &Node{raw: expr}})
	}
	return out, nil
}

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

// SetOpArms returns the operands of a SET_OPERATION_NODE in order. DuckDB
// 2.0 lists them under `children`: a chain of the same operation,
// `a UNION ALL b UNION ALL c`, is one node with three arms, and `left` and
// `right` are null. A RECURSIVE_CTE_NODE still has its anchor and recursive
// part under `left` and `right`.
func (n *Node) SetOpArms() []*Node {
	if n.NodeType() == "RECURSIVE_CTE_NODE" {
		var arms []*Node
		for _, side := range []*Node{n.Field("left"), n.Field("right")} {
			if !side.IsNil() {
				arms = append(arms, side)
			}
		}
		return arms
	}
	return n.FieldList("children")
}

// FirstSelect returns the SELECT that names a query's columns: n itself
// unless n is a set operation or recursive CTE, and otherwise the first arm,
// descending through nested set operations. `(a UNION BY NAME b) UNION ALL c`
// is a UNION ALL whose first arm is itself a set operation, with an empty
// select list of its own. Returns nil when a set operation has no arms.
func (n *Node) FirstSelect() *Node {
	for n.IsSetOpNode() || n.NodeType() == "RECURSIVE_CTE_NODE" {
		arms := n.SetOpArms()
		if len(arms) == 0 {
			return nil
		}
		n = arms[0]
	}
	if n.IsNil() {
		return nil
	}
	return n
}

// SetOpType returns the kind of set operation (UNION/INTERSECT/EXCEPT).
func (n *Node) SetOpType() string { return n.String("setop_type") }

// ----- CTE accessors -----

// CTE binds a name to its body node. Aliases holds the column names of
// `WITH c(a, b) AS ...`, which rename the body's leading output columns.
type CTE struct {
	Name    string
	Node    *Node
	Aliases []string
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
		body, _ := value["query_node"].(map[string]any)
		if body != nil {
			var aliases []string
			if list, ok := value["aliases"].([]any); ok {
				for _, a := range list {
					if str, ok := a.(string); ok {
						aliases = append(aliases, str)
					}
				}
			}
			out = append(out, CTE{Name: name, Node: &Node{raw: body}, Aliases: aliases})
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

// SetTableRef points a BASE_TABLE node at catalog.schema.table. DuckDB 2.0
// emits the reference twice, as the separate name fields and as
// `qualified_name.path`, and json_deserialize_sql reads only the path:
// changing `catalog_name` alone leaves the rendered SQL unqualified.
func (n *Node) SetTableRef(catalog, schema, table string) {
	if n.IsNil() {
		return
	}
	n.raw["catalog_name"] = catalog
	n.raw["schema_name"] = schema
	n.raw["table_name"] = table
	n.raw["qualified_name"] = QualifiedName(catalog, schema, table)
}

// QualifiedName builds the `qualified_name` field of a 2.0 BASE_TABLE node
// from its non-empty parts.
func QualifiedName(parts ...string) map[string]any {
	path := []any{}
	for _, p := range parts {
		if p != "" {
			path = append(path, p)
		}
	}
	return map[string]any{"path": path}
}

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

// Children returns the child expressions: the `children` of an operator or
// conjunction, or the argument expressions of a function call, which
// DuckDB 2.0 lists under `arguments` instead. Parse guarantees every
// argument has an expression; use Arguments where position or name matters.
func (n *Node) Children() []*Node {
	if n.IsNil() {
		return nil
	}
	if _, ok := n.raw["arguments"]; ok {
		args, _ := n.Arguments()
		out := make([]*Node, 0, len(args))
		for _, a := range args {
			out = append(out, a.Expr)
		}
		return out
	}
	return n.FieldList("children")
}

// ConstantString returns a CONSTANT node's value as the string DuckDB gives
// for CAST(<literal> AS VARCHAR), for the literal kinds where that is the
// literal itself: a string, an integer that fits in BIGINT (`1_000` is
// "1000", `010` is "10"), a boolean ("true"/"false") and NULL (""). ok is
// false for every other kind (decimals, floats, huge integers, blobs); a
// caller that needs those as text has to cast them through DuckDB.
func (n *Node) ConstantString() (s string, ok bool) {
	if n.IsNil() || n.Class() != "CONSTANT" {
		return "", false
	}
	lit := n.Field("literal")
	text := lit.String("text")
	switch lit.String("kind") {
	case "STRING":
		return text, true
	case "BOOLEAN":
		b, err := strconv.ParseBool(text)
		if err != nil {
			return "", false
		}
		return strconv.FormatBool(b), true
	case "NULL_LITERAL":
		return "", true
	case "INTEGER":
		// Base 10 rather than base 0 so that a leading zero is not read as
		// octal, as DuckDB does not.
		i, err := strconv.ParseInt(strings.ReplaceAll(text, "_", ""), 10, 64)
		if err != nil {
			return "", false
		}
		return strconv.FormatInt(i, 10), true
	}
	return "", false
}

// ----- Types -----

// TypeRef is the target type of a CAST, as written in the query.
//
// Name is the type name as DuckDB reports it: its own spelling for the
// forms it normalizes (`double precision` is "DOUBLE", `int[]` is "list",
// `int[3]` is "array", `timestamp without time zone` is "TIMESTAMP"), the
// user's spelling otherwise (`int4`, `myschema.mytype`). Compare it
// case-insensitively. Catalog and Schema are set for a qualified name.
//
// Params are the type modifiers in order, as SQL literals: `DECIMAL(10,2)`
// has "10" and "2", `int[3]` has "3", `GEOMETRY('OGC:CRS84')` has
// "'OGC:CRS84'" with its quotes. Fields are the nested types: the element
// of a LIST or ARRAY, the key and value of a MAP, and the named members of
// a STRUCT or UNION.
type TypeRef struct {
	Catalog string
	Schema  string
	Name    string
	Params  []string
	Fields  []TypeField
}

// TypeField is one nested type of a TypeRef. Name is set for STRUCT and
// UNION members.
type TypeField struct {
	Name string
	Type *TypeRef
}

// Qualified reports whether the type name carries a schema or catalog.
func (t *TypeRef) Qualified() bool { return t.Catalog != "" || t.Schema != "" }

// QualifiedName returns catalog.schema.name with the parts that are set.
func (t *TypeRef) QualifiedName() string {
	var parts []string
	for _, p := range []string{t.Catalog, t.Schema, t.Name} {
		if p != "" {
			parts = append(parts, p)
		}
	}
	return strings.Join(parts, ".")
}

// CastType returns the target type of a CAST node. DuckDB 2.0 puts it under
// `type_expr`, a TYPE node whose children are nested TYPE nodes or constant
// modifiers. A CAST without one, or a modifier that is not a constant, is an
// error: falling back to some default type would silently retype a column.
func (n *Node) CastType() (*TypeRef, error) {
	if n.IsNil() || n.Class() != "CAST" {
		return nil, fmt.Errorf("not a CAST node")
	}
	return typeRefOf(n.Field("type_expr"))
}

func typeRefOf(t *Node) (*TypeRef, error) {
	if t.IsNil() || t.Class() != "TYPE" {
		return nil, fmt.Errorf("CAST has no target type")
	}
	ref := &TypeRef{Catalog: t.String("catalog"), Schema: t.String("schema"), Name: t.String("type_name")}
	if ref.Name == "" {
		return nil, fmt.Errorf("CAST target type has no name")
	}
	for _, c := range t.FieldList("children") {
		switch c.Class() {
		case "TYPE":
			field, err := typeRefOf(c)
			if err != nil {
				return nil, err
			}
			ref.Fields = append(ref.Fields, TypeField{Name: c.Alias(), Type: field})
		case "CONSTANT":
			lit := c.Field("literal")
			text := lit.String("text")
			if lit.String("kind") == "STRING" {
				text = "'" + strings.ReplaceAll(text, "'", "''") + "'"
			}
			ref.Params = append(ref.Params, text)
		default:
			return nil, fmt.Errorf("type %s has a modifier that is not a constant (%s)", ref.QualifiedName(), c.Class())
		}
	}
	return ref, nil
}

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

// ChildExpressions returns the expression nodes nested directly inside n —
// the values of its fields that are themselves expressions, reached through
// lists and through maps that are not expressions (`order_bys`, `case_checks`
// and the like). It is what lets a walker handle an expression class it does
// not know by name: the operands are still found, so a new or overlooked class
// contributes its sources instead of silently contributing none.
//
// The `subquery` field is left out: a subquery resolves its names against its
// own FROM clause, so it has to be traced with its own scope rather than the
// enclosing one.
func (n *Node) ChildExpressions() []*Node {
	if n.IsNil() {
		return nil
	}
	var out []*Node
	collectExpressions(n.raw, &out)
	return out
}

func collectExpressions(m map[string]any, out *[]*Node) {
	keys := make([]string, 0, len(m))
	for k := range m {
		if k != "subquery" {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	for _, k := range keys {
		collectExpressionValue(m[k], out)
	}
}

func collectExpressionValue(v any, out *[]*Node) {
	switch x := v.(type) {
	case map[string]any:
		node := &Node{raw: x}
		if node.Class() != "" {
			*out = append(*out, node)
			return
		}
		collectExpressions(x, out)
	case []any:
		for _, item := range x {
			collectExpressionValue(item, out)
		}
	}
}

// ----- Mutation: ReplaceBaseTables (used by CDC) -----

func replaceInValue(v any, typeCheck func(*Node) bool, match func(*Node) bool, build func(*Node) map[string]any) any {
	switch x := v.(type) {
	case map[string]any:
		node := &Node{raw: x}
		if typeCheck(node) && match(node) {
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
			x[k] = replaceInValue(child, typeCheck, match, build)
		}
		return x
	case []any:
		for i, item := range x {
			x[i] = replaceInValue(item, typeCheck, match, build)
		}
		return x
	default:
		return v
	}
}
