// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

// Package lineage extracts column-level lineage from SQL using DuckDB's AST.
//
// All AST traversal goes through internal/duckast, which stores the AST as
// a raw map[string]any with typed accessors layered on top. This means new
// AST shapes from DuckDB upgrades are seen by the walker automatically —
// the historical "missing field on a typed struct silently dropped data"
// bug class (UNION nodes, COMPARISON left/right, JOIN ON conditions,
// LIMIT/OFFSET/ORDER BY modifier subqueries) cannot recur.
package lineage

import (
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strings"

	"github.com/ondatra-labs/ondatrasql/internal/duckast"
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
	Table          string             `json:"table"`              // Source table (e.g., "staging.orders")
	Column         string             `json:"column"`             // Source column name (e.g., "amount")
	Transformation TransformationType `json:"transformation"`     // How it was transformed
	FunctionName   string             `json:"function,omitempty"` // Function name if applicable
}

// ColumnLineage represents the lineage of a single output column.
type ColumnLineage struct {
	Column  string         `json:"column"`  // Output column name
	Sources []SourceColumn `json:"sources"` // Source columns with table/column/transformation

	// unexpanded marks the `?` entry standing in for a star that could not be
	// expanded. It is not serialised: the name alone cannot tell it apart from
	// an unaliased expression without column references, such as `SELECT 1`,
	// which is also named `?` and has no sources.
	unexpanded bool
}

// unexpandedStar is the single entry recorded for a star whose columns are
// unknown.
func unexpandedStar() []ColumnLineage {
	return []ColumnLineage{{Column: "?", unexpanded: true}}
}

// ColumnResolver returns a table's column names in declaration order, or
// false when they cannot be determined. It is how `SELECT *` over a physical
// table is expanded: the AST names the table but not its columns. The name
// arrives in its parts, as written; catalog and schema may be empty.
type ColumnResolver func(catalog, schema, table string) ([]string, bool)

// Extractor holds state for recursive CTE resolution.
type Extractor struct {
	scope     *cteFrame                   // innermost WITH in scope; nil when none
	resolved  map[uintptr][]ColumnLineage // CTE body node -> ordered output columns
	resolving map[uintptr]bool            // CTE body nodes currently being resolved
	columns   ColumnResolver              // physical table columns for star expansion; may be nil
}

// cteFrame is one WITH clause: the CTEs it declares and the scope it sits in.
type cteFrame struct {
	defs   map[string]*cteDef // lower-cased name -> definition
	parent *cteFrame
}

// cteDef is one CTE together with the frame that declared it. A CTE body is
// always resolved in that frame, not in whatever scope happens to reference
// it: an outer CTE named from inside a subquery that shadows one of its
// dependencies must still see the outer definition.
type cteDef struct {
	node    *duckast.Node
	aliases []string // `WITH c(a, b)` column names
	frame   *cteFrame
}

// newExtractor creates an extractor seeded with the AST's CTE definitions.
func newExtractor(a *duckast.AST, columns ColumnResolver) *Extractor {
	e := &Extractor{
		resolved:  make(map[uintptr][]ColumnLineage),
		resolving: make(map[uintptr]bool),
		columns:   columns,
	}
	if stmts := a.Statements(); len(stmts) > 0 {
		e.pushCTEScope(stmts[0])
	}
	return e
}

// lookupCTE finds the CTE a name refers to from the current scope, innermost
// first. Identifiers are case-insensitive in DuckDB, quoted or not, so
// `WITH C AS (...) SELECT * FROM c` names the same CTE.
func (e *Extractor) lookupCTE(name string) *cteDef {
	key := strings.ToLower(name)
	for f := e.scope; f != nil; f = f.parent {
		if def, ok := f.defs[key]; ok {
			return def
		}
	}
	return nil
}

// resolveCTEOrdered resolves a CTE's output columns, recursively down to their
// source tables, in select-list order. The order is what lets `SELECT *` over
// a CTE be expanded; byName gives the by-name view. It reports false for an
// unknown name and for a CTE reached again while it is still being resolved.
func (e *Extractor) resolveCTEOrdered(cteName string) ([]ColumnLineage, bool) {
	def := e.lookupCTE(cteName)
	if def == nil {
		return nil, false
	}

	// Cache and recursion guard are both keyed on the body node, not the
	// name. Two different CTEs can share a name — an inner `WITH x` shadowing
	// an outer one — and since a body always resolves in its own defining
	// scope, the same body always has the same answer wherever it is named.
	ptr := reflect.ValueOf(def.node.Raw()).Pointer()
	if cols, ok := e.resolved[ptr]; ok {
		return cols, true
	}
	if e.resolving[ptr] {
		// A genuine self-reference, as in a RECURSIVE CTE's own body. Stop
		// here; the non-recursive arm supplies the real sources.
		return nil, false
	}
	e.resolving[ptr] = true
	defer delete(e.resolving, ptr)

	saved := e.scope
	e.scope = def.frame
	// Set operation CTEs (UNION / INTERSECT / EXCEPT, and every RECURSIVE
	// CTE) are handled by extractMainQuery like any other query body: names
	// from the left arm, sources merged positionally from both. A body may
	// also declare its own WITH.
	restore := e.pushCTEScope(def.node)
	cols := e.extractMainQuery(def.node)
	restore()
	e.scope = saved

	// `WITH c(a, b) AS ...` renames the body's leading columns; both a star
	// over c and a reference to c.a see the new names.
	if len(def.aliases) > 0 {
		renamed := make([]ColumnLineage, len(cols))
		copy(renamed, cols)
		for i := range renamed {
			// Past an unexpanded star the positions are unknown, so no later
			// column can be matched to its alias.
			if i >= len(def.aliases) || isPlaceholder(renamed[i]) {
				break
			}
			renamed[i].Column = def.aliases[i]
		}
		cols = renamed
	}
	e.resolved[ptr] = cols
	return cols, true
}

// isSetOp reports whether n combines a left and a right query. A RECURSIVE
// CTE body is its own node type in DuckDB's AST, but it has the same shape as
// a UNION: the anchor on the left, the recursive step on the right.
func isSetOp(n *duckast.Node) bool {
	return n.IsSetOpNode() || n.NodeType() == "RECURSIVE_CTE_NODE"
}

// byName indexes output columns by lower-cased name, since DuckDB resolves
// identifiers case-insensitively; look names up with strings.ToLower. On a
// duplicate name the first column wins.
func byName(cols []ColumnLineage) map[string][]SourceColumn {
	m := make(map[string][]SourceColumn, len(cols))
	for _, c := range cols {
		key := strings.ToLower(c.Column)
		if _, dup := m[key]; !dup {
			m[key] = c.Sources
		}
	}
	return m
}

// traceExprWithType traces an expression and returns detailed source info with transformation types.
func (e *Extractor) traceExprWithType(expr *duckast.Node, info aliasInfo) []SourceColumn {
	if expr.IsNil() {
		return nil
	}
	var sources []SourceColumn

	switch expr.Class() {
	case "COLUMN_REF":
		colNames := expr.ColumnNames()
		if len(colNames) == 0 {
			break
		}
		// DuckDB emits ColumnNames in the order
		//   [column], [tableAlias, column], [schema, table, column],
		//   [catalog, schema, table, column].
		// The right-most token is always the column; the token immediately
		// before it is the table alias (when present). Anything earlier
		// is schema/catalog and is dropped here — primary-table lookup
		// happens via the alias map. Without this, a 3-part reference
		// silently corrupted the column to "schema" and the table to "".
		var table, col, tableAlias string
		switch {
		case len(colNames) == 1:
			// A lambda parameter is bound by the lambda, not read from a
			// table. Resolving it against the FROM clause invented a column:
			// `list_transform(xs, x -> x || name)` reported a source column
			// literally called `x`.
			if info.bound[strings.ToLower(colNames[0])] {
				return nil
			}
			col = colNames[0]
			table = info.primaryTable
		case len(colNames) >= 2:
			col = colNames[len(colNames)-1]
			tableAlias = colNames[len(colNames)-2]
			table = info.aliases[strings.ToLower(tableAlias)]
			if table == "" {
				// 3-/4-part reference where the alias map didn't resolve:
				// fall back to the literal table name. The schema/catalog
				// prefix is intentionally not preserved on SourceColumn.
				table = tableAlias
			}
		}

		// Subquery alias: resolve via the subquery's own select list.
		// Has to be checked BEFORE the table-name path because subqueries
		// don't have a table name.
		if tableAlias != "" {
			if sub, isSubquery := info.subqueries[strings.ToLower(tableAlias)]; isSubquery {
				sources = append(sources, e.resolveSubqueryColumn(sub, col, info.subqueryCols[sub])...)
				return sources
			}
		}
		// Unqualified column with no primary table but a primary
		// subquery in FROM: resolve via the subquery's select list.
		if tableAlias == "" && table == "" && info.primarySubquery != nil {
			sources = append(sources, e.resolveSubqueryColumn(info.primarySubquery, col, info.subqueryCols[info.primarySubquery])...)
			return sources
		}

		if table != "" {
			aliasKey := tableAlias
			if aliasKey == "" {
				aliasKey = info.primaryAlias
			}
			if f := info.tableNodes[strings.ToLower(aliasKey)]; f != nil {
				col = e.unaliasColumn(f, col)
			}
			// Check if table is a CTE - resolve recursively
			if e.lookupCTE(table) != nil {
				resolved, ok := e.resolveCTEOrdered(table)
				if !ok {
					// A RECURSIVE body naming its own CTE. The anchor arm
					// supplies the real sources; recording the CTE's name
					// here would invent a table that does not exist.
					break
				}
				if deeper, ok := byName(resolved)[strings.ToLower(col)]; ok {
					sources = append(sources, deeper...)
				} else {
					sources = append(sources, SourceColumn{
						Table: table, Column: col, Transformation: TransformIdentity,
					})
				}
			} else {
				sources = append(sources, SourceColumn{
					Table: table, Column: col, Transformation: TransformIdentity,
				})
			}
		}

	case "FUNCTION":
		transType := classifyFunction(expr.FunctionName(), expr.IsOperator())
		appendFunc := func(cs SourceColumn) {
			transform, fn := keepAggregate(cs, transType, expr.FunctionName())
			sources = append(sources, SourceColumn{
				Table: cs.Table, Column: cs.Column,
				Transformation: transform, FunctionName: fn,
			})
		}
		for _, child := range expr.Children() {
			for _, cs := range e.traceExprWithType(child, info) {
				appendFunc(cs)
			}
		}
		// A plain aggregate carries FILTER and its own ORDER BY outside
		// children, under different field names from a WINDOW node: `filter`
		// rather than `filter_expr`, and `order_bys.orders` rather than
		// `orders`. `COUNT(*) FILTER (WHERE flag)` reads a column that decides
		// which rows count at all, so leaving it out understates the model's
		// dependencies. Shapes verified against json_serialize_sql.
		for _, cs := range e.traceExprWithType(expr.Field("filter"), info) {
			appendFunc(cs)
		}
		for _, ord := range expr.Field("order_bys").FieldList("orders") {
			for _, cs := range e.traceExprWithType(ord.Field("expression"), info) {
				appendFunc(cs)
			}
		}

	case "WINDOW":
		// Window function: SUM(amount) OVER (...). The aggregate function
		// name is on the WINDOW node itself; the aggregated expression is
		// in children. (Bug 23)
		transType := classifyFunction(expr.FunctionName(), expr.IsOperator())
		appendWindow := func(cs SourceColumn) {
			transform, fn := keepAggregate(cs, transType, expr.FunctionName())
			sources = append(sources, SourceColumn{
				Table: cs.Table, Column: cs.Column,
				Transformation: transform, FunctionName: fn,
			})
		}
		for _, child := range expr.Children() {
			for _, cs := range e.traceExprWithType(child, info) {
				appendWindow(cs)
			}
		}
		// PARTITION BY and ORDER BY are not children: DuckDB keeps them in
		// `partitions` and `orders`, and for a rank-style function like
		// row_number() `children` is empty altogether. Walking only children
		// left the columns that decide the result out of the lineage, so a
		// change to them never reached CDC. Shape verified against
		// json_serialize_sql.
		for _, part := range expr.FieldList("partitions") {
			for _, cs := range e.traceExprWithType(part, info) {
				appendWindow(cs)
			}
		}
		for _, ord := range expr.FieldList("orders") {
			for _, cs := range e.traceExprWithType(ord.Field("expression"), info) {
				appendWindow(cs)
			}
		}
		// The remaining single-expression fields also carry real column
		// references: FILTER (WHERE ...) decides which rows contribute at all,
		// and lag/lead take their offset and default as expressions. Frame
		// bounds are usually constants but need not be.
		for _, field := range []string{"filter_expr", "offset_expr", "default_expr", "start_expr", "end_expr"} {
			for _, cs := range e.traceExprWithType(expr.Field(field), info) {
				appendWindow(cs)
			}
		}
		// arg_orders is the aggregate's own ORDER BY inside OVER, as in
		// string_agg(v, ',' ORDER BY ts) — it changes the result, so the
		// columns it names are dependencies too.
		for _, ord := range expr.FieldList("arg_orders") {
			for _, cs := range e.traceExprWithType(ord.Field("expression"), info) {
				appendWindow(cs)
			}
		}

	case "CASE":
		appendConditional := func(inner SourceColumn) {
			transform, fn := keepInner(inner, TransformConditional)
			sources = append(sources, SourceColumn{
				Table: inner.Table, Column: inner.Column,
				Transformation: transform, FunctionName: fn,
			})
		}
		for _, check := range expr.CaseChecks() {
			for _, ts := range e.traceExprWithType(check.Then, info) {
				appendConditional(ts)
			}
			for _, ws := range e.traceExprWithType(check.When, info) {
				appendConditional(ws)
			}
		}
		// ELSE clause. DuckDB puts it under `else_expr`; a CASE node has no
		// `children` field at all, so the previous loop over Children() was a
		// silent no-op and an aggregate reachable only through ELSE never
		// appeared in the lineage — the same wrong-field mistake as the CAST
		// branch. Shape verified against json_serialize_sql.
		for _, cs := range e.traceExprWithType(expr.Field("else_expr"), info) {
			appendConditional(cs)
		}

	case "CAST":
		// The switch is on class, which DuckDB reports as "CAST"; the node's
		// `type` is "OPERATOR_CAST". Matching the type here silently disabled
		// the branch, so every `col::TYPE` and `CAST(col AS TYPE)` projection
		// produced a column with no sources at all.
		//
		// The operand also lives under `child` (singular), not `children`, so
		// the old body would have found nothing even had the case matched.
		for _, cs := range e.traceExprWithType(expr.Child(), info) {
			// A cast changes the type, not the derivation, so a more
			// informative inner classification survives it: SUM(x)::BIGINT
			// stays an aggregation of x. GetCDCTables depends on that to give
			// aggregated JOIN sources CDC, and the lineage view depends on
			// FunctionName to render [SUM] rather than a bare [CAST].
			transform, fn := keepInner(cs, TransformCast)
			sources = append(sources, SourceColumn{
				Table: cs.Table, Column: cs.Column,
				Transformation: transform, FunctionName: fn,
			})
		}

	case "SUBQUERY":
		// A subquery resolves names against its own FROM clause, so its
		// expressions must be traced with the subquery's aliases rather than
		// the outer query's. Without this case a scalar aggregate such as
		// `(SELECT SUM(x) FROM raw.events)` contributed no lineage at all, so
		// the runtime never learned that raw.events is aggregated here and
		// served a stale total after that source changed.
		if sub := expr.Field("subquery"); !sub.IsNil() {
			// A body may be a set operation, whose own select list is empty:
			// both arms are walked, or an aggregate inside a UNION arm would
			// contribute nothing. Every level may declare its own WITH — the
			// set operation itself as well as each arm — and each is in scope
			// for what lies beneath it.
			var visit func(inner *duckast.Node)
			visit = func(inner *duckast.Node) {
				if inner.IsNil() {
					return
				}
				restore := e.pushCTEScope(inner)
				defer restore()
				if isSetOp(inner) {
					visit(inner.SetOpLeft())
					visit(inner.SetOpRight())
					return
				}
				innerInfo := collectAliases(inner.FromTable())
				// A correlated subquery may name an outer alias, so the outer
				// scope is a fallback for anything its own FROM does not
				// define — inner shadows outer, as SQL resolves it. Without
				// this, `(SELECT MIN(b.label) FROM raw.events e ...)` recorded
				// a source table literally called "b", the alias, because the
				// COLUMN_REF lookup falls back to the bare name it was given.
				for alias, table := range info.aliases {
					if _, shadowed := innerInfo.aliases[alias]; !shadowed {
						innerInfo.aliases[alias] = table
					}
				}
				for alias, node := range info.subqueries {
					if _, shadowed := innerInfo.subqueries[alias]; !shadowed {
						innerInfo.subqueries[alias] = node
						innerInfo.subqueryCols[node] = info.subqueryCols[node]
					}
				}
				for alias, node := range info.tableNodes {
					if _, shadowed := innerInfo.tableNodes[alias]; !shadowed {
						innerInfo.tableNodes[alias] = node
					}
				}
				// Lambda parameters are only in scope for a subquery that
				// resolves against the outer query. One with its own FROM
				// defines its own names, and inheriting the binding there
				// silently dropped every column it reads.
				if innerInfo.primaryTable == "" && innerInfo.primarySubquery == nil {
					innerInfo.bound = info.bound
				}
				// A subquery with no FROM of its own — `(SELECT amount)` —
				// resolves an unqualified column entirely against the outer
				// query, so it inherits the outer primary too. Copying only
				// the alias map left such a column with an empty source table.
				if innerInfo.primaryTable == "" && innerInfo.primarySubquery == nil {
					innerInfo.primaryTable = info.primaryTable
					innerInfo.primarySubquery = info.primarySubquery
					innerInfo.primaryAlias = info.primaryAlias
					innerInfo.subqueryCols[info.primarySubquery] = info.subqueryCols[info.primarySubquery]
				}
				// A SCALAR subquery yields the value itself, so its column is
				// copied. An ANY / EXISTS subquery is a test: both the outer
				// expression and the columns it is tested against are
				// conditions, not copies.
				if subqueryType := expr.String("subquery_type"); subqueryType == "SCALAR" {
					for _, sel := range inner.SelectList() {
						sources = append(sources, e.traceExprWithType(sel, innerInfo)...)
					}
				} else {
					sources = append(sources, e.traceWrapped(inner.SelectList(), innerInfo,
						TransformConditional, operatorName(subqueryType))...)
				}
			}
			visit(sub.Field("node"))
		}
		// IN-style subqueries also test an outer expression, held in `child`,
		// which belongs to the enclosing scope. It is tested, not copied:
		// `name IN (SELECT ...)` reported IDENTITY, which is what
		// DetectRenames reads as a renamed column.
		sources = append(sources, e.traceWrapped([]*duckast.Node{expr.Child()}, info,
			TransformConditional, operatorName(expr.String("subquery_type")))...)

	case "COMPARISON":
		// Binary comparison operators (used in CASE WHEN, JOIN ON, etc.).
		// DuckDB stores operands in `left`/`right`, NOT `children`.
		// A comparison is a condition, not a copy: reporting IDENTITY would
		// let DetectRenames read `a = b` as a renamed column.
		operands := append([]*duckast.Node{expr.ExprLeft(), expr.ExprRight()}, expr.Children()...)
		sources = append(sources, e.traceWrapped(operands, info, TransformConditional, operatorName(expr.NodeType()))...)

	case "LAMBDA":
		// DuckDB writes the JSON arrow as a lambda too: `j -> 'a'` is
		// `lhs` = the column j, `expr` = the constant key. There `lhs` is a
		// real column, so binding it as a parameter emptied the lineage —
		// the very failure this walker exists to prevent. A lambda whose
		// body is a bare constant is that arrow, never a useful lambda.
		if expr.Field("expr").Class() == "CONSTANT" {
			sources = append(sources, e.traceExprWithType(expr.Field("lhs"), info)...)
			break
		}
		// `x -> x || name`: `lhs` binds the parameters, `expr` is the body.
		// Only the body reads columns, and the parameter names must not be
		// resolved against the FROM clause while it is traced.
		inner := info
		inner.bound = make(map[string]bool, len(info.bound)+1)
		for k := range info.bound {
			inner.bound[k] = true
		}
		for _, param := range lambdaParams(expr.Field("lhs")) {
			inner.bound[strings.ToLower(param)] = true
		}
		sources = append(sources, e.traceExprWithType(expr.Field("expr"), inner)...)

	case "OPERATOR", "CONJUNCTION", "BETWEEN":
		// COALESCE, IS NULL, NOT, IN, AND / OR, BETWEEN. None of them is a
		// FUNCTION node, so none was traced: `COALESCE(a.x, b.y)` produced a
		// column with no sources at all, which is most of what a dimension
		// model coalescing several registers is made of.
		transform, name := operatorTransform(expr.NodeType())
		sources = append(sources, e.traceWrapped(expr.ChildExpressions(), info, transform, name)...)

	default:
		// An expression class this walker does not name explicitly still has
		// operands, and they still carry the column references. Walking them
		// keeps an unknown or newly added class from contributing nothing —
		// the failure mode that hid casts, CASE else-branches and every
		// operator above until someone measured the lineage.
		// The wrapper is recorded as a FUNCTION rather than passed through:
		// an unknown construct is not a direct copy, and IDENTITY is what
		// DetectRenames reads as a renamed column.
		sources = append(sources, e.traceWrapped(expr.ChildExpressions(), info,
			TransformFunction, operatorName(expr.NodeType()))...)
	}

	return sources
}

// pushCTEScope registers the CTEs a subquery declares for itself and returns a
// function that undoes it.
//
// Only the top-level statement's CTEs are collected when the Extractor is
// built, so a `WITH` inside a scalar subquery was unknown: its name resolved
// to nothing and the COLUMN_REF fallback recorded the CTE alias as the source
// table. `(WITH x AS (SELECT SUM(f.amount) ... ) SELECT x.t FROM x)` came back
// as a source called "x", the aggregation was never seen, and a change to the
// real table was silently served stale.
//
// Each WITH opens a new frame over the current one, so an inner CTE reusing
// an outer name shadows it while in scope and the outer one is visible again
// once the returned function pops the frame.
func (e *Extractor) pushCTEScope(n *duckast.Node) func() {
	ctes := n.CTEs()
	if len(ctes) == 0 {
		return func() {}
	}
	frame := &cteFrame{defs: make(map[string]*cteDef, len(ctes)), parent: e.scope}
	for _, cte := range ctes {
		frame.defs[strings.ToLower(cte.Name)] = &cteDef{node: cte.Node, aliases: cte.Aliases, frame: frame}
	}
	e.scope = frame
	return func() { e.scope = frame.parent }
}

// keepInner decides the transformation recorded for a source reached through a
// wrapper expression (a cast, a CASE arm). A wrapper changes the type or the
// selection, not the derivation, so a more specific inner classification is
// what callers need: GetCDCTables reads AGGREGATION to decide that delta CDC
// is unsound for a model, and the lineage view reads FunctionName to render
// [SUM] rather than a bare label. Only an unclassified or identity source
// takes the wrapper's own label.
func keepInner(inner SourceColumn, wrapper TransformationType) (TransformationType, string) {
	if inner.Transformation == "" || inner.Transformation == TransformIdentity {
		return wrapper, ""
	}
	return inner.Transformation, inner.FunctionName
}

// traceWrapped traces operands and records each source as reached through a
// wrapper expression: the wrapper's classification and name apply unless the
// source already has a more specific one, as for a cast or a CASE arm.
func (e *Extractor) traceWrapped(operands []*duckast.Node, info aliasInfo, wrapper TransformationType, name string) []SourceColumn {
	var out []SourceColumn
	for _, operand := range operands {
		for _, cs := range e.traceExprWithType(operand, info) {
			transform, fn := keepInner(cs, wrapper)
			// Only a function-shaped wrapper names itself. A CASE arm records
			// a CONDITIONAL without a name, and a comparison that labelled
			// itself made the two disagree: the rendered view then showed
			// [MIX] for `CASE WHEN status = 'x' THEN name END`, and `model
			// impact` printed labels like `EQUAL()`.
			if wrapper == TransformFunction &&
				(cs.Transformation == "" || cs.Transformation == TransformIdentity) {
				fn = name
			}
			out = append(out, SourceColumn{
				Table: cs.Table, Column: cs.Column,
				Transformation: transform, FunctionName: fn,
			})
		}
	}
	return out
}

// operatorName turns an AST node type into a readable label: OPERATOR_IS_NULL
// becomes "IS NULL".
func operatorName(nodeType string) string {
	name := nodeType
	for _, prefix := range []string{"OPERATOR_", "CONJUNCTION_", "COMPARE_"} {
		name = strings.TrimPrefix(name, prefix)
	}
	return strings.ReplaceAll(name, "_", " ")
}

// lambdaParams returns the names a lambda's left-hand side binds. DuckDB
// writes a single parameter as a COLUMN_REF and several as a function node
// whose children are the names.
func lambdaParams(lhs *duckast.Node) []string {
	if lhs.IsNil() {
		return nil
	}
	if names := lhs.ColumnNames(); len(names) > 0 {
		return names[len(names)-1:]
	}
	var out []string
	for _, child := range lhs.ChildExpressions() {
		out = append(out, lambdaParams(child)...)
	}
	return out
}

// operatorTransform classifies an OPERATOR / CONJUNCTION / BETWEEN node from
// its AST type and gives it a readable name. The class is not only predicates:
// `COALESCE` picks a value and `a[1]`, `a[1:2]`, `(a).b` and `grouping(a)`
// read one out of a value, so those read as functions. What is left — IS NULL,
// NOT, IN, AND / OR, BETWEEN — is a condition, which is what CONDITIONAL
// already means for a CASE arm.
func operatorTransform(nodeType string) (TransformationType, string) {
	name := operatorName(nodeType)
	switch {
	case name == "COALESCE", name == "TRY", strings.Contains(name, "EXTRACT"),
		strings.Contains(name, "SLICE"), strings.Contains(name, "GROUPING"),
		strings.Contains(name, "UNPACK"):
		return TransformFunction, name
	}
	return TransformConditional, name
}

// keepAggregate decides what a scalar wrapper records for a source it reached
// through another expression. The wrapper's own classification normally wins —
// `SUM(a + b)` is an AGGREGATION of a and b, not arithmetic — but an
// AGGREGATION underneath survives, because GetCDCTables reads it to decide
// that a delta is unsound for the model. `upper(max(x))` used to come back as
// a plain function, and the source was then given delta CDC it cannot take.
func keepAggregate(inner SourceColumn, wrapper TransformationType, wrapperName string) (TransformationType, string) {
	if wrapper == TransformAggregation {
		return wrapper, wrapperName
	}
	// Outside an aggregate the rule is the one a cast and a CASE arm follow:
	// a more specific inner classification survives the wrapper.
	transform, fn := keepInner(inner, wrapper)
	if transform == wrapper && fn == "" {
		fn = wrapperName
	}
	return transform, fn
}

// classifyFunction determines the transformation type based on function name.
func classifyFunction(funcName string, isOperator bool) TransformationType {
	funcName = strings.ToUpper(funcName)
	if isOperator {
		switch funcName {
		case "+", "-", "*", "/", "%", "^":
			return TransformArithmetic
		}
	}
	switch funcName {
	case "SUM", "COUNT", "AVG", "MIN", "MAX", "FIRST", "LAST",
		"STDDEV", "STDDEV_POP", "STDDEV_SAMP", "VARIANCE", "VAR_POP", "VAR_SAMP",
		"STRING_AGG", "ARRAY_AGG", "LIST", "LISTAGG", "GROUP_CONCAT":
		return TransformAggregation
	}
	return TransformFunction
}

// extractMainQuery extracts lineage from the main query (after CTE resolution).
func (e *Extractor) extractMainQuery(n *duckast.Node) []ColumnLineage {
	// Set operation node: output schema from LEFT, each output column has
	// sources from BOTH sides (positionally aligned).
	if isSetOp(n) {
		// Each arm may carry its own WITH — `(WITH x AS (...) SELECT * FROM x)
		// UNION ALL ...` — which is in scope for that arm only.
		arm := func(side *duckast.Node) []ColumnLineage {
			if side.IsNil() {
				return nil
			}
			restore := e.pushCTEScope(side)
			defer restore()
			return e.extractMainQuery(side)
		}
		leftCols, rightCols := arm(n.SetOpLeft()), arm(n.SetOpRight())
		// An arm whose star could not be expanded has no positional columns
		// to merge. Keeping the other arm's columns would present sources
		// that silently leave that arm out, so the whole result is unknown.
		for _, cols := range [][]ColumnLineage{leftCols, rightCols} {
			for _, c := range cols {
				if isPlaceholder(c) {
					return unexpandedStar()
				}
			}
		}
		// UNION BY NAME matches columns by name, not position: the output is
		// the left side's columns followed by any the right side adds.
		if strings.HasSuffix(n.SetOpType(), "_BY_NAME") {
			merged := make([]ColumnLineage, 0, len(leftCols)+len(rightCols))
			index := make(map[string]int, len(leftCols))
			for _, lc := range leftCols {
				index[strings.ToLower(lc.Column)] = len(merged)
				merged = append(merged, ColumnLineage{Column: lc.Column, Sources: append([]SourceColumn{}, lc.Sources...)})
			}
			for _, rc := range rightCols {
				if at, ok := index[strings.ToLower(rc.Column)]; ok {
					merged[at].Sources = append(merged[at].Sources, rc.Sources...)
					continue
				}
				index[strings.ToLower(rc.Column)] = len(merged)
				merged = append(merged, ColumnLineage{Column: rc.Column, Sources: append([]SourceColumn{}, rc.Sources...)})
			}
			return merged
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
	aliases := collectAliases(n.FromTable())
	for _, expr := range n.SelectList() {
		if expr.Class() == "STAR" {
			result = append(result, e.expandStar(expr, n.FromTable(), aliases)...)
			continue
		}
		result = append(result, ColumnLineage{
			Column:  getOutputName(expr),
			Sources: e.traceExprWithType(expr, aliases),
		})
	}
	return result
}

// starColumn is one column a star expands to, with the relation that
// supplies it so a qualified EXCLUDE or RENAME can tell `a.id` from `b.id`.
type starColumn struct {
	relation string // alias, or table name when unaliased
	schema   string // schema and catalog as written, for an unaliased table
	catalog  string
	ColumnLineage
}

// matches reports whether a qualified EXCLUDE or RENAME key names c. Each part
// the key states must agree, so `EXCLUDE (s1.t.id)` over `s1.t JOIN s2.t`
// drops only s1's id, as DuckDB does.
func (c starColumn) matches(key *duckast.Node) bool {
	if !strings.EqualFold(key.String("column"), c.Column) {
		return false
	}
	for _, part := range []struct{ want, have string }{
		{key.String("table"), c.relation},
		{key.String("schema"), c.schema},
		{key.String("catalog"), c.catalog},
	} {
		if part.want != "" && !strings.EqualFold(part.want, part.have) {
			return false
		}
	}
	return true
}

// expandStar turns `*`, `t.*` and their EXCLUDE / REPLACE / RENAME forms into
// one lineage entry per column the star produces.
//
// A star used to become a single `?` column with no sources, which erased the
// lineage of every model written as `SELECT * FROM ...` or `* EXCLUDE (...)`.
// A CTE or a derived table is expanded from its own select list; a physical
// table needs its schema, which the AST does not carry, so it goes through
// the extractor's ColumnResolver. When any relation the star covers cannot be
// expanded — a table function, VALUES, COLUMNS(...), no resolver — the old
// single `?` entry is kept rather than a partial column list that would look
// complete.
func (e *Extractor) expandStar(star *duckast.Node, from *duckast.Node, aliases aliasInfo) []ColumnLineage {
	unknown := unexpandedStar()
	// COLUMNS(...) selects by pattern or lambda and `*` over an expression
	// unpacks a struct; neither is a plain relation expansion.
	if star.Bool("columns") || !star.Field("expr").IsNil() || from.IsNil() {
		return unknown
	}
	relation := star.String("relation_name")
	cols, ok := e.relationColumns(from, relation)
	if !ok || len(cols) == 0 {
		return unknown
	}

	excluded := func(c starColumn) bool {
		for _, name := range star.StringList("exclude_list") {
			if strings.EqualFold(name, c.Column) {
				return true
			}
		}
		for _, q := range star.FieldList("qualified_exclude_list") {
			if c.matches(q) {
				return true
			}
		}
		return false
	}
	renamed := func(c starColumn) string {
		for _, r := range star.FieldList("rename_list") {
			if c.matches(r.Field("key")) {
				return r.String("value")
			}
		}
		return c.Column
	}
	replacement := func(c starColumn) *duckast.Node {
		for _, r := range star.FieldList("replace_list") {
			if strings.EqualFold(r.String("key"), c.Column) {
				return r.Field("value")
			}
		}
		return nil
	}

	out := make([]ColumnLineage, 0, len(cols))
	replaced := make(map[string]bool)
	for _, c := range cols {
		if excluded(c) {
			continue
		}
		col := ColumnLineage{Column: renamed(c), Sources: c.Sources}
		if expr := replacement(c); !expr.IsNil() {
			// DuckDB replaces the first column of that name and drops any
			// later one: over a join where both sides have `ort`,
			// `* REPLACE (upper(b.ort) AS ort)` yields a single `ort`.
			key := strings.ToLower(c.Column)
			if replaced[key] {
				continue
			}
			replaced[key] = true
			col.Sources = e.traceExprWithType(expr, aliases)
		}
		out = append(out, col)
	}
	return out
}

// relationColumns lists the columns a star over the FROM item f produces, in
// output order. relation restricts it to the item with that alias (`t.*`);
// empty means every item. It reports false when a covered item cannot be
// expanded.
func (e *Extractor) relationColumns(f *duckast.Node, relation string) ([]starColumn, bool) {
	if f.IsNil() {
		return nil, true
	}
	if f.IsJoin() {
		left, ok := e.relationColumns(f.JoinLeft(), relation)
		if !ok {
			return nil, false
		}
		// A SEMI or ANTI join only filters the left side; its output has no
		// columns from the right.
		if jt := f.String("join_type"); jt == "SEMI" || jt == "ANTI" {
			return left, true
		}
		right, ok := e.relationColumns(f.JoinRight(), relation)
		if !ok {
			return nil, false
		}
		if relation == "" {
			left, right = mergeJoinedColumns(f, left, right)
		}
		return append(left, right...), true
	}

	name := f.Alias()
	if name == "" {
		name = f.TableName()
	}
	if relation != "" && !strings.EqualFold(name, relation) {
		return nil, true
	}

	var cols []ColumnLineage
	switch f.NodeType() {
	case "BASE_TABLE":
		table := f.TableName()
		if schema := f.SchemaName(); schema != "" {
			table = schema + "." + table
		}
		// A CTE name is never qualified, so only a bare name can be one —
		// otherwise `FROM s.t` would match a CTE quoted as "s.t".
		if f.SchemaName() == "" && f.CatalogName() == "" && e.lookupCTE(f.TableName()) != nil {
			resolved, ok := e.resolveCTEOrdered(f.TableName())
			if !ok {
				return nil, false
			}
			cols = resolved
			break
		}
		if e.columns == nil {
			return nil, false
		}
		names, ok := e.columns(f.CatalogName(), f.SchemaName(), f.TableName())
		if !ok || len(names) == 0 {
			return nil, false
		}
		for _, col := range names {
			cols = append(cols, ColumnLineage{
				Column:  col,
				Sources: []SourceColumn{{Table: table, Column: col, Transformation: TransformIdentity}},
			})
		}
	case "SUBQUERY":
		sub := f.SubqueryNode()
		restore := e.pushCTEScope(sub)
		cols = e.extractMainQuery(sub)
		restore()
	default:
		// Table functions, VALUES lists and the like have no column list
		// in the AST.
		return nil, false
	}
	// A CTE or subquery whose own star could not be expanded carries the
	// placeholder. Passing it on would put a `?` among real columns — or,
	// through a column alias list, give it a real name.
	for _, c := range cols {
		if isPlaceholder(c) {
			return nil, false
		}
	}

	// `FROM t AS x(a, b)` renames the leading columns positionally.
	aliasNames := f.StringList("column_name_alias")
	out := make([]starColumn, len(cols))
	for i, c := range cols {
		if i < len(aliasNames) {
			c.Column = aliasNames[i]
		}
		out[i] = starColumn{relation: name, ColumnLineage: c}
		if f.Alias() == "" {
			out[i].schema, out[i].catalog = f.SchemaName(), f.CatalogName()
		}
	}
	return out, true
}

// unaliasColumn maps a name from a FROM item's column alias list — `FROM t AS
// x(a, b)` — back to the column it renames, by position. For a CTE that is
// the CTE's own output column; for a table it needs the table's schema. When
// the position cannot be resolved the name is returned unchanged.
func (e *Extractor) unaliasColumn(f *duckast.Node, col string) string {
	idx := -1
	for i, a := range f.StringList("column_name_alias") {
		if strings.EqualFold(a, col) {
			idx = i
			break
		}
	}
	if idx < 0 {
		return col
	}
	if f.SchemaName() == "" && f.CatalogName() == "" && e.lookupCTE(f.TableName()) != nil {
		if cols, ok := e.resolveCTEOrdered(f.TableName()); ok && idx < len(cols) {
			return cols[idx].Column
		}
		return col
	}
	if e.columns != nil {
		if names, ok := e.columns(f.CatalogName(), f.SchemaName(), f.TableName()); ok && idx < len(names) {
			return names[idx]
		}
	}
	return col
}

// isPlaceholder reports whether c is the entry for a star that could not be
// expanded.
func isPlaceholder(c ColumnLineage) bool {
	return c.unexpanded
}

// mergeJoinedColumns handles the columns an unqualified star emits only once:
// those named in USING, and for a NATURAL join every column both sides share.
// They are dropped from the right side. For a RIGHT or FULL join the emitted
// value can come from the right table, so its sources are merged into the
// surviving left column rather than lost.
func mergeJoinedColumns(join *duckast.Node, left, right []starColumn) ([]starColumn, []starColumn) {
	shared := make(map[string]bool)
	for _, c := range join.StringList("using_columns") {
		shared[strings.ToLower(c)] = true
	}
	if join.String("ref_type") == "NATURAL" {
		names := make(map[string]bool, len(right))
		for _, c := range right {
			names[strings.ToLower(c.Column)] = true
		}
		for _, c := range left {
			if key := strings.ToLower(c.Column); names[key] {
				shared[key] = true
			}
		}
	}
	if len(shared) == 0 {
		return left, right
	}
	joinType := join.String("join_type")
	mergeRight := joinType == "RIGHT" || joinType == "FULL"

	kept := right[:0:0]
	for _, r := range right {
		key := strings.ToLower(r.Column)
		if !shared[key] {
			kept = append(kept, r)
			continue
		}
		if !mergeRight {
			continue
		}
		for i := range left {
			if strings.ToLower(left[i].Column) == key {
				left[i].Sources = append(append([]SourceColumn{}, left[i].Sources...), r.Sources...)
				break
			}
		}
	}
	return left, kept
}

// aliasInfo contains table alias mapping and the primary table.
type aliasInfo struct {
	aliases         map[string]string          // lower-cased alias -> table name
	subqueries      map[string]*duckast.Node   // lower-cased alias -> subquery body node
	subqueryCols    map[*duckast.Node][]string // subquery body -> `AS d(a, b)` column names
	tableNodes      map[string]*duckast.Node   // lower-cased alias -> FROM item naming a table or CTE
	bound           map[string]bool            // lower-cased lambda parameters, which name no column
	primaryAlias    string                     // alias of the primary table
	primaryTable    string                     // first table in FROM (for unqualified columns)
	primarySubquery *duckast.Node              // first FROM source if it's a subquery
}

// collectAliases builds a map of table aliases to fully qualified table
// names from a FROM clause. Subquery aliases (FROM (SELECT ...) AS sub)
// are recorded separately so column lineage can resolve them via the
// subquery's own select list.
func collectAliases(ft *duckast.Node) aliasInfo {
	info := aliasInfo{
		aliases:      make(map[string]string),
		subqueries:   make(map[string]*duckast.Node),
		subqueryCols: make(map[*duckast.Node][]string),
		tableNodes:   make(map[string]*duckast.Node),
	}
	if ft.IsNil() {
		return info
	}
	first := true
	var collect func(*duckast.Node)
	collect = func(f *duckast.Node) {
		if f.IsNil() {
			return
		}
		if name := f.TableName(); name != "" {
			qualifiedName := name
			if schema := f.SchemaName(); schema != "" {
				qualifiedName = schema + "." + name
			}
			alias := f.Alias()
			if alias == "" {
				alias = name
			}
			// Keyed lower-cased: DuckDB matches `X.id` to `FROM t AS x`.
			key := strings.ToLower(alias)
			info.aliases[key] = qualifiedName
			info.tableNodes[key] = f
			if first {
				info.primaryTable = qualifiedName
				info.primaryAlias = alias
				first = false
			}
		}
		// Subquery in FROM
		if sub := f.SubqueryNode(); !sub.IsNil() && f.Alias() != "" {
			info.subqueries[strings.ToLower(f.Alias())] = sub
			if cols := f.StringList("column_name_alias"); len(cols) > 0 {
				info.subqueryCols[sub] = cols
			}
			if first {
				info.primarySubquery = sub
				first = false
			}
		}
		if l := f.JoinLeft(); !l.IsNil() {
			collect(l)
		}
		if r := f.JoinRight(); !r.IsNil() {
			collect(r)
		}
	}
	collect(ft)
	return info
}

// resolveSubqueryColumn finds the lineage of a column inside a FROM-subquery.
// Set-op subqueries (UNION inside the subquery) merge LEFT/RIGHT positionally
// in extractMainQuery, and a star inside the subquery is expanded there too.
// colAliases is the derived table's `AS d(a, b)` list: those names replace
// the body's leading output names, so `d.a` is the body's first column.
func (e *Extractor) resolveSubqueryColumn(sub *duckast.Node, col string, colAliases []string) []SourceColumn {
	if sub.IsNil() {
		return nil
	}
	// A derived table may declare its own CTEs — `FROM (WITH x AS (...)
	// SELECT ...) d`. Only the top-level statement's CTEs are collected when
	// the Extractor is built, so without this the name resolves to nothing and
	// the CTE alias is recorded as the source table, hiding whatever it wraps.
	restore := e.pushCTEScope(sub)
	defer restore()

	cols := e.extractMainQuery(sub)
	for i, name := range colAliases {
		// Past an unexpanded star the positions are unknown.
		if i >= len(cols) || isPlaceholder(cols[i]) {
			break
		}
		cols[i].Column = name
	}
	return byName(cols)[strings.ToLower(col)]
}

// getOutputName extracts the output column name from an expression.
func getOutputName(expr *duckast.Node) string {
	if expr.IsNil() {
		return "?"
	}
	if alias := expr.Alias(); alias != "" {
		return alias
	}
	if names := expr.ColumnNames(); len(names) > 0 {
		return names[len(names)-1]
	}
	return "?"
}

// ----------------------------------------------------------------------
// Public API — extracts column lineage and table dependencies from SQL.
// ----------------------------------------------------------------------

// ExtractFromAST extracts column-level lineage from a pre-parsed AST JSON.
// A star over a physical table stays unexpanded; use ExtractFromASTWithColumns
// when a session is available to read table schemas.
func ExtractFromAST(astJSON string) ([]ColumnLineage, error) {
	return ExtractFromASTWithColumns(astJSON, nil)
}

// ExtractFromASTWithColumns is ExtractFromAST with a resolver for the columns
// of physical tables, which lets `SELECT *` over them be expanded.
func ExtractFromASTWithColumns(astJSON string, columns ColumnResolver) ([]ColumnLineage, error) {
	a, err := duckast.Parse(astJSON)
	if err != nil {
		return nil, fmt.Errorf("failed to parse AST: %w", err)
	}
	stmts := a.Statements()
	if len(stmts) == 0 {
		return nil, fmt.Errorf("no statements in AST")
	}
	extractor := newExtractor(a, columns)
	return extractor.extractMainQuery(stmts[0]), nil
}

// SessionColumns returns a ColumnResolver that reads table schemas from sess.
// It resolves names the way the model's own query does, against the session's
// default catalog, and caches each answer for the resolver's lifetime.
func SessionColumns(sess *duckdb.Session) ColumnResolver {
	type entry struct {
		names []string
		ok    bool
	}
	cache := make(map[[3]string]entry)
	return func(catalog, schema, table string) ([]string, bool) {
		key := [3]string{catalog, schema, table}
		if hit, found := cache[key]; found {
			return hit.names, hit.ok
		}
		var parts []string
		for _, p := range key {
			if p != "" {
				parts = append(parts, duckdb.QuoteIdentifier(p))
			}
		}
		// A failed DESCRIBE — the table is gone, or the name is not a table
		// at all — leaves the star unexpanded rather than failing lineage
		// for the whole model.
		names, err := sess.QueryRows(fmt.Sprintf("SELECT column_name FROM (DESCRIBE %s)", strings.Join(parts, ".")))
		res := entry{names: names, ok: err == nil && len(names) > 0}
		cache[key] = res
		return res.names, res.ok
	}
}

// GetAST fetches the parsed AST JSON from DuckDB for a SQL query.
// The result can be passed to ExtractFromASTWithColumns and
// ExtractTablesFromAST to avoid duplicate queries when both column lineage and
// tables are needed.
//
// DuckDB's json_serialize_sql only supports SELECT statements. For
// non-SELECT statements (CREATE, INSERT, UPDATE, DELETE, COPY, PIVOT,
// MERGE, etc.) it returns an error JSON of the form
// {"error":true,"error_type":"...","error_message":"..."} which we
// detect and surface as a real error rather than letting downstream
// parsers fail with a confusing "no statements in AST".
func GetAST(sess *duckdb.Session, sql string) (string, error) {
	// Escape single quotes for the SQL string
	escaped := strings.ReplaceAll(sql, "'", "''")

	// Use DuckDB's json_serialize_sql to get the AST.
	// Strip newlines so the CSV output isn't broken by multi-line values.
	query := fmt.Sprintf("SELECT REPLACE(REPLACE(CAST(json_serialize_sql('%s') AS VARCHAR), chr(10), ''), chr(13), '') AS ast", escaped)

	astJSON, err := sess.QueryValue(query)
	if err != nil {
		return "", fmt.Errorf("failed to serialize SQL: %w", err)
	}
	if astJSON == "" {
		return "", fmt.Errorf("empty AST returned")
	}
	// Detect json_serialize_sql's error envelope without parsing the
	// whole document — the error fields appear at the very start.
	if strings.HasPrefix(astJSON, `{"error":true`) {
		var probe struct {
			Error        bool   `json:"error"`
			ErrorType    string `json:"error_type"`
			ErrorMessage string `json:"error_message"`
		}
		if jsonErr := json.Unmarshal([]byte(astJSON), &probe); jsonErr == nil && probe.Error {
			return "", fmt.Errorf("DuckDB cannot serialize this statement (%s): %s",
				probe.ErrorType, probe.ErrorMessage)
		}
	}
	return astJSON, nil
}

// Extract extracts column-level lineage from a SQL query using DuckDB's AST parser.
// Note: If you also need table references, use GetAST +
// ExtractFromASTWithColumns(ast, SessionColumns(sess)) + ExtractTablesFromAST to
// avoid duplicate queries. Plain ExtractFromAST leaves a star over a physical
// table unexpanded.
func Extract(sess *duckdb.Session, sql string) ([]ColumnLineage, error) {
	astJSON, err := GetAST(sess, sql)
	if err != nil {
		return nil, err
	}
	return ExtractFromASTWithColumns(astJSON, SessionColumns(sess))
}

// TableRef represents a table reference with its role in the query.
type TableRef struct {
	Table       string // Full table name (schema.table)
	Alias       string // Alias used in query
	IsFirstFrom bool   // True if this is the first table in FROM clause (primary source)
	IsJoin      bool   // True if this table is from a JOIN clause
}

// ExtractTables extracts all table references from a SQL query using DuckDB's AST parser.
func ExtractTables(sess *duckdb.Session, sql string) ([]TableRef, error) {
	astJSON, err := GetAST(sess, sql)
	if err != nil {
		return nil, err
	}
	return ExtractTablesFromAST(astJSON)
}

// ExtractTablesFromAST extracts table references from a pre-parsed AST JSON.
//
// Implementation: walks the entire tree via duckast.AST.Walk so it discovers
// every BASE_TABLE node regardless of which AST shape contains it. CTEs are
// excluded since they aren't physical tables.
//
// The "primary" table (the leftmost physical FROM in the outermost SELECT,
// used by CDC to choose the fact table) is located by a separate structural
// traversal — NOT by walk order — because the walker visits map fields in
// sorted-key order, which is deterministic but bears no relation to the
// SQL FROM-clause semantics. The structural traversal descends
// SET_OPERATION_NODE.left chains, then JOIN.left chains, then through
// derived-table SUBQUERYs, returning the first non-CTE BASE_TABLE found.
//
// Self-joins and multiple references to the same table are preserved as
// distinct TableRef entries (with their respective aliases). Callers that
// want a unique-name list should use GetAllTablesFromRefs.
func ExtractTablesFromAST(astJSON string) ([]TableRef, error) {
	a, err := duckast.Parse(astJSON)
	if err != nil {
		return nil, fmt.Errorf("failed to parse AST: %w", err)
	}
	stmts := a.Statements()
	if len(stmts) == 0 {
		return nil, fmt.Errorf("no statements in AST")
	}

	// Locate the primary FROM table structurally, walking the tree
	// top-down with a per-scope CTE set so a CTE in one subquery cannot
	// shadow a physical table in an outer scope. We compare the located
	// node against walker hits by underlying-map pointer identity, so
	// even self-joins disambiguate cleanly.
	primary := findPrimaryBaseTable(stmts[0], nil)
	var primaryPtr uintptr
	if primary != nil {
		primaryPtr = reflect.ValueOf(primary.Raw()).Pointer()
	}

	var tables []TableRef
	collectTablesScoped(stmts[0], nil, primaryPtr, &tables)
	return tables, nil
}

// collectTablesScoped recursively walks the AST and collects every
// physical BASE_TABLE node into out, respecting SQL CTE scoping.
//
// Each SELECT_NODE that defines CTEs adds those names to the scope
// visible to its descendants (and to its sibling CTE bodies, matching
// DuckDB's WITH semantics). A BASE_TABLE is treated as a CTE reference
// only when it is unqualified (no schema, no catalog) and its name is
// in the active scope. Schema- or catalog-qualified BASE_TABLE nodes
// are always physical, even if a same-named CTE is in scope.
//
// Generic descent into child fields preserves the walker's "find every
// BASE_TABLE regardless of AST shape" guarantee — only the SELECT_NODE
// case is special-cased, for the scope push.
func collectTablesScoped(n *duckast.Node, parentScope map[string]bool, primaryPtr uintptr, out *[]TableRef) {
	if n.IsNil() {
		return
	}

	// Push this node's local CTE names onto the scope visible to its
	// children (and to itself, for RECURSIVE CTEs that reference their
	// own name from inside their body). Only allocate when there are
	// actually local CTEs to add.
	scope := parentScope
	if ctes := n.CTEs(); len(ctes) > 0 {
		scope = make(map[string]bool, len(parentScope)+len(ctes))
		for k := range parentScope {
			scope[k] = true
		}
		for _, cte := range ctes {
			scope[strings.ToLower(cte.Name)] = true
		}
	}

	if n.IsBaseTable() {
		name := n.TableName()
		if name == "" {
			return
		}
		// CTEs only shadow unqualified table names. A schema- or
		// catalog-qualified ref is always physical.
		schema := n.SchemaName()
		catalog := n.CatalogName()
		if schema == "" && catalog == "" && scope[strings.ToLower(name)] {
			return
		}
		fullName := name
		if schema != "" {
			fullName = schema + "." + name
		}
		if catalog != "" {
			fullName = catalog + "." + fullName
		}
		isPrimary := primaryPtr != 0 && reflect.ValueOf(n.Raw()).Pointer() == primaryPtr
		*out = append(*out, TableRef{
			Table:       fullName,
			Alias:       n.Alias(),
			IsFirstFrom: isPrimary,
			IsJoin:      !isPrimary,
		})
		return
	}

	// Recurse into every child field with the current scope. Iterate
	// keys in sorted order so the resulting TableRef order is stable.
	raw := n.Raw()
	keys := make([]string, 0, len(raw))
	for k := range raw {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		recurseScopedValue(raw[k], scope, primaryPtr, out)
	}
}

func recurseScopedValue(v any, scope map[string]bool, primaryPtr uintptr, out *[]TableRef) {
	switch x := v.(type) {
	case map[string]any:
		collectTablesScoped(duckast.NewNode(x), scope, primaryPtr, out)
	case []any:
		for _, item := range x {
			recurseScopedValue(item, scope, primaryPtr, out)
		}
	}
}

// findPrimaryBaseTable returns the leftmost physical BASE_TABLE node
// reachable from the outermost SELECT's FROM clause, skipping CTE refs
// according to SQL scope rules.
//
// Used by ExtractTablesFromAST to deterministically tag the "primary"
// (fact) table for CDC. The result is matched back against the walker
// pass by raw-map pointer identity, so callers don't need to worry
// about name collisions across self-joins.
//
// Returns nil if the statement has no physical primary (e.g. only
// references CTEs, only references table functions, or is a SELECT
// with no FROM at all).
//
// parentScope holds CTE names from enclosing scopes; this function
// adds the current SELECT's local CTE names before descending.
func findPrimaryBaseTable(stmt *duckast.Node, parentScope map[string]bool) *duckast.Node {
	if stmt.IsNil() {
		return nil
	}
	// Descend SET_OPERATION_NODE.left chain to the leftmost SELECT-shaped
	// branch — the "primary" of `A UNION B` is whatever A's primary is.
	n := stmt
	for isSetOp(n) {
		n = n.SetOpLeft()
		if n.IsNil() {
			return nil
		}
	}
	if !n.IsSelectNode() {
		return nil
	}
	scope := parentScope
	if ctes := n.CTEs(); len(ctes) > 0 {
		scope = make(map[string]bool, len(parentScope)+len(ctes))
		for k := range parentScope {
			scope[k] = true
		}
		for _, cte := range ctes {
			scope[strings.ToLower(cte.Name)] = true
		}
	}
	return descendFromTable(n.FromTable(), scope)
}

// descendFromTable left-first walks a from_table subtree and returns
// the first physical BASE_TABLE encountered, descending through joins
// (left first) and through derived-table SUBQUERYs. CTE shadowing only
// applies to unqualified names.
func descendFromTable(node *duckast.Node, scope map[string]bool) *duckast.Node {
	if node.IsNil() {
		return nil
	}
	switch node.NodeType() {
	case "BASE_TABLE":
		if node.SchemaName() == "" && node.CatalogName() == "" && scope[strings.ToLower(node.TableName())] {
			return nil
		}
		return node
	case "JOIN":
		if found := descendFromTable(node.JoinLeft(), scope); found != nil {
			return found
		}
		return descendFromTable(node.JoinRight(), scope)
	case "SUBQUERY":
		// Derived table — descend into the inner SELECT, which may add
		// its own CTE names on top of the inherited scope.
		return findPrimaryBaseTable(node.SubqueryNode(), scope)
	}
	return nil
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

// ExtractAll extracts both column lineage and table dependencies from a single
// AST serialisation. More efficient than calling Extract and GetAllTables
// separately; a star over a physical table still costs a DESCRIBE per table.
func ExtractAll(sess *duckdb.Session, sql string) ([]ColumnLineage, []string, error) {
	astJSON, err := GetAST(sess, sql)
	if err != nil {
		return nil, nil, err
	}
	colLineage, err := ExtractFromASTWithColumns(astJSON, SessionColumns(sess))
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
				cdcTables[src.Table] = true
			}
		}
	}
	return cdcTables
}
