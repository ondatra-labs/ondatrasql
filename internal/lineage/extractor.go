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
}

// Extractor holds state for recursive CTE resolution.
type Extractor struct {
	cteNodes  map[string]*duckast.Node             // CTE name -> body node
	resolved  map[string]map[string][]SourceColumn // CTE name -> col -> final sources
	resolving map[uintptr]bool                     // CTE body nodes currently being resolved
}

// newExtractor creates an extractor seeded with the AST's CTE definitions.
func newExtractor(a *duckast.AST) *Extractor {
	e := &Extractor{
		cteNodes:  make(map[string]*duckast.Node),
		resolved:  make(map[string]map[string][]SourceColumn),
		resolving: make(map[uintptr]bool),
	}
	stmts := a.Statements()
	if len(stmts) == 0 {
		return e
	}
	for _, cte := range stmts[0].CTEs() {
		e.cteNodes[cte.Name] = cte.Node
	}
	return e
}

// resolveCTE recursively resolves a CTE's columns to their ultimate source tables.
func (e *Extractor) resolveCTE(cteName string) map[string][]SourceColumn {
	if cols, ok := e.resolved[cteName]; ok {
		return cols
	}

	n, ok := e.cteNodes[cteName]
	if !ok {
		return nil
	}

	// Recursion guard, kept separate from the cache. It used to be the cache
	// entry itself, which made the two jobs collide: pushCTEScope has to clear
	// the cache to shadow a name, and doing that to an in-flight entry left
	// the writes below landing in a map that was no longer there. Results are
	// built locally and published once, so nothing depends on the entry
	// staying put mid-resolution.
	// Keyed on the body node, not the name. Two different CTEs can share a
	// name — an inner `WITH x` shadowing an outer one — and a name-keyed guard
	// would mistake the inner for a self-reference and drop its sources.
	ptr := reflect.ValueOf(n.Raw()).Pointer()
	if e.resolving[ptr] {
		// A genuine self-reference, as in a RECURSIVE CTE's own body. Stop
		// here; the non-recursive arm supplies the real sources.
		return nil
	}
	e.resolving[ptr] = true
	defer delete(e.resolving, ptr)

	cols := make(map[string][]SourceColumn)

	// Set operation CTE (UNION / UNION ALL / INTERSECT / EXCEPT — also how
	// RECURSIVE CTEs are represented). The SET_OPERATION node has empty
	// SelectList; column names come from LEFT, sources are the positional
	// merge of LEFT + RIGHT.
	if n.IsSetOpNode() {
		for k, v := range e.resolveSetOpCols(n) {
			cols[k] = v
		}
		e.resolved[cteName] = cols
		return cols
	}

	aliases := collectAliases(n.FromTable())
	for _, expr := range n.SelectList() {
		cols[getOutputName(expr)] = e.traceExprWithType(expr, aliases)
	}
	e.resolved[cteName] = cols
	return cols
}

// resolveSetOpCols resolves the column lineage for a SET_OPERATION node.
// The result map mirrors the LEFT side's output names, with sources merged
// positionally from both sides.
func (e *Extractor) resolveSetOpCols(n *duckast.Node) map[string][]SourceColumn {
	leftCols := e.resolveSelectNodeCols(n.SetOpLeft())
	rightCols := e.resolveSelectNodeCols(n.SetOpRight())

	type orderedCol struct {
		name    string
		sources []SourceColumn
	}
	var ordered []orderedCol
	if left := n.SetOpLeft(); !left.IsNil() {
		for _, expr := range nestedSelectList(left) {
			outName := getOutputName(expr)
			ordered = append(ordered, orderedCol{name: outName, sources: leftCols[outName]})
		}
	}
	if right := n.SetOpRight(); !right.IsNil() {
		rightExprs := nestedSelectList(right)
		for i, expr := range rightExprs {
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
func (e *Extractor) resolveSelectNodeCols(n *duckast.Node) map[string][]SourceColumn {
	if n.IsNil() {
		return nil
	}
	if n.IsSetOpNode() {
		return e.resolveSetOpCols(n)
	}
	result := make(map[string][]SourceColumn)
	aliases := collectAliases(n.FromTable())
	for _, expr := range n.SelectList() {
		outName := getOutputName(expr)
		result[outName] = e.traceExprWithType(expr, aliases)
	}
	return result
}

// nestedSelectList returns the LEFT-most SELECT_NODE's select list for a
// possibly-nested set-op node. Used to derive positional column order from
// arbitrarily nested UNIONs.
func nestedSelectList(n *duckast.Node) []*duckast.Node {
	for n.IsSetOpNode() {
		left := n.SetOpLeft()
		if left.IsNil() {
			break
		}
		n = left
	}
	return n.SelectList()
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
			col = colNames[0]
			table = info.primaryTable
		case len(colNames) >= 2:
			col = colNames[len(colNames)-1]
			tableAlias = colNames[len(colNames)-2]
			table = info.aliases[tableAlias]
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
			if sub, isSubquery := info.subqueries[tableAlias]; isSubquery {
				sources = append(sources, e.resolveSubqueryColumn(sub, col)...)
				return sources
			}
		}
		// Unqualified column with no primary table but a primary
		// subquery in FROM: resolve via the subquery's select list.
		if tableAlias == "" && table == "" && info.primarySubquery != nil {
			sources = append(sources, e.resolveSubqueryColumn(info.primarySubquery, col)...)
			return sources
		}

		if table != "" {
			// Check if table is a CTE - resolve recursively
			if _, isCTE := e.cteNodes[table]; isCTE {
				cteCols := e.resolveCTE(table)
				if deeper, ok := cteCols[col]; ok {
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
			sources = append(sources, SourceColumn{
				Table: cs.Table, Column: cs.Column,
				Transformation: transType, FunctionName: expr.FunctionName(),
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
			sources = append(sources, SourceColumn{
				Table: cs.Table, Column: cs.Column,
				Transformation: transType, FunctionName: expr.FunctionName(),
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
			for _, inner := range subqueryBodies(sub.Field("node")) {
				restore := e.pushCTEScope(inner)
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
					}
				}
				// A subquery with no FROM of its own — `(SELECT amount)` —
				// resolves an unqualified column entirely against the outer
				// query, so it inherits the outer primary too. Copying only
				// the alias map left such a column with an empty source table.
				if innerInfo.primaryTable == "" && innerInfo.primarySubquery == nil {
					innerInfo.primaryTable = info.primaryTable
					innerInfo.primarySubquery = info.primarySubquery
				}
				for _, sel := range inner.SelectList() {
					sources = append(sources, e.traceExprWithType(sel, innerInfo)...)
				}
				restore()
			}
		}
		// IN-style subqueries also test an outer expression, held in `child`,
		// which belongs to the enclosing scope.
		sources = append(sources, e.traceExprWithType(expr.Child(), info)...)

	case "COMPARISON":
		// Binary comparison operators (used in CASE WHEN, JOIN ON, etc.).
		// DuckDB stores operands in `left`/`right`, NOT `children`.
		sources = append(sources, e.traceExprWithType(expr.ExprLeft(), info)...)
		sources = append(sources, e.traceExprWithType(expr.ExprRight(), info)...)
		for _, child := range expr.Children() {
			sources = append(sources, e.traceExprWithType(child, info)...)
		}
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
// Names are shadowed rather than merged, so an inner CTE reusing an outer
// name resolves to its own body while it is in scope and the outer one is put
// back afterwards. Cached resolutions are dropped on both edges, since the
// same name now means a different query.
func (e *Extractor) pushCTEScope(n *duckast.Node) func() {
	ctes := n.CTEs()
	if len(ctes) == 0 {
		return func() {}
	}
	type saved struct {
		node *duckast.Node
		had  bool
	}
	prev := make(map[string]saved, len(ctes))
	for _, cte := range ctes {
		old, had := e.cteNodes[cte.Name]
		prev[cte.Name] = saved{node: old, had: had}
		e.cteNodes[cte.Name] = cte.Node
		delete(e.resolved, cte.Name)
	}
	return func() {
		for name, s := range prev {
			delete(e.resolved, name)
			if s.had {
				e.cteNodes[name] = s.node
			} else {
				delete(e.cteNodes, name)
			}
		}
	}
}

// subqueryBodies flattens a subquery body into the SELECT nodes that actually
// carry a select list. A body may be a set operation, whose own SelectList()
// is empty — without descending into both sides, an aggregate inside a
// `UNION` arm contributes no lineage and the runtime never learns the source
// is aggregated.
func subqueryBodies(n *duckast.Node) []*duckast.Node {
	if n.IsNil() {
		return nil
	}
	if n.IsSetOpNode() {
		return append(subqueryBodies(n.SetOpLeft()), subqueryBodies(n.SetOpRight())...)
	}
	return []*duckast.Node{n}
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
	if n.IsSetOpNode() {
		var leftCols, rightCols []ColumnLineage
		if left := n.SetOpLeft(); !left.IsNil() {
			leftCols = e.extractMainQuery(left)
		}
		if right := n.SetOpRight(); !right.IsNil() {
			rightCols = e.extractMainQuery(right)
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
		result = append(result, ColumnLineage{
			Column:  getOutputName(expr),
			Sources: e.traceExprWithType(expr, aliases),
		})
	}
	return result
}

// aliasInfo contains table alias mapping and the primary table.
type aliasInfo struct {
	aliases         map[string]string        // alias -> table name
	subqueries      map[string]*duckast.Node // alias -> subquery body node
	primaryTable    string                   // first table in FROM (for unqualified columns)
	primarySubquery *duckast.Node            // first FROM source if it's a subquery
}

// collectAliases builds a map of table aliases to fully qualified table
// names from a FROM clause. Subquery aliases (FROM (SELECT ...) AS sub)
// are recorded separately so column lineage can resolve them via the
// subquery's own select list.
func collectAliases(ft *duckast.Node) aliasInfo {
	info := aliasInfo{
		aliases:    make(map[string]string),
		subqueries: make(map[string]*duckast.Node),
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
			info.aliases[alias] = qualifiedName
			if first {
				info.primaryTable = qualifiedName
				first = false
			}
		}
		// Subquery in FROM
		if sub := f.SubqueryNode(); !sub.IsNil() && f.Alias() != "" {
			info.subqueries[f.Alias()] = sub
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
// Handles set-op subqueries (UNION inside the subquery) by delegating to
// resolveSelectNodeCols which knows how to merge LEFT/RIGHT positionally.
func (e *Extractor) resolveSubqueryColumn(sub *duckast.Node, col string) []SourceColumn {
	if sub.IsNil() {
		return nil
	}
	// A derived table may declare its own CTEs — `FROM (WITH x AS (...)
	// SELECT ...) d`. Only the top-level statement's CTEs are collected when
	// the Extractor is built, so without this the name resolves to nothing and
	// the CTE alias is recorded as the source table, hiding whatever it wraps.
	restore := e.pushCTEScope(sub)
	defer restore()

	if sub.IsSetOpNode() {
		cols := e.resolveSelectNodeCols(sub)
		return cols[col]
	}
	subAliases := collectAliases(sub.FromTable())
	for _, expr := range sub.SelectList() {
		if getOutputName(expr) == col {
			return e.traceExprWithType(expr, subAliases)
		}
	}
	return nil
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
func ExtractFromAST(astJSON string) ([]ColumnLineage, error) {
	a, err := duckast.Parse(astJSON)
	if err != nil {
		return nil, fmt.Errorf("failed to parse AST: %w", err)
	}
	stmts := a.Statements()
	if len(stmts) == 0 {
		return nil, fmt.Errorf("no statements in AST")
	}
	extractor := newExtractor(a)
	return extractor.extractMainQuery(stmts[0]), nil
}

// GetAST fetches the parsed AST JSON from DuckDB for a SQL query.
// The result can be passed to ExtractFromAST and ExtractTablesFromAST
// to avoid duplicate queries when both column lineage and tables are needed.
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
			scope[cte.Name] = true
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
		if schema == "" && catalog == "" && scope[name] {
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
	for n.IsSetOpNode() {
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
			scope[cte.Name] = true
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
		if node.SchemaName() == "" && node.CatalogName() == "" && scope[node.TableName()] {
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
				cdcTables[src.Table] = true
			}
		}
	}
	return cdcTables
}
