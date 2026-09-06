// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package lineage

import (
	"testing"

	"github.com/ondatra-labs/ondatrasql/internal/duckast"
)

// A CAST node reports class "CAST" and type "OPERATOR_CAST", and holds its
// operand under `child`, not `children`. The extractor switched on class but
// matched the type, so the branch never fired and every `col::TYPE` projection
// came back with no sources at all — including every `@fetch` and `@push`
// model, whose strict contracts require `col::TYPE AS alias` per projection.

const castColumnAST = `{"error":false,"statements":[{"node":{"type":"SELECT_NODE","cte_map":{"map":[]},"select_list":[` +
	`{"class":"CAST","type":"OPERATOR_CAST","alias":"total","child":` +
	`{"class":"COLUMN_REF","type":"COLUMN_REF","alias":"","column_names":["f","amount"]},` +
	`"cast_type":{"id":"BIGINT"},"try_cast":false}],` +
	`"from_table":{"type":"BASE_TABLE","alias":"f","table_name":"events"}}}]}`

const castAggregateAST = `{"error":false,"statements":[{"node":{"type":"SELECT_NODE","cte_map":{"map":[]},"select_list":[` +
	`{"class":"CAST","type":"OPERATOR_CAST","alias":"total","child":` +
	`{"class":"FUNCTION","type":"FUNCTION","alias":"","function_name":"sum","schema":"","children":[` +
	`{"class":"COLUMN_REF","type":"COLUMN_REF","alias":"","column_names":["f","amount"]}],"is_operator":false},` +
	`"cast_type":{"id":"BIGINT"},"try_cast":false}],` +
	`"from_table":{"type":"BASE_TABLE","alias":"f","table_name":"events"}}}]}`

func TestExtractFromAST_CastKeepsSource(t *testing.T) {
	t.Parallel()
	lineages, err := ExtractFromAST(castColumnAST)
	if err != nil {
		t.Fatalf("ExtractFromAST failed: %v", err)
	}
	if len(lineages) != 1 {
		t.Fatalf("expected 1 column, got %d", len(lineages))
	}
	got := lineages[0]
	if len(got.Sources) != 1 {
		t.Fatalf("a cast must not drop the source column, got %+v", got.Sources)
	}
	src := got.Sources[0]
	if src.Table != "events" || src.Column != "amount" {
		t.Errorf("expected source events.amount, got %s.%s", src.Table, src.Column)
	}
	if src.Transformation != TransformCast {
		t.Errorf("expected transformation %q, got %q", TransformCast, src.Transformation)
	}
}

func TestExtractFromAST_CastKeepsInnerAggregation(t *testing.T) {
	t.Parallel()
	// SUM(x)::BIGINT is still an aggregation of x. The cast changes the type,
	// not the derivation, so the inner classification has to survive it —
	// GetCDCTables reads it to decide whether an aggregated JOIN source needs
	// change data capture.
	lineages, err := ExtractFromAST(castAggregateAST)
	if err != nil {
		t.Fatalf("ExtractFromAST failed: %v", err)
	}
	if len(lineages) != 1 || len(lineages[0].Sources) != 1 {
		t.Fatalf("expected 1 column with 1 source, got %+v", lineages)
	}
	src := lineages[0].Sources[0]
	if src.Transformation != TransformAggregation {
		t.Errorf("a cast around SUM must stay an aggregation, got %q", src.Transformation)
	}
	if src.FunctionName != "sum" {
		t.Errorf("the function name must survive the cast, got %q", src.FunctionName)
	}

	if cdc := GetCDCTables(lineages); !cdc["events"] {
		t.Errorf("a cast-wrapped aggregated source must reach GetCDCTables, got %v", cdc)
	}
}

// A CASE arm is a wrapper too: it selects which value flows through, it does
// not change how that value was derived. Flattening SUM(x) to CONDITIONAL
// would hide the aggregation from GetCDCTables, and an aggregation missed
// there is one the runtime will incorrectly serve from a CDC delta.
const caseAggregateAST = `{"error":false,"statements":[{"node":{"type":"SELECT_NODE","cte_map":{"map":[]},"select_list":[` +
	`{"class":"CAST","type":"OPERATOR_CAST","alias":"total","child":` +
	`{"class":"CASE","type":"CASE_EXPR","alias":"","case_checks":[{` +
	`"when_expr":{"class":"COLUMN_REF","type":"COLUMN_REF","column_names":["f","flag"]},` +
	`"then_expr":{"class":"FUNCTION","type":"FUNCTION","function_name":"sum","schema":"","children":[` +
	`{"class":"COLUMN_REF","type":"COLUMN_REF","column_names":["f","amount"]}],"is_operator":false}}],` +
	`"else_expr":null},"cast_type":{"id":"BIGINT"},"try_cast":false}],` +
	`"from_table":{"type":"BASE_TABLE","alias":"f","table_name":"events"}}}]}`

func TestExtractFromAST_CaseKeepsInnerAggregation(t *testing.T) {
	t.Parallel()
	lineages, err := ExtractFromAST(caseAggregateAST)
	if err != nil {
		t.Fatalf("ExtractFromAST failed: %v", err)
	}
	if len(lineages) != 1 {
		t.Fatalf("expected 1 column, got %d", len(lineages))
	}

	var agg *SourceColumn
	for i := range lineages[0].Sources {
		if lineages[0].Sources[i].Column == "amount" {
			agg = &lineages[0].Sources[i]
		}
	}
	if agg == nil {
		t.Fatalf("the aggregated source must survive CASE and CAST, got %+v", lineages[0].Sources)
	}
	if agg.Transformation != TransformAggregation {
		t.Errorf("SUM inside a CASE arm must stay an aggregation, got %q", agg.Transformation)
	}
	if agg.FunctionName != "sum" {
		t.Errorf("the function name must survive both wrappers, got %q", agg.FunctionName)
	}
	if cdc := GetCDCTables(lineages); !cdc["events"] {
		t.Errorf("the source must reach GetCDCTables, got %v", cdc)
	}

	// The WHEN predicate is a plain column, so it keeps the wrapper's label.
	for _, src := range lineages[0].Sources {
		if src.Column == "flag" && src.Transformation != TransformConditional {
			t.Errorf("an unclassified CASE source should read as conditional, got %q", src.Transformation)
		}
	}
}

// A correlated subquery may name an outer alias. Its own FROM does not define
// that alias, and the COLUMN_REF fallback records whatever bare name it was
// given — so without an outer-scope fallback the lineage claimed a source
// table literally called "b".
const correlatedSubqueryAST = `{"error":false,"statements":[{"node":{"type":"SELECT_NODE","cte_map":{"map":[]},"select_list":[` +
	`{"class":"SUBQUERY","type":"SUBQUERY","alias":"lbl","subquery_type":"SCALAR","child":null,"subquery":{"node":{` +
	`"type":"SELECT_NODE","cte_map":{"map":[]},"select_list":[` +
	`{"class":"FUNCTION","type":"FUNCTION","function_name":"min","schema":"","children":[` +
	`{"class":"COLUMN_REF","type":"COLUMN_REF","column_names":["b","label"]}],"is_operator":false}],` +
	`"from_table":{"type":"BASE_TABLE","alias":"e","table_name":"events"}}}}],` +
	`"from_table":{"type":"BASE_TABLE","alias":"b","table_name":"base"}}}]}`

func TestExtractFromAST_CorrelatedSubqueryResolvesOuterAlias(t *testing.T) {
	t.Parallel()
	lineages, err := ExtractFromAST(correlatedSubqueryAST)
	if err != nil {
		t.Fatalf("ExtractFromAST failed: %v", err)
	}
	if len(lineages) != 1 || len(lineages[0].Sources) != 1 {
		t.Fatalf("expected 1 column with 1 source, got %+v", lineages)
	}
	src := lineages[0].Sources[0]
	if src.Table != "base" {
		t.Errorf("an outer alias must resolve to its table, got %q (an alias leaked as a table name)", src.Table)
	}
	if src.Column != "label" {
		t.Errorf("expected column label, got %q", src.Column)
	}
}

// A subquery body can be a set operation, whose own select_list is empty. Its
// arms have to be descended into, or an aggregate inside a UNION contributes
// no lineage and the runtime never learns the source is aggregated.
const setOpSubqueryAST = `{"error":false,"statements":[{"node":{"type":"SELECT_NODE","cte_map":{"map":[]},"select_list":[` +
	`{"class":"SUBQUERY","type":"SUBQUERY","alias":"total","subquery_type":"SCALAR","child":null,"subquery":{"node":{` +
	`"type":"SET_OPERATION_NODE","setop_type":"UNION",` +
	`"left":{"type":"SELECT_NODE","cte_map":{"map":[]},"select_list":[` +
	`{"class":"FUNCTION","type":"FUNCTION","function_name":"sum","schema":"","children":[` +
	`{"class":"COLUMN_REF","type":"COLUMN_REF","column_names":["e","amount"]}],"is_operator":false}],` +
	`"from_table":{"type":"BASE_TABLE","alias":"e","table_name":"events"}},` +
	`"right":{"type":"SELECT_NODE","cte_map":{"map":[]},"select_list":[` +
	`{"class":"FUNCTION","type":"FUNCTION","function_name":"sum","schema":"","children":[` +
	`{"class":"COLUMN_REF","type":"COLUMN_REF","column_names":["a","amount"]}],"is_operator":false}],` +
	`"from_table":{"type":"BASE_TABLE","alias":"a","table_name":"archive"}}}}}],` +
	`"from_table":{"type":"BASE_TABLE","alias":"b","table_name":"base"}}}]}`

func TestExtractFromAST_SetOperationSubqueryArms(t *testing.T) {
	t.Parallel()
	lineages, err := ExtractFromAST(setOpSubqueryAST)
	if err != nil {
		t.Fatalf("ExtractFromAST failed: %v", err)
	}
	if len(lineages) != 1 {
		t.Fatalf("expected 1 column, got %d", len(lineages))
	}

	seen := map[string]TransformationType{}
	for _, src := range lineages[0].Sources {
		seen[src.Table] = src.Transformation
	}
	for _, table := range []string{"events", "archive"} {
		if seen[table] != TransformAggregation {
			t.Errorf("both UNION arms must contribute an aggregation, %q has %q", table, seen[table])
		}
	}
	if cdc := GetCDCTables(lineages); !cdc["events"] || !cdc["archive"] {
		t.Errorf("both arms must reach GetCDCTables, got %v", cdc)
	}
}

// A `WITH` inside a scalar subquery declares a CTE the Extractor never saw:
// only the top-level statement's CTEs are collected up front. The name then
// resolved to nothing and the COLUMN_REF fallback recorded the CTE alias
// itself as the source table, hiding the aggregation underneath it.
//
// cte_map shape verified against json_serialize_sql.
const innerCTESubqueryAST = `{"error":false,"statements":[{"node":{"type":"SELECT_NODE","cte_map":{"map":[]},"select_list":[` +
	`{"class":"SUBQUERY","type":"SUBQUERY","alias":"total","subquery_type":"SCALAR","child":null,"subquery":{"node":{` +
	`"type":"SELECT_NODE","cte_map":{"map":[{"key":"x","value":{"aliases":[],"query":{"node":{` +
	`"type":"SELECT_NODE","cte_map":{"map":[]},"select_list":[` +
	`{"class":"FUNCTION","type":"FUNCTION","alias":"t","function_name":"sum","schema":"","children":[` +
	`{"class":"COLUMN_REF","type":"COLUMN_REF","column_names":["f","amount"]}],"is_operator":false}],` +
	`"from_table":{"type":"BASE_TABLE","alias":"f","table_name":"events"}}}}}]},` +
	`"select_list":[{"class":"COLUMN_REF","type":"COLUMN_REF","column_names":["x","t"]}],` +
	`"from_table":{"type":"BASE_TABLE","alias":"","table_name":"x"}}}}],` +
	`"from_table":{"type":"BASE_TABLE","alias":"b","table_name":"base"}}}]}`

func TestExtractFromAST_SubqueryLocalCTEResolves(t *testing.T) {
	t.Parallel()
	lineages, err := ExtractFromAST(innerCTESubqueryAST)
	if err != nil {
		t.Fatalf("ExtractFromAST failed: %v", err)
	}
	if len(lineages) != 1 || len(lineages[0].Sources) == 0 {
		t.Fatalf("expected 1 column with sources, got %+v", lineages)
	}
	src := lineages[0].Sources[0]
	if src.Table == "x" {
		t.Fatalf("the CTE alias leaked as a source table: %+v", src)
	}
	if src.Table != "events" || src.Column != "amount" {
		t.Errorf("expected source events.amount, got %s.%s", src.Table, src.Column)
	}
	if src.Transformation != TransformAggregation {
		t.Errorf("the aggregation under the CTE must survive, got %q", src.Transformation)
	}
	if cdc := GetCDCTables(lineages); !cdc["events"] {
		t.Errorf("the physical table must reach GetCDCTables, got %v", cdc)
	}
}

// A subquery-local CTE shadows an outer one of the same name only while it is
// in scope; the outer definition has to be intact afterwards.
func TestExtractFromAST_SubqueryCTEShadowingIsRestored(t *testing.T) {
	t.Parallel()
	// Register an outer CTE, push a subquery scope that reuses the name, then
	// confirm the outer body is restored.
	e := &Extractor{
		cteNodes: map[string]*duckast.Node{
			"x": duckast.NewNode(map[string]any{"type": "SELECT_NODE", "marker": "outer"}),
		},
		resolved:  map[string]map[string][]SourceColumn{},
		resolving: map[uintptr]bool{},
	}
	inner := duckast.NewNode(map[string]any{
		"type": "SELECT_NODE",
		"cte_map": map[string]any{"map": []any{map[string]any{
			"key":   "x",
			"value": map[string]any{"query": map[string]any{"node": map[string]any{"type": "SELECT_NODE", "marker": "inner"}}},
		}}},
	})
	restore := e.pushCTEScope(inner)
	if got := e.cteNodes["x"].String("marker"); got != "inner" {
		t.Errorf("inner CTE must shadow the outer one, got %q", got)
	}
	restore()
	if got := e.cteNodes["x"].String("marker"); got != "outer" {
		t.Errorf("the outer CTE must be restored after the subquery, got %q", got)
	}
}

// A subquery-local CTE may reuse a name that is currently being resolved, and
// its body may reference that name. resolveCTE uses the entry in e.resolved as
// its recursion guard, so clearing that entry to shadow the name left the
// in-flight resolution writing into a map that no longer existed — a panic on
// user SQL, not a wrong answer.
const selfReferencingShadowedCTEAST = `{"error":false,"statements":[{"node":{"type":"SELECT_NODE",` +
	`"cte_map":{"map":[{"key":"x","value":{"query":{"node":{` +
	`"type":"SELECT_NODE","cte_map":{"map":[]},"select_list":[` +
	`{"class":"SUBQUERY","type":"SUBQUERY","alias":"v","subquery_type":"SCALAR","child":null,"subquery":{"node":{` +
	`"type":"SELECT_NODE","cte_map":{"map":[{"key":"x","value":{"query":{"node":{` +
	`"type":"SELECT_NODE","cte_map":{"map":[]},"select_list":[` +
	`{"class":"COLUMN_REF","type":"COLUMN_REF","alias":"c","column_names":["x","c"]}],` +
	`"from_table":{"type":"BASE_TABLE","alias":"","table_name":"x"}}}}}]},` +
	`"select_list":[{"class":"COLUMN_REF","type":"COLUMN_REF","column_names":["x","c"]}],` +
	`"from_table":{"type":"BASE_TABLE","alias":"","table_name":"x"}}}}],` +
	`"from_table":{"type":"BASE_TABLE","alias":"","table_name":"src"}}}}}]},` +
	`"select_list":[{"class":"COLUMN_REF","type":"COLUMN_REF","column_names":["x","v"]}],` +
	`"from_table":{"type":"BASE_TABLE","alias":"","table_name":"x"}}}]}`

func TestExtractFromAST_SelfReferencingShadowedCTEDoesNotPanic(t *testing.T) {
	t.Parallel()
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("extraction must not panic on a shadowed self-referencing CTE: %v", r)
		}
	}()
	if _, err := ExtractFromAST(selfReferencingShadowedCTEAST); err != nil {
		t.Logf("returned an error, which is an acceptable outcome: %v", err)
	}
}

// A CASE node has no `children` field: DuckDB puts the ELSE branch under
// `else_expr`. Iterating Children() was a silent no-op, so an aggregate
// reachable only through ELSE contributed no lineage and the source never
// reached GetCDCTables.
const caseElseAggregateAST = `{"error":false,"statements":[{"node":{"type":"SELECT_NODE","cte_map":{"map":[]},"select_list":[` +
	`{"class":"CASE","type":"CASE_EXPR","alias":"total","case_checks":[{` +
	`"when_expr":{"class":"COLUMN_REF","type":"COLUMN_REF","column_names":["f","flag"]},` +
	`"then_expr":{"class":"COLUMN_REF","type":"COLUMN_REF","column_names":["f","fallback"]}}],` +
	`"else_expr":{"class":"FUNCTION","type":"FUNCTION","function_name":"sum","schema":"","children":[` +
	`{"class":"COLUMN_REF","type":"COLUMN_REF","column_names":["f","amount"]}],"is_operator":false}}],` +
	`"from_table":{"type":"BASE_TABLE","alias":"f","table_name":"events"}}}]}`

func TestExtractFromAST_CaseElseBranchIsTraced(t *testing.T) {
	t.Parallel()
	lineages, err := ExtractFromAST(caseElseAggregateAST)
	if err != nil {
		t.Fatalf("ExtractFromAST failed: %v", err)
	}
	if len(lineages) != 1 {
		t.Fatalf("expected 1 column, got %d", len(lineages))
	}

	var found *SourceColumn
	for i := range lineages[0].Sources {
		if lineages[0].Sources[i].Column == "amount" {
			found = &lineages[0].Sources[i]
		}
	}
	if found == nil {
		t.Fatalf("the ELSE branch must contribute lineage, got %+v", lineages[0].Sources)
	}
	if found.Transformation != TransformAggregation {
		t.Errorf("an aggregate in ELSE must stay an aggregation, got %q", found.Transformation)
	}
	if cdc := GetCDCTables(lineages); !cdc["events"] {
		t.Errorf("the ELSE source must reach GetCDCTables, got %v", cdc)
	}
}

// A derived table may declare its own CTEs. resolveSubqueryColumn resolved
// names against the derived table's FROM only, so `FROM (WITH x AS (SELECT
// SUM(...) ...) SELECT x.t) d` recorded the CTE alias as the source table and
// the aggregation underneath it never reached GetCDCTables.
const derivedTableCTEAST = `{"error":false,"statements":[{"node":{"type":"SELECT_NODE","modifiers":[],"cte_map":{"map":[]},"select_list":[{"class":"COLUMN_REF","type":"COLUMN_REF","alias":"total","query_location":7,"column_names":["d","t"]}],"from_table":{"type":"SUBQUERY","alias":"d","sample":null,"query_location":18446744073709551615,"subquery":{"node":{"type":"SELECT_NODE","modifiers":[],"cte_map":{"map":[{"key":"x","value":{"aliases":[],"query":{"node":{"type":"SELECT_NODE","modifiers":[],"cte_map":{"map":[]},"select_list":[{"class":"FUNCTION","type":"FUNCTION","alias":"t","query_location":44,"function_name":"sum","schema":"","children":[{"class":"COLUMN_REF","type":"COLUMN_REF","alias":"","query_location":48,"column_names":["f","amount"]}],"filter":null,"order_bys":{"type":"ORDER_MODIFIER","orders":[]},"distinct":false,"is_operator":false,"export_state":false,"catalog":""}],"from_table":{"type":"BASE_TABLE","alias":"f","sample":null,"query_location":68,"schema_name":"","table_name":"events","column_name_alias":[],"catalog_name":"","at_clause":null},"where_clause":null,"group_expressions":[],"group_sets":[],"aggregate_handling":"STANDARD_HANDLING","having":null,"sample":null,"qualify":null},"named_param_map":[]},"materialized":"CTE_MATERIALIZE_DEFAULT","key_targets":[]}}]},"select_list":[{"class":"COLUMN_REF","type":"COLUMN_REF","alias":"t","query_location":85,"column_names":["x","t"]}],"from_table":{"type":"BASE_TABLE","alias":"","sample":null,"query_location":99,"schema_name":"","table_name":"x","column_name_alias":[],"catalog_name":"","at_clause":null},"where_clause":null,"group_expressions":[],"group_sets":[],"aggregate_handling":"STANDARD_HANDLING","having":null,"sample":null,"qualify":null},"named_param_map":[]},"column_name_alias":[]},"where_clause":null,"group_expressions":[],"group_sets":[],"aggregate_handling":"STANDARD_HANDLING","having":null,"sample":null,"qualify":null},"named_param_map":[]}]}`

func TestExtractFromAST_DerivedTableLocalCTEResolves(t *testing.T) {
	t.Parallel()
	lineages, err := ExtractFromAST(derivedTableCTEAST)
	if err != nil {
		t.Fatalf("ExtractFromAST failed: %v", err)
	}
	if len(lineages) != 1 || len(lineages[0].Sources) == 0 {
		t.Fatalf("expected 1 column with sources, got %+v", lineages)
	}
	src := lineages[0].Sources[0]
	if src.Table == "x" {
		t.Fatalf("the CTE alias leaked as a source table: %+v", src)
	}
	if src.Table != "events" {
		t.Errorf("expected source table events, got %q", src.Table)
	}
	if cdc := GetCDCTables(lineages); !cdc["events"] {
		t.Errorf("the physical table must reach GetCDCTables, got %v", cdc)
	}
}

// A scalar subquery inside a CTE's own body may declare a CTE with the same
// name. The recursion guard is keyed on the body node rather than the name, so
// the inner one is resolved as the distinct query it is instead of being
// mistaken for a self-reference and dropped.
//
// Generated with json_serialize_sql from:
//
//	WITH x AS (SELECT (WITH x AS (SELECT SUM(b.amount) AS t FROM archive b)
//	                   SELECT x.t FROM x) AS v FROM events e)
//	SELECT x.v AS out FROM x
const shadowedInFlightCTEAST = `{"error":false,"statements":[{"node":{"type":"SELECT_NODE","modifiers":[],"cte_map":{"map":[{"key":"x","value":{"aliases":[],"query":{"node":{"type":"SELECT_NODE","modifiers":[],"cte_map":{"map":[]},"select_list":[{"class":"SUBQUERY","type":"SUBQUERY","alias":"v","query_location":18,"subquery_type":"SCALAR","subquery":{"node":{"type":"SELECT_NODE","modifiers":[],"cte_map":{"map":[{"key":"x","value":{"aliases":[],"query":{"node":{"type":"SELECT_NODE","modifiers":[],"cte_map":{"map":[]},"select_list":[{"class":"FUNCTION","type":"FUNCTION","alias":"t","query_location":37,"function_name":"sum","schema":"","children":[{"class":"COLUMN_REF","type":"COLUMN_REF","alias":"","query_location":41,"column_names":["b","amount"]}],"filter":null,"order_bys":{"type":"ORDER_MODIFIER","orders":[]},"distinct":false,"is_operator":false,"export_state":false,"catalog":""}],"from_table":{"type":"BASE_TABLE","alias":"b","sample":null,"query_location":61,"schema_name":"","table_name":"archive","column_name_alias":[],"catalog_name":"","at_clause":null},"where_clause":null,"group_expressions":[],"group_sets":[],"aggregate_handling":"STANDARD_HANDLING","having":null,"sample":null,"qualify":null},"named_param_map":[]},"materialized":"CTE_MATERIALIZE_DEFAULT","key_targets":[]}}]},"select_list":[{"class":"COLUMN_REF","type":"COLUMN_REF","alias":"","query_location":79,"column_names":["x","t"]}],"from_table":{"type":"BASE_TABLE","alias":"","sample":null,"query_location":88,"schema_name":"","table_name":"x","column_name_alias":[],"catalog_name":"","at_clause":null},"where_clause":null,"group_expressions":[],"group_sets":[],"aggregate_handling":"STANDARD_HANDLING","having":null,"sample":null,"qualify":null},"named_param_map":[]},"child":null,"comparison_type":"INVALID"}],"from_table":{"type":"BASE_TABLE","alias":"e","sample":null,"query_location":101,"schema_name":"","table_name":"events","column_name_alias":[],"catalog_name":"","at_clause":null},"where_clause":null,"group_expressions":[],"group_sets":[],"aggregate_handling":"STANDARD_HANDLING","having":null,"sample":null,"qualify":null},"named_param_map":[]},"materialized":"CTE_MATERIALIZE_DEFAULT","key_targets":[]}}]},"select_list":[{"class":"COLUMN_REF","type":"COLUMN_REF","alias":"out","query_location":118,"column_names":["x","v"]}],"from_table":{"type":"BASE_TABLE","alias":"","sample":null,"query_location":134,"schema_name":"","table_name":"x","column_name_alias":[],"catalog_name":"","at_clause":null},"where_clause":null,"group_expressions":[],"group_sets":[],"aggregate_handling":"STANDARD_HANDLING","having":null,"sample":null,"qualify":null},"named_param_map":[]}]}`

func TestExtractFromAST_ShadowedCTEInFlightKeepsInnerSources(t *testing.T) {
	t.Parallel()
	lineages, err := ExtractFromAST(shadowedInFlightCTEAST)
	if err != nil {
		t.Fatalf("ExtractFromAST failed: %v", err)
	}
	if len(lineages) != 1 || len(lineages[0].Sources) != 1 {
		t.Fatalf("expected 1 column with 1 source, got %+v", lineages)
	}
	src := lineages[0].Sources[0]
	if src.Table == "x" {
		t.Fatalf("the shadowing CTE alias leaked as a source table: %+v", src)
	}
	if src.Table != "archive" || src.Column != "amount" {
		t.Errorf("expected source archive.amount, got %s.%s", src.Table, src.Column)
	}
	if src.Transformation != TransformAggregation {
		t.Errorf("the inner aggregate must survive, got %q", src.Transformation)
	}
	if cdc := GetCDCTables(lineages); !cdc["archive"] {
		t.Errorf("the inner aggregated source must reach GetCDCTables, got %v", cdc)
	}
}

// A WINDOW node keeps PARTITION BY in `partitions` and ORDER BY in `orders`,
// not in `children` — which for row_number() is empty altogether. Walking only
// children left the columns that decide the result out of the lineage, so a
// change to them never reached CDC.
//
// Generated with json_serialize_sql from:
//
//	SELECT row_number() OVER (PARTITION BY f.grp ORDER BY f.ts) AS rn FROM events f
const windowPartitionOrderAST = `{"error":false,"statements":[{"node":{"type":"SELECT_NODE","modifiers":[],"cte_map":{"map":[]},"select_list":[{"class":"WINDOW","type":"WINDOW_ROW_NUMBER","alias":"rn","query_location":7,"function_name":"row_number","schema":"","catalog":"","children":[],"partitions":[{"class":"COLUMN_REF","type":"COLUMN_REF","alias":"","query_location":39,"column_names":["f","grp"]}],"orders":[{"type":"ORDER_DEFAULT","null_order":"ORDER_DEFAULT","expression":{"class":"COLUMN_REF","type":"COLUMN_REF","alias":"","query_location":54,"column_names":["f","ts"]}}],"start":"UNBOUNDED_PRECEDING","end":"CURRENT_ROW_RANGE","start_expr":null,"end_expr":null,"offset_expr":null,"default_expr":null,"ignore_nulls":false,"filter_expr":null,"exclude_clause":"NO_OTHER","distinct":false,"arg_orders":[]}],"from_table":{"type":"BASE_TABLE","alias":"f","sample":null,"query_location":71,"schema_name":"","table_name":"events","column_name_alias":[],"catalog_name":"","at_clause":null},"where_clause":null,"group_expressions":[],"group_sets":[],"aggregate_handling":"STANDARD_HANDLING","having":null,"sample":null,"qualify":null},"named_param_map":[]}]}`

func TestExtractFromAST_WindowPartitionAndOrderAreTraced(t *testing.T) {
	t.Parallel()
	lineages, err := ExtractFromAST(windowPartitionOrderAST)
	if err != nil {
		t.Fatalf("ExtractFromAST failed: %v", err)
	}
	if len(lineages) != 1 {
		t.Fatalf("expected 1 column, got %d", len(lineages))
	}

	seen := map[string]bool{}
	for _, src := range lineages[0].Sources {
		if src.Table != "events" {
			t.Errorf("expected every source in events, got %s.%s", src.Table, src.Column)
		}
		seen[src.Column] = true
	}
	for _, col := range []string{"grp", "ts"} {
		if !seen[col] {
			t.Errorf("the window's %s column must appear in lineage, got %+v", col, lineages[0].Sources)
		}
	}
}

// A window function's FILTER clause decides which rows contribute at all, and
// lag/lead take their offset and default as expressions — none of which are
// children. Generated with json_serialize_sql from:
//
//	SELECT sum(f.amt) FILTER (WHERE f.ok) OVER (PARTITION BY f.grp ORDER BY f.ts) AS s
//	FROM events f
const windowFilterAST = `{"error":false,"statements":[{"node":{"type":"SELECT_NODE","modifiers":[],"cte_map":{"map":[]},"select_list":[{"class":"WINDOW","type":"WINDOW_AGGREGATE","alias":"s","query_location":7,"function_name":"sum","schema":"","catalog":"","children":[{"class":"COLUMN_REF","type":"COLUMN_REF","alias":"","query_location":11,"column_names":["f","amt"]}],"partitions":[{"class":"COLUMN_REF","type":"COLUMN_REF","alias":"","query_location":57,"column_names":["f","grp"]}],"orders":[{"type":"ORDER_DEFAULT","null_order":"ORDER_DEFAULT","expression":{"class":"COLUMN_REF","type":"COLUMN_REF","alias":"","query_location":72,"column_names":["f","ts"]}}],"start":"UNBOUNDED_PRECEDING","end":"CURRENT_ROW_RANGE","start_expr":null,"end_expr":null,"offset_expr":null,"default_expr":null,"ignore_nulls":false,"filter_expr":{"class":"COLUMN_REF","type":"COLUMN_REF","alias":"","query_location":32,"column_names":["f","ok"]},"exclude_clause":"NO_OTHER","distinct":false,"arg_orders":[]}],"from_table":{"type":"BASE_TABLE","alias":"f","sample":null,"query_location":88,"schema_name":"","table_name":"events","column_name_alias":[],"catalog_name":"","at_clause":null},"where_clause":null,"group_expressions":[],"group_sets":[],"aggregate_handling":"STANDARD_HANDLING","having":null,"sample":null,"qualify":null},"named_param_map":[]}]}`

func TestExtractFromAST_WindowFilterIsTraced(t *testing.T) {
	t.Parallel()
	lineages, err := ExtractFromAST(windowFilterAST)
	if err != nil {
		t.Fatalf("ExtractFromAST failed: %v", err)
	}
	if len(lineages) != 1 {
		t.Fatalf("expected 1 column, got %d", len(lineages))
	}
	seen := map[string]bool{}
	for _, src := range lineages[0].Sources {
		seen[src.Column] = true
	}
	for _, col := range []string{"amt", "ok", "grp", "ts"} {
		if !seen[col] {
			t.Errorf("window column %s must appear in lineage, got %+v", col, lineages[0].Sources)
		}
	}
}

// A plain aggregate keeps FILTER under `filter` and its own ORDER BY under
// `order_bys.orders` — different field names from a WINDOW node, and neither
// of them children. Generated with json_serialize_sql from:
//
//	SELECT f.g AS g, count(*) FILTER (WHERE f.flag) AS c,
//	       string_agg(f.v, chr(44) ORDER BY f.ts) AS s
//	FROM events f GROUP BY f.g
const aggregateFilterOrderAST = `{"error":false,"statements":[{"node":{"type":"SELECT_NODE","modifiers":[],"cte_map":{"map":[]},"select_list":[{"class":"COLUMN_REF","type":"COLUMN_REF","alias":"g","query_location":7,"column_names":["f","g"]},{"class":"FUNCTION","type":"FUNCTION","alias":"c","query_location":17,"function_name":"count_star","schema":"","children":[],"filter":{"class":"COLUMN_REF","type":"COLUMN_REF","alias":"","query_location":40,"column_names":["f","flag"]},"order_bys":{"type":"ORDER_MODIFIER","orders":[]},"distinct":false,"is_operator":false,"export_state":false,"catalog":""},{"class":"FUNCTION","type":"FUNCTION","alias":"s","query_location":54,"function_name":"string_agg","schema":"","children":[{"class":"COLUMN_REF","type":"COLUMN_REF","alias":"","query_location":65,"column_names":["f","v"]},{"class":"FUNCTION","type":"FUNCTION","alias":"","query_location":70,"function_name":"chr","schema":"","children":[{"class":"CONSTANT","type":"VALUE_CONSTANT","alias":"","query_location":74,"value":{"type":{"id":"INTEGER","type_info":null},"is_null":false,"value":44}}],"filter":null,"order_bys":{"type":"ORDER_MODIFIER","orders":[]},"distinct":false,"is_operator":false,"export_state":false,"catalog":""}],"filter":null,"order_bys":{"type":"ORDER_MODIFIER","orders":[{"type":"ORDER_DEFAULT","null_order":"ORDER_DEFAULT","expression":{"class":"COLUMN_REF","type":"COLUMN_REF","alias":"","query_location":87,"column_names":["f","ts"]}}]},"distinct":false,"is_operator":false,"export_state":false,"catalog":""}],"from_table":{"type":"BASE_TABLE","alias":"f","sample":null,"query_location":103,"schema_name":"","table_name":"events","column_name_alias":[],"catalog_name":"","at_clause":null},"where_clause":null,"group_expressions":[{"class":"COLUMN_REF","type":"COLUMN_REF","alias":"","query_location":121,"column_names":["f","g"]}],"group_sets":[[0]],"aggregate_handling":"STANDARD_HANDLING","having":null,"sample":null,"qualify":null},"named_param_map":[]}]}`

func TestExtractFromAST_AggregateFilterAndOrderAreTraced(t *testing.T) {
	t.Parallel()
	lineages, err := ExtractFromAST(aggregateFilterOrderAST)
	if err != nil {
		t.Fatalf("ExtractFromAST failed: %v", err)
	}
	seen := map[string]bool{}
	for _, col := range lineages {
		for _, src := range col.Sources {
			seen[src.Column] = true
		}
	}
	// `flag` decides which rows COUNT sees; `ts` decides string_agg's output.
	for _, col := range []string{"flag", "ts"} {
		if !seen[col] {
			t.Errorf("aggregate column %s must appear in lineage, got %v", col, seen)
		}
	}
	if cdc := GetCDCTables(lineages); !cdc["events"] {
		t.Errorf("the aggregated source must reach GetCDCTables, got %v", cdc)
	}
}
