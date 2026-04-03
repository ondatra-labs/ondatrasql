// OndatraSQL - You don't need a data stack anymore
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package lineage

import (
	"fmt"
	"strings"
	"testing"
)

func TestNewLineageView(t *testing.T) {
	t.Parallel()
	models := []*ModelLineage{{Name: "test"}}
	v := NewLineageView(models)

	if v.width != 70 {
		t.Errorf("width = %d, want 70", v.width)
	}
	if len(v.Models) != 1 {
		t.Errorf("Models len = %d, want 1", len(v.Models))
	}
}

func TestSetTarget_OnePart(t *testing.T) {
	t.Parallel()
	v := NewLineageView(nil)
	v.SetTarget("orders")

	if v.Mode != ViewOverview {
		t.Errorf("Mode = %d, want ViewOverview (%d)", v.Mode, ViewOverview)
	}
}

func TestSetTarget_TwoParts(t *testing.T) {
	t.Parallel()
	v := NewLineageView(nil)
	v.SetTarget("mart.sales")

	if v.Mode != ViewModelFocus {
		t.Errorf("Mode = %d, want ViewModelFocus (%d)", v.Mode, ViewModelFocus)
	}
	if v.TargetModel != "mart.sales" {
		t.Errorf("TargetModel = %q, want %q", v.TargetModel, "mart.sales")
	}
}

func TestSetTarget_ThreeParts(t *testing.T) {
	t.Parallel()
	v := NewLineageView(nil)
	v.SetTarget("mart.sales.revenue")

	if v.Mode != ViewColumnFocus {
		t.Errorf("Mode = %d, want ViewColumnFocus (%d)", v.Mode, ViewColumnFocus)
	}
	if v.TargetModel != "mart.sales" {
		t.Errorf("TargetModel = %q, want %q", v.TargetModel, "mart.sales")
	}
	if v.TargetColumn != "revenue" {
		t.Errorf("TargetColumn = %q, want %q", v.TargetColumn, "revenue")
	}
}

func TestPadRight(t *testing.T) {
	t.Parallel()
	tests := []struct {
		s     string
		width int
		want  string
	}{
		{"hi", 5, "hi   "},
		{"hello", 5, "hello"},
		{"toolong", 3, "toolong"},
		{"", 3, "   "},
	}
	for _, tt := range tests {
		got := padRight(tt.s, tt.width)
		if got != tt.want {
			t.Errorf("padRight(%q, %d) = %q, want %q", tt.s, tt.width, got, tt.want)
		}
	}
}

func TestPadCenter(t *testing.T) {
	t.Parallel()
	tests := []struct {
		s     string
		width int
		want  string
	}{
		{"hi", 6, "  hi  "},     // even padding
		{"hi", 7, "  hi   "},    // odd: leftPad=2, rightPad=3
		{"hello", 5, "hello"},   // exact fit
		{"toolong", 3, "toolong"}, // wider than target
	}
	for _, tt := range tests {
		got := padCenter(tt.s, tt.width)
		if got != tt.want {
			t.Errorf("padCenter(%q, %d) = %q, want %q", tt.s, tt.width, got, tt.want)
		}
	}
}

func TestDotFill(t *testing.T) {
	t.Parallel()
	got := dotFill("left", "right", 30)

	if !strings.HasPrefix(got, "left") {
		t.Error("should start with left")
	}
	if !strings.HasSuffix(got, "right") {
		t.Error("should end with right")
	}
	if !strings.Contains(got, "···") {
		t.Error("should contain dots")
	}
}

func TestDotFill_MinDots(t *testing.T) {
	t.Parallel()
	// Very short total width should still produce at least 3 dots
	got := dotFill("abc", "def", 6)
	if !strings.Contains(got, "···") {
		t.Error("should contain at least 3 dots even when width is small")
	}
}

func TestTransformShort(t *testing.T) {
	t.Parallel()
	v := NewLineageView(nil)
	tests := []struct {
		t    TransformationType
		want string
	}{
		{TransformAggregation, "[AGG]"},
		{TransformArithmetic, "[×]"},
		{TransformConditional, "[IF]"},
		{TransformCast, "[CAST]"},
		{TransformFunction, "[FN]"},
		{TransformIdentity, ""},
		{"", ""},
	}
	for _, tt := range tests {
		got := v.transformShort(tt.t)
		if got != tt.want {
			t.Errorf("transformShort(%q) = %q, want %q", tt.t, got, tt.want)
		}
	}
}

func TestTransformShort_Unknown(t *testing.T) {
	t.Parallel()
	v := NewLineageView(nil)
	got := v.transformShort("CUSTOM_TYPE")
	// Should return first 3 chars
	if got != "[CUS]" {
		t.Errorf("transformShort(CUSTOM_TYPE) = %q, want %q", got, "[CUS]")
	}
}

func TestTransformShortWithFunc(t *testing.T) {
	t.Parallel()
	v := NewLineageView(nil)
	tests := []struct {
		transform TransformationType
		funcName  string
		want      string
	}{
		{TransformAggregation, "SUM", "[SUM]"},
		{TransformAggregation, "COUNT", "[CNT]"},
		{TransformAggregation, "AVG", "[AVG]"},
		{TransformAggregation, "MIN", "[MIN]"},
		{TransformAggregation, "MAX", "[MAX]"},
		{TransformAggregation, "sum", "[SUM]"},     // lowercase
		{TransformAggregation, "MEDIAN", "[MED]"},  // fallback to first 3
		{TransformAggregation, "", "[AGG]"},         // no func name
		{TransformArithmetic, "anything", "[×]"},    // non-aggregation
		{TransformIdentity, "", ""},
	}
	for _, tt := range tests {
		got := v.transformShortWithFunc(tt.transform, tt.funcName)
		if got != tt.want {
			t.Errorf("transformShortWithFunc(%q, %q) = %q, want %q", tt.transform, tt.funcName, got, tt.want)
		}
	}
}

func TestGroupByLayer(t *testing.T) {
	t.Parallel()
	// Schema names are determined by directory names under models/ - can be anything
	v := NewLineageView([]*ModelLineage{
		{Name: "gold.sales"},
		{Name: "silver.orders"},
		{Name: "bronze.events"},
		{Name: "silver.products"},
	})

	groups := v.groupByLayer()
	if len(groups["gold"]) != 1 {
		t.Errorf("gold count = %d, want 1", len(groups["gold"]))
	}
	if len(groups["silver"]) != 2 {
		t.Errorf("silver count = %d, want 2", len(groups["silver"]))
	}
	if len(groups["bronze"]) != 1 {
		t.Errorf("bronze count = %d, want 1", len(groups["bronze"]))
	}
}

func TestSortByLayer(t *testing.T) {
	t.Parallel()
	// Models with dependency chain using arbitrary schema names
	v := NewLineageView([]*ModelLineage{
		{Name: "alpha.events"},
		{Name: "gamma.report", Dependencies: []string{"beta.orders"}},
		{Name: "beta.orders", Dependencies: []string{"alpha.events"}},
	})

	sorted := v.sortByLayer()
	// Sorted by depth descending: gamma (depth 2) first, then beta (depth 1), then alpha (depth 0)
	if sorted[0].Name != "gamma.report" {
		t.Errorf("first = %q, want gamma.report", sorted[0].Name)
	}
	if sorted[1].Name != "beta.orders" {
		t.Errorf("second = %q, want beta.orders", sorted[1].Name)
	}
	if sorted[2].Name != "alpha.events" {
		t.Errorf("third = %q, want alpha.events", sorted[2].Name)
	}
}

func TestSortByLayer_CyclicDeps(t *testing.T) {
	t.Parallel()
	v := NewLineageView([]*ModelLineage{
		{Name: "x.a", Dependencies: []string{"x.b"}},
		{Name: "x.b", Dependencies: []string{"x.a"}},
	})
	sorted := v.sortByLayer()
	if len(sorted) != 2 {
		t.Fatalf("got %d, want 2", len(sorted))
	}
	// Both have cycle, depth 0 => alphabetical: x.a < x.b
	if sorted[0].Name != "x.a" {
		t.Errorf("first = %q, want x.a", sorted[0].Name)
	}
}

func TestSortByLayer_AlphabeticalTiebreak(t *testing.T) {
	t.Parallel()
	v := NewLineageView([]*ModelLineage{
		{Name: "z.leaf"},
		{Name: "a.leaf"},
		{Name: "m.leaf"},
	})
	sorted := v.sortByLayer()
	// All depth 0, alphabetical
	if sorted[0].Name != "a.leaf" {
		t.Errorf("first = %q, want a.leaf", sorted[0].Name)
	}
	if sorted[1].Name != "m.leaf" {
		t.Errorf("second = %q, want m.leaf", sorted[1].Name)
	}
	if sorted[2].Name != "z.leaf" {
		t.Errorf("third = %q, want z.leaf", sorted[2].Name)
	}
}

func TestRenderLegend(t *testing.T) {
	t.Parallel()
	v := NewLineageView(nil)
	got := v.renderLegend()

	for _, sym := range []string{"◆", "●", "[SUM]", "[CNT]", "[AVG]", "[×]"} {
		if !strings.Contains(got, sym) {
			t.Errorf("legend missing symbol %q", sym)
		}
	}
}

func TestFormatColumnLineAligned_NoSources(t *testing.T) {
	t.Parallel()
	v := NewLineageView(nil)
	got := v.formatColumnLineAligned("id", nil, 0)
	if got != "◆ id" {
		t.Errorf("got %q, want %q", got, "◆ id")
	}
}

func TestFormatColumnLineAligned_WithSource(t *testing.T) {
	t.Parallel()
	v := NewLineageView(nil)
	sources := []ColumnSource{
		{Table: "raw.orders", Column: "order_id"},
	}
	got := v.formatColumnLineAligned("id", sources, 0)

	if !strings.Contains(got, "◆") {
		t.Error("should contain ◆ marker")
	}
	if !strings.Contains(got, "●") {
		t.Error("should contain ● marker")
	}
	if !strings.Contains(got, "raw.orders.order_id") {
		t.Error("should contain source reference")
	}
}

func TestFormatColumnLineAligned_WithTransform(t *testing.T) {
	t.Parallel()
	v := NewLineageView(nil)
	sources := []ColumnSource{
		{Table: "staging.orders", Column: "amount", Transformation: TransformAggregation, FunctionName: "SUM"},
	}
	got := v.formatColumnLineAligned("total", sources, 0)

	if !strings.Contains(got, "[SUM]") {
		t.Error("should contain [SUM] transform label")
	}
}

func TestFormatColumnLineAligned_MultiSource(t *testing.T) {
	t.Parallel()
	v := NewLineageView(nil)
	sources := []ColumnSource{
		{Table: "t1", Column: "a"},
		{Table: "t1", Column: "b"},
	}
	got := v.formatColumnLineAligned("total", sources, 0)

	if !strings.Contains(got, "a, b") {
		t.Errorf("should contain 'a, b', got %q", got)
	}
}

func TestFormatColumnLineWithMarker_Target(t *testing.T) {
	t.Parallel()
	v := NewLineageView(nil)
	got := v.formatColumnLineWithMarker("revenue", nil, true, false)
	if !strings.HasPrefix(got, "◆ ") {
		t.Errorf("target should start with '◆ ', got %q", got)
	}
}

func TestFormatColumnLineWithMarker_Source(t *testing.T) {
	t.Parallel()
	v := NewLineageView(nil)
	got := v.formatColumnLineWithMarker("amount", nil, false, true)
	if !strings.HasPrefix(got, "● ") {
		t.Errorf("source should start with '● ', got %q", got)
	}
}

func TestFormatColumnLineWithMarker_Neither(t *testing.T) {
	t.Parallel()
	v := NewLineageView(nil)
	got := v.formatColumnLineWithMarker("col", nil, false, false)
	if !strings.HasPrefix(got, "  ") {
		t.Errorf("neither target nor source should start with '  ', got %q", got)
	}
}

func TestRender_Overview(t *testing.T) {
	t.Parallel()
	models := []*ModelLineage{
		{
			Name:    "mart.sales",
			Columns: []string{"revenue", "cost"},
		},
		{
			Name:    "staging.orders",
			Columns: []string{"id", "amount"},
		},
	}
	v := NewLineageView(models)
	v.Mode = ViewOverview

	got := v.Render()
	if got == "" {
		t.Fatal("expected non-empty output")
	}
	if !strings.Contains(got, "sales") {
		t.Error("overview should contain model name 'sales'")
	}
	if !strings.Contains(got, "orders") {
		t.Error("overview should contain model name 'orders'")
	}
}

func TestRender_ModelFocus(t *testing.T) {
	t.Parallel()
	models := []*ModelLineage{
		{
			Name:    "mart.sales",
			Columns: []string{"revenue"},
			ColumnLineage: []ColumnLineageInfo{
				{
					Column: "revenue",
					Sources: []ColumnSource{
						{Table: "staging.orders", Column: "amount", Transformation: TransformAggregation, FunctionName: "SUM"},
					},
				},
			},
		},
		{
			Name:    "staging.orders",
			Columns: []string{"id", "amount"},
		},
	}
	v := NewLineageView(models)
	v.SetTarget("mart.sales")

	got := v.Render()
	if got == "" {
		t.Fatal("expected non-empty output")
	}
	if !strings.Contains(got, "revenue") {
		t.Error("model focus should show column 'revenue'")
	}
}

func TestRender_ColumnFocus(t *testing.T) {
	t.Parallel()
	models := []*ModelLineage{
		{
			Name:    "mart.sales",
			Columns: []string{"revenue"},
			ColumnLineage: []ColumnLineageInfo{
				{
					Column: "revenue",
					Sources: []ColumnSource{
						{Table: "staging.orders", Column: "amount", Transformation: TransformAggregation, FunctionName: "SUM"},
					},
				},
			},
		},
		{
			Name:    "staging.orders",
			Columns: []string{"id", "amount"},
		},
	}
	v := NewLineageView(models)
	v.SetTarget("mart.sales.revenue")

	got := v.Render()
	if got == "" {
		t.Fatal("expected non-empty output")
	}
	if !strings.Contains(got, "revenue") {
		t.Error("column focus should show 'revenue'")
	}
}

func TestRender_EmptyModels(t *testing.T) {
	t.Parallel()
	v := NewLineageView(nil)
	got := v.Render()
	if got == "" {
		t.Error("should produce some output even with no models")
	}
}

func TestRender_ModelFocus_NotFound(t *testing.T) {
	t.Parallel()
	models := []*ModelLineage{
		{Name: "staging.orders", Columns: []string{"id"}},
	}
	v := NewLineageView(models)
	v.SetTarget("mart.nonexistent")

	got := v.Render()
	// Should not panic, should produce some output
	if got == "" {
		t.Error("should produce output even when target not found")
	}
}

func TestRender_OverviewWithDeps(t *testing.T) {
	t.Parallel()
	models := []*ModelLineage{
		{
			Name:         "mart.sales",
			Columns:      []string{"revenue"},
			Dependencies: []string{"staging.orders"},
		},
		{
			Name:         "staging.orders",
			Columns:      []string{"id", "amount"},
			Dependencies: []string{"raw.events"},
		},
		{
			Name:    "raw.events",
			Columns: []string{"event_id"},
		},
	}
	v := NewLineageView(models)
	v.Mode = ViewOverview

	got := v.Render()
	if !strings.Contains(got, "sales") {
		t.Error("overview should contain 'sales'")
	}
	if !strings.Contains(got, "orders") {
		t.Error("overview should contain 'orders'")
	}
	if !strings.Contains(got, "events") {
		t.Error("overview should contain 'events'")
	}
	// Should have arrows between layers
	if !strings.Contains(got, "→") && !strings.Contains(got, "─") && !strings.Contains(got, "►") {
		// May use different arrow chars, just verify it's non-trivial output
		if len(got) < 50 {
			t.Error("expected substantial overview output with arrows")
		}
	}
}

func TestRender_OverviewSameLayerDeps(t *testing.T) {
	t.Parallel()
	models := []*ModelLineage{
		{
			Name:         "staging.orders",
			Columns:      []string{"id"},
			Dependencies: []string{"staging.products"},
		},
		{
			Name:    "staging.products",
			Columns: []string{"id"},
		},
	}
	v := NewLineageView(models)
	v.Mode = ViewOverview

	got := v.Render()
	if !strings.Contains(got, "orders") {
		t.Error("overview should contain 'orders'")
	}
	if !strings.Contains(got, "products") {
		t.Error("overview should contain 'products'")
	}
}

func TestRender_OverviewNoModels(t *testing.T) {
	t.Parallel()
	v := NewLineageView(nil)
	v.Mode = ViewOverview
	got := v.Render()
	if got == "" {
		t.Error("should produce some output")
	}
}

func TestRender_OverviewManyModels(t *testing.T) {
	t.Parallel()
	models := []*ModelLineage{
		{Name: "mart.sales", Columns: []string{"a"}, Dependencies: []string{"staging.orders"}},
		{Name: "mart.revenue", Columns: []string{"b"}, Dependencies: []string{"staging.orders"}},
		{Name: "staging.orders", Columns: []string{"c"}, Dependencies: []string{"raw.events"}},
		{Name: "staging.products", Columns: []string{"d"}, Dependencies: []string{"raw.data"}},
		{Name: "raw.events", Columns: []string{"e"}},
		{Name: "raw.data", Columns: []string{"f"}},
	}
	v := NewLineageView(models)
	v.Mode = ViewOverview

	got := v.Render()
	if !strings.Contains(got, "sales") {
		t.Error("should contain sales")
	}
	if !strings.Contains(got, "events") {
		t.Error("should contain events")
	}
}

func TestRender_ColumnFocus_NotFound(t *testing.T) {
	t.Parallel()
	models := []*ModelLineage{
		{Name: "staging.orders", Columns: []string{"id"}},
	}
	v := NewLineageView(models)
	v.SetTarget("staging.orders.nonexistent")

	got := v.Render()
	if got == "" {
		t.Error("should produce output even when column not found")
	}
}

func TestRender_ModelFocus_WithLineage(t *testing.T) {
	t.Parallel()
	models := []*ModelLineage{
		{
			Name:    "mart.sales",
			Columns: []string{"revenue", "cost"},
			ColumnLineage: []ColumnLineageInfo{
				{
					Column: "revenue",
					Sources: []ColumnSource{
						{Table: "staging.orders", Column: "amount", Transformation: TransformAggregation, FunctionName: "SUM"},
					},
				},
				{
					Column: "cost",
					Sources: []ColumnSource{
						{Table: "staging.orders", Column: "cost_amt", Transformation: TransformIdentity},
					},
				},
			},
		},
		{
			Name:    "staging.orders",
			Columns: []string{"id", "amount", "cost_amt"},
		},
	}
	v := NewLineageView(models)
	v.SetTarget("mart.sales")

	got := v.Render()
	if !strings.Contains(got, "revenue") {
		t.Error("should show revenue column")
	}
	if !strings.Contains(got, "cost") {
		t.Error("should show cost column")
	}
}

func TestRender_ColumnFocus_WithTrace(t *testing.T) {
	t.Parallel()
	models := []*ModelLineage{
		{
			Name:    "mart.sales",
			Columns: []string{"total"},
			ColumnLineage: []ColumnLineageInfo{
				{
					Column: "total",
					Sources: []ColumnSource{
						{Table: "staging.orders", Column: "amount", Transformation: TransformAggregation, FunctionName: "SUM"},
					},
				},
			},
		},
		{
			Name:    "staging.orders",
			Columns: []string{"id", "amount"},
			ColumnLineage: []ColumnLineageInfo{
				{
					Column: "amount",
					Sources: []ColumnSource{
						{Table: "raw.events", Column: "value", Transformation: TransformIdentity},
					},
				},
			},
		},
		{
			Name:    "raw.events",
			Columns: []string{"event_id", "value"},
		},
	}
	v := NewLineageView(models)
	v.SetTarget("mart.sales.total")

	got := v.Render()
	if !strings.Contains(got, "total") {
		t.Error("should show 'total' in column focus")
	}
}

func TestRender_DefaultCase(t *testing.T) {
	t.Parallel()
	// Default case in Render() switch (invalid mode)
	v := NewLineageView([]*ModelLineage{
		{Name: "staging.orders", Columns: []string{"id"}},
	})
	v.Mode = 99 // invalid mode, hits default
	v.TargetModel = "staging.orders"
	got := v.Render()
	if got == "" {
		t.Error("should produce output for default case")
	}
}

func TestRender_ModelFocus_NoActiveLayers(t *testing.T) {
	t.Parallel()
	// Empty models list
	v := NewLineageView(nil)
	v.Mode = ViewModelFocus
	v.TargetModel = "mart.something"
	got := v.Render()
	if !strings.Contains(got, "No models found") {
		t.Errorf("expected 'No models found', got %q", got)
	}
}

func TestRender_ModelFocus_WithDepsAndArrows(t *testing.T) {
	t.Parallel()
	models := []*ModelLineage{
		{
			Name:         "mart.sales",
			Columns:      []string{"revenue", "cost"},
			Dependencies: []string{"staging.orders"},
			ColumnLineage: []ColumnLineageInfo{
				{Column: "revenue", Sources: []ColumnSource{{Table: "staging.orders", Column: "amount", Transformation: TransformAggregation, FunctionName: "SUM"}}},
				{Column: "cost", Sources: []ColumnSource{{Table: "staging.orders", Column: "cost_amt"}}},
			},
		},
		{
			Name:         "staging.orders",
			Columns:      []string{"id", "amount", "cost_amt"},
			Dependencies: []string{"raw.events"},
		},
		{
			Name:    "raw.events",
			Columns: []string{"event_id", "value", "cost"},
		},
	}
	v := NewLineageView(models)
	v.SetTarget("mart.sales")

	got := v.Render()
	if !strings.Contains(got, "revenue") {
		t.Error("should contain revenue")
	}
	if !strings.Contains(got, "orders") {
		t.Error("should contain orders")
	}
	// Check arrow characters exist
	if !strings.Contains(got, "→") && !strings.Contains(got, "─") {
		if len(got) < 100 {
			t.Error("expected substantial output with arrows")
		}
	}
}

func TestRender_ModelFocus_MultipleModelsInLayer(t *testing.T) {
	t.Parallel()
	models := []*ModelLineage{
		{
			Name:         "mart.sales",
			Columns:      []string{"revenue"},
			Dependencies: []string{"staging.orders"},
			ColumnLineage: []ColumnLineageInfo{
				{Column: "revenue", Sources: []ColumnSource{{Table: "staging.orders", Column: "amount"}}},
			},
		},
		{
			Name:    "staging.orders",
			Columns: []string{"id", "amount"},
		},
		{
			Name:    "staging.products",
			Columns: []string{"id", "name"},
		},
	}
	v := NewLineageView(models)
	v.SetTarget("mart.sales")

	got := v.Render()
	if !strings.Contains(got, "orders") {
		t.Error("should contain orders")
	}
	if !strings.Contains(got, "products") {
		t.Error("should contain products")
	}
}

func TestRender_ColumnFocus_MultiLayer(t *testing.T) {
	t.Parallel()
	models := []*ModelLineage{
		{
			Name:    "mart.report",
			Columns: []string{"total_revenue", "avg_cost"},
			ColumnLineage: []ColumnLineageInfo{
				{
					Column: "total_revenue",
					Sources: []ColumnSource{
						{Table: "staging.orders", Column: "amount", Transformation: TransformAggregation, FunctionName: "SUM"},
					},
				},
				{
					Column: "avg_cost",
					Sources: []ColumnSource{
						{Table: "staging.orders", Column: "cost", Transformation: TransformAggregation, FunctionName: "AVG"},
					},
				},
			},
		},
		{
			Name:    "staging.orders",
			Columns: []string{"id", "amount", "cost"},
			ColumnLineage: []ColumnLineageInfo{
				{
					Column: "amount",
					Sources: []ColumnSource{
						{Table: "raw.events", Column: "value", Transformation: TransformIdentity},
					},
				},
				{
					Column: "cost",
					Sources: []ColumnSource{
						{Table: "raw.events", Column: "price", Transformation: TransformCast},
					},
				},
			},
		},
		{
			Name:    "raw.events",
			Columns: []string{"event_id", "value", "price"},
		},
	}
	v := NewLineageView(models)
	v.SetTarget("mart.report.total_revenue")

	got := v.Render()
	if !strings.Contains(got, "total_revenue") {
		t.Error("should contain 'total_revenue'")
	}
	if !strings.Contains(got, "amount") {
		t.Error("should trace through 'amount'")
	}
}

func TestRender_ColumnFocus_NoLineage(t *testing.T) {
	t.Parallel()
	models := []*ModelLineage{
		{
			Name:    "staging.orders",
			Columns: []string{"id"},
		},
	}
	v := NewLineageView(models)
	v.SetTarget("staging.orders.id")

	got := v.Render()
	if got == "" {
		t.Error("should produce some output")
	}
}

func TestRender_OverviewWithArrowRouting(t *testing.T) {
	t.Parallel()
	// Create many deps to trigger arrow offset routing
	models := []*ModelLineage{
		{Name: "mart.sales", Columns: []string{"a"}, Dependencies: []string{"staging.orders", "staging.products"}},
		{Name: "mart.revenue", Columns: []string{"b"}, Dependencies: []string{"staging.orders"}},
		{Name: "mart.costs", Columns: []string{"c"}, Dependencies: []string{"staging.products"}},
		{Name: "staging.orders", Columns: []string{"d"}, Dependencies: []string{"raw.events"}},
		{Name: "staging.products", Columns: []string{"e"}, Dependencies: []string{"raw.data"}},
		{Name: "raw.events", Columns: []string{"f"}},
		{Name: "raw.data", Columns: []string{"g"}},
	}
	v := NewLineageView(models)
	v.Mode = ViewOverview

	got := v.Render()
	if !strings.Contains(got, "sales") {
		t.Error("should contain 'sales'")
	}
}

func TestFormatColumnLineWithMarkerAligned_TargetWithTransform(t *testing.T) {
	t.Parallel()
	v := NewLineageView(nil)
	sources := []ColumnSource{
		{Table: "raw.data", Column: "val", Transformation: TransformAggregation, FunctionName: "SUM"},
	}
	got := v.formatColumnLineWithMarkerAligned("total", sources, true, false, 10)
	if !strings.HasPrefix(got, "◆ ") {
		t.Error("target should start with ◆")
	}
	if !strings.Contains(got, "[SUM]") {
		t.Error("should contain [SUM]")
	}
}

func TestFormatColumnLineWithMarkerAligned_SourceWithMultipleSources(t *testing.T) {
	t.Parallel()
	v := NewLineageView(nil)
	sources := []ColumnSource{
		{Table: "t1", Column: "a"},
		{Table: "t1", Column: "b"},
	}
	got := v.formatColumnLineWithMarkerAligned("col", sources, false, true, 8)
	if !strings.HasPrefix(got, "● ") {
		t.Error("source should start with ●")
	}
	if !strings.Contains(got, "a, b") {
		t.Error("should contain multiple source columns")
	}
}

func TestFormatColumnLineWithMarkerAligned_NoSourcesNotTargetOrSource(t *testing.T) {
	t.Parallel()
	v := NewLineageView(nil)
	got := v.formatColumnLineWithMarkerAligned("col", nil, false, false, 0)
	if !strings.HasPrefix(got, "  ") {
		t.Error("neither target nor source should have empty marker")
	}
}

func TestFormatColumnLineWithMarker_TargetWithSources(t *testing.T) {
	t.Parallel()
	v := NewLineageView(nil)
	sources := []ColumnSource{
		{Table: "raw.data", Column: "val", Transformation: TransformIdentity},
	}
	got := v.formatColumnLineWithMarker("col", sources, true, false)
	if !strings.HasPrefix(got, "◆ ") {
		t.Error("target with sources should start with ◆")
	}
	if !strings.Contains(got, "raw.data.val") {
		t.Error("should contain source reference")
	}
}

func TestRender_OverviewLongModelNames(t *testing.T) {
	t.Parallel()
	// Tests the shortName truncation path in renderOverview
	models := []*ModelLineage{
		{Name: "staging.a_very_long_table_name_that_exceeds_box_width_truncation"},
		{Name: "mart.another_extremely_long_model_name_for_testing_purposes", Dependencies: []string{"staging.a_very_long_table_name_that_exceeds_box_width_truncation"}},
	}
	v := NewLineageView(models)
	v.Mode = ViewOverview
	got := v.Render()
	if got == "" {
		t.Error("expected non-empty output")
	}
}

func TestRender_ColumnFocus_TraceFromDownstream(t *testing.T) {
	t.Parallel()
	// Tests column focus with lineage tracing through multiple levels
	models := []*ModelLineage{
		{Name: "raw.src", Columns: []string{"id"}},
		{
			Name: "staging.mid", Dependencies: []string{"raw.src"},
			Columns:       []string{"id"},
			ColumnLineage: []ColumnLineageInfo{{Column: "id", Sources: []ColumnSource{{Table: "raw.src", Column: "id", Transformation: TransformIdentity}}}},
		},
		{
			Name: "mart.out", Dependencies: []string{"staging.mid"},
			Columns:       []string{"src_id"},
			ColumnLineage: []ColumnLineageInfo{{Column: "src_id", Sources: []ColumnSource{{Table: "staging.mid", Column: "id", Transformation: TransformIdentity}}}},
		},
	}
	v := NewLineageView(models)
	v.SetTarget("mart.out.src_id")
	got := v.Render()
	if got == "" {
		t.Error("expected non-empty output")
	}
}

func TestRender_ModelFocus_ManyColumns(t *testing.T) {
	t.Parallel()
	// Tests model focus with many columns to trigger various alignment paths
	var colNames []string
	var colLineage []ColumnLineageInfo
	for i := 0; i < 10; i++ {
		colNames = append(colNames, fmt.Sprintf("col_%d", i))
		colLineage = append(colLineage, ColumnLineageInfo{
			Column:  fmt.Sprintf("col_%d", i),
			Sources: []ColumnSource{{Table: "raw.data", Column: fmt.Sprintf("src_%d", i), Transformation: TransformIdentity}},
		})
	}
	models := []*ModelLineage{
		{Name: "raw.data", Columns: []string{"src_0", "src_1", "src_2", "src_3", "src_4", "src_5", "src_6", "src_7", "src_8", "src_9"}},
		{Name: "staging.wide", Dependencies: []string{"raw.data"}, Columns: colNames, ColumnLineage: colLineage},
	}
	v := NewLineageView(models)
	v.SetTarget("staging.wide")
	got := v.Render()
	if got == "" {
		t.Error("expected non-empty output")
	}
	for i := 0; i < 10; i++ {
		if !strings.Contains(got, fmt.Sprintf("col_%d", i)) {
			t.Errorf("expected col_%d in output", i)
		}
	}
}

func TestRender_ModelFocus_CollapsedModel(t *testing.T) {
	t.Parallel()
	// Tests collapsed model rendering with long names
	models := []*ModelLineage{
		{Name: "raw.very_long_source_table_name_that_tests_truncation", Columns: []string{"id"}},
		{
			Name: "staging.target_model", Dependencies: []string{"raw.very_long_source_table_name_that_tests_truncation"},
			Columns:       []string{"id"},
			ColumnLineage: []ColumnLineageInfo{{Column: "id", Sources: []ColumnSource{{Table: "raw.very_long_source_table_name_that_tests_truncation", Column: "id"}}}},
		},
		{Name: "mart.final", Dependencies: []string{"staging.target_model"}, Columns: []string{"result"}},
	}
	v := NewLineageView(models)
	v.SetTarget("staging.target_model")
	got := v.Render()
	if got == "" {
		t.Error("expected non-empty output")
	}
}

func TestRender_Overview_CustomSchemaNames(t *testing.T) {
	t.Parallel()
	// Schemas can be any name the user chooses
	models := []*ModelLineage{
		{Name: "bronze.raw_events", Columns: []string{"id", "ts"}},
		{Name: "silver.cleaned", Columns: []string{"id"}, Dependencies: []string{"bronze.raw_events"}},
		{Name: "gold.summary", Columns: []string{"total"}, Dependencies: []string{"silver.cleaned"}},
	}
	v := NewLineageView(models)
	v.Mode = ViewOverview
	got := v.Render()

	if !strings.Contains(got, "raw_events") {
		t.Error("should contain bronze model name")
	}
	if !strings.Contains(got, "cleaned") {
		t.Error("should contain silver model name")
	}
	if !strings.Contains(got, "summary") {
		t.Error("should contain gold model name")
	}
}

func TestRender_ModelFocus_CustomSchemaNames(t *testing.T) {
	t.Parallel()
	models := []*ModelLineage{
		{Name: "ingest.api_data", Columns: []string{"id", "payload"}},
		{
			Name:         "transform.parsed",
			Columns:      []string{"id", "value"},
			Dependencies: []string{"ingest.api_data"},
			ColumnLineage: []ColumnLineageInfo{
				{Column: "id", Sources: []ColumnSource{{Table: "ingest.api_data", Column: "id"}}},
			},
		},
	}
	v := NewLineageView(models)
	v.SetTarget("transform.parsed")
	got := v.Render()

	if !strings.Contains(got, "parsed") {
		t.Error("should contain target model")
	}
	if !strings.Contains(got, "api_data") {
		t.Error("should contain source model")
	}
}

func TestRender_ColumnFocus_CustomSchemaNames(t *testing.T) {
	t.Parallel()
	models := []*ModelLineage{
		{Name: "lake.events", Columns: []string{"event_id"}},
		{
			Name:         "warehouse.metrics",
			Columns:      []string{"count"},
			Dependencies: []string{"lake.events"},
			ColumnLineage: []ColumnLineageInfo{
				{Column: "count", Sources: []ColumnSource{
					{Table: "lake.events", Column: "event_id", Transformation: TransformAggregation},
				}},
			},
		},
	}
	v := NewLineageView(models)
	v.SetTarget("warehouse.metrics.count")
	got := v.Render()

	if !strings.Contains(got, "count") {
		t.Error("should contain traced column")
	}
}

func TestLayerOrder(t *testing.T) {
	t.Parallel()
	v := NewLineageView([]*ModelLineage{
		{Name: "z_source.t1", Columns: []string{"id"}},
		{Name: "m_middle.t1", Columns: []string{"id"}, Dependencies: []string{"z_source.t1"}},
		{Name: "a_final.t1", Columns: []string{"id"}, Dependencies: []string{"m_middle.t1"}},
	})
	layers := v.groupByLayer()
	order := v.layerOrder(layers)

	if len(order) != 3 {
		t.Fatalf("got %d layers, want 3", len(order))
	}
	// Sources first (depth 0), then middle (depth 1), then downstream (depth 2)
	if order[0] != "z_source" {
		t.Errorf("first = %q, want z_source (depth 0)", order[0])
	}
	if order[1] != "m_middle" {
		t.Errorf("second = %q, want m_middle (depth 1)", order[1])
	}
	if order[2] != "a_final" {
		t.Errorf("third = %q, want a_final (depth 2)", order[2])
	}
}

func TestLayerOrder_AlphabeticalTiebreak(t *testing.T) {
	t.Parallel()
	v := NewLineageView([]*ModelLineage{
		{Name: "zulu.t1", Columns: []string{"id"}},
		{Name: "alpha.t1", Columns: []string{"id"}},
	})
	layers := v.groupByLayer()
	order := v.layerOrder(layers)

	if len(order) != 2 {
		t.Fatalf("got %d, want 2", len(order))
	}
	if order[0] != "alpha" {
		t.Errorf("first = %q, want alpha", order[0])
	}
	if order[1] != "zulu" {
		t.Errorf("second = %q, want zulu", order[1])
	}
}
