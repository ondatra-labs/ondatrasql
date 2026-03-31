// OndatraSQL - A data pipeline framework for DuckDB + DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package lineage

import (
	"strings"
	"testing"
)

func TestNewCanvas(t *testing.T) {
	t.Parallel()
	c := NewCanvas(10, 5)
	if c.width != 10 {
		t.Errorf("width = %d, want 10", c.width)
	}
	if c.height != 5 {
		t.Errorf("height = %d, want 5", c.height)
	}
	if len(c.grid) != 5 {
		t.Fatalf("grid rows = %d, want 5", len(c.grid))
	}
	for y, row := range c.grid {
		if len(row) != 10 {
			t.Errorf("row %d len = %d, want 10", y, len(row))
		}
		for x, ch := range row {
			if ch != ' ' {
				t.Errorf("cell (%d,%d) = %q, want space", x, y, ch)
			}
		}
	}
}

func TestCanvasSetGet(t *testing.T) {
	t.Parallel()
	c := NewCanvas(5, 5)

	c.Set(2, 3, 'X')
	if got := c.Get(2, 3); got != 'X' {
		t.Errorf("Get(2,3) = %q, want 'X'", got)
	}

	// Out of bounds should return space
	if got := c.Get(-1, 0); got != ' ' {
		t.Errorf("Get(-1,0) = %q, want space", got)
	}
	if got := c.Get(0, -1); got != ' ' {
		t.Errorf("Get(0,-1) = %q, want space", got)
	}
	if got := c.Get(5, 0); got != ' ' {
		t.Errorf("Get(5,0) = %q, want space", got)
	}
	if got := c.Get(0, 5); got != ' ' {
		t.Errorf("Get(0,5) = %q, want space", got)
	}

	// Set out of bounds should not panic
	c.Set(-1, 0, 'Z')
	c.Set(0, -1, 'Z')
	c.Set(5, 0, 'Z')
	c.Set(0, 5, 'Z')
}

func TestCanvasWriteString(t *testing.T) {
	t.Parallel()
	c := NewCanvas(10, 1)
	c.WriteString(2, 0, "hello")

	want := "  hello   "
	got := string(c.grid[0])
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestCanvasWriteString_Unicode(t *testing.T) {
	t.Parallel()
	c := NewCanvas(10, 1)
	c.WriteString(0, 0, "hej!åäö")

	// Each rune takes one position
	if got := c.Get(4, 0); got != 'å' {
		t.Errorf("pos 4 = %q, want 'å'", got)
	}
}

func TestCanvasString(t *testing.T) {
	t.Parallel()
	c := NewCanvas(5, 3)
	c.Set(0, 0, 'A')
	c.Set(1, 1, 'B')

	got := c.String()
	lines := strings.Split(got, "\n")
	if lines[0] != "A" {
		t.Errorf("line 0 = %q, want %q", lines[0], "A")
	}
	if lines[1] != " B" {
		t.Errorf("line 1 = %q, want %q", lines[1], " B")
	}
}

func TestCanvasString_TrimsTrailingNewlines(t *testing.T) {
	t.Parallel()
	c := NewCanvas(5, 5)
	c.Set(0, 0, 'X')
	// Last rows are all spaces -> should be trimmed

	got := c.String()
	if strings.HasSuffix(got, "\n") {
		t.Error("trailing newlines should be trimmed")
	}
}

func TestCalcWidth_NameOnly(t *testing.T) {
	t.Parallel()
	box := &GraphBox{
		Name:    "short",
		Columns: nil,
	}
	w := box.CalcWidth()
	if w < 30 {
		t.Errorf("width = %d, should be at least 30 (minimum)", w)
	}
}

func TestCalcWidth_LongColumns(t *testing.T) {
	t.Parallel()
	box := &GraphBox{
		Name:    "test",
		Columns: []string{"a_very_long_column_name_that_is_definitely_wider"},
	}
	w := box.CalcWidth()
	if w < len("a_very_long_column_name_that_is_definitely_wider")+4 {
		t.Errorf("width %d too small for long column", w)
	}
}

func TestCalcWidth_WithLineage(t *testing.T) {
	t.Parallel()
	box := &GraphBox{
		Name:    "test",
		Columns: []string{"id"},
		ColumnLineage: []ColumnLineageInfo{
			{
				Column: "total",
				Sources: []ColumnSource{
					{Table: "staging.orders", Column: "amount", Transformation: TransformAggregation},
				},
			},
		},
	}
	w := box.CalcWidth()
	// Width should account for lineage expression
	if w < 30 {
		t.Errorf("width = %d, should be at least 30", w)
	}
}

func TestCalcWidth_MultiSourceLineage(t *testing.T) {
	t.Parallel()
	box := &GraphBox{
		Name: "test",
		ColumnLineage: []ColumnLineageInfo{
			{
				Column: "total",
				Sources: []ColumnSource{
					{Table: "t1", Column: "a", Transformation: TransformIdentity},
					{Table: "t1", Column: "b", Transformation: TransformIdentity},
				},
			},
		},
	}
	w := box.CalcWidth()
	if w < 30 {
		t.Errorf("width = %d, should be at least 30", w)
	}
}

func TestDrawBox(t *testing.T) {
	t.Parallel()
	box := &GraphBox{
		Name:    "orders",
		Columns: []string{"id", "amount"},
		X:       0,
		Y:       0,
		Width:   30,
	}
	c := NewCanvas(32, 10)
	c.DrawBox(box, DoubleBox)

	s := c.String()
	// Should contain corners
	if !strings.Contains(s, "╔") {
		t.Error("missing top-left corner ╔")
	}
	if !strings.Contains(s, "╗") {
		t.Error("missing top-right corner ╗")
	}
	if !strings.Contains(s, "╚") {
		t.Error("missing bottom-left corner ╚")
	}
	if !strings.Contains(s, "╝") {
		t.Error("missing bottom-right corner ╝")
	}
	// Should contain title
	if !strings.Contains(s, "orders") {
		t.Error("missing title 'orders'")
	}
	// Should contain columns
	if !strings.Contains(s, "id") {
		t.Error("missing column 'id'")
	}
	if !strings.Contains(s, "amount") {
		t.Error("missing column 'amount'")
	}
	// Height should be set
	if box.Height != 6 { // top + title + separator + 2 cols + bottom
		t.Errorf("Height = %d, want 6", box.Height)
	}
}

func TestDrawBox_SingleBox(t *testing.T) {
	t.Parallel()
	box := &GraphBox{
		Name:    "test",
		Columns: []string{"col1"},
		X:       0,
		Y:       0,
		Width:   30,
	}
	c := NewCanvas(32, 8)
	c.DrawBox(box, SingleBox)

	s := c.String()
	if !strings.Contains(s, "┌") {
		t.Error("missing single-box top-left corner ┌")
	}
}

func TestDrawVerticalLine(t *testing.T) {
	t.Parallel()
	c := NewCanvas(5, 10)
	c.DrawVerticalLine(2, 1, 5)

	for y := 1; y <= 5; y++ {
		got := c.Get(2, y)
		if got != '│' {
			t.Errorf("pos (2,%d) = %q, want '│'", y, got)
		}
	}
}

func TestDrawVerticalLine_Reversed(t *testing.T) {
	t.Parallel()
	c := NewCanvas(5, 10)
	c.DrawVerticalLine(2, 5, 1) // y1 > y2, should swap

	for y := 1; y <= 5; y++ {
		got := c.Get(2, y)
		if got != '│' {
			t.Errorf("pos (2,%d) = %q, want '│'", y, got)
		}
	}
}

func TestDrawVerticalLine_Intersections(t *testing.T) {
	t.Parallel()
	c := NewCanvas(5, 5)

	// Place horizontal chars first
	c.Set(2, 1, '─')
	c.Set(2, 2, '═')

	c.DrawVerticalLine(2, 0, 3)

	if got := c.Get(2, 1); got != '┼' {
		t.Errorf("intersection with '─' = %q, want '┼'", got)
	}
	if got := c.Get(2, 2); got != '┼' {
		t.Errorf("intersection with '═' = %q, want '┼'", got)
	}
}

func TestDrawHorizontalLine(t *testing.T) {
	t.Parallel()
	c := NewCanvas(10, 3)
	c.DrawHorizontalLine(1, 5, 1)

	for x := 1; x <= 5; x++ {
		got := c.Get(x, 1)
		if got != '─' {
			t.Errorf("pos (%d,1) = %q, want '─'", x, got)
		}
	}
}

func TestDrawHorizontalLine_Reversed(t *testing.T) {
	t.Parallel()
	c := NewCanvas(10, 3)
	c.DrawHorizontalLine(5, 1, 1) // x1 > x2, should swap

	for x := 1; x <= 5; x++ {
		if got := c.Get(x, 1); got != '─' {
			t.Errorf("pos (%d,1) = %q, want '─'", x, got)
		}
	}
}

func TestDrawHorizontalLine_Intersection(t *testing.T) {
	t.Parallel()
	c := NewCanvas(10, 3)
	c.Set(3, 1, '│')
	c.DrawHorizontalLine(1, 5, 1)

	if got := c.Get(3, 1); got != '┼' {
		t.Errorf("intersection with '│' = %q, want '┼'", got)
	}
}

func TestDrawConnector(t *testing.T) {
	t.Parallel()
	c := NewCanvas(20, 20)
	c.DrawConnector(5, 2, 5, 10, "")

	// Should have arrow at bottom
	if got := c.Get(5, 10); got != '▼' {
		t.Errorf("arrow at bottom = %q, want '▼'", got)
	}
}

func TestDrawConnector_WithOffset(t *testing.T) {
	t.Parallel()
	c := NewCanvas(30, 20)
	c.DrawConnector(5, 2, 15, 10, "")

	// Should have arrow at destination
	if got := c.Get(15, 10); got != '▼' {
		t.Errorf("arrow at dest = %q, want '▼'", got)
	}
}

func TestDrawConnector_WithLabel(t *testing.T) {
	t.Parallel()
	c := NewCanvas(30, 20)
	c.DrawConnector(5, 2, 15, 10, "SUM")

	if got := c.Get(15, 10); got != '▼' {
		t.Errorf("arrow at dest = %q, want '▼'", got)
	}
}

func TestDrawConnector_RightToLeft(t *testing.T) {
	t.Parallel()
	c := NewCanvas(30, 20)
	c.DrawConnector(20, 2, 5, 10, "")

	// Should have proper corner chars for right-to-left
	if got := c.Get(5, 10); got != '▼' {
		t.Errorf("arrow = %q, want '▼'", got)
	}
}

func TestDrawConnector_WithLabelRightToLeft(t *testing.T) {
	t.Parallel()
	c := NewCanvas(30, 20)
	c.DrawConnector(20, 2, 5, 10, "JOIN")

	if got := c.Get(5, 10); got != '▼' {
		t.Errorf("arrow = %q, want '▼'", got)
	}
}

func TestRender_MultiLayer(t *testing.T) {
	t.Parallel()
	models := []*ModelLineage{
		{
			Name:    "mart.revenue",
			Columns: []string{"total"},
			ColumnLineage: []ColumnLineageInfo{
				{Column: "total", Sources: []ColumnSource{
					{Table: "staging.orders", Column: "amount", Transformation: TransformAggregation},
				}},
			},
		},
		{
			Name:    "staging.orders",
			Columns: []string{"id", "amount"},
			ColumnLineage: []ColumnLineageInfo{
				{Column: "id", Sources: []ColumnSource{
					{Table: "raw.events", Column: "event_id", Transformation: TransformIdentity},
				}},
			},
		},
		{
			Name:    "raw.events",
			Columns: []string{"event_id", "event_type"},
		},
	}

	graph := BuildLineageGraph(models)
	got := graph.Render()

	if !strings.Contains(got, "revenue") {
		t.Error("should contain mart model name")
	}
	if !strings.Contains(got, "orders") {
		t.Error("should contain staging model name")
	}
	if !strings.Contains(got, "events") {
		t.Error("should contain raw model name")
	}
}

func TestCalcWidth_NoSources(t *testing.T) {
	t.Parallel()
	box := &GraphBox{
		Name:    "test",
		Columns: []string{"id"},
		ColumnLineage: []ColumnLineageInfo{
			{Column: "total", Sources: nil}, // empty sources -> continue
		},
	}
	w := box.CalcWidth()
	if w < 30 {
		t.Errorf("width = %d, should be at least 30", w)
	}
}

func TestCalcWidth_MultiSourceWithTransform(t *testing.T) {
	t.Parallel()
	box := &GraphBox{
		Name: "test",
		ColumnLineage: []ColumnLineageInfo{
			{
				Column: "total",
				Sources: []ColumnSource{
					{Table: "t1", Column: "a", Transformation: TransformArithmetic},
					{Table: "t1", Column: "b", Transformation: TransformArithmetic},
				},
			},
		},
	}
	w := box.CalcWidth()
	if w < 30 {
		t.Errorf("width = %d, should be at least 30", w)
	}
}

func TestDrawBox_NarrowTitle(t *testing.T) {
	t.Parallel()
	// Title wider than box width - forces pad < 1
	box := &GraphBox{
		Name:    "a_very_long_title_that_exceeds_width",
		Columns: nil,
		X:       0,
		Y:       0,
		Width:   10, // much narrower than title
	}
	c := NewCanvas(50, 10)
	c.DrawBox(box, DoubleBox)
	// Should not panic
	s := c.String()
	if s == "" {
		t.Error("expected non-empty output")
	}
}

func TestRender_WithSourceLayer(t *testing.T) {
	t.Parallel()
	// Create a graph with a "source" layer to trigger SingleBox style
	models := []*ModelLineage{
		{
			Name:    "staging.orders",
			Columns: []string{"id"},
			ColumnLineage: []ColumnLineageInfo{
				{Column: "id", Sources: []ColumnSource{
					{Table: "source.api_data", Column: "event_id", Transformation: TransformIdentity},
				}},
			},
		},
		{
			Name:    "source.api_data",
			Columns: []string{"event_id"},
		},
	}
	graph := BuildLineageGraph(models)
	got := graph.Render()
	if !strings.Contains(got, "orders") {
		t.Error("should contain orders")
	}
	if !strings.Contains(got, "api_data") {
		t.Error("should contain api_data")
	}
}

func TestCalcWidth_EmptyName(t *testing.T) {
	t.Parallel()
	box := &GraphBox{
		Name:    "",
		Columns: nil,
	}
	w := box.CalcWidth()
	if w < 30 {
		t.Errorf("width = %d, should be at least 30 (minimum)", w)
	}
}

func TestTotalWidth_Empty(t *testing.T) {
	t.Parallel()
	layer := &GraphLayer{Boxes: nil}
	if w := layer.TotalWidth(); w != 0 {
		t.Errorf("TotalWidth() = %d, want 0", w)
	}
}

func TestTotalWidth_OneBox(t *testing.T) {
	t.Parallel()
	layer := &GraphLayer{
		Boxes: []*GraphBox{{Width: 30}},
	}
	if w := layer.TotalWidth(); w != 36 { // 30 + 6
		t.Errorf("TotalWidth() = %d, want 36", w)
	}
}

func TestTotalWidth_MultipleBoxes(t *testing.T) {
	t.Parallel()
	layer := &GraphLayer{
		Boxes: []*GraphBox{{Width: 30}, {Width: 40}},
	}
	want := 30 + 6 + 40 + 6
	if w := layer.TotalWidth(); w != want {
		t.Errorf("TotalWidth() = %d, want %d", w, want)
	}
}

func TestSchemaOf(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		want string
	}{
		{"mart.sales", "mart"},
		{"staging.orders", "staging"},
		{"raw.events", "raw"},
		{"bronze.events", "bronze"},
		{"gold.summary", "gold"},
		{"custom.table", "custom"},
		{"single_name", "main"}, // no schema prefix defaults to main
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := schemaOf(tt.name)
			if got != tt.want {
				t.Errorf("schemaOf(%q) = %q, want %q", tt.name, got, tt.want)
			}
		})
	}
}

func TestFormatColumnWithLineage_NoSources(t *testing.T) {
	t.Parallel()
	lineageMap := map[string][]ColumnSource{}
	got := formatColumnWithLineage("id", lineageMap)
	if got != "id" {
		t.Errorf("got %q, want %q", got, "id")
	}
}

func TestFormatColumnWithLineage_Identity(t *testing.T) {
	t.Parallel()
	lineageMap := map[string][]ColumnSource{
		"id": {{Table: "raw.orders", Column: "order_id", Transformation: TransformIdentity}},
	}
	got := formatColumnWithLineage("id", lineageMap)
	want := "id ← raw.orders.order_id"
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestFormatColumnWithLineage_EmptyTransform(t *testing.T) {
	t.Parallel()
	lineageMap := map[string][]ColumnSource{
		"id": {{Table: "raw.orders", Column: "order_id", Transformation: ""}},
	}
	got := formatColumnWithLineage("id", lineageMap)
	want := "id ← raw.orders.order_id"
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestFormatColumnWithLineage_Transform(t *testing.T) {
	t.Parallel()
	lineageMap := map[string][]ColumnSource{
		"total": {{Table: "staging.orders", Column: "amount", Transformation: TransformAggregation}},
	}
	got := formatColumnWithLineage("total", lineageMap)
	want := "total ← AGGREGATION(staging.orders.amount)"
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestFormatColumnWithLineage_MultiSource(t *testing.T) {
	t.Parallel()
	lineageMap := map[string][]ColumnSource{
		"total": {
			{Table: "t1", Column: "a", Transformation: TransformIdentity},
			{Table: "t1", Column: "b", Transformation: TransformIdentity},
		},
	}
	got := formatColumnWithLineage("total", lineageMap)
	want := "total ← a, b"
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestFormatColumnWithLineage_MultiSourceWithTransform(t *testing.T) {
	t.Parallel()
	lineageMap := map[string][]ColumnSource{
		"total": {
			{Table: "t1", Column: "a", Transformation: TransformArithmetic},
			{Table: "t1", Column: "b", Transformation: TransformArithmetic},
		},
	}
	got := formatColumnWithLineage("total", lineageMap)
	want := "total ← ARITHMETIC(a, b)"
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestRuneLen(t *testing.T) {
	t.Parallel()
	tests := []struct {
		s    string
		want int
	}{
		{"hello", 5},
		{"", 0},
		{"åäö", 3},
		{"◆ test", 6},
		{"日本語", 3},
	}
	for _, tt := range tests {
		got := runeLen(tt.s)
		if got != tt.want {
			t.Errorf("runeLen(%q) = %d, want %d", tt.s, got, tt.want)
		}
	}
}

func TestBuildLineageGraph(t *testing.T) {
	t.Parallel()
	models := []*ModelLineage{
		{
			Name:    "mart.sales",
			Columns: []string{"revenue"},
			ColumnLineage: []ColumnLineageInfo{
				{Column: "revenue", Sources: []ColumnSource{
					{Table: "staging.orders", Column: "amount", Transformation: TransformAggregation},
				}},
			},
		},
		{
			Name:    "staging.orders",
			Columns: []string{"amount"},
		},
	}

	graph := BuildLineageGraph(models)

	if len(graph.Layers) != 2 {
		t.Fatalf("got %d layers, want 2", len(graph.Layers))
	}

	// Check layer grouping
	layerNames := make(map[string]bool)
	for _, l := range graph.Layers {
		layerNames[l.Name] = true
	}
	if !layerNames["mart"] {
		t.Error("missing mart layer")
	}
	if !layerNames["staging"] {
		t.Error("missing staging layer")
	}

	// Check connectors
	if len(graph.Connectors) != 1 {
		t.Fatalf("got %d connectors, want 1", len(graph.Connectors))
	}
	conn := graph.Connectors[0]
	if conn.FromBox != "staging.orders" || conn.ToBox != "mart.sales" {
		t.Errorf("connector from=%q to=%q", conn.FromBox, conn.ToBox)
	}
}

func TestBuildLineageGraph_DepthOrdering(t *testing.T) {
	t.Parallel()
	// Models with explicit dependencies to trigger depth calculation and schema ordering
	models := []*ModelLineage{
		{
			Name:         "alpha.final",
			Columns:      []string{"result"},
			Dependencies: []string{"beta.mid"},
		},
		{
			Name:         "beta.mid",
			Columns:      []string{"val"},
			Dependencies: []string{"gamma.src"},
		},
		{
			Name:    "gamma.src",
			Columns: []string{"id"},
		},
	}

	graph := BuildLineageGraph(models)

	if len(graph.Layers) != 3 {
		t.Fatalf("got %d layers, want 3", len(graph.Layers))
	}

	// Layers should be ordered by depth: alpha (depth 2) > beta (depth 1) > gamma (depth 0)
	if graph.Layers[0].Name != "alpha" {
		t.Errorf("layer 0 = %q, want alpha (highest depth)", graph.Layers[0].Name)
	}
	if graph.Layers[1].Name != "beta" {
		t.Errorf("layer 1 = %q, want beta", graph.Layers[1].Name)
	}
	if graph.Layers[2].Name != "gamma" {
		t.Errorf("layer 2 = %q, want gamma (lowest depth)", graph.Layers[2].Name)
	}
}

func TestBuildLineageGraph_CyclicDeps(t *testing.T) {
	t.Parallel()
	// Models with a dependency cycle should not infinite loop
	models := []*ModelLineage{
		{
			Name:         "x.a",
			Columns:      []string{"id"},
			Dependencies: []string{"x.b"},
		},
		{
			Name:         "x.b",
			Columns:      []string{"id"},
			Dependencies: []string{"x.a"},
		},
	}

	graph := BuildLineageGraph(models)

	if len(graph.Layers) != 1 {
		t.Fatalf("got %d layers, want 1", len(graph.Layers))
	}
	if len(graph.Layers[0].Boxes) != 2 {
		t.Errorf("got %d boxes, want 2", len(graph.Layers[0].Boxes))
	}
}

func TestBuildLineageGraph_SameSchemaMultipleDepths(t *testing.T) {
	t.Parallel()
	// Multiple models in same schema with different depths
	// schema depth should track the maximum
	models := []*ModelLineage{
		{
			Name:         "data.downstream",
			Columns:      []string{"val"},
			Dependencies: []string{"data.upstream"},
		},
		{
			Name:    "data.upstream",
			Columns: []string{"id"},
		},
		{
			Name:    "other.leaf",
			Columns: []string{"x"},
		},
	}

	graph := BuildLineageGraph(models)

	if len(graph.Layers) != 2 {
		t.Fatalf("got %d layers, want 2", len(graph.Layers))
	}

	// "data" schema has max depth 1 (from data.downstream), "other" has depth 0
	// So "data" should come first
	if graph.Layers[0].Name != "data" {
		t.Errorf("layer 0 = %q, want data (higher depth)", graph.Layers[0].Name)
	}
	if graph.Layers[1].Name != "other" {
		t.Errorf("layer 1 = %q, want other (lower depth)", graph.Layers[1].Name)
	}
}

func TestBuildLineageGraph_AlphabeticalTiebreak(t *testing.T) {
	t.Parallel()
	// Two schemas with same depth should sort alphabetically
	models := []*ModelLineage{
		{
			Name:    "zebra.t1",
			Columns: []string{"id"},
		},
		{
			Name:    "alpha.t1",
			Columns: []string{"id"},
		},
	}

	graph := BuildLineageGraph(models)

	if len(graph.Layers) != 2 {
		t.Fatalf("got %d layers, want 2", len(graph.Layers))
	}

	// Both have depth 0, alphabetical: alpha < zebra
	if graph.Layers[0].Name != "alpha" {
		t.Errorf("layer 0 = %q, want alpha", graph.Layers[0].Name)
	}
	if graph.Layers[1].Name != "zebra" {
		t.Errorf("layer 1 = %q, want zebra", graph.Layers[1].Name)
	}
}

func TestBuildLineageGraph_EmptyModels(t *testing.T) {
	t.Parallel()
	graph := BuildLineageGraph(nil)
	if len(graph.Layers) != 0 {
		t.Errorf("got %d layers, want 0", len(graph.Layers))
	}
}

func TestRender_Empty(t *testing.T) {
	t.Parallel()
	graph := &LineageGraph{}
	got := graph.Render()
	if got != "No lineage data" {
		t.Errorf("got %q, want %q", got, "No lineage data")
	}
}

func TestRender_SingleLayer(t *testing.T) {
	t.Parallel()
	models := []*ModelLineage{
		{
			Name:    "staging.orders",
			Columns: []string{"id", "amount"},
		},
	}
	graph := BuildLineageGraph(models)
	got := graph.Render()

	if !strings.Contains(got, "orders") {
		t.Error("rendered output should contain box name 'orders'")
	}
}
