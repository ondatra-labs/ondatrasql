// OndatraSQL - You don't need a data stack anymore
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

// Package lineage provides column-level lineage tracking and visualization.
package lineage

import (
	"fmt"
	"sort"
	"strings"
)

// Canvas for ASCII rendering
type Canvas struct {
	grid   [][]rune
	width  int
	height int
}

// NewCanvas creates a new canvas with given dimensions
func NewCanvas(width, height int) *Canvas {
	grid := make([][]rune, height)
	for i := range grid {
		grid[i] = make([]rune, width)
		for j := range grid[i] {
			grid[i][j] = ' '
		}
	}
	return &Canvas{grid: grid, width: width, height: height}
}

// Set places a character at position
func (c *Canvas) Set(x, y int, ch rune) {
	if y >= 0 && y < c.height && x >= 0 && x < c.width {
		c.grid[y][x] = ch
	}
}

// Get returns character at position
func (c *Canvas) Get(x, y int) rune {
	if y >= 0 && y < c.height && x >= 0 && x < c.width {
		return c.grid[y][x]
	}
	return ' '
}

// WriteString writes a string starting at position
func (c *Canvas) WriteString(x, y int, s string) {
	runePos := 0
	for _, ch := range s {
		c.Set(x+runePos, y, ch)
		runePos++
	}
}

// String returns the canvas as a string
func (c *Canvas) String() string {
	var sb strings.Builder
	for _, row := range c.grid {
		line := strings.TrimRight(string(row), " ")
		sb.WriteString(line)
		sb.WriteRune('\n')
	}
	return strings.TrimRight(sb.String(), "\n")
}

// BoxStyle defines characters for box drawing
type BoxStyle struct {
	TL, TR, BL, BR rune // Corners
	H, V           rune // Horizontal, Vertical
	ML, MR         rune // Middle connectors
	T, B           rune // Top/Bottom T-connectors
}

var (
	// DoubleBox uses double-line characters
	DoubleBox = BoxStyle{'╔', '╗', '╚', '╝', '═', '║', '╠', '╣', '╤', '╧'}
	// SingleBox uses single-line characters
	SingleBox = BoxStyle{'┌', '┐', '└', '┘', '─', '│', '├', '┤', '┬', '┴'}
)

// GraphBox represents a box in the lineage graph
type GraphBox struct {
	Name          string
	Columns       []string
	ColumnLineage []ColumnLineageInfo // Column-level lineage
	X, Y          int                 // Position on canvas
	Width         int
	Height        int
	Layer         string // Schema name used as layer
}

// CalcWidth calculates the required width for the box
func (b *GraphBox) CalcWidth() int {
	width := runeLen(b.Name) + 6 // Title + padding

	// Find longest column line (with lineage)
	for _, col := range b.Columns {
		lineWidth := runeLen(col) + 4
		if lineWidth > width {
			width = lineWidth
		}
	}

	// Check lineage expressions - build same format as formatColumnWithLineage
	for _, cl := range b.ColumnLineage {
		var expr string
		if len(cl.Sources) == 0 {
			continue
		} else if len(cl.Sources) == 1 {
			src := cl.Sources[0]
			if src.Transformation == TransformIdentity || src.Transformation == "" {
				expr = fmt.Sprintf("%s ← %s.%s", cl.Column, src.Table, src.Column)
			} else {
				expr = fmt.Sprintf("%s ← %s(%s.%s)", cl.Column, src.Transformation, src.Table, src.Column)
			}
		} else {
			// Multiple sources
			transform := cl.Sources[0].Transformation
			var colNames []string
			for _, src := range cl.Sources {
				colNames = append(colNames, src.Column)
			}
			if transform == TransformIdentity || transform == "" {
				expr = fmt.Sprintf("%s ← %s", cl.Column, strings.Join(colNames, ", "))
			} else {
				expr = fmt.Sprintf("%s ← %s(%s)", cl.Column, transform, strings.Join(colNames, ", "))
			}
		}
		lineWidth := runeLen(expr) + 4
		if lineWidth > width {
			width = lineWidth
		}
	}

	// Minimum width for readability
	if width < 30 {
		width = 30
	}

	return width
}

// runeLen returns the number of runes (display width) in a string
func runeLen(s string) int {
	return len([]rune(s))
}

// formatColumnWithLineage formats a column name with its lineage source
func formatColumnWithLineage(col string, lineageMap map[string][]ColumnSource) string {
	sources, ok := lineageMap[col]
	if !ok || len(sources) == 0 {
		return col
	}

	// Single source - simple format
	if len(sources) == 1 {
		src := sources[0]
		if src.Transformation == TransformIdentity || src.Transformation == "" {
			return fmt.Sprintf("%s ← %s.%s", col, src.Table, src.Column)
		}
		return fmt.Sprintf("%s ← %s(%s.%s)", col, src.Transformation, src.Table, src.Column)
	}

	// Multiple sources - group by transformation type
	transform := sources[0].Transformation
	var colNames []string
	for _, src := range sources {
		// Use short name if all from same table
		colNames = append(colNames, src.Column)
	}

	if transform == TransformIdentity || transform == "" {
		return fmt.Sprintf("%s ← %s", col, strings.Join(colNames, ", "))
	}
	return fmt.Sprintf("%s ← %s(%s)", col, transform, strings.Join(colNames, ", "))
}

// DrawBox draws a box on the canvas with columns stacked vertically
func (c *Canvas) DrawBox(box *GraphBox, style BoxStyle) {
	x, y := box.X, box.Y
	w := box.Width

	// Build lineage map for quick lookup
	lineageMap := make(map[string][]ColumnSource)
	for _, cl := range box.ColumnLineage {
		lineageMap[cl.Column] = cl.Sources
	}

	// Format columns with lineage
	displayCols := make([]string, len(box.Columns))
	for i, col := range box.Columns {
		displayCols[i] = formatColumnWithLineage(col, lineageMap)
	}
	numCols := len(displayCols)

	// Top border
	c.Set(x, y, style.TL)
	for i := 1; i < w-1; i++ {
		c.Set(x+i, y, style.H)
	}
	c.Set(x+w-1, y, style.TR)

	// Title row
	c.Set(x, y+1, style.V)
	title := box.Name
	titleLen := runeLen(title)
	pad := (w - 2 - titleLen) / 2
	if pad < 1 {
		pad = 1
	}
	c.WriteString(x+1+pad, y+1, title)
	c.Set(x+w-1, y+1, style.V)

	// Separator
	c.Set(x, y+2, style.ML)
	for i := 1; i < w-1; i++ {
		c.Set(x+i, y+2, style.H)
	}
	c.Set(x+w-1, y+2, style.MR)

	// Columns - one per row with lineage
	for i, col := range displayCols {
		rowY := y + 3 + i
		c.Set(x, rowY, style.V)
		c.WriteString(x+2, rowY, col)
		c.Set(x+w-1, rowY, style.V)
	}

	// Bottom border
	bottomY := y + 3 + numCols
	c.Set(x, bottomY, style.BL)
	for i := 1; i < w-1; i++ {
		c.Set(x+i, bottomY, style.H)
	}
	c.Set(x+w-1, bottomY, style.BR)

	box.Height = 4 + numCols // top + title + separator + columns + bottom
}

// DrawVerticalLine draws a vertical line
func (c *Canvas) DrawVerticalLine(x, y1, y2 int) {
	if y1 > y2 {
		y1, y2 = y2, y1
	}
	for y := y1; y <= y2; y++ {
		existing := c.Get(x, y)
		switch existing {
		case '─', '═':
			c.Set(x, y, '┼')
		case '┐', '╗':
			c.Set(x, y, '┤')
		case '┌', '╔':
			c.Set(x, y, '├')
		default:
			c.Set(x, y, '│')
		}
	}
}

// DrawHorizontalLine draws a horizontal line
func (c *Canvas) DrawHorizontalLine(x1, x2, y int) {
	if x1 > x2 {
		x1, x2 = x2, x1
	}
	for x := x1; x <= x2; x++ {
		existing := c.Get(x, y)
		switch existing {
		case '│':
			c.Set(x, y, '┼')
		default:
			c.Set(x, y, '─')
		}
	}
}

// DrawConnector draws a connector between boxes
func (c *Canvas) DrawConnector(x1, y1, x2, y2 int, label string) {
	// Draw from bottom of upper box to top of lower box
	midY := (y1 + y2) / 2

	// Vertical from source
	c.DrawVerticalLine(x1, y1, midY)
	c.Set(x1, y1, '│')

	// Horizontal connector
	if x1 != x2 {
		c.DrawHorizontalLine(x1, x2, midY)
		if x1 < x2 {
			c.Set(x1, midY, '└')
			c.Set(x2, midY, '┐')
		} else {
			c.Set(x1, midY, '┘')
			c.Set(x2, midY, '┌')
		}
	}

	// Vertical to target
	c.DrawVerticalLine(x2, midY, y2)
	c.Set(x2, y2, '▼')

	// Label
	if label != "" {
		labelX := x1 + 2
		if x1 > x2 {
			labelX = x2 + 2
		}
		c.WriteString(labelX, midY-1, label)
	}
}

// GraphLayer represents a layer in the lineage graph
type GraphLayer struct {
	Name  string      // Layer name (schema name)
	Boxes []*GraphBox
}

// TotalWidth calculates total width needed for the layer
func (l *GraphLayer) TotalWidth() int {
	w := 0
	for _, box := range l.Boxes {
		w += box.Width + 6 // Box width + spacing
	}
	return w
}

// LineageGraph represents the full lineage visualization
type LineageGraph struct {
	Layers     []*GraphLayer
	Connectors []Connector
}

// Connector represents a connection between columns
type Connector struct {
	FromBox    string
	FromColumn string
	ToBox      string
	ToColumn   string
	Transform  TransformationType
}

// Render renders the lineage graph to ASCII
func (g *LineageGraph) Render() string {
	if len(g.Layers) == 0 {
		return "No lineage data"
	}

	// Calculate dimensions - find widest box and total height
	maxBoxWidth := 0
	totalHeight := 5 // Initial padding
	for _, layer := range g.Layers {
		for _, box := range layer.Boxes {
			box.Width = box.CalcWidth()
			if box.Width > maxBoxWidth {
				maxBoxWidth = box.Width
			}
			// Height: 4 (frame) + numColumns + 3 (connector)
			totalHeight += 4 + len(box.Columns) + 3
		}
	}

	// Canvas width based on widest box
	canvasWidth := maxBoxWidth + 10
	if canvasWidth < 60 {
		canvasWidth = 60
	}

	canvas := NewCanvas(canvasWidth, totalHeight)

	// Position and draw boxes, stacked vertically
	y := 1

	for i, layer := range g.Layers {
		style := DoubleBox

		for j, box := range layer.Boxes {
			// Center box horizontally
			x := (canvasWidth - box.Width) / 2
			if x < 2 {
				x = 2
			}

			box.X = x
			box.Y = y
			canvas.DrawBox(box, style)

			// Draw connector to next box (unless last box overall)
			isLastBox := (i == len(g.Layers)-1) && (j == len(layer.Boxes)-1)
			if !isLastBox {
				centerX := canvasWidth / 2
				connY := y + box.Height
				canvas.Set(centerX, connY, '│')
				canvas.Set(centerX, connY+1, '│')
				canvas.Set(centerX, connY+2, '▼')
			}

			y += box.Height + 3 // Box height + connector space
		}
	}

	return canvas.String()
}

// BuildLineageGraph builds a lineage graph from model info.
// Models are grouped by schema (first part of schema.table name) and
// ordered by dependency depth (models with no dependencies first).
func BuildLineageGraph(models []*ModelLineage) *LineageGraph {
	graph := &LineageGraph{
		Layers: make([]*GraphLayer, 0),
	}

	// Build dependency set for depth calculation
	depSet := make(map[string][]string)
	for _, model := range models {
		depSet[model.Name] = model.Dependencies
	}

	// Calculate dependency depth for each model
	depths := make(map[string]int)
	var calcDepth func(name string, visited map[string]bool) int
	calcDepth = func(name string, visited map[string]bool) int {
		if d, ok := depths[name]; ok {
			return d
		}
		if visited[name] {
			return 0 // Cycle
		}
		visited[name] = true
		maxDep := 0
		for _, dep := range depSet[name] {
			d := calcDepth(dep, visited) + 1
			if d > maxDep {
				maxDep = d
			}
		}
		depths[name] = maxDep
		return maxDep
	}
	for _, model := range models {
		calcDepth(model.Name, make(map[string]bool))
	}

	// Group models by schema
	layerMap := make(map[string]*GraphLayer)
	schemaDepth := make(map[string]int) // Track max depth per schema for ordering

	for _, model := range models {
		schema := schemaOf(model.Name)
		if _, ok := layerMap[schema]; !ok {
			layerMap[schema] = &GraphLayer{Name: schema, Boxes: make([]*GraphBox, 0)}
		}
		box := &GraphBox{
			Name:          model.Name,
			Columns:       model.Columns,
			Layer:         schema,
			ColumnLineage: model.ColumnLineage,
		}
		layerMap[schema].Boxes = append(layerMap[schema].Boxes, box)

		// Track the maximum depth in this schema (higher depth = downstream)
		if d := depths[model.Name]; d > schemaDepth[schema] {
			schemaDepth[schema] = d
		}
	}

	// Sort schemas by depth (deepest/downstream first, sources last)
	var schemas []string
	for s := range layerMap {
		schemas = append(schemas, s)
	}
	sort.Slice(schemas, func(i, j int) bool {
		di, dj := schemaDepth[schemas[i]], schemaDepth[schemas[j]]
		if di != dj {
			return di > dj // Higher depth first (downstream)
		}
		return schemas[i] < schemas[j] // Alphabetical tiebreak
	})

	// Add non-empty layers in depth order
	for _, name := range schemas {
		if layer := layerMap[name]; len(layer.Boxes) > 0 {
			graph.Layers = append(graph.Layers, layer)
		}
	}

	// Build connectors from column lineage
	for _, model := range models {
		for _, cl := range model.ColumnLineage {
			for _, src := range cl.Sources {
				graph.Connectors = append(graph.Connectors, Connector{
					FromBox:    src.Table,
					FromColumn: src.Column,
					ToBox:      model.Name,
					ToColumn:   cl.Column,
					Transform:  src.Transformation,
				})
			}
		}
	}

	return graph
}

// ModelLineage contains lineage info for a single model
type ModelLineage struct {
	Name          string
	Columns       []string
	ColumnLineage []ColumnLineageInfo
	Transforms    map[string]string
	Dependencies  []string
}

// ColumnLineageInfo contains lineage for a single column
type ColumnLineageInfo struct {
	Column  string
	Sources []ColumnSource
}

// ColumnSource represents a source column
type ColumnSource struct {
	Table          string
	Column         string
	Transformation TransformationType
	FunctionName   string
}

// schemaOf extracts the schema name from a qualified model name (schema.table).
// Returns "main" if the name has no schema prefix.
func schemaOf(name string) string {
	if i := strings.IndexByte(name, '.'); i > 0 {
		return name[:i]
	}
	return "main"
}

