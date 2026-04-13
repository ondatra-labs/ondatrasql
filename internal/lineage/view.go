// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

// Package lineage provides column-level lineage tracking and visualization.
package lineage

import (
	"fmt"
	"sort"
	"strings"
)

// ViewMode represents the lineage visualization mode
type ViewMode int

const (
	ViewOverview    ViewMode = iota // All models collapsed
	ViewModelFocus                  // Selected model expanded
	ViewColumnFocus                 // Single column trace
)

// RoundedBox style for unified look
var RoundedBox = struct {
	TL, TR, BL, BR rune // Corners: ╭ ╮ ╰ ╯
	H, V           rune // Lines: ─ │
	ML, MR         rune // Middle: ├ ┤
}{
	TL: '╭', TR: '╮', BL: '╰', BR: '╯',
	H: '─', V: '│',
	ML: '├', MR: '┤',
}

// LineageView handles rendering of lineage graphs
type LineageView struct {
	Models       []*ModelLineage
	TargetModel  string
	TargetColumn string
	Mode         ViewMode
	width        int
}

// NewLineageView creates a new lineage view
func NewLineageView(models []*ModelLineage) *LineageView {
	return &LineageView{
		Models: models,
		width:  70,
	}
}

// SetTarget sets the target model and/or column for focused views
func (v *LineageView) SetTarget(target string) {
	parts := strings.Split(target, ".")
	if len(parts) >= 3 {
		// model.column format (e.g., mart.product_sales.total_revenue)
		v.TargetModel = parts[0] + "." + parts[1]
		v.TargetColumn = parts[2]
		v.Mode = ViewColumnFocus
	} else if len(parts) == 2 {
		// Just model (e.g., mart.product_sales)
		v.TargetModel = target
		v.Mode = ViewModelFocus
	} else {
		v.Mode = ViewOverview
	}
}

// Render renders the lineage view based on the current mode
func (v *LineageView) Render() string {
	switch v.Mode {
	case ViewOverview:
		return v.renderOverview()
	case ViewModelFocus:
		return v.renderModelFocus()
	case ViewColumnFocus:
		return v.renderColumnFocus()
	default:
		return v.renderModelFocus()
	}
}

// padRight pads a string to a specific rune width
func padRight(s string, width int) string {
	runeCount := len([]rune(s))
	if runeCount >= width {
		return s
	}
	return s + strings.Repeat(" ", width-runeCount)
}

// padCenter centers a string within a width
func padCenter(s string, width int) string {
	runeCount := len([]rune(s))
	if runeCount >= width {
		return s
	}
	leftPad := (width - runeCount) / 2
	rightPad := width - runeCount - leftPad
	return strings.Repeat(" ", leftPad) + s + strings.Repeat(" ", rightPad)
}

// dotFill creates a dotted line between two strings
func dotFill(left, right string, totalWidth int) string {
	leftLen := len([]rune(left))
	rightLen := len([]rune(right))
	dotsNeeded := totalWidth - leftLen - rightLen
	if dotsNeeded < 3 {
		dotsNeeded = 3
	}
	dots := strings.Repeat("·", dotsNeeded)
	return left + " " + dots + " " + right
}

// renderOverview renders all models with canvas-based flexible arrows
func (v *LineageView) renderOverview() string {
	// Group models by layer
	layers := v.groupByLayer()
	layerOrder := v.layerOrder(layers)

	// Collect active layers
	type layerData struct {
		name   string
		models []*ModelLineage
	}
	var activeLayers []layerData
	for _, layerName := range layerOrder {
		if models, ok := layers[layerName]; ok && len(models) > 0 {
			activeLayers = append(activeLayers, layerData{layerName, models})
		}
	}

	if len(activeLayers) == 0 {
		return "No models found\n"
	}

	// Layout constants
	boxWidth := 18
	boxHeight := 3
	layerGap := 18 // space between layers for arrows (increased for clarity)
	rowGap := 1    // vertical gap between boxes

	// Find max models in any layer
	maxModels := 0
	for _, layer := range activeLayers {
		if len(layer.models) > maxModels {
			maxModels = len(layer.models)
		}
	}

	// Calculate canvas size (add extra margin for arrows)
	canvasWidth := len(activeLayers)*boxWidth + (len(activeLayers)-1)*layerGap + 10
	canvasHeight := 2 + maxModels*(boxHeight+rowGap) + 2 // +2 for header, +2 for margin

	// Create canvas
	canvas := make([][]rune, canvasHeight)
	for i := range canvas {
		canvas[i] = make([]rune, canvasWidth)
		for j := range canvas[i] {
			canvas[i][j] = ' '
		}
	}

	// Helper to write string to canvas
	writeStr := func(x, y int, s string) {
		for i, r := range s {
			if x+i >= 0 && x+i < canvasWidth && y >= 0 && y < canvasHeight {
				canvas[y][x+i] = r
			}
		}
	}

	// Helper to write rune to canvas
	writeRune := func(x, y int, r rune) {
		if x >= 0 && x < canvasWidth && y >= 0 && y < canvasHeight {
			canvas[y][x] = r
		}
	}

	// Track box positions: model name -> (x of right edge, y of center)
	type boxPos struct {
		left, right, centerY int
	}
	positions := make(map[string]boxPos)

	// Draw layer headers and boxes
	for li, layer := range activeLayers {
		layerX := li * (boxWidth + layerGap)

		// Layer header
		header := layer.name
		headerX := layerX + (boxWidth-len(header))/2
		writeStr(headerX, 0, header)

		// Draw boxes for this layer
		for ri, model := range layer.models {
			boxY := 2 + ri*(boxHeight+rowGap)

			// Get short name
			shortName := model.Name
			if parts := strings.Split(model.Name, "."); len(parts) > 1 {
				shortName = parts[1]
			}
			if len(shortName) > boxWidth-2 {
				shortName = shortName[:boxWidth-2]
			}

			// Draw box (plain - colors added later)
			// Top
			writeRune(layerX, boxY, RoundedBox.TL)
			for i := 1; i < boxWidth-1; i++ {
				writeRune(layerX+i, boxY, RoundedBox.H)
			}
			writeRune(layerX+boxWidth-1, boxY, RoundedBox.TR)

			// Middle with name
			writeRune(layerX, boxY+1, RoundedBox.V)
			nameX := layerX + 1 + (boxWidth-2-len(shortName))/2
			writeStr(nameX, boxY+1, shortName)
			writeRune(layerX+boxWidth-1, boxY+1, RoundedBox.V)

			// Bottom
			writeRune(layerX, boxY+2, RoundedBox.BL)
			for i := 1; i < boxWidth-1; i++ {
				writeRune(layerX+i, boxY+2, RoundedBox.H)
			}
			writeRune(layerX+boxWidth-1, boxY+2, RoundedBox.BR)

			// Store position
			positions[model.Name] = boxPos{
				left:    layerX,
				right:   layerX + boxWidth - 1,
				centerY: boxY + 1,
			}
		}
	}

	// Build a map of model name -> layer index
	modelToLayer := make(map[string]int)
	for li, layer := range activeLayers {
		for _, model := range layer.models {
			modelToLayer[model.Name] = li
		}
	}

	// Collect all arrows first, then draw with offset to avoid overlaps
	type arrow struct {
		startX, startY, endX, endY int
		offset                      int  // vertical offset in the connector area
		sameLayer                   bool // true if same-layer dependency
	}
	var arrows []arrow
	var sameLayerArrows []arrow

	arrowOffset := 0
	for li, layer := range activeLayers {
		for _, targetModel := range layer.models {
			targetPos, ok := positions[targetModel.Name]
			if !ok {
				continue
			}

			for _, dep := range targetModel.Dependencies {
				depLayer, ok := modelToLayer[dep]
				if !ok {
					continue
				}

				sourcePos, ok := positions[dep]
				if !ok {
					continue
				}

				if depLayer == li {
					// Same-layer dependency - draw vertical arrow on the right
					sameLayerArrows = append(sameLayerArrows, arrow{
						startX:    sourcePos.right + 1,
						startY:    sourcePos.centerY,
						endX:      targetPos.right + 1,
						endY:      targetPos.centerY,
						sameLayer: true,
					})
				} else if depLayer < li {
					// Cross-layer dependency (left to right)
					arrows = append(arrows, arrow{
						startX: sourcePos.right + 1,
						startY: sourcePos.centerY,
						endX:   targetPos.left - 1,
						endY:   targetPos.centerY,
						offset: arrowOffset,
					})
					arrowOffset++
				}
				// Skip reverse dependencies (depLayer > li)
			}
		}
	}

	// Helper to safely write without overwriting box chars or other arrows
	safeWriteArrow := func(x, y int, r rune) {
		if x < 0 || x >= canvasWidth || y < 0 || y >= canvasHeight {
			return
		}
		existing := canvas[y][x]
		// Don't overwrite box characters
		if existing == RoundedBox.TL || existing == RoundedBox.TR ||
			existing == RoundedBox.BL || existing == RoundedBox.BR ||
			existing == RoundedBox.V || existing == RoundedBox.H ||
			existing == RoundedBox.ML || existing == RoundedBox.MR {
			return
		}
		// Handle arrow intersections
		if existing == '│' && (r == '─' || r == '→') {
			canvas[y][x] = '┼'
			return
		}
		if existing == '─' && r == '│' {
			canvas[y][x] = '┼'
			return
		}
		canvas[y][x] = r
	}

	// Draw arrows (with bounds checking)
	for _, a := range arrows {
		if a.startY == a.endY {
			// Straight horizontal arrow
			for x := a.startX; x < a.endX; x++ {
				safeWriteArrow(x, a.startY, '─')
			}
			safeWriteArrow(a.endX, a.endY, '→')
		} else {
			// Use offset to prevent overlapping vertical segments
			// Spread offsets across available space
			availableSpace := a.endX - a.startX - 4
			if availableSpace < 4 {
				availableSpace = 4
			}
			midX := a.startX + 2 + (a.offset%8)*(availableSpace/8+1)
			if midX >= a.endX-2 {
				midX = a.startX + (a.endX-a.startX)/2
			}

			// Horizontal from source to midpoint
			for x := a.startX; x < midX; x++ {
				safeWriteArrow(x, a.startY, '─')
			}

			// Vertical segment with corners
			if a.startY < a.endY {
				safeWriteArrow(midX, a.startY, '╮')
				for y := a.startY + 1; y < a.endY; y++ {
					safeWriteArrow(midX, y, '│')
				}
				safeWriteArrow(midX, a.endY, '╰')
			} else {
				safeWriteArrow(midX, a.startY, '╯')
				for y := a.endY + 1; y < a.startY; y++ {
					safeWriteArrow(midX, y, '│')
				}
				safeWriteArrow(midX, a.endY, '╭')
			}

			// Horizontal from midpoint to target
			for x := midX + 1; x < a.endX; x++ {
				safeWriteArrow(x, a.endY, '─')
			}
			safeWriteArrow(a.endX, a.endY, '→')
		}
	}

	// Draw same-layer arrows (vertical arrows pointing into target)
	// Group by layer to handle offsets
	for i, a := range sameLayerArrows {
		// Offset each arrow horizontally to avoid overlaps
		offsetX := (i % 3) * 2 // 0, 2, 4, 0, 2, 4, ...
		arrowX := a.startX + 1 + offsetX

		if arrowX >= canvasWidth-1 {
			arrowX = a.startX + 1
		}

		if a.startY < a.endY {
			// Arrow going down: source above target
			// Draw:  ─╮
			//         │
			//        ←╯  (pointing INTO target)
			safeWriteArrow(a.startX, a.startY, '─')
			safeWriteArrow(arrowX, a.startY, '╮')
			for y := a.startY + 1; y < a.endY; y++ {
				safeWriteArrow(arrowX, y, '│')
			}
			safeWriteArrow(arrowX, a.endY, '╯')
			safeWriteArrow(arrowX-1, a.endY, '←')
		} else {
			// Arrow going up: source below target
			// Draw:  ─╯
			//         │
			//        ←╮  (pointing INTO target)
			safeWriteArrow(a.startX, a.startY, '─')
			safeWriteArrow(arrowX, a.startY, '╯')
			for y := a.endY + 1; y < a.startY; y++ {
				safeWriteArrow(arrowX, y, '│')
			}
			safeWriteArrow(arrowX, a.endY, '╮')
			safeWriteArrow(arrowX-1, a.endY, '←')
		}
	}

	// Redraw boxes on top to ensure they're not overwritten by arrows
	for li, layer := range activeLayers {
		layerX := li * (boxWidth + layerGap)

		for ri, model := range layer.models {
			boxY := 2 + ri*(boxHeight+rowGap)

			shortName := model.Name
			if parts := strings.Split(model.Name, "."); len(parts) > 1 {
				shortName = parts[1]
			}
			if len(shortName) > boxWidth-2 {
				shortName = shortName[:boxWidth-2]
			}

			// Redraw box
			writeRune(layerX, boxY, RoundedBox.TL)
			for i := 1; i < boxWidth-1; i++ {
				writeRune(layerX+i, boxY, RoundedBox.H)
			}
			writeRune(layerX+boxWidth-1, boxY, RoundedBox.TR)

			writeRune(layerX, boxY+1, RoundedBox.V)
			for i := 1; i < boxWidth-1; i++ {
				writeRune(layerX+i, boxY+1, ' ')
			}
			nameX := layerX + 1 + (boxWidth-2-len(shortName))/2
			writeStr(nameX, boxY+1, shortName)
			writeRune(layerX+boxWidth-1, boxY+1, RoundedBox.V)

			writeRune(layerX, boxY+2, RoundedBox.BL)
			for i := 1; i < boxWidth-1; i++ {
				writeRune(layerX+i, boxY+2, RoundedBox.H)
			}
			writeRune(layerX+boxWidth-1, boxY+2, RoundedBox.BR)
		}
	}

	// Convert canvas to string
	var sb strings.Builder
	sb.WriteString("\n")
	for _, row := range canvas {
		line := strings.TrimRight(string(row), " ")
		sb.WriteString(line)
		sb.WriteString("\n")
	}
	sb.WriteString("\n")
	sb.WriteString(v.renderLegend())

	return sb.String()
}

// renderModelFocus renders the target model expanded with horizontal layout (left to right)
func (v *LineageView) renderModelFocus() string {
	// Group models by layer
	layers := v.groupByLayer()
	layerOrder := v.layerOrder(layers)

	type layerData struct {
		name   string
		models []*ModelLineage
	}
	var activeLayers []layerData
	for _, layerName := range layerOrder {
		if models, ok := layers[layerName]; ok && len(models) > 0 {
			activeLayers = append(activeLayers, layerData{layerName, models})
		}
	}

	if len(activeLayers) == 0 {
		return "No models found\n"
	}

	// Build lineage map for target
	var targetModel *ModelLineage
	for _, m := range v.Models {
		if m.Name == v.TargetModel {
			targetModel = m
			break
		}
	}

	lineageMap := make(map[string][]ColumnSource)
	if targetModel != nil {
		for _, cl := range targetModel.ColumnLineage {
			lineageMap[cl.Column] = cl.Sources
		}
	}

	// Calculate dimensions for each layer
	collapsedWidth := 20
	arrowWidth := 6

	// Find target layer and calculate expanded box width
	targetLayerIdx := -1
	expandedWidth := 40
	expandedHeight := 3

	// Calculate max column name width for alignment
	maxColWidth := 0
	for li, layer := range activeLayers {
		for _, model := range layer.models {
			if model.Name == v.TargetModel {
				targetLayerIdx = li
				for _, col := range model.Columns {
					if len(col) > maxColWidth {
						maxColWidth = len(col)
					}
				}
				// Calculate width needed for expanded box
				minWidth := len(model.Name) + 6
				for _, col := range model.Columns {
					sources := lineageMap[col]
					line := v.formatColumnLineAligned(col, sources, maxColWidth)
					lineWidth := len([]rune(line)) + 4
					if lineWidth > minWidth {
						minWidth = lineWidth
					}
				}
				expandedWidth = minWidth + 2
				expandedHeight = len(model.Columns) + 2 // header + columns + footer
				break
			}
		}
	}

	// Calculate layer widths and positions
	type layerLayout struct {
		x, width, height int
	}
	layerLayouts := make([]layerLayout, len(activeLayers))
	currentX := 0

	for li, layer := range activeLayers {
		var maxHeight, maxWidth int
		for _, model := range layer.models {
			if model.Name == v.TargetModel {
				if expandedWidth > maxWidth {
					maxWidth = expandedWidth
				}
				if expandedHeight > maxHeight {
					maxHeight = expandedHeight
				}
			} else {
				if collapsedWidth > maxWidth {
					maxWidth = collapsedWidth
				}
				if 3 > maxHeight {
					maxHeight = 3
				}
			}
		}
		// Account for multiple models stacked
		totalHeight := len(layer.models)*3 + (len(layer.models)-1)*1
		if li == targetLayerIdx {
			totalHeight = (len(layer.models)-1)*4 + expandedHeight
		}

		layerLayouts[li] = layerLayout{x: currentX, width: maxWidth, height: totalHeight}
		currentX += maxWidth + arrowWidth
	}

	// Calculate canvas size
	canvasWidth := currentX
	maxHeight := 0
	for _, ll := range layerLayouts {
		if ll.height > maxHeight {
			maxHeight = ll.height
		}
	}
	canvasHeight := maxHeight + 4 // +2 for header, +2 for margin

	// Create canvas
	canvas := make([][]rune, canvasHeight)
	for i := range canvas {
		canvas[i] = make([]rune, canvasWidth)
		for j := range canvas[i] {
			canvas[i][j] = ' '
		}
	}

	// Helper to write string to canvas
	writeStr := func(x, y int, s string) {
		for _, r := range s {
			if x >= 0 && x < canvasWidth && y >= 0 && y < canvasHeight {
				canvas[y][x] = r
			}
			x++
		}
	}

	// Helper to write rune to canvas
	writeRune := func(x, y int, r rune) {
		if x >= 0 && x < canvasWidth && y >= 0 && y < canvasHeight {
			canvas[y][x] = r
		}
	}

	// Draw layer headers
	for li, layer := range activeLayers {
		ll := layerLayouts[li]
		headerX := ll.x + (ll.width-len(layer.name))/2
		writeStr(headerX, 0, layer.name)
	}

	// Draw boxes for each layer
	for li, layer := range activeLayers {
		ll := layerLayouts[li]
		boxY := 2

		for _, model := range layer.models {
			if model.Name == v.TargetModel {
				// Draw expanded box
				innerWidth := expandedWidth - 2
				header := model.Name
				headerDashes := innerWidth - 3 - len(header)
				if headerDashes < 1 {
					headerDashes = 1
				}

				// Top line with header
				writeRune(ll.x, boxY, '╭')
				writeStr(ll.x+1, boxY, "─ "+header+" "+strings.Repeat("─", headerDashes))
				writeRune(ll.x+expandedWidth-1, boxY, '╮')

				// Column lines
				for ci, col := range model.Columns {
					sources := lineageMap[col]
					line := v.formatColumnLineAligned(col, sources, maxColWidth)
					contentWidth := innerWidth - 2
					padding := contentWidth - len([]rune(line))
					if padding < 0 {
						padding = 0
					}

					y := boxY + 1 + ci
					writeRune(ll.x, y, '│')
					writeStr(ll.x+1, y, " "+line+strings.Repeat(" ", padding)+" ")
					writeRune(ll.x+expandedWidth-1, y, '│')
				}

				// Bottom line
				bottomY := boxY + len(model.Columns) + 1
				writeRune(ll.x, bottomY, '╰')
				writeStr(ll.x+1, bottomY, strings.Repeat("─", innerWidth))
				writeRune(ll.x+expandedWidth-1, bottomY, '╯')

				boxY = bottomY + 2
			} else {
				// Draw collapsed box
				shortName := model.Name
				if parts := strings.Split(model.Name, "."); len(parts) > 1 {
					shortName = parts[1]
				}
				if len(shortName) > collapsedWidth-4 {
					shortName = shortName[:collapsedWidth-4]
				}

				// Top
				writeRune(ll.x, boxY, '╭')
				writeStr(ll.x+1, boxY, strings.Repeat("─", collapsedWidth-2))
				writeRune(ll.x+collapsedWidth-1, boxY, '╮')

				// Middle
				writeRune(ll.x, boxY+1, '│')
				nameX := ll.x + 1 + (collapsedWidth-2-len(shortName))/2
				writeStr(nameX, boxY+1, shortName)
				writeRune(ll.x+collapsedWidth-1, boxY+1, '│')

				// Bottom
				writeRune(ll.x, boxY+2, '╰')
				writeStr(ll.x+1, boxY+2, strings.Repeat("─", collapsedWidth-2))
				writeRune(ll.x+collapsedWidth-1, boxY+2, '╯')

				boxY += 4
			}
		}
	}

	// Track box positions for arrow drawing
	type boxPos struct {
		right, centerY int
		left           int
	}
	positions := make(map[string]boxPos)

	// Re-traverse to record positions
	for li, layer := range activeLayers {
		ll := layerLayouts[li]
		boxY := 2

		for _, model := range layer.models {
			var height int
			var width int
			if model.Name == v.TargetModel {
				height = expandedHeight
				width = expandedWidth
			} else {
				height = 3
				width = collapsedWidth
			}

			positions[model.Name] = boxPos{
				left:    ll.x,
				right:   ll.x + width - 1,
				centerY: boxY + height/2,
			}

			boxY += height + 1
		}
	}

	// Collect arrows based on actual dependencies
	type arrow struct {
		startX, startY, endX, endY int
		offset                      int
	}
	var arrows []arrow
	arrowOffset := 0

	for _, layer := range activeLayers {
		for _, model := range layer.models {
			targetPos, ok := positions[model.Name]
			if !ok {
				continue
			}

			for _, dep := range model.Dependencies {
				sourcePos, ok := positions[dep]
				if !ok {
					continue
				}

				arrows = append(arrows, arrow{
					startX: sourcePos.right + 1,
					startY: sourcePos.centerY,
					endX:   targetPos.left - 1,
					endY:   targetPos.centerY,
					offset: arrowOffset,
				})
				arrowOffset++
			}
		}
	}

	// Draw arrows with flexible routing
	for _, a := range arrows {
		if a.startY == a.endY {
			// Straight horizontal
			for x := a.startX; x < a.endX; x++ {
				writeRune(x, a.startY, '─')
			}
			writeRune(a.endX, a.endY, '→')
		} else {
			// Routed arrow with corner
			midX := a.startX + 2 + (a.offset%4)*2
			if midX >= a.endX-1 {
				midX = a.startX + (a.endX-a.startX)/2
			}

			// Horizontal from source
			for x := a.startX; x < midX; x++ {
				writeRune(x, a.startY, '─')
			}

			// Vertical with corners
			if a.startY < a.endY {
				writeRune(midX, a.startY, '╮')
				for y := a.startY + 1; y < a.endY; y++ {
					writeRune(midX, y, '│')
				}
				writeRune(midX, a.endY, '╰')
			} else {
				writeRune(midX, a.startY, '╯')
				for y := a.endY + 1; y < a.startY; y++ {
					writeRune(midX, y, '│')
				}
				writeRune(midX, a.endY, '╭')
			}

			// Horizontal to target
			for x := midX + 1; x < a.endX; x++ {
				writeRune(x, a.endY, '─')
			}
			writeRune(a.endX, a.endY, '→')
		}
	}

	// Convert canvas to string
	var sb strings.Builder
	sb.WriteString("\n")
	for _, row := range canvas {
		line := strings.TrimRight(string(row), " ")
		sb.WriteString(line)
		sb.WriteString("\n")
	}
	sb.WriteString("\n")
	sb.WriteString(v.renderLegend())

	return sb.String()
}

// renderColumnFocus renders column lineage horizontally (left to right)
func (v *LineageView) renderColumnFocus() string {
	// Build column trace
	traceColumns := v.buildColumnTrace()

	// Group models by layer, filter to only relevant ones
	layers := v.groupByLayer()
	layerOrder := v.layerOrder(layers)

	type layerData struct {
		name   string
		models []*ModelLineage
	}
	var activeLayers []layerData

	for _, layerName := range layerOrder {
		if models, ok := layers[layerName]; ok {
			var relevant []*ModelLineage
			for _, m := range models {
				if m.Name == v.TargetModel || traceColumns[m.Name] != nil {
					relevant = append(relevant, m)
				}
			}
			if len(relevant) > 0 {
				activeLayers = append(activeLayers, layerData{layerName, relevant})
			}
		}
	}

	if len(activeLayers) == 0 {
		return "No lineage found\n"
	}

	// Build lineage maps for all models
	lineageMaps := make(map[string]map[string][]ColumnSource)
	for _, m := range v.Models {
		lineageMaps[m.Name] = make(map[string][]ColumnSource)
		for _, cl := range m.ColumnLineage {
			lineageMaps[m.Name][cl.Column] = cl.Sources
		}
	}

	// Calculate box dimensions for each model
	type boxInfo struct {
		model       *ModelLineage
		width       int
		height      int
		tracedCols  []string
		isTarget    map[string]bool
		isSource    map[string]bool
		lineageMap  map[string][]ColumnSource
		maxColWidth int
	}

	arrowWidth := 6

	// Pre-calculate all box dimensions
	layerBoxes := make([][]boxInfo, len(activeLayers))
	layerWidths := make([]int, len(activeLayers))

	for li, layer := range activeLayers {
		layerBoxes[li] = make([]boxInfo, 0)
		maxWidth := 0

		for _, model := range layer.models {
			modelTrace := traceColumns[model.Name]
			lineageMap := lineageMaps[model.Name]

			bi := boxInfo{
				model:      model,
				isTarget:   make(map[string]bool),
				isSource:   make(map[string]bool),
				lineageMap: lineageMap,
			}

			// First pass: find traced columns and max column width
			for _, col := range model.Columns {
				isTarget := model.Name == v.TargetModel && col == v.TargetColumn
				isSource := modelTrace != nil && modelTrace[col]

				if isTarget || isSource {
					bi.tracedCols = append(bi.tracedCols, col)
					bi.isTarget[col] = isTarget
					bi.isSource[col] = isSource
					if len(col) > bi.maxColWidth {
						bi.maxColWidth = len(col)
					}
				}
			}

			// Second pass: calculate width with alignment
			minWidth := len(model.Name) + 6
			for _, col := range bi.tracedCols {
				sources := lineageMap[col]
				line := v.formatColumnLineWithMarkerAligned(col, sources, bi.isTarget[col], bi.isSource[col], bi.maxColWidth)
				lineWidth := len([]rune(line)) + 4
				if lineWidth > minWidth {
					minWidth = lineWidth
				}
			}

			bi.width = minWidth + 2
			if bi.width < 40 {
				bi.width = 40
			}
			bi.height = len(bi.tracedCols) + 2 // header + cols + footer

			if bi.width > maxWidth {
				maxWidth = bi.width
			}

			layerBoxes[li] = append(layerBoxes[li], bi)
		}

		layerWidths[li] = maxWidth
	}

	// Calculate canvas dimensions
	canvasWidth := 0
	for li := range activeLayers {
		canvasWidth += layerWidths[li]
		if li < len(activeLayers)-1 {
			canvasWidth += arrowWidth
		}
	}

	maxHeight := 0
	for _, boxes := range layerBoxes {
		layerHeight := 0
		for _, bi := range boxes {
			layerHeight += bi.height + 1
		}
		if layerHeight > maxHeight {
			maxHeight = layerHeight
		}
	}
	canvasHeight := maxHeight + 4

	// Create canvas
	canvas := make([][]rune, canvasHeight)
	for i := range canvas {
		canvas[i] = make([]rune, canvasWidth)
		for j := range canvas[i] {
			canvas[i][j] = ' '
		}
	}

	// Helper to write string to canvas
	writeStr := func(x, y int, s string) {
		for _, r := range s {
			if x >= 0 && x < canvasWidth && y >= 0 && y < canvasHeight {
				canvas[y][x] = r
			}
			x++
		}
	}

	// Helper to write rune to canvas
	writeRune := func(x, y int, r rune) {
		if x >= 0 && x < canvasWidth && y >= 0 && y < canvasHeight {
			canvas[y][x] = r
		}
	}

	// Calculate layer X positions
	layerX := make([]int, len(activeLayers))
	currentX := 0
	for li := range activeLayers {
		layerX[li] = currentX
		currentX += layerWidths[li] + arrowWidth
	}

	// Draw layer headers
	for li, layer := range activeLayers {
		headerX := layerX[li] + (layerWidths[li]-len(layer.name))/2
		writeStr(headerX, 0, layer.name)
	}

	// Draw boxes
	for li, boxes := range layerBoxes {
		boxY := 2
		lx := layerX[li]

		for _, bi := range boxes {
			innerWidth := bi.width - 2
			header := bi.model.Name
			headerDashes := innerWidth - 3 - len(header)
			if headerDashes < 1 {
				headerDashes = 1
			}

			// Top line with header
			writeRune(lx, boxY, '╭')
			writeStr(lx+1, boxY, "─ "+header+" "+strings.Repeat("─", headerDashes))
			writeRune(lx+bi.width-1, boxY, '╮')

			// Column lines
			for ci, col := range bi.tracedCols {
				sources := bi.lineageMap[col]
				line := v.formatColumnLineWithMarkerAligned(col, sources, bi.isTarget[col], bi.isSource[col], bi.maxColWidth)
				contentWidth := innerWidth - 2
				padding := contentWidth - len([]rune(line))
				if padding < 0 {
					padding = 0
				}

				y := boxY + 1 + ci
				writeRune(lx, y, '│')
				writeStr(lx+1, y, " "+line+strings.Repeat(" ", padding)+" ")
				writeRune(lx+bi.width-1, y, '│')
			}

			// Bottom line
			bottomY := boxY + len(bi.tracedCols) + 1
			writeRune(lx, bottomY, '╰')
			writeStr(lx+1, bottomY, strings.Repeat("─", innerWidth))
			writeRune(lx+bi.width-1, bottomY, '╯')

			boxY = bottomY + 2
		}
	}

	// Track box positions for arrow drawing
	type boxPos struct {
		left, right, centerY int
	}
	positions := make(map[string]boxPos)

	// Re-traverse to record positions
	for li, boxes := range layerBoxes {
		lx := layerX[li]
		boxY := 2

		for _, bi := range boxes {
			positions[bi.model.Name] = boxPos{
				left:    lx,
				right:   lx + bi.width - 1,
				centerY: boxY + bi.height/2,
			}
			boxY += bi.height + 1
		}
	}

	// Collect arrows based on actual dependencies (only for traced models)
	type arrow struct {
		startX, startY, endX, endY int
		offset                      int
	}
	var arrows []arrow
	arrowOffset := 0

	for _, boxes := range layerBoxes {
		for _, bi := range boxes {
			targetPos, ok := positions[bi.model.Name]
			if !ok {
				continue
			}

			for _, dep := range bi.model.Dependencies {
				sourcePos, ok := positions[dep]
				if !ok {
					continue
				}

				arrows = append(arrows, arrow{
					startX: sourcePos.right + 1,
					startY: sourcePos.centerY,
					endX:   targetPos.left - 1,
					endY:   targetPos.centerY,
					offset: arrowOffset,
				})
				arrowOffset++
			}
		}
	}

	// Draw arrows with flexible routing
	for _, a := range arrows {
		if a.startY == a.endY {
			// Straight horizontal
			for x := a.startX; x < a.endX; x++ {
				writeRune(x, a.startY, '─')
			}
			writeRune(a.endX, a.endY, '→')
		} else {
			// Routed arrow with corner
			midX := a.startX + 2 + (a.offset%4)*2
			if midX >= a.endX-1 {
				midX = a.startX + (a.endX-a.startX)/2
			}

			// Horizontal from source
			for x := a.startX; x < midX; x++ {
				writeRune(x, a.startY, '─')
			}

			// Vertical with corners
			if a.startY < a.endY {
				writeRune(midX, a.startY, '╮')
				for y := a.startY + 1; y < a.endY; y++ {
					writeRune(midX, y, '│')
				}
				writeRune(midX, a.endY, '╰')
			} else {
				writeRune(midX, a.startY, '╯')
				for y := a.endY + 1; y < a.startY; y++ {
					writeRune(midX, y, '│')
				}
				writeRune(midX, a.endY, '╭')
			}

			// Horizontal to target
			for x := midX + 1; x < a.endX; x++ {
				writeRune(x, a.endY, '─')
			}
			writeRune(a.endX, a.endY, '→')
		}
	}

	// Convert canvas to string
	var sb strings.Builder
	sb.WriteString("\n")
	sb.WriteString(fmt.Sprintf("  Column trace: %s.%s\n", v.TargetModel, v.TargetColumn))
	sb.WriteString("\n")
	for _, row := range canvas {
		line := strings.TrimRight(string(row), " ")
		sb.WriteString(line)
		sb.WriteString("\n")
	}
	sb.WriteString("\n")
	sb.WriteString(v.renderLegend())

	return sb.String()
}



// formatColumnLineAligned formats a column with aligned source column
func (v *LineageView) formatColumnLineAligned(col string, sources []ColumnSource, colWidth int) string {
	if len(sources) == 0 {
		return "◆ " + col
	}

	src := sources[0]
	transform := v.transformShortWithFunc(src.Transformation, src.FunctionName)

	// Calculate padding to align sources
	colPart := col
	if colWidth > 0 && len(col) < colWidth {
		colPart = col + strings.Repeat(" ", colWidth-len(col))
	}

	sourceStr := fmt.Sprintf("%s.%s", src.Table, src.Column)
	if len(sources) > 1 {
		var colNames []string
		for _, s := range sources {
			colNames = append(colNames, s.Column)
		}
		sourceStr = strings.Join(colNames, ", ")
	}

	// Align transform labels - reserve 6 chars for [SUM] etc + space
	if transform == "" {
		return fmt.Sprintf("◆ %s ··············· ● %s", colPart, sourceStr)
	}
	// Pad transform to 6 chars (e.g., "[SUM] ")
	transformPart := fmt.Sprintf("%-6s", transform)
	return fmt.Sprintf("◆ %s ········ %s ● %s", colPart, transformPart, sourceStr)
}

// formatColumnLineWithMarker formats a column with target/source markers
func (v *LineageView) formatColumnLineWithMarker(col string, sources []ColumnSource, isTarget, isSource bool) string {
	return v.formatColumnLineWithMarkerAligned(col, sources, isTarget, isSource, 0)
}

// formatColumnLineWithMarkerAligned formats a column with target/source markers and alignment
func (v *LineageView) formatColumnLineWithMarkerAligned(col string, sources []ColumnSource, isTarget, isSource bool, colWidth int) string {
	marker := "  "
	if isTarget {
		marker = "◆ "
	} else if isSource {
		marker = "● "
	}

	if len(sources) == 0 || (!isTarget && !isSource) {
		return marker + col
	}

	// Pad column name for alignment
	colPart := col
	if colWidth > 0 && len(col) < colWidth {
		colPart = col + strings.Repeat(" ", colWidth-len(col))
	}

	src := sources[0]
	transform := v.transformShortWithFunc(src.Transformation, src.FunctionName)

	sourceStr := fmt.Sprintf("%s.%s", src.Table, src.Column)
	if len(sources) > 1 {
		var colNames []string
		for _, s := range sources {
			colNames = append(colNames, s.Column)
		}
		sourceStr = strings.Join(colNames, ", ")
	}

	// Align transform labels - reserve 6 chars for [SUM] etc
	if transform == "" {
		return fmt.Sprintf("%s%s        ← ● %s", marker, colPart, sourceStr)
	}
	transformPart := fmt.Sprintf("%-6s", transform)
	return fmt.Sprintf("%s%s %s ← ● %s", marker, colPart, transformPart, sourceStr)
}

// transformShort returns a short transformation label
func (v *LineageView) transformShort(t TransformationType) string {
	switch t {
	case TransformAggregation:
		return "[AGG]"
	case TransformArithmetic:
		return "[×]"
	case TransformConditional:
		return "[IF]"
	case TransformCast:
		return "[CAST]"
	case TransformFunction:
		return "[FN]"
	case TransformIdentity, "":
		return ""
	default:
		return fmt.Sprintf("[%s]", string(t)[:3])
	}
}

// transformShortWithFunc returns a short transformation label with function name
func (v *LineageView) transformShortWithFunc(t TransformationType, funcName string) string {
	if t == TransformAggregation && funcName != "" {
		fn := strings.ToUpper(funcName)
		switch fn {
		case "SUM":
			return "[SUM]"
		case "COUNT":
			return "[CNT]"
		case "AVG":
			return "[AVG]"
		case "MIN":
			return "[MIN]"
		case "MAX":
			return "[MAX]"
		default:
			return fmt.Sprintf("[%s]", fn[:3])
		}
	}
	return v.transformShort(t)
}

// buildColumnTrace builds a map of columns in the target column's lineage
func (v *LineageView) buildColumnTrace() map[string]map[string]bool {
	trace := make(map[string]map[string]bool)

	// Find the target model
	var targetModel *ModelLineage
	for _, m := range v.Models {
		if m.Name == v.TargetModel {
			targetModel = m
			break
		}
	}
	if targetModel == nil {
		return trace
	}

	// Find the target column's sources
	var targetSources []ColumnSource
	for _, cl := range targetModel.ColumnLineage {
		if cl.Column == v.TargetColumn {
			targetSources = cl.Sources
			break
		}
	}

	// Recursively trace sources
	v.traceSourcesRecursive(targetSources, trace)

	return trace
}

// traceSourcesRecursive recursively traces column sources
func (v *LineageView) traceSourcesRecursive(sources []ColumnSource, trace map[string]map[string]bool) {
	for _, src := range sources {
		if trace[src.Table] == nil {
			trace[src.Table] = make(map[string]bool)
		}
		trace[src.Table][src.Column] = true

		// Find this model and trace its sources
		for _, m := range v.Models {
			if m.Name == src.Table {
				for _, cl := range m.ColumnLineage {
					if cl.Column == src.Column {
						v.traceSourcesRecursive(cl.Sources, trace)
					}
				}
			}
		}
	}
}

// layerOrder returns schema names sorted by dependency depth (sources first, downstream last).
// This ordering is used for the horizontal left-to-right layout in overview/model/column focus.
func (v *LineageView) layerOrder(layers map[string][]*ModelLineage) []string {
	// Build dependency depth per model
	depSet := make(map[string][]string)
	for _, m := range v.Models {
		depSet[m.Name] = m.Dependencies
	}
	depths := make(map[string]int)
	var calcDepth func(string, map[string]bool) int
	calcDepth = func(name string, visited map[string]bool) int {
		if d, ok := depths[name]; ok {
			return d
		}
		if visited[name] {
			return 0
		}
		visited[name] = true
		maxD := 0
		for _, dep := range depSet[name] {
			if d := calcDepth(dep, visited) + 1; d > maxD {
				maxD = d
			}
		}
		depths[name] = maxD
		return maxD
	}
	for _, m := range v.Models {
		calcDepth(m.Name, make(map[string]bool))
	}

	// Find max depth per schema
	schemaDepth := make(map[string]int)
	for schema, models := range layers {
		for _, m := range models {
			if d := depths[m.Name]; d > schemaDepth[schema] {
				schemaDepth[schema] = d
			}
		}
	}

	// Collect and sort schemas by depth (ascending: sources first for left-to-right layout)
	var schemas []string
	for s := range layers {
		schemas = append(schemas, s)
	}
	sort.Slice(schemas, func(i, j int) bool {
		di, dj := schemaDepth[schemas[i]], schemaDepth[schemas[j]]
		if di != dj {
			return di < dj // Lower depth first (sources on left)
		}
		return schemas[i] < schemas[j]
	})

	return schemas
}

// groupByLayer groups models by their schema (first part of schema.table name).
func (v *LineageView) groupByLayer() map[string][]*ModelLineage {
	groups := make(map[string][]*ModelLineage)
	for _, m := range v.Models {
		schema := schemaOf(m.Name)
		groups[schema] = append(groups[schema], m)
	}
	return groups
}

// sortByLayer sorts models by dependency depth (downstream first, sources last).
func (v *LineageView) sortByLayer() []*ModelLineage {
	sorted := make([]*ModelLineage, len(v.Models))
	copy(sorted, v.Models)

	// Build dependency depth
	depSet := make(map[string][]string)
	for _, m := range v.Models {
		depSet[m.Name] = m.Dependencies
	}
	depths := make(map[string]int)
	var calcDepth func(string, map[string]bool) int
	calcDepth = func(name string, visited map[string]bool) int {
		if d, ok := depths[name]; ok {
			return d
		}
		if visited[name] {
			return 0
		}
		visited[name] = true
		maxD := 0
		for _, dep := range depSet[name] {
			if d := calcDepth(dep, visited) + 1; d > maxD {
				maxD = d
			}
		}
		depths[name] = maxD
		return maxD
	}
	for _, m := range v.Models {
		calcDepth(m.Name, make(map[string]bool))
	}

	sort.Slice(sorted, func(i, j int) bool {
		di, dj := depths[sorted[i].Name], depths[sorted[j].Name]
		if di != dj {
			return di > dj // Higher depth first (downstream)
		}
		return sorted[i].Name < sorted[j].Name
	})

	return sorted
}

// renderLegend renders the symbol legend
func (v *LineageView) renderLegend() string {
	return "    ◆ target  ● source  [SUM][CNT][AVG] aggregate  [×] arithmetic\n"
}
