// OndatraSQL - A data pipeline framework for DuckDB + DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package lineage

// ColumnRename represents a detected column rename based on lineage analysis.
type ColumnRename struct {
	OldName string // Previous column name
	NewName string // New column name
	Source  string // Source reference (table.column) that links them
}

// DetectRenames compares old and new lineage to find column renames.
// A rename is detected when:
// 1. Both old and new columns have IDENTITY transformation (direct copy)
// 2. They share the same source (table.column)
// 3. There's a 1:1 mapping (no ambiguity)
//
// Returns a list of detected renames and the remaining added/dropped columns.
func DetectRenames(oldLineage, newLineage []ColumnLineage) (renames []ColumnRename, addedCols, droppedCols []string) {
	// Build maps: source -> column name for IDENTITY-only columns
	oldSourceToCol := make(map[string]string)  // source -> old column name
	newSourceToCol := make(map[string]string)  // source -> new column name
	oldColToSource := make(map[string]string)  // old column -> source
	newColToSource := make(map[string]string)  // new column -> source

	// Track ambiguous sources (multiple columns from same source)
	oldSourceCount := make(map[string]int)
	newSourceCount := make(map[string]int)

	// Process old lineage - only IDENTITY with single source
	for _, col := range oldLineage {
		if len(col.Sources) == 1 && col.Sources[0].Transformation == TransformIdentity {
			source := col.Sources[0].Table + "." + col.Sources[0].Column
			oldSourceToCol[source] = col.Column
			oldColToSource[col.Column] = source
			oldSourceCount[source]++
		}
	}

	// Process new lineage - only IDENTITY with single source
	for _, col := range newLineage {
		if len(col.Sources) == 1 && col.Sources[0].Transformation == TransformIdentity {
			source := col.Sources[0].Table + "." + col.Sources[0].Column
			newSourceToCol[source] = col.Column
			newColToSource[col.Column] = source
			newSourceCount[source]++
		}
	}

	// Build sets of column names
	oldCols := make(map[string]bool)
	newCols := make(map[string]bool)
	for _, col := range oldLineage {
		oldCols[col.Column] = true
	}
	for _, col := range newLineage {
		newCols[col.Column] = true
	}

	// Track which columns are matched as renames
	renamedOld := make(map[string]bool)
	renamedNew := make(map[string]bool)

	// Find renames: same source, different name, 1:1 mapping
	for source, oldCol := range oldSourceToCol {
		newCol, existsInNew := newSourceToCol[source]
		if !existsInNew {
			continue
		}

		// Skip if same name (not a rename)
		if oldCol == newCol {
			continue
		}

		// Skip if ambiguous (multiple columns from same source)
		if oldSourceCount[source] > 1 || newSourceCount[source] > 1 {
			continue
		}

		// Skip if old column still exists in new schema (not renamed, just added another)
		if newCols[oldCol] {
			continue
		}

		// Skip if new column existed in old schema (not renamed, just removed another)
		if oldCols[newCol] {
			continue
		}

		// Detected rename!
		renames = append(renames, ColumnRename{
			OldName: oldCol,
			NewName: newCol,
			Source:  source,
		})
		renamedOld[oldCol] = true
		renamedNew[newCol] = true
	}

	// Find truly dropped columns (in old but not in new, and not renamed)
	for col := range oldCols {
		if !newCols[col] && !renamedOld[col] {
			droppedCols = append(droppedCols, col)
		}
	}

	// Find truly added columns (in new but not in old, and not renamed)
	for col := range newCols {
		if !oldCols[col] && !renamedNew[col] {
			addedCols = append(addedCols, col)
		}
	}

	return renames, addedCols, droppedCols
}
