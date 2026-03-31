// OndatraSQL - A data pipeline framework for DuckDB + DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package lineage

import (
	"testing"
)

func TestDetectRenames(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name           string
		oldLineage     []ColumnLineage
		newLineage     []ColumnLineage
		wantRenames    int
		wantAdded      int
		wantDropped    int
		checkRename    *ColumnRename // Optional: verify specific rename
	}{
		{
			name: "simple rename",
			oldLineage: []ColumnLineage{
				{Column: "id", Sources: []SourceColumn{{Table: "orders", Column: "id", Transformation: TransformIdentity}}},
				{Column: "name", Sources: []SourceColumn{{Table: "orders", Column: "name", Transformation: TransformIdentity}}},
			},
			newLineage: []ColumnLineage{
				{Column: "id", Sources: []SourceColumn{{Table: "orders", Column: "id", Transformation: TransformIdentity}}},
				{Column: "customer_name", Sources: []SourceColumn{{Table: "orders", Column: "name", Transformation: TransformIdentity}}},
			},
			wantRenames: 1,
			wantAdded:   0,
			wantDropped: 0,
			checkRename: &ColumnRename{OldName: "name", NewName: "customer_name", Source: "orders.name"},
		},
		{
			name: "no changes",
			oldLineage: []ColumnLineage{
				{Column: "id", Sources: []SourceColumn{{Table: "orders", Column: "id", Transformation: TransformIdentity}}},
			},
			newLineage: []ColumnLineage{
				{Column: "id", Sources: []SourceColumn{{Table: "orders", Column: "id", Transformation: TransformIdentity}}},
			},
			wantRenames: 0,
			wantAdded:   0,
			wantDropped: 0,
		},
		{
			name: "add column - not a rename",
			oldLineage: []ColumnLineage{
				{Column: "id", Sources: []SourceColumn{{Table: "orders", Column: "id", Transformation: TransformIdentity}}},
			},
			newLineage: []ColumnLineage{
				{Column: "id", Sources: []SourceColumn{{Table: "orders", Column: "id", Transformation: TransformIdentity}}},
				{Column: "name", Sources: []SourceColumn{{Table: "orders", Column: "name", Transformation: TransformIdentity}}},
			},
			wantRenames: 0,
			wantAdded:   1,
			wantDropped: 0,
		},
		{
			name: "drop column - not a rename",
			oldLineage: []ColumnLineage{
				{Column: "id", Sources: []SourceColumn{{Table: "orders", Column: "id", Transformation: TransformIdentity}}},
				{Column: "name", Sources: []SourceColumn{{Table: "orders", Column: "name", Transformation: TransformIdentity}}},
			},
			newLineage: []ColumnLineage{
				{Column: "id", Sources: []SourceColumn{{Table: "orders", Column: "id", Transformation: TransformIdentity}}},
			},
			wantRenames: 0,
			wantAdded:   0,
			wantDropped: 1,
		},
		{
			name: "aggregation not detected as rename",
			oldLineage: []ColumnLineage{
				{Column: "amount", Sources: []SourceColumn{{Table: "orders", Column: "amount", Transformation: TransformIdentity}}},
			},
			newLineage: []ColumnLineage{
				{Column: "total", Sources: []SourceColumn{{Table: "orders", Column: "amount", Transformation: TransformAggregation}}},
			},
			wantRenames: 0,
			wantAdded:   1,
			wantDropped: 1,
		},
		{
			name: "ambiguous source - no rename detected",
			oldLineage: []ColumnLineage{
				{Column: "name1", Sources: []SourceColumn{{Table: "orders", Column: "name", Transformation: TransformIdentity}}},
				{Column: "name2", Sources: []SourceColumn{{Table: "orders", Column: "name", Transformation: TransformIdentity}}},
			},
			newLineage: []ColumnLineage{
				{Column: "new_name", Sources: []SourceColumn{{Table: "orders", Column: "name", Transformation: TransformIdentity}}},
			},
			wantRenames: 0,
			wantAdded:   1,
			wantDropped: 2,
		},
		{
			name: "multiple renames",
			oldLineage: []ColumnLineage{
				{Column: "id", Sources: []SourceColumn{{Table: "orders", Column: "id", Transformation: TransformIdentity}}},
				{Column: "name", Sources: []SourceColumn{{Table: "orders", Column: "name", Transformation: TransformIdentity}}},
				{Column: "amt", Sources: []SourceColumn{{Table: "orders", Column: "amount", Transformation: TransformIdentity}}},
			},
			newLineage: []ColumnLineage{
				{Column: "order_id", Sources: []SourceColumn{{Table: "orders", Column: "id", Transformation: TransformIdentity}}},
				{Column: "customer_name", Sources: []SourceColumn{{Table: "orders", Column: "name", Transformation: TransformIdentity}}},
				{Column: "amount", Sources: []SourceColumn{{Table: "orders", Column: "amount", Transformation: TransformIdentity}}},
			},
			wantRenames: 3,
			wantAdded:   0,
			wantDropped: 0,
		},
		{
			name: "rename with schema qualified source",
			oldLineage: []ColumnLineage{
				{Column: "name", Sources: []SourceColumn{{Table: "staging.customers", Column: "name", Transformation: TransformIdentity}}},
			},
			newLineage: []ColumnLineage{
				{Column: "customer_name", Sources: []SourceColumn{{Table: "staging.customers", Column: "name", Transformation: TransformIdentity}}},
			},
			wantRenames: 1,
			wantAdded:   0,
			wantDropped: 0,
			checkRename: &ColumnRename{OldName: "name", NewName: "customer_name", Source: "staging.customers.name"},
		},
		{
			name: "old column still exists - not a rename",
			oldLineage: []ColumnLineage{
				{Column: "name", Sources: []SourceColumn{{Table: "orders", Column: "name", Transformation: TransformIdentity}}},
			},
			newLineage: []ColumnLineage{
				{Column: "name", Sources: []SourceColumn{{Table: "orders", Column: "name", Transformation: TransformIdentity}}},
				{Column: "customer_name", Sources: []SourceColumn{{Table: "orders", Column: "name", Transformation: TransformIdentity}}},
			},
			wantRenames: 0,
			wantAdded:   1,
			wantDropped: 0,
		},
		{
			name: "new column existed in old schema - not a rename",
			oldLineage: []ColumnLineage{
				{Column: "customer_name", Sources: []SourceColumn{{Table: "orders", Column: "name", Transformation: TransformIdentity}}},
				{Column: "name", Sources: []SourceColumn{{Table: "orders", Column: "first_name", Transformation: TransformIdentity}}},
			},
			newLineage: []ColumnLineage{
				{Column: "name", Sources: []SourceColumn{{Table: "orders", Column: "name", Transformation: TransformIdentity}}},
			},
			wantRenames: 0,
			wantAdded:   0,
			wantDropped: 1,
		},
		{
			name: "multi-source column not detected",
			oldLineage: []ColumnLineage{
				{Column: "full_name", Sources: []SourceColumn{
					{Table: "orders", Column: "first", Transformation: TransformFunction},
					{Table: "orders", Column: "last", Transformation: TransformFunction},
				}},
			},
			newLineage: []ColumnLineage{
				{Column: "name", Sources: []SourceColumn{
					{Table: "orders", Column: "first", Transformation: TransformFunction},
					{Table: "orders", Column: "last", Transformation: TransformFunction},
				}},
			},
			wantRenames: 0,
			wantAdded:   1,
			wantDropped: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			renames, added, dropped := DetectRenames(tt.oldLineage, tt.newLineage)

			if len(renames) != tt.wantRenames {
				t.Errorf("renames = %d, want %d (got: %+v)", len(renames), tt.wantRenames, renames)
			}
			if len(added) != tt.wantAdded {
				t.Errorf("added = %d, want %d (got: %v)", len(added), tt.wantAdded, added)
			}
			if len(dropped) != tt.wantDropped {
				t.Errorf("dropped = %d, want %d (got: %v)", len(dropped), tt.wantDropped, dropped)
			}

			if tt.checkRename != nil && len(renames) > 0 {
				found := false
				for _, r := range renames {
					if r.OldName == tt.checkRename.OldName &&
						r.NewName == tt.checkRename.NewName &&
						r.Source == tt.checkRename.Source {
						found = true
						break
					}
				}
				if !found {
					t.Errorf("expected rename %+v not found in %+v", tt.checkRename, renames)
				}
			}
		})
	}
}
