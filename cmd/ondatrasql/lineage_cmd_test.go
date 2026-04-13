// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package main

import (
	"testing"

	"github.com/ondatra-labs/ondatrasql/internal/backfill"
	"github.com/ondatra-labs/ondatrasql/internal/lineage"
)

func TestConvertToModelLineage(t *testing.T) {
	t.Parallel()
	info := &backfill.CommitInfo{
		Columns: []backfill.Column{
			{Name: "id"},
			{Name: "name"},
			{Name: "amount"},
		},
		ColumnLineage: []lineage.ColumnLineage{
			{
				Column: "id",
				Sources: []lineage.SourceColumn{
					{Table: "raw.orders", Column: "order_id", Transformation: "identity"},
				},
			},
			{
				Column: "name",
				Sources: []lineage.SourceColumn{
					{Table: "raw.customers", Column: "name", FunctionName: "UPPER"},
				},
			},
		},
		Depends: []string{"raw.orders", "raw.customers"},
	}

	result := convertToModelLineage("staging.orders", info)

	if result.Name != "staging.orders" {
		t.Errorf("Name = %q, want %q", result.Name, "staging.orders")
	}
	if len(result.Columns) != 3 {
		t.Errorf("Columns len = %d, want 3", len(result.Columns))
	}
	if result.Columns[0] != "id" || result.Columns[1] != "name" || result.Columns[2] != "amount" {
		t.Errorf("Columns = %v, want [id name amount]", result.Columns)
	}
	if len(result.ColumnLineage) != 2 {
		t.Errorf("ColumnLineage len = %d, want 2", len(result.ColumnLineage))
	}
	if result.ColumnLineage[0].Column != "id" {
		t.Errorf("ColumnLineage[0].Column = %q, want %q", result.ColumnLineage[0].Column, "id")
	}
	if result.ColumnLineage[0].Sources[0].Table != "raw.orders" {
		t.Errorf("source table = %q, want %q", result.ColumnLineage[0].Sources[0].Table, "raw.orders")
	}
	if result.ColumnLineage[1].Sources[0].FunctionName != "UPPER" {
		t.Errorf("function = %q, want %q", result.ColumnLineage[1].Sources[0].FunctionName, "UPPER")
	}
	if len(result.Dependencies) != 2 {
		t.Errorf("Dependencies len = %d, want 2", len(result.Dependencies))
	}
}

func TestConvertToModelLineage_Empty(t *testing.T) {
	t.Parallel()
	info := &backfill.CommitInfo{}
	result := convertToModelLineage("empty.model", info)

	if result.Name != "empty.model" {
		t.Errorf("Name = %q, want %q", result.Name, "empty.model")
	}
	if len(result.Columns) != 0 {
		t.Errorf("Columns len = %d, want 0", len(result.Columns))
	}
}

func TestRenderLineageGraph_Empty(t *testing.T) {
	t.Parallel()
	got := renderLineageGraph("target", nil)
	if got != "No lineage data available" {
		t.Errorf("empty graph = %q, want 'No lineage data available'", got)
	}
}

func TestRenderLineageGraph_NonEmpty(t *testing.T) {
	t.Parallel()
	models := []*lineage.ModelLineage{
		{
			Name:    "staging.orders",
			Columns: []string{"id", "amount"},
		},
	}
	got := renderLineageGraph("staging.orders", models)
	if got == "" {
		t.Error("non-empty graph should produce output")
	}
}
