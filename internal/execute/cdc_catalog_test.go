// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package execute

import (
	"testing"

	"github.com/ondatra-labs/ondatrasql/internal/duckast"
)

// The catalog-awareness helpers are pure, so they belong in `make test` rather
// than behind the integration tag where the rest of the CDC coverage lives.

func TestIsLakeAlias(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name         string
		in           string
		catalogAlias string
		prodAlias    string
		want         bool
	}{
		{"lake alias", "lake", "lake", "", true},
		{"case-insensitive", "LAKE", "lake", "", true},
		{"prod alias in sandbox", "prod", "fork", "prod", true},
		{"foreign catalog", "crm", "lake", "prod", false},
		{"schema is not an alias", "raw", "lake", "prod", false},
		// An empty alias must never match, or an unqualified name would be
		// classified as the lake by accident.
		{"empty name against empty catalog alias", "", "", "", false},
		{"empty name against empty prod alias", "", "lake", "", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := isLakeAlias(tt.in, tt.catalogAlias, tt.prodAlias); got != tt.want {
				t.Errorf("isLakeAlias(%q, %q, %q) = %v, want %v",
					tt.in, tt.catalogAlias, tt.prodAlias, got, tt.want)
			}
		})
	}
}

func TestSplitCDCTableName(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name                           string
		in                             string
		active                         string
		wantCat, wantSchema, wantTable string
		wantOK                         bool
	}{
		// Two parts are schema.table, scoped to whatever catalog is active —
		// the lake in a normal run, the fork in a sandbox run.
		{"schema.table", "raw.orders", "lake", "lake", "raw", "orders", true},
		{"schema.table in sandbox", "raw.orders", "fork", "fork", "raw", "orders", true},
		// Three parts carry their own catalog and must not be re-scoped: this
		// is what let a catalog-qualified lake read keep its CDC rewrite.
		{"catalog.schema.table", "lake.raw.orders", "fork", "lake", "raw", "orders", true},
		{"foreign catalog", "crm.public.decision", "lake", "crm", "public", "decision", true},
		// Anything else is not a shape the gate can qualify.
		{"unqualified", "orders", "lake", "", "", "", false},
		{"four parts", "a.b.c.d", "lake", "", "", "", false},
		{"empty", "", "lake", "", "", "", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cat, schema, table, ok := splitCDCTableName(tt.in, tt.active)
			if ok != tt.wantOK {
				t.Fatalf("splitCDCTableName(%q, %q) ok = %v, want %v", tt.in, tt.active, ok, tt.wantOK)
			}
			if !ok {
				return
			}
			if cat != tt.wantCat || schema != tt.wantSchema || table != tt.wantTable {
				t.Errorf("splitCDCTableName(%q, %q) = %q, %q, %q; want %q, %q, %q",
					tt.in, tt.active, cat, schema, table, tt.wantCat, tt.wantSchema, tt.wantTable)
			}
		})
	}
}

func TestCDCMatchName(t *testing.T) {
	t.Parallel()
	// duckast's FullTableName() drops catalog_name, so `lake.raw.src` and a
	// foreign `raw.src` both render as "raw.src". cdcMatchName keeps the
	// catalog the SQL actually wrote, which is what separates them.
	tests := []struct {
		name    string
		catalog string
		schema  string
		table   string
		want    string
	}{
		{"unqualified", "", "", "orders", "orders"},
		{"schema-qualified", "", "raw", "src", "raw.src"},
		{"catalog-qualified lake", "lake", "raw", "src", "lake.raw.src"},
		{"foreign catalog", "crm", "public", "decision", "crm.public.decision"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			n := duckast.NewNode(map[string]any{
				"type":         "BASE_TABLE",
				"catalog_name": tt.catalog,
				"schema_name":  tt.schema,
				"table_name":   tt.table,
			})
			if got := cdcMatchName(n); got != tt.want {
				t.Errorf("cdcMatchName(%s) = %q, want %q", tt.name, got, tt.want)
			}
		})
	}

	// The pair that must not collapse onto one another.
	lake := duckast.NewNode(map[string]any{
		"type": "BASE_TABLE", "catalog_name": "lake", "schema_name": "raw", "table_name": "src",
	})
	foreign := duckast.NewNode(map[string]any{
		"type": "BASE_TABLE", "catalog_name": "", "schema_name": "raw", "table_name": "src",
	})
	if lake.FullTableName() != foreign.FullTableName() {
		t.Fatalf("premise broken: FullTableName() no longer collides (%q vs %q)",
			lake.FullTableName(), foreign.FullTableName())
	}
	if cdcMatchName(lake) == cdcMatchName(foreign) {
		t.Errorf("cdcMatchName must separate the pair FullTableName() collapses, got %q for both",
			cdcMatchName(lake))
	}
}

func TestQualifyTablesInAST_LeavesExplicitCatalogAlone(t *testing.T) {
	t.Parallel()
	// Two BASE_TABLE nodes that share schema.table but not catalog: the lake's
	// own raw.src, and raw.src inside an ATTACHed `crm`. The qualification key
	// carries no catalog, so without the guard the second node is moved into
	// prod as well and the query reads a table that is not there.
	lakeNode := map[string]any{
		"type": "BASE_TABLE", "catalog_name": "", "schema_name": "raw", "table_name": "src",
	}
	foreignNode := map[string]any{
		"type": "BASE_TABLE", "catalog_name": "crm", "schema_name": "raw", "table_name": "src",
	}
	root := map[string]any{
		"statements": []any{
			map[string]any{"node": map[string]any{"from_table": lakeNode}},
			map[string]any{"node": map[string]any{"from_table": foreignNode}},
		},
	}

	qualifyTablesInAST(root, map[string]bool{"raw.src": true}, "prod")

	if got, _ := lakeNode["catalog_name"].(string); got != "prod" {
		t.Errorf("unqualified lake table should be moved to prod, got %q", got)
	}
	if got, _ := foreignNode["catalog_name"].(string); got != "crm" {
		t.Errorf("explicit catalog must survive qualification, got %q", got)
	}
}

func TestStripLakeAlias(t *testing.T) {
	t.Parallel()
	// The column-lineage extractor keys aggregationTables on schema.table,
	// while ExtractTablesFromAST keeps the catalog. Only a lake alias may be
	// stripped to bridge them: folding a foreign catalog's leading segment
	// would let `crm.raw.events` collide with the lake's own `raw.events`.
	tests := []struct {
		name         string
		in           string
		catalogAlias string
		prodAlias    string
		want         string
	}{
		{"lake alias stripped", "lake.raw.events", "lake", "", "raw.events"},
		{"case-insensitive", "LAKE.raw.events", "lake", "", "raw.events"},
		{"prod alias stripped in sandbox", "prod.raw.events", "fork", "prod", "raw.events"},
		{"foreign catalog kept", "crm.raw.events", "lake", "prod", "crm.raw.events"},
		{"schema-qualified untouched", "raw.events", "lake", "prod", "raw.events"},
		{"unqualified untouched", "events", "lake", "prod", "events"},
		{"empty aliases strip nothing", "raw.events", "", "", "raw.events"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := stripLakeAlias(tt.in, tt.catalogAlias, tt.prodAlias); got != tt.want {
				t.Errorf("stripLakeAlias(%q, %q, %q) = %q, want %q",
					tt.in, tt.catalogAlias, tt.prodAlias, got, tt.want)
			}
		})
	}
}
