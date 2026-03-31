// OndatraSQL - A data pipeline framework for DuckDB + DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package sql

import (
	"strings"
	"sync"
	"testing"
)

func FuzzSetCatalogAlias(f *testing.F) {
	f.Add("lake")
	f.Add("")
	f.Add("my_catalog")
	f.Add(`"; DROP TABLE x; --`)
	f.Add("with space")
	f.Add("a.b.c")

	f.Fuzz(func(t *testing.T, alias string) {
		SetCatalogAlias(alias)
		// Property: non-empty alias should be reflected in Load output
		if alias != "" {
			sql, err := Load("execute/commit.sql")
			if err != nil {
				t.Fatalf("Load after SetCatalogAlias(%q): %v", alias, err)
			}
			if strings.Contains(sql, "{{catalog}}") {
				t.Errorf("{{catalog}} placeholder not replaced after SetCatalogAlias(%q)", alias)
			}
		}
	})
}

// Race: concurrent SetCatalogAlias + Load to detect data races.
// Run with: go test -race -fuzz=FuzzSetCatalogAliasRace
func FuzzSetCatalogAliasRace(f *testing.F) {
	f.Add("lake", "my_cat")
	f.Add("", "x")
	f.Add("a", "b")

	f.Fuzz(func(t *testing.T, alias1, alias2 string) {
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			SetCatalogAlias(alias1)
			// Load triggers a read of catalogAlias
			Load("execute/create_table.sql")
		}()
		go func() {
			defer wg.Done()
			SetCatalogAlias(alias2)
			Load("execute/create_table.sql")
		}()
		wg.Wait()
	})
}
