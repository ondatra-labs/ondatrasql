// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package duckdb

// SetProdAliasForTest sets the prodAlias field for testing sandbox code paths.
func (s *Session) SetProdAliasForTest(alias string) {
	s.prodAlias = alias
}
