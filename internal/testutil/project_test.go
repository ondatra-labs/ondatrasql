// OndatraSQL - A data pipeline framework for DuckDB + DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

//go:build integration

package testutil

import (
	"testing"
)

func TestNewProject(t *testing.T) {
	p := NewProject(t)
	if p == nil {
		t.Fatal("expected non-nil project")
	}
	if p.Dir == "" {
		t.Fatal("expected non-empty Dir")
	}
	if p.Sess == nil {
		t.Fatal("expected non-nil Sess")
	}
}

func TestAddModel(t *testing.T) {
	p := NewProject(t)
	p.AddModel("staging/test.sql", "SELECT 1 AS id")
}
