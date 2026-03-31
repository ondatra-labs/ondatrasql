// OndatraSQL - A data pipeline framework for DuckDB + DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

//go:build integration

package testutil

import (
	"testing"
)

func TestNewSession(t *testing.T) {
	sess := NewSession(t)
	if sess == nil {
		t.Fatal("expected non-nil session")
	}
}

func TestNewSessionWithMacros(t *testing.T) {
	sess := NewSessionWithMacros(t)
	if sess == nil {
		t.Fatal("expected non-nil session")
	}
}
