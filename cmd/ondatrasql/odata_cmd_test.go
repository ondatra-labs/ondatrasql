// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

//go:build integration

package main

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/ondatra-labs/ondatrasql/internal/config"
	"github.com/ondatra-labs/ondatrasql/internal/testutil"
)

func TestRunOData_NoExposeModels(t *testing.T) {
	p := testutil.NewProject(t)
	oldWd, _ := os.Getwd()
	os.Chdir(p.Dir)
	defer os.Chdir(oldWd)

	// Add a model without @expose
	p.AddModel("mart/revenue.sql", `-- @kind: table
SELECT 1 AS id, 100.0 AS amount`)

	cfg, err := config.Load(p.Dir)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = runOData(ctx, cfg, "0")
	if err == nil {
		t.Fatal("expected error for no @expose models")
	}
	if !strings.Contains(err.Error(), "@expose") {
		t.Errorf("expected @expose error, got: %v", err)
	}
}

func TestRunOData_UnmaterializedTable(t *testing.T) {
	p := testutil.NewProject(t)
	oldWd, _ := os.Getwd()
	os.Chdir(p.Dir)
	defer os.Chdir(oldWd)

	// Add model with @expose but don't run it (table doesn't exist yet)
	p.AddModel("mart/revenue.sql", `-- @kind: table
-- @expose
SELECT 1 AS id, 100.0 AS amount`)

	cfg, err := config.Load(p.Dir)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = runOData(ctx, cfg, "0")
	if err == nil {
		t.Fatal("expected error for unmaterialized table")
	}
	if !strings.Contains(err.Error(), "no columns found") {
		t.Errorf("expected 'no columns found' error, got: %v", err)
	}
}

func TestRunOData_InvalidExposeKey(t *testing.T) {
	p := testutil.NewProject(t)
	oldWd, _ := os.Getwd()
	os.Chdir(p.Dir)
	defer os.Chdir(oldWd)

	// Add and materialize the model
	p.AddModel("mart/revenue.sql", `-- @kind: table
-- @expose nonexistent_col
SELECT 1 AS id, 100.0 AS amount`)

	// Run the model to materialize it
	err := run([]string{"run", "mart.revenue"})
	if err != nil {
		t.Fatalf("run model: %v", err)
	}

	cfg, err := config.Load(p.Dir)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = runOData(ctx, cfg, "0")
	if err == nil {
		t.Fatal("expected error for invalid expose key column")
	}
	if !strings.Contains(err.Error(), "nonexistent_col") {
		t.Errorf("expected key column error, got: %v", err)
	}
}
