// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

//go:build integration

package execute

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ondatra-labs/ondatrasql/internal/backfill"
	"github.com/ondatra-labs/ondatrasql/internal/dag"
	"github.com/ondatra-labs/ondatrasql/internal/parser"
	"github.com/ondatra-labs/ondatrasql/internal/testutil"
)

// An AST that cannot be read fails the run. It used to be a warning: with no
// AST the LIMIT and strict checks passed vacuously and the model had "no
// sources", so an append model skipped CDC and appended its whole source
// again on every run.
func TestRun_UnreadableASTFailsInsteadOfReappending(t *testing.T) {
	p := testutil.NewProject(t)
	p.AddModel("raw/src.sql", "-- @kind: table\nSELECT * FROM (VALUES (1), (2), (3)) t(id)\n")
	p.AddModel("mart/acc.sql", "-- @kind: append\nSELECT id FROM raw.src\n")
	run := func(rel string, poison string) error {
		t.Helper()
		model, err := parser.ParseModel(filepath.Join(p.Dir, "models", rel), p.Dir)
		if err != nil {
			t.Fatal(err)
		}
		r := NewRunner(p.Sess, ModeRun, dag.GenerateRunID())
		if poison != "" {
			r.astCache[backfill.Hash(model.SQL)] = poison
		}
		_, err = r.Run(context.Background(), model)
		return err
	}
	for _, rel := range []string{"raw/src.sql", "mart/acc.sql"} {
		if err := run(rel, ""); err != nil {
			t.Fatalf("%s: %v", rel, err)
		}
	}
	for name, poison := range map[string]string{
		"malformed JSON":   "{not json",
		"DuckDB 1.5 shape": `{"error":false,"statements":[{"node":{"type":"SELECT_NODE","select_list":[{"class":"FUNCTION","type":"FUNCTION","function_name":"sum","children":[]}],"from_table":{"type":"EMPTY"}}}]}`,
	} {
		err := run("mart/acc.sql", poison)
		if err == nil || !strings.Contains(err.Error(), "cannot read the model's AST") {
			t.Errorf("%s: err = %v, want the run to fail on the AST", name, err)
		}
	}
	if got, err := p.Sess.QueryValue("SELECT count(*) FROM mart.acc"); err != nil || got != "3" {
		t.Errorf("mart.acc has %s rows (%v), want 3: a failed AST must not append the source again", got, err)
	}
}
