// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

//go:build integration

package script

import (
	"os"
	"testing"

	"github.com/ondatra-labs/ondatrasql/internal/duckdb"
)

var sharedSess *duckdb.Session

func TestMain(m *testing.M) {
	sess, err := duckdb.NewSession(":memory:?threads=4&memory_limit=2GB")
	if err != nil {
		panic("create shared session: " + err.Error())
	}
	sharedSess = sess
	code := m.Run()
	sharedSess.Close()
	os.Exit(code)
}
