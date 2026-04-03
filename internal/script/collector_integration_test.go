// OndatraSQL - You don't need a data stack anymore
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

//go:build integration

package script

import (
	"testing"
)

func TestCreateTempTable(t *testing.T) {
	c := &saveCollector{
		target: "test.table",
		sess:   sharedSess,
	}

	c.add(map[string]interface{}{"id": 1, "name": "alice"})
	c.add(map[string]interface{}{"id": 2, "name": "bob", "extra": "val"})

	tmpTable, err := c.createTempTable()
	if err != nil {
		t.Fatalf("createTempTable: %v", err)
	}
	defer sharedSess.Exec("DROP TABLE IF EXISTS " + tmpTable)

	if tmpTable != "tmp_test_table" {
		t.Errorf("table name = %q, want %q", tmpTable, "tmp_test_table")
	}

	// Verify row count
	count, err := sharedSess.QueryValue("SELECT count(*) FROM " + tmpTable)
	if err != nil {
		t.Fatalf("count query: %v", err)
	}
	if count != "2" {
		t.Errorf("row count = %s, want 2", count)
	}

	// Verify data
	rows, err := sharedSess.QueryRowsMap("SELECT * FROM " + tmpTable + " ORDER BY id")
	if err != nil {
		t.Fatalf("select query: %v", err)
	}

	if len(rows) != 2 {
		t.Fatalf("got %d rows, want 2", len(rows))
	}
	if rows[0]["name"] != "alice" {
		t.Errorf("row 0 name = %q, want 'alice'", rows[0]["name"])
	}
	if rows[1]["extra"] != "val" {
		t.Errorf("row 1 extra = %q, want 'val'", rows[1]["extra"])
	}
	// First row should have NULL for extra column
	if rows[0]["extra"] != "" {
		t.Errorf("row 0 extra = %q, want empty (NULL)", rows[0]["extra"])
	}
}

func TestCreateTempTableEmpty(t *testing.T) {
	c := &saveCollector{target: "empty", sess: sharedSess}

	tmpTable, err := c.createTempTable()
	if err != nil {
		t.Fatalf("createTempTable: %v", err)
	}
	if tmpTable != "" {
		t.Errorf("expected empty table name for no data, got %q", tmpTable)
	}
}

func TestCreateTempTableSpecialChars(t *testing.T) {
	c := &saveCollector{target: "test", sess: sharedSess}
	c.add(map[string]interface{}{
		"col with space": "val",
		"it's-a-col":     42,
	})

	tmpTable, err := c.createTempTable()
	if err != nil {
		t.Fatalf("createTempTable with special column names: %v", err)
	}
	defer sharedSess.Exec("DROP TABLE IF EXISTS " + tmpTable)

	count, err := sharedSess.QueryValue("SELECT count(*) FROM " + tmpTable)
	if err != nil {
		t.Fatalf("count query: %v", err)
	}
	if count != "1" {
		t.Errorf("row count = %s, want 1", count)
	}
}
