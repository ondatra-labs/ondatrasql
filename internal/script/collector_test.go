// OndatraSQL - A data pipeline framework for DuckDB + DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package script

import "testing"

func TestCollectorAdd(t *testing.T) {
	t.Parallel()
	c := &saveCollector{target: "test"}

	c.add(map[string]interface{}{"id": 1, "name": "a"})
	c.add(map[string]interface{}{"id": 2, "name": "b", "extra": "c"})

	if c.count() != 2 {
		t.Errorf("count = %d, want 2", c.count())
	}

	// Should have all three columns, sorted
	want := []string{"extra", "id", "name"}
	if len(c.columns) != len(want) {
		t.Fatalf("columns = %v, want %v", c.columns, want)
	}
	for i, col := range c.columns {
		if col != want[i] {
			t.Errorf("columns[%d] = %q, want %q", i, col, want[i])
		}
	}
}

func TestInferTypes(t *testing.T) {
	t.Parallel()
	c := &saveCollector{
		columns: []string{"age", "active", "name", "score", "unknown"},
		data: []map[string]interface{}{
			{"age": nil, "active": true, "name": "alice", "score": 3.14, "unknown": nil},
			{"age": int64(30), "active": false, "name": "bob", "score": 2.71, "unknown": nil},
		},
	}

	types := c.inferTypes()

	tests := map[string]string{
		"age":     "BIGINT",
		"active":  "BOOLEAN",
		"name":    "VARCHAR",
		"score":   "DOUBLE",
		"unknown": "VARCHAR", // all nil → default VARCHAR
	}
	for col, want := range tests {
		if types[col] != want {
			t.Errorf("inferTypes()[%q] = %q, want %q", col, types[col], want)
		}
	}
}
