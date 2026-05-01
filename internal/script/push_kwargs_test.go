package script

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestPushKwargs_KeyColumnsAsList(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	libDir := filepath.Join(dir, "lib")
	os.MkdirAll(libDir, 0755)
	os.WriteFile(filepath.Join(libDir, "test_push.star"), []byte(`
API = {
    "base_url": "https://example.com",
    "push": {"batch_size": 100},
}

def push(rows=[], key_columns=[]):
    if type(key_columns) != "list":
        fail("key_columns should be list, got " + type(key_columns))
    if len(key_columns) != 2:
        fail("expected 2 key columns, got " + str(len(key_columns)))
    if key_columns[0] != "region":
        fail("first key should be region, got " + key_columns[0])
    if key_columns[1] != "year":
        fail("second key should be year, got " + key_columns[1])
`), 0644)

	rt := NewRuntime(nil, nil, dir)
	rows := []map[string]any{
		{"region": "EU", "year": 2024, "amount": 100, "__ondatra_rowid": 1, "__ondatra_change_type": "insert"},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := rt.RunPush(ctx, "test_push", rows, 1, "tracked", "region, year", nil, nil)
	if err != nil {
		t.Fatalf("RunPush: %v", err)
	}
}

func TestPushKwargs_ColumnsSorted(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	libDir := filepath.Join(dir, "lib")
	os.MkdirAll(libDir, 0755)
	os.WriteFile(filepath.Join(libDir, "sort_push.star"), []byte(`
API = {
    "base_url": "https://example.com",
    "push": {"batch_size": 100},
}

def push(rows=[], columns=[]):
    # Columns should be sorted alphabetically
    for i in range(len(columns) - 1):
        if columns[i] > columns[i+1]:
            fail("columns not sorted: " + columns[i] + " > " + columns[i+1])
`), 0644)

	rt := NewRuntime(nil, nil, dir)
	// Use unsorted keys — Go map iteration is random
	rows := []map[string]any{
		{"zebra": 1, "apple": 2, "mango": 3, "__ondatra_rowid": 1, "__ondatra_change_type": "insert"},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := rt.RunPush(ctx, "sort_push", rows, 1, "table", "", nil, nil)
	if err != nil {
		t.Fatalf("RunPush: %v", err)
	}
}
