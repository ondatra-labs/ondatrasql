// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package execute

import (
	"errors"
	"reflect"
	"testing"
)

// fakeSession stubs the small session surface loadPushColumnsFromTable uses.
type fakeSession struct {
	rows []map[string]string
	err  error
	last string
}

func (f *fakeSession) QueryRowsMap(sql string) ([]map[string]string, error) {
	f.last = sql
	return f.rows, f.err
}

func TestLoadPushColumnsFromTable_HappyPath(t *testing.T) {
	t.Parallel()
	sess := &fakeSession{
		rows: []map[string]string{
			{"column_name": "id", "data_type": "BIGINT"},
			{"column_name": "Email__c", "data_type": "VARCHAR"},
			{"column_name": "amount", "data_type": "DECIMAL(18,3)"},
		},
	}

	got := loadPushColumnsFromTable(sess, "raw.contacts")
	want := []map[string]any{
		{"name": "id", "type": "BIGINT"},
		{"name": "Email__c", "type": "VARCHAR"},
		{"name": "amount", "type": "DECIMAL(18,3)"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestLoadPushColumnsFromTable_FallbackOnError(t *testing.T) {
	t.Parallel()
	// Catalog error → return nil so caller can fall back to row-derived
	// (untyped) columns instead of failing the push attempt.
	sess := &fakeSession{err: errors.New("boom")}
	if got := loadPushColumnsFromTable(sess, "raw.contacts"); got != nil {
		t.Errorf("expected nil on catalog error, got %v", got)
	}
}

func TestLoadPushColumnsFromTable_FallbackOnEmpty(t *testing.T) {
	t.Parallel()
	// Empty result (table doesn't exist or no columns) → nil.
	sess := &fakeSession{rows: nil}
	if got := loadPushColumnsFromTable(sess, "raw.contacts"); got != nil {
		t.Errorf("expected nil on empty schema, got %v", got)
	}
}

func TestLoadPushColumnsFromTable_RejectsBadTarget(t *testing.T) {
	t.Parallel()
	// "schemaonly" without a dot → can't form a schema.table query.
	sess := &fakeSession{}
	if got := loadPushColumnsFromTable(sess, "schemaonly"); got != nil {
		t.Errorf("expected nil on missing dot, got %v", got)
	}
	if sess.last != "" {
		t.Errorf("should not query when target is malformed, got: %s", sess.last)
	}
}

func TestLoadPushColumnsFromTable_PreservesOrdinal(t *testing.T) {
	t.Parallel()
	// Ordering comes from `ORDER BY ordinal_position` in the SQL — the
	// helper passes results through in source order. Test that nothing
	// alphabetizes them.
	sess := &fakeSession{
		rows: []map[string]string{
			{"column_name": "z_last", "data_type": "VARCHAR"},
			{"column_name": "a_first", "data_type": "VARCHAR"},
			{"column_name": "m_middle", "data_type": "VARCHAR"},
		},
	}
	got := loadPushColumnsFromTable(sess, "schema.t")
	if len(got) != 3 {
		t.Fatalf("got %d cols, want 3", len(got))
	}
	if got[0]["name"] != "z_last" || got[1]["name"] != "a_first" || got[2]["name"] != "m_middle" {
		t.Errorf("order not preserved: %v", got)
	}
}
