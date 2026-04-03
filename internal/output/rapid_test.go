// OndatraSQL - You don't need a data stack anymore
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package output

import (
	"encoding/json"
	"testing"

	"pgregory.net/rapid"
)

func genModelResult() *rapid.Generator[ModelResult] {
	return rapid.Custom(func(t *rapid.T) ModelResult {
		mr := ModelResult{
			Model:        rapid.StringMatching(`^[a-z]+\.[a-z_]+$`).Draw(t, "model"),
			Kind:         rapid.SampledFrom([]string{"table", "append", "merge", "scd2", "partition"}).Draw(t, "kind"),
			RunType:      rapid.SampledFrom([]string{"full", "incremental", "backfill"}).Draw(t, "runType"),
			RowsAffected: int64(rapid.IntRange(0, 1000000).Draw(t, "rows")),
			DurationMs:   int64(rapid.IntRange(0, 60000).Draw(t, "duration")),
			Status:       rapid.SampledFrom([]string{"ok", "error", "warning"}).Draw(t, "status"),
		}

		// Optionally add errors
		if rapid.Bool().Draw(t, "hasErrors") {
			n := rapid.IntRange(1, 3).Draw(t, "nErrors")
			for i := 0; i < n; i++ {
				mr.Errors = append(mr.Errors, rapid.String().Draw(t, "error"))
			}
		}

		// Optionally add warnings
		if rapid.Bool().Draw(t, "hasWarnings") {
			n := rapid.IntRange(1, 3).Draw(t, "nWarnings")
			for i := 0; i < n; i++ {
				mr.Warnings = append(mr.Warnings, rapid.String().Draw(t, "warning"))
			}
		}

		if rapid.Bool().Draw(t, "hasDagRunID") {
			mr.DagRunID = rapid.StringMatching(`^[0-9]{8}-[0-9]{6}-[a-f0-9]{6}$`).Draw(t, "dagRunID")
		}

		mr.Sandbox = rapid.Bool().Draw(t, "sandbox")
		return mr
	})
}

// Property: JSON marshal/unmarshal is a roundtrip (no data loss).
func TestRapid_ModelResult_JSONRoundtrip(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		original := genModelResult().Draw(t, "mr")

		data, err := json.Marshal(original)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}

		var decoded ModelResult
		if err := json.Unmarshal(data, &decoded); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}

		// Verify all fields
		if decoded.Model != original.Model {
			t.Fatalf("Model: %q != %q", decoded.Model, original.Model)
		}
		if decoded.Kind != original.Kind {
			t.Fatalf("Kind: %q != %q", decoded.Kind, original.Kind)
		}
		if decoded.RunType != original.RunType {
			t.Fatalf("RunType: %q != %q", decoded.RunType, original.RunType)
		}
		if decoded.RowsAffected != original.RowsAffected {
			t.Fatalf("RowsAffected: %d != %d", decoded.RowsAffected, original.RowsAffected)
		}
		if decoded.DurationMs != original.DurationMs {
			t.Fatalf("DurationMs: %d != %d", decoded.DurationMs, original.DurationMs)
		}
		if decoded.Status != original.Status {
			t.Fatalf("Status: %q != %q", decoded.Status, original.Status)
		}
		if decoded.DagRunID != original.DagRunID {
			t.Fatalf("DagRunID: %q != %q", decoded.DagRunID, original.DagRunID)
		}
		if decoded.Sandbox != original.Sandbox {
			t.Fatalf("Sandbox: %v != %v", decoded.Sandbox, original.Sandbox)
		}

		// Errors and Warnings: omitempty means nil becomes nil after roundtrip
		if len(decoded.Errors) != len(original.Errors) {
			t.Fatalf("Errors len: %d != %d", len(decoded.Errors), len(original.Errors))
		}
		for i := range original.Errors {
			if decoded.Errors[i] != original.Errors[i] {
				t.Fatalf("Errors[%d]: %q != %q", i, decoded.Errors[i], original.Errors[i])
			}
		}
		if len(decoded.Warnings) != len(original.Warnings) {
			t.Fatalf("Warnings len: %d != %d", len(decoded.Warnings), len(original.Warnings))
		}
		for i := range original.Warnings {
			if decoded.Warnings[i] != original.Warnings[i] {
				t.Fatalf("Warnings[%d]: %q != %q", i, decoded.Warnings[i], original.Warnings[i])
			}
		}
	})
}

// Property: JSON output always contains all required fields.
func TestRapid_ModelResult_RequiredFields(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		mr := genModelResult().Draw(t, "mr")
		data, err := json.Marshal(mr)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}

		var raw map[string]json.RawMessage
		if err := json.Unmarshal(data, &raw); err != nil {
			t.Fatalf("unmarshal to map: %v", err)
		}

		for _, field := range []string{"model", "kind", "run_type", "rows_affected", "duration_ms", "status"} {
			if _, ok := raw[field]; !ok {
				t.Fatalf("missing required field %q in JSON: %s", field, data)
			}
		}
	})
}
