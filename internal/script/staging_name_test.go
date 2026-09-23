// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package script

import (
	"strings"
	"testing"
)

func TestStagingTableName_ShortNameUnchanged(t *testing.T) {
	t.Parallel()
	got := stagingTableName("raw.audit_log/_lib_audit_log_0")
	if got != "fetch:raw.audit_log/_lib_audit_log_0" {
		t.Fatalf("got %q", got)
	}
}

// A Postgres state backend truncates identifiers over 63 bytes, so long
// targets must map to a name within the limit that is still unique.
func TestStagingTableName_LongNameFitsAndStaysDistinct(t *testing.T) {
	t.Parallel()
	prefix := "staging.a_rather_long_model_name_for_testing_limits"
	a := stagingTableName(prefix + "_one/_lib_some_long_lib_name_0")
	b := stagingTableName(prefix + "_two/_lib_some_long_lib_name_0")
	for _, n := range []string{a, b} {
		if len(n) > maxStagingNameLen {
			t.Fatalf("%q is %d bytes, over %d", n, len(n), maxStagingNameLen)
		}
		if !strings.HasPrefix(n, "fetch:staging.a_rather_long") {
			t.Fatalf("%q lost its readable prefix", n)
		}
	}
	if a == b {
		t.Fatalf("distinct targets collided on %q", a)
	}
}
