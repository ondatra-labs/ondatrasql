// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package configenv

import (
	"errors"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/ondatra-labs/ondatrasql/internal/redact"
)

func TestExpand(t *testing.T) {
	t.Setenv("CFGENV_SET", "db.internal")
	t.Setenv("CFGENV_EMPTY", "")
	os.Unsetenv("CFGENV_UNSET_A")
	os.Unsetenv("CFGENV_UNSET_B")

	in := "host=${CFGENV_SET} x=${CFGENV_EMPTY} host=${CFGENV_UNSET_B} port=$CFGENV_UNSET_A again=${CFGENV_UNSET_B}"
	got, unset := Expand(in)

	if want := os.ExpandEnv(in); got != want {
		t.Fatalf("expansion differs from os.ExpandEnv:\n got  %q\n want %q", got, want)
	}
	if want := []string{"CFGENV_UNSET_A", "CFGENV_UNSET_B"}; !reflect.DeepEqual(unset, want) {
		t.Fatalf("unset = %v, want %v (sorted, deduplicated, empty-but-set excluded)", unset, want)
	}
}

// DuckDB SQL uses $1 and $$ in its own syntax; os.Expand hands those to the
// mapping too, and they are not environment variables anyone forgot to set.
func TestExpand_IgnoresNonVariableNames(t *testing.T) {
	_, unset := Expand("SELECT $1, $$body$$")
	if len(unset) != 0 {
		t.Fatalf("reported %v as unset variables", unset)
	}
}

func TestExpand_IgnoresCommentLines(t *testing.T) {
	os.Unsetenv("CFGENV_IN_COMMENT")
	_, unset := Expand("-- e.g. host=${CFGENV_IN_COMMENT}\n  -- ${CFGENV_IN_COMMENT}\nSELECT 1; -- ${CFGENV_IN_COMMENT}")
	if len(unset) != 0 {
		t.Fatalf("reported %v from a comment line", unset)
	}
}

// Go's expansion has no ${NAME:-default}; the whole reference expands to ""
// and must be reported, since that is the mistake the hint is for.
func TestExpand_ReportsUnsupportedShellDefault(t *testing.T) {
	os.Unsetenv("CFGENV_DEFAULTED")
	out, unset := Expand("host=${CFGENV_DEFAULTED:-localhost}")
	if out != "host=" {
		t.Fatalf("expansion changed: %q", out)
	}
	if len(unset) != 1 || unset[0] != "CFGENV_DEFAULTED:-localhost" {
		t.Fatalf("unset = %v, want the unsupported reference named", unset)
	}
}

// A -- inside a quoted connection string is part of the password, not a
// comment, so the host variable after it is still reported.
func TestExpand_DashesInsideALiteralAreNotAComment(t *testing.T) {
	os.Unsetenv("CFGENV_AFTER_DASHES")
	_, unset := Expand("ATTACH 'postgres://u:pa--ss@${CFGENV_AFTER_DASHES}/db' AS x;")
	if len(unset) != 1 || unset[0] != "CFGENV_AFTER_DASHES" {
		t.Fatalf("unset = %v, want CFGENV_AFTER_DASHES", unset)
	}
}

func TestAnnotate(t *testing.T) {
	t.Parallel()
	base := errors.New("could not translate host name \"port=5432\"")
	if Annotate(base, nil) != base {
		t.Fatal("no unset variables must leave the error unchanged")
	}
	if Annotate(nil, []string{"PG_HOST"}) != nil {
		t.Fatal("nil must stay nil")
	}
	got := Annotate(base, []string{"PG_HOST", "PG_PASSWORD"})
	if want := "not set in the environment: PG_HOST, PG_PASSWORD; " + base.Error(); got.Error() != want {
		t.Fatalf("got  %q\nwant %q", got, want)
	}
	if !errors.Is(got, base) {
		t.Fatal("annotated error must unwrap to the original")
	}
	// A wrapper annotating again must not add a second note.
	wrapped := Annotate(fmt.Errorf("init sandbox session: %w", got), []string{"OTHER"})
	if strings.Count(wrapped.Error(), "not set in the environment") != 1 || strings.Contains(wrapped.Error(), "OTHER") {
		t.Fatalf("second note added: %q", wrapped)
	}
}

// The note must survive the CLI's credential redaction. With the note after
// the error, a message ending in the empty password= of the missing variable
// lost the note's first word to the password rule.
func TestAnnotate_SurvivesRedaction(t *testing.T) {
	t.Parallel()
	err := Annotate(errors.New("connect failed: host=db password="), []string{"PG_PASSWORD"})
	if got := redact.String(err.Error()); !strings.HasPrefix(got, "not set in the environment: PG_PASSWORD; ") {
		t.Fatalf("note damaged by redaction: %q", got)
	}
}
