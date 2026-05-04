package script

import (
	"testing"

	"go.starlark.net/starlark"
	"go.starlark.net/syntax"
)

func TestFilterKwargs_DropsUndeclared(t *testing.T) {
	t.Parallel()
	thread := &starlark.Thread{Name: "test"}
	opts := &syntax.FileOptions{}
	globals, _ := starlark.ExecFileOptions(opts, thread, "test.star", `
def fetch(pattern, page=None):
    pass
`, nil)
	fn := globals["fetch"].(*starlark.Function)

	kwargs := []starlark.Tuple{
		{starlark.String("pattern"), starlark.String("*.pdf")},
		{starlark.String("page"), starlark.None},
		{starlark.String("columns"), starlark.NewList(nil)},
		{starlark.String("is_backfill"), starlark.True},
	}

	filtered := filterKwargs(fn, kwargs)
	if len(filtered) != 2 {
		t.Fatalf("expected 2 kwargs (pattern, page), got %d", len(filtered))
	}
	for _, kv := range filtered {
		name, _ := starlark.AsString(kv[0])
		if name != "pattern" && name != "page" {
			t.Errorf("unexpected kwarg: %s", name)
		}
	}
}

func TestFilterKwargs_PassesAllWithStarKwargs(t *testing.T) {
	t.Parallel()
	thread := &starlark.Thread{Name: "test"}
	opts := &syntax.FileOptions{}
	globals, _ := starlark.ExecFileOptions(opts, thread, "test.star", `
def fetch(pattern, **kwargs):
    pass
`, nil)
	fn := globals["fetch"].(*starlark.Function)

	kwargs := []starlark.Tuple{
		{starlark.String("pattern"), starlark.String("*.pdf")},
		{starlark.String("columns"), starlark.NewList(nil)},
		{starlark.String("extra"), starlark.True},
	}

	filtered := filterKwargs(fn, kwargs)
	if len(filtered) != 3 {
		t.Fatalf("expected all 3 kwargs with **kwargs, got %d", len(filtered))
	}
}

func TestFilterKwargs_NonFunction(t *testing.T) {
	t.Parallel()
	builtin := starlark.NewBuiltin("test", func(thread *starlark.Thread, fn *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
		return starlark.None, nil
	})

	kwargs := []starlark.Tuple{
		{starlark.String("a"), starlark.True},
	}

	filtered := filterKwargs(builtin, kwargs)
	if len(filtered) != 1 {
		t.Fatalf("expected all kwargs passed for non-Function, got %d", len(filtered))
	}
}
