// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package script

import (
	"testing"

	"go.starlark.net/starlark"
	"go.starlark.net/starlarkstruct"
)

func TestGoToStarlarkString(t *testing.T) {
	t.Parallel()
	obj, err := goToStarlark("hello")
	if err != nil {
		t.Fatal(err)
	}
	s, ok := obj.(starlark.String)
	if !ok || string(s) != "hello" {
		t.Errorf("got %v, want string 'hello'", obj)
	}
}

func TestGoToStarlarkInt(t *testing.T) {
	t.Parallel()
	obj, err := goToStarlark(int64(42))
	if err != nil {
		t.Fatal(err)
	}
	n, ok := obj.(starlark.Int)
	if !ok {
		t.Fatalf("got %T, want starlark.Int", obj)
	}
	i, _ := n.Int64()
	if i != 42 {
		t.Errorf("got %d, want 42", i)
	}
}

func TestGoToStarlarkFloat(t *testing.T) {
	t.Parallel()
	obj, err := goToStarlark(3.14)
	if err != nil {
		t.Fatal(err)
	}
	f, ok := obj.(starlark.Float)
	if !ok || float64(f) != 3.14 {
		t.Errorf("got %v, want float 3.14", obj)
	}
}

func TestGoToStarlarkBool(t *testing.T) {
	t.Parallel()
	obj, err := goToStarlark(true)
	if err != nil {
		t.Fatal(err)
	}
	if obj != starlark.True {
		t.Errorf("got %v, want True", obj)
	}

	obj, err = goToStarlark(false)
	if err != nil {
		t.Fatal(err)
	}
	if obj != starlark.False {
		t.Errorf("got %v, want False", obj)
	}
}

func TestGoToStarlarkNil(t *testing.T) {
	t.Parallel()
	obj, err := goToStarlark(nil)
	if err != nil {
		t.Fatal(err)
	}
	if obj != starlark.None {
		t.Errorf("got %v, want None", obj)
	}
}

func TestGoToStarlarkSlice(t *testing.T) {
	t.Parallel()
	obj, err := goToStarlark([]interface{}{"a", int64(1)})
	if err != nil {
		t.Fatal(err)
	}
	list, ok := obj.(*starlark.List)
	if !ok || list.Len() != 2 {
		t.Fatalf("got %v, want list of length 2", obj)
	}
	if s, ok := list.Index(0).(starlark.String); !ok || string(s) != "a" {
		t.Errorf("list[0] = %v, want 'a'", list.Index(0))
	}
}

func TestGoToStarlarkMap(t *testing.T) {
	t.Parallel()
	obj, err := goToStarlark(map[string]interface{}{"key": "val"})
	if err != nil {
		t.Fatal(err)
	}
	d, ok := obj.(*starlark.Dict)
	if !ok {
		t.Fatalf("got %T, want *starlark.Dict", obj)
	}
	v, found, _ := d.Get(starlark.String("key"))
	if !found {
		t.Fatal("key not found")
	}
	if s, ok := v.(starlark.String); !ok || string(s) != "val" {
		t.Errorf("dict[key] = %v, want 'val'", v)
	}
}

func TestStarlarkToGoString(t *testing.T) {
	t.Parallel()
	v, err := starlarkToGo(starlark.String("hello"))
	if err != nil {
		t.Fatal(err)
	}
	if v != "hello" {
		t.Errorf("got %v, want 'hello'", v)
	}
}

func TestStarlarkToGoInt(t *testing.T) {
	t.Parallel()
	v, err := starlarkToGo(starlark.MakeInt64(99))
	if err != nil {
		t.Fatal(err)
	}
	if v != int64(99) {
		t.Errorf("got %v, want 99", v)
	}
}

func TestStarlarkToGoFloat(t *testing.T) {
	t.Parallel()
	v, err := starlarkToGo(starlark.Float(2.5))
	if err != nil {
		t.Fatal(err)
	}
	if v != 2.5 {
		t.Errorf("got %v, want 2.5", v)
	}
}

func TestStarlarkToGoBool(t *testing.T) {
	t.Parallel()
	v, err := starlarkToGo(starlark.True)
	if err != nil {
		t.Fatal(err)
	}
	if v != true {
		t.Errorf("got %v, want true", v)
	}

	v, err = starlarkToGo(starlark.False)
	if err != nil {
		t.Fatal(err)
	}
	if v != false {
		t.Errorf("got %v, want false", v)
	}
}

func TestStarlarkToGoNone(t *testing.T) {
	t.Parallel()
	v, err := starlarkToGo(starlark.None)
	if err != nil {
		t.Fatal(err)
	}
	if v != nil {
		t.Errorf("got %v, want nil", v)
	}
}

func TestStarlarkToGoList(t *testing.T) {
	t.Parallel()
	list := starlark.NewList([]starlark.Value{
		starlark.String("x"),
		starlark.MakeInt64(7),
	})
	v, err := starlarkToGo(list)
	if err != nil {
		t.Fatal(err)
	}
	slice, ok := v.([]interface{})
	if !ok || len(slice) != 2 {
		t.Fatalf("got %v, want slice of length 2", v)
	}
	if slice[0] != "x" {
		t.Errorf("slice[0] = %v, want 'x'", slice[0])
	}
}

func TestStarlarkToGoDict(t *testing.T) {
	t.Parallel()
	d := starlark.NewDict(1)
	d.SetKey(starlark.String("a"), starlark.MakeInt64(1))
	v, err := starlarkToGo(d)
	if err != nil {
		t.Fatal(err)
	}
	goMap, ok := v.(map[string]interface{})
	if !ok {
		t.Fatalf("got %T, want map[string]interface{}", v)
	}
	if goMap["a"] != int64(1) {
		t.Errorf("map[a] = %v, want 1", goMap["a"])
	}
}

func TestStarlarkToGoStruct(t *testing.T) {
	t.Parallel()
	s := starlarkstruct.FromStringDict(starlark.String("test"), starlark.StringDict{
		"name": starlark.String("alice"),
		"age":  starlark.MakeInt(30),
	})
	v, err := starlarkToGo(s)
	if err != nil {
		t.Fatal(err)
	}
	goMap, ok := v.(map[string]interface{})
	if !ok {
		t.Fatalf("got %T, want map[string]interface{}", v)
	}
	if goMap["name"] != "alice" {
		t.Errorf("name = %v, want 'alice'", goMap["name"])
	}
}

func TestStarlarkDictToGo(t *testing.T) {
	t.Parallel()
	d := starlark.NewDict(3)
	d.SetKey(starlark.String("name"), starlark.String("alice"))
	d.SetKey(starlark.String("age"), starlark.MakeInt64(30))
	d.SetKey(starlark.String("score"), starlark.Float(9.5))

	result, err := starlarkDictToGo(d)
	if err != nil {
		t.Fatal(err)
	}
	if result["name"] != "alice" {
		t.Errorf("name = %v, want 'alice'", result["name"])
	}
	if result["age"] != int64(30) {
		t.Errorf("age = %v, want 30", result["age"])
	}
	if result["score"] != 9.5 {
		t.Errorf("score = %v, want 9.5", result["score"])
	}
}

func TestStarlarkDictToGoEmpty(t *testing.T) {
	t.Parallel()
	d := starlark.NewDict(0)
	result, err := starlarkDictToGo(d)
	if err != nil {
		t.Fatal(err)
	}
	if len(result) != 0 {
		t.Errorf("expected empty map, got %v", result)
	}
}

func TestStarlarkToGoTuple(t *testing.T) {
	t.Parallel()
	tuple := starlark.Tuple{starlark.String("a"), starlark.MakeInt64(1)}
	v, err := starlarkToGo(tuple)
	if err != nil {
		t.Fatal(err)
	}
	slice, ok := v.([]interface{})
	if !ok || len(slice) != 2 {
		t.Fatalf("got %v, want slice of length 2", v)
	}
	if slice[0] != "a" {
		t.Errorf("slice[0] = %v, want 'a'", slice[0])
	}
}

func TestStarlarkToGoUnknownType(t *testing.T) {
	t.Parallel()
	// starlark.Bytes is a type that will fall through to default
	v, err := starlarkToGo(starlark.Bytes("raw"))
	if err != nil {
		t.Fatal(err)
	}
	s, ok := v.(string)
	if !ok {
		t.Fatalf("got %T, want string", v)
	}
	if s == "" {
		t.Error("expected non-empty string representation")
	}
}

func TestGoToStarlarkPlainInt(t *testing.T) {
	t.Parallel()
	// Test int (not int64) conversion
	obj, err := goToStarlark(42)
	if err != nil {
		t.Fatal(err)
	}
	n, ok := obj.(starlark.Int)
	if !ok {
		t.Fatalf("got %T, want starlark.Int", obj)
	}
	i, _ := n.Int64()
	if i != 42 {
		t.Errorf("got %d, want 42", i)
	}
}

func TestGoToStarlarkUnknownType(t *testing.T) {
	t.Parallel()
	// Unknown type falls through to string representation
	obj, err := goToStarlark(struct{ Name string }{"test"})
	if err != nil {
		t.Fatal(err)
	}
	s, ok := obj.(starlark.String)
	if !ok {
		t.Fatalf("got %T, want starlark.String", obj)
	}
	if string(s) == "" {
		t.Error("expected non-empty string")
	}
}

func TestStarlarkDictToGoNonStringKey(t *testing.T) {
	t.Parallel()
	d := starlark.NewDict(1)
	d.SetKey(starlark.MakeInt64(1), starlark.String("val"))
	_, err := starlarkDictToGo(d)
	if err == nil {
		t.Error("expected error for non-string key")
	}
}

func TestStarlarkToGoLargeInt_Fallback(t *testing.T) {
	t.Parallel()
	// Create a very large integer that doesn't fit in int64
	// Use starlark.MakeUint64 with a value > math.MaxInt64
	bigInt := starlark.MakeUint64(18446744073709551615) // math.MaxUint64
	v, err := starlarkToGo(bigInt)
	if err != nil {
		t.Fatal(err)
	}
	s, ok := v.(string)
	if !ok {
		t.Fatalf("large int should fallback to string, got %T", v)
	}
	if s != "18446744073709551615" {
		t.Errorf("got %q, want 18446744073709551615", s)
	}
}

func TestRoundtripGoStarlarkGo(t *testing.T) {
	t.Parallel()
	original := map[string]interface{}{
		"name":   "test",
		"count":  int64(42),
		"rate":   3.14,
		"active": true,
		"tags":   []interface{}{"a", "b"},
		"nested": map[string]interface{}{"x": int64(1)},
	}

	starlarkObj, err := goToStarlark(original)
	if err != nil {
		t.Fatal(err)
	}

	back, err := starlarkToGo(starlarkObj)
	if err != nil {
		t.Fatal(err)
	}

	m, ok := back.(map[string]interface{})
	if !ok {
		t.Fatalf("roundtrip returned %T, want map", back)
	}
	if m["name"] != "test" {
		t.Errorf("name = %v", m["name"])
	}
	if m["count"] != int64(42) {
		t.Errorf("count = %v", m["count"])
	}
	if m["active"] != true {
		t.Errorf("active = %v", m["active"])
	}
	nested, ok := m["nested"].(map[string]interface{})
	if !ok || nested["x"] != int64(1) {
		t.Errorf("nested = %v", m["nested"])
	}
}

// TestMarshalStarlarkJSON_PreservesInsertionOrder pins the contract that
// HTTP request bodies retain Starlark dict insertion order — a regression
// would silently break Mistral OCR (it echoes the schema verbatim when
// `properties` arrives before `type` in document_annotation_format) and
// other order-sensitive APIs. Both Go's json.Marshal and Starlark's
// stdlib json.encode sort map keys alphabetically, so a custom encoder
// is required.
func TestMarshalStarlarkJSON_PreservesInsertionOrder(t *testing.T) {
	t.Parallel()
	d := starlark.NewDict(3)
	_ = d.SetKey(starlark.String("type"), starlark.String("object"))
	props := starlark.NewDict(1)
	_ = props.SetKey(starlark.String("total"), starlark.String("number"))
	_ = d.SetKey(starlark.String("properties"), props)
	_ = d.SetKey(starlark.String("name"), starlark.String("extraction"))

	got, err := MarshalStarlarkJSON(d)
	if err != nil {
		t.Fatalf("MarshalStarlarkJSON: %v", err)
	}
	want := `{"type":"object","properties":{"total":"number"},"name":"extraction"}`
	if string(got) != want {
		t.Errorf("got  %s\nwant %s", got, want)
	}
}

func TestMarshalStarlarkJSON_Primitives(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		v    starlark.Value
		want string
	}{
		{"none", starlark.None, "null"},
		{"true", starlark.Bool(true), "true"},
		{"false", starlark.Bool(false), "false"},
		{"int", starlark.MakeInt(42), "42"},
		{"float", starlark.Float(3.14), "3.14"},
		{"string", starlark.String("hello"), `"hello"`},
		{"string with quote", starlark.String(`a"b`), `"a\"b"`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := MarshalStarlarkJSON(c.v)
			if err != nil {
				t.Fatalf("MarshalStarlarkJSON: %v", err)
			}
			if string(got) != c.want {
				t.Errorf("got %s, want %s", got, c.want)
			}
		})
	}
}

func TestMarshalStarlarkJSON_NestedDictAndList(t *testing.T) {
	t.Parallel()
	inner := starlark.NewDict(2)
	_ = inner.SetKey(starlark.String("a"), starlark.MakeInt(1))
	_ = inner.SetKey(starlark.String("b"), starlark.MakeInt(2))
	list := starlark.NewList([]starlark.Value{inner, starlark.MakeInt(3)})
	outer := starlark.NewDict(1)
	_ = outer.SetKey(starlark.String("items"), list)

	got, err := MarshalStarlarkJSON(outer)
	if err != nil {
		t.Fatalf("MarshalStarlarkJSON: %v", err)
	}
	want := `{"items":[{"a":1,"b":2},3]}`
	if string(got) != want {
		t.Errorf("got %s, want %s", got, want)
	}
}
