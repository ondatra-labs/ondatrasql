// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package script

import (
	"fmt"
	"strings"

	"go.starlark.net/starlark"
	"go.starlark.net/starlarkstruct"
)

// libHelpersModule exposes helpers that bridge OndatraSQL's typed-column
// dicts to formats other systems use. Currently:
//
//   - to_json_schema(t): convert a DuckDB-native type value into a
//     JSON Schema dict. The input is whatever a column dict's `type`
//     field carries — a string ("VARCHAR", "DECIMAL(18,3)", ...), a
//     list (LIST of inner type), or a dict (STRUCT of name → type).
//
// Module is exposed as a global named `lib_helpers` (no `load()`).
func libHelpersModule() *starlarkstruct.Module {
	return &starlarkstruct.Module{
		Name: "lib_helpers",
		Members: starlark.StringDict{
			"to_json_schema": starlark.NewBuiltin("lib_helpers.to_json_schema",
				func(thread *starlark.Thread, fn *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
					var t starlark.Value
					if err := starlark.UnpackPositionalArgs(fn.Name(), args, kwargs, 1, &t); err != nil {
						return nil, err
					}
					return duckTypeToJSONSchema(t)
				}),
		},
	}
}

// duckTypeToJSONSchema recursively maps a DuckDB-native type to JSON Schema:
//
//	primitive (string) → {"type": ...} or {"type": "string", "format": ...}
//	LIST (Starlark list with one element) → {"type": "array", "items": <recurse>}
//	STRUCT (Starlark dict) → {"type": "object", "properties": {field: <recurse>, ...}}
//
// MAP, UNION, and unrecognized types fall through to {"type": "string"}.
func duckTypeToJSONSchema(t starlark.Value) (starlark.Value, error) {
	switch v := t.(type) {
	case starlark.String:
		return primitiveDuckTypeToJSONSchema(string(v)), nil

	case *starlark.List:
		// LIST → array. The inner type is the first element.
		if v.Len() == 0 {
			return jsonSchemaDict("type", starlark.String("string")), nil
		}
		inner := v.Index(0)
		items, err := duckTypeToJSONSchema(inner)
		if err != nil {
			return nil, err
		}
		return jsonSchemaDict(
			"type", starlark.String("array"),
			"items", items,
		), nil

	case *starlark.Dict:
		// STRUCT → object. Keys are field names; values are types.
		properties := starlark.NewDict(v.Len())
		for _, key := range v.Keys() {
			val, _, err := v.Get(key)
			if err != nil {
				return nil, err
			}
			schema, err := duckTypeToJSONSchema(val)
			if err != nil {
				return nil, err
			}
			if err := properties.SetKey(key, schema); err != nil {
				return nil, err
			}
		}
		return jsonSchemaDict(
			"type", starlark.String("object"),
			"properties", properties,
		), nil
	}

	return nil, fmt.Errorf("to_json_schema: expected string, list, or dict, got %s", t.Type())
}

// primitiveDuckTypeToJSONSchema maps a DuckDB primitive type string
// (the canonical syntax emitted by normalizeType) to a JSON Schema dict.
//
// Width/precision distinctions inside DuckDB (TINYINT vs BIGINT, FLOAT
// vs DOUBLE, TIMESTAMP_NS vs TIMESTAMPTZ) all collapse to the same
// JSON Schema type — JSON Schema doesn't carry that information.
func primitiveDuckTypeToJSONSchema(t string) starlark.Value {
	upper := strings.ToUpper(strings.TrimSpace(t))

	// DECIMAL(p,s) / NUMERIC(p,s) → number
	if strings.HasPrefix(upper, "DECIMAL") || strings.HasPrefix(upper, "NUMERIC") {
		return jsonSchemaDict("type", starlark.String("number"))
	}
	// TIMESTAMP variants → date-time string. Covers TIMESTAMP, TIMESTAMPTZ,
	// TIMESTAMP_NS, TIMESTAMP_MS, TIMESTAMP_S, TIMESTAMP_US.
	if strings.HasPrefix(upper, "TIMESTAMP") {
		return jsonSchemaDict(
			"type", starlark.String("string"),
			"format", starlark.String("date-time"),
		)
	}

	switch upper {
	// Signed integers
	case "TINYINT", "SMALLINT", "INTEGER", "BIGINT", "HUGEINT":
		return jsonSchemaDict("type", starlark.String("integer"))
	// Unsigned integers
	case "UTINYINT", "USMALLINT", "UINTEGER", "UBIGINT", "UHUGEINT":
		return jsonSchemaDict("type", starlark.String("integer"))
	// Floats (also DECIMAL handled above as prefix)
	case "FLOAT", "DOUBLE", "REAL":
		return jsonSchemaDict("type", starlark.String("number"))
	case "BOOLEAN":
		return jsonSchemaDict("type", starlark.String("boolean"))
	case "DATE":
		return jsonSchemaDict(
			"type", starlark.String("string"),
			"format", starlark.String("date"),
		)
	}

	// Catch-all: VARCHAR, UUID, JSON, BLOB, BIT, INTERVAL, TIME (any variant),
	// MAP(...), UNION(...) — all surface as plain strings to JSON Schema.
	return jsonSchemaDict("type", starlark.String("string"))
}

// jsonSchemaDict builds a Starlark dict from key/value pairs. Pairs are
// passed as alternating string keys and starlark.Value values. Used for
// the small JSON Schema shapes this module produces — keep insertion
// order so the dict iterates "type" first.
func jsonSchemaDict(pairs ...any) *starlark.Dict {
	if len(pairs)%2 != 0 {
		panic("jsonSchemaDict: odd number of arguments")
	}
	d := starlark.NewDict(len(pairs) / 2)
	for i := 0; i < len(pairs); i += 2 {
		key, ok := pairs[i].(string)
		if !ok {
			panic("jsonSchemaDict: keys must be strings")
		}
		val, ok := pairs[i+1].(starlark.Value)
		if !ok {
			panic("jsonSchemaDict: values must be starlark.Value")
		}
		_ = d.SetKey(starlark.String(key), val)
	}
	return d
}
