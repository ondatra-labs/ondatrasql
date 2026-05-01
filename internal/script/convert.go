// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package script

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"strconv"

	"go.starlark.net/starlark"
	"go.starlark.net/starlarkstruct"
)

// MarshalStarlarkJSON encodes a Starlark value as JSON, preserving dict
// insertion order. Go's json.Marshal sorts map keys alphabetically and
// Starlark's stdlib json.encode does the same — both break APIs that
// are sensitive to JSON Schema key order (e.g. Mistral OCR's
// document_annotation_format silently echoes the schema verbatim when
// `properties` arrives before `type`). Use this instead of json.Marshal
// when the caller's dict order is part of the contract with the remote.
func MarshalStarlarkJSON(v starlark.Value) ([]byte, error) {
	var buf bytes.Buffer
	if err := emitStarlarkJSON(&buf, v); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func emitStarlarkJSON(buf *bytes.Buffer, v starlark.Value) error {
	switch x := v.(type) {
	case nil:
		buf.WriteString("null")
	case starlark.NoneType:
		buf.WriteString("null")
	case starlark.Bool:
		if bool(x) {
			buf.WriteString("true")
		} else {
			buf.WriteString("false")
		}
	case starlark.Int:
		buf.WriteString(x.String())
	case starlark.Float:
		f := float64(x)
		if math.IsNaN(f) || math.IsInf(f, 0) {
			return fmt.Errorf("cannot encode non-finite float %v", x)
		}
		buf.WriteString(strconv.FormatFloat(f, 'g', -1, 64))
	case starlark.String:
		data, err := json.Marshal(string(x))
		if err != nil {
			return err
		}
		buf.Write(data)
	case *starlark.Dict:
		buf.WriteByte('{')
		for i, item := range x.Items() {
			if i > 0 {
				buf.WriteByte(',')
			}
			k, ok := starlark.AsString(item[0])
			if !ok {
				return fmt.Errorf("dict key must be string, got %s", item[0].Type())
			}
			data, err := json.Marshal(k)
			if err != nil {
				return err
			}
			buf.Write(data)
			buf.WriteByte(':')
			if err := emitStarlarkJSON(buf, item[1]); err != nil {
				return err
			}
		}
		buf.WriteByte('}')
	case *starlark.List:
		buf.WriteByte('[')
		for i := 0; i < x.Len(); i++ {
			if i > 0 {
				buf.WriteByte(',')
			}
			if err := emitStarlarkJSON(buf, x.Index(i)); err != nil {
				return err
			}
		}
		buf.WriteByte(']')
	case starlark.Tuple:
		buf.WriteByte('[')
		for i, elem := range x {
			if i > 0 {
				buf.WriteByte(',')
			}
			if err := emitStarlarkJSON(buf, elem); err != nil {
				return err
			}
		}
		buf.WriteByte(']')
	case *starlarkstruct.Struct:
		buf.WriteByte('{')
		first := true
		for _, name := range x.AttrNames() {
			attr, err := x.Attr(name)
			if err != nil {
				return err
			}
			if !first {
				buf.WriteByte(',')
			}
			first = false
			data, err := json.Marshal(name)
			if err != nil {
				return err
			}
			buf.Write(data)
			buf.WriteByte(':')
			if err := emitStarlarkJSON(buf, attr); err != nil {
				return err
			}
		}
		buf.WriteByte('}')
	default:
		return fmt.Errorf("cannot encode %s as JSON", v.Type())
	}
	return nil
}

// starlarkDictToGo converts a Starlark dict to a Go map.
func starlarkDictToGo(d *starlark.Dict) (map[string]interface{}, error) {
	result := make(map[string]interface{})
	for _, item := range d.Items() {
		key, ok := starlark.AsString(item[0])
		if !ok {
			return nil, fmt.Errorf("dict key is not a string: %s", item[0].Type())
		}
		val, err := starlarkToGo(item[1])
		if err != nil {
			return nil, err
		}
		result[key] = val
	}
	return result, nil
}

// starlarkToGo converts a Starlark value to a Go value.
func starlarkToGo(v starlark.Value) (interface{}, error) {
	switch val := v.(type) {
	case starlark.NoneType:
		return nil, nil
	case starlark.String:
		return string(val), nil
	case starlark.Int:
		i, ok := val.Int64()
		if ok {
			return i, nil
		}
		// Fall back to string representation for very large ints
		return val.String(), nil
	case starlark.Float:
		return float64(val), nil
	case starlark.Bool:
		return bool(val), nil
	case *starlark.List:
		result := make([]interface{}, val.Len())
		for i := 0; i < val.Len(); i++ {
			item, err := starlarkToGo(val.Index(i))
			if err != nil {
				return nil, err
			}
			result[i] = item
		}
		return result, nil
	case *starlark.Dict:
		return starlarkDictToGo(val)
	case starlark.Tuple:
		result := make([]interface{}, len(val))
		for i, item := range val {
			v, err := starlarkToGo(item)
			if err != nil {
				return nil, err
			}
			result[i] = v
		}
		return result, nil
	case *starlarkstruct.Struct:
		result := make(map[string]interface{})
		for _, name := range val.AttrNames() {
			attr, err := val.Attr(name)
			if err != nil {
				return nil, err
			}
			goVal, err := starlarkToGo(attr)
			if err != nil {
				return nil, err
			}
			result[name] = goVal
		}
		return result, nil
	default:
		return fmt.Sprintf("%v", v), nil
	}
}

// goToStarlark converts a Go value to a Starlark value.
func goToStarlark(v interface{}) (starlark.Value, error) {
	if v == nil {
		return starlark.None, nil
	}

	switch val := v.(type) {
	case string:
		return starlark.String(val), nil
	case int:
		return starlark.MakeInt64(int64(val)), nil
	case int64:
		return starlark.MakeInt64(val), nil
	case int32:
		return starlark.MakeInt64(int64(val)), nil
	case int16:
		return starlark.MakeInt64(int64(val)), nil
	case int8:
		return starlark.MakeInt64(int64(val)), nil
	case uint:
		return starlark.MakeUint64(uint64(val)), nil
	case uint64:
		return starlark.MakeUint64(val), nil
	case uint32:
		return starlark.MakeUint64(uint64(val)), nil
	case uint16:
		return starlark.MakeUint64(uint64(val)), nil
	case uint8:
		return starlark.MakeUint64(uint64(val)), nil
	case float64:
		return starlark.Float(val), nil
	case float32:
		return starlark.Float(float64(val)), nil
	case bool:
		return starlark.Bool(val), nil
	case []interface{}:
		elems := make([]starlark.Value, len(val))
		for i, item := range val {
			obj, err := goToStarlark(item)
			if err != nil {
				return nil, err
			}
			elems[i] = obj
		}
		return starlark.NewList(elems), nil
	case map[string]interface{}:
		d := starlark.NewDict(len(val))
		for k, item := range val {
			obj, err := goToStarlark(item)
			if err != nil {
				return nil, err
			}
			if err := d.SetKey(starlark.String(k), obj); err != nil {
				return nil, err
			}
		}
		return d, nil
	default:
		return starlark.String(fmt.Sprintf("%v", v)), nil
	}
}
