// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package script

import (
	"fmt"

	"go.starlark.net/starlark"
	"go.starlark.net/starlarkstruct"
)

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
