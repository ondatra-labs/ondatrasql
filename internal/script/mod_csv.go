// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package script

import (
	"encoding/csv"
	"fmt"
	"strings"

	"go.starlark.net/starlark"
	"go.starlark.net/starlarkstruct"
)

// csvModule provides CSV parsing and encoding functions.
func csvModule() *starlarkstruct.Module {
	return &starlarkstruct.Module{
		Name: "csv",
		Members: starlark.StringDict{
			// decode(data, delimiter=",", header=True) -> list of dicts or list of lists
			"decode": starlark.NewBuiltin("csv.decode", csvDecode),

			// encode(rows, header=None) -> string
			"encode": starlark.NewBuiltin("csv.encode", csvEncode),
		},
	}
}

func csvDecode(thread *starlark.Thread, fn *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
	var data string
	var delimiter string = ","
	var header bool = true
	if err := starlark.UnpackArgs(fn.Name(), args, kwargs,
		"data", &data,
		"delimiter?", &delimiter,
		"header?", &header,
	); err != nil {
		return nil, err
	}

	if data == "" {
		return nil, fmt.Errorf("csv.decode: empty string")
	}

	r := csv.NewReader(strings.NewReader(data))
	if len(delimiter) > 0 {
		r.Comma = rune(delimiter[0])
	}

	records, err := r.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("csv.decode: %w", err)
	}

	if len(records) == 0 {
		return starlark.NewList(nil), nil
	}

	if header {
		headers := records[0]
		rows := make([]starlark.Value, 0, len(records)-1)
		for _, record := range records[1:] {
			dict := starlark.NewDict(len(headers))
			for i, h := range headers {
				val := ""
				if i < len(record) {
					val = record[i]
				}
				if err := dict.SetKey(starlark.String(h), starlark.String(val)); err != nil {
					return nil, err
				}
			}
			rows = append(rows, dict)
		}
		return starlark.NewList(rows), nil
	}

	rows := make([]starlark.Value, 0, len(records))
	for _, record := range records {
		elems := make([]starlark.Value, len(record))
		for i, field := range record {
			elems[i] = starlark.String(field)
		}
		rows = append(rows, starlark.NewList(elems))
	}
	return starlark.NewList(rows), nil
}

func csvEncode(thread *starlark.Thread, fn *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
	var rowsList *starlark.List
	var headerList *starlark.List
	if err := starlark.UnpackArgs(fn.Name(), args, kwargs,
		"rows", &rowsList,
		"header?", &headerList,
	); err != nil {
		return nil, err
	}

	if rowsList.Len() == 0 {
		return starlark.String(""), nil
	}

	var buf strings.Builder
	w := csv.NewWriter(&buf)

	// Detect whether rows are dicts or lists based on first element
	first := rowsList.Index(0)
	switch first.(type) {
	case *starlark.Dict:
		// Extract headers from first dict's keys
		firstDict := first.(*starlark.Dict)
		headers := make([]string, 0, firstDict.Len())
		for _, kv := range firstDict.Items() {
			k, _ := starlark.AsString(kv[0])
			headers = append(headers, k)
		}
		if err := w.Write(headers); err != nil {
			return nil, fmt.Errorf("csv.encode: %w", err)
		}

		for i := 0; i < rowsList.Len(); i++ {
			d, ok := rowsList.Index(i).(*starlark.Dict)
			if !ok {
				return nil, fmt.Errorf("csv.encode: expected dict at row %d, got %s", i, rowsList.Index(i).Type())
			}
			record := make([]string, len(headers))
			for j, h := range headers {
				v, _, _ := d.Get(starlark.String(h))
				if v != nil && v != starlark.None {
					s, ok := starlark.AsString(v)
					if !ok {
						s = v.String()
					}
					record[j] = s
				}
			}
			if err := w.Write(record); err != nil {
				return nil, fmt.Errorf("csv.encode: %w", err)
			}
		}

	case *starlark.List:
		// Write header row if provided
		if headerList != nil {
			hdr := make([]string, headerList.Len())
			for i := 0; i < headerList.Len(); i++ {
				hdr[i], _ = starlark.AsString(headerList.Index(i))
			}
			if err := w.Write(hdr); err != nil {
				return nil, fmt.Errorf("csv.encode: %w", err)
			}
		}

		for i := 0; i < rowsList.Len(); i++ {
			l, ok := rowsList.Index(i).(*starlark.List)
			if !ok {
				return nil, fmt.Errorf("csv.encode: expected list at row %d, got %s", i, rowsList.Index(i).Type())
			}
			record := make([]string, l.Len())
			for j := 0; j < l.Len(); j++ {
				s, ok := starlark.AsString(l.Index(j))
				if !ok {
					s = l.Index(j).String()
				}
				record[j] = s
			}
			if err := w.Write(record); err != nil {
				return nil, fmt.Errorf("csv.encode: %w", err)
			}
		}

	default:
		return nil, fmt.Errorf("csv.encode: expected list of dicts or list of lists, got list of %s", first.Type())
	}

	w.Flush()
	if err := w.Error(); err != nil {
		return nil, fmt.Errorf("csv.encode: %w", err)
	}
	return starlark.String(buf.String()), nil
}
