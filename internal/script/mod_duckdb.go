// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package script

import (
	"fmt"
	"strings"

	"go.starlark.net/starlark"

	"github.com/ondatra-labs/ondatrasql/internal/duckdb"
)

// globBuiltin returns file paths matching a pattern.
// Usage: files = glob("data/*.pdf")
func globBuiltin(sess *duckdb.Session) *starlark.Builtin {
	return starlark.NewBuiltin("glob", func(thread *starlark.Thread, fn *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
		var pattern string
		if err := starlark.UnpackPositionalArgs(fn.Name(), args, kwargs, 1, &pattern); err != nil {
			return nil, err
		}
		if sess == nil {
			return starlark.NewList(nil), nil
		}
		escaped := strings.ReplaceAll(pattern, "'", "''")
		rows, err := sess.QueryRows(fmt.Sprintf("SELECT file FROM glob('%s') ORDER BY file", escaped))
		if err != nil {
			return nil, fmt.Errorf("glob: %w", err)
		}
		var items []starlark.Value
		for _, row := range rows {
			items = append(items, starlark.String(row))
		}
		return starlark.NewList(items), nil
	})
}

// md5FileBuiltin hashes a file's contents.
// Usage: hash = md5_file("data/invoice.pdf")
func md5FileBuiltin(sess *duckdb.Session) *starlark.Builtin {
	return starlark.NewBuiltin("md5_file", func(thread *starlark.Thread, fn *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
		var path string
		if err := starlark.UnpackPositionalArgs(fn.Name(), args, kwargs, 1, &path); err != nil {
			return nil, err
		}
		if sess == nil {
			return starlark.String(""), nil
		}
		escaped := strings.ReplaceAll(path, "'", "''")
		val, err := sess.QueryValue(fmt.Sprintf("SELECT md5(content) FROM read_blob('%s')", escaped))
		if err != nil {
			return nil, fmt.Errorf("md5_file: %w", err)
		}
		return starlark.String(val), nil
	})
}

// readTextBuiltin reads a text file.
// Usage: text = read_text("data/config.json")
func readTextBuiltin(sess *duckdb.Session) *starlark.Builtin {
	return starlark.NewBuiltin("read_text", func(thread *starlark.Thread, fn *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
		var path string
		if err := starlark.UnpackPositionalArgs(fn.Name(), args, kwargs, 1, &path); err != nil {
			return nil, err
		}
		if sess == nil {
			return starlark.String(""), nil
		}
		escaped := strings.ReplaceAll(path, "'", "''")
		val, err := sess.QueryValue(fmt.Sprintf("SELECT content FROM read_text('%s')", escaped))
		if err != nil {
			return nil, fmt.Errorf("read_text: %w", err)
		}
		return starlark.String(val), nil
	})
}

// readBlobBuiltin reads a binary file as bytes.
// Usage: data = read_blob("data/image.png")
func readBlobBuiltin(sess *duckdb.Session) *starlark.Builtin {
	return starlark.NewBuiltin("read_blob", func(thread *starlark.Thread, fn *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
		var path string
		if err := starlark.UnpackPositionalArgs(fn.Name(), args, kwargs, 1, &path); err != nil {
			return nil, err
		}
		if sess == nil {
			return starlark.String(""), nil
		}
		escaped := strings.ReplaceAll(path, "'", "''")
		val, err := sess.QueryValue(fmt.Sprintf("SELECT content FROM read_blob('%s')", escaped))
		if err != nil {
			return nil, fmt.Errorf("read_blob: %w", err)
		}
		return starlark.String(val), nil
	})
}

// fileExistsBuiltin checks if a file exists.
// Usage: exists = file_exists("data/file.pdf")
func fileExistsBuiltin(sess *duckdb.Session) *starlark.Builtin {
	return starlark.NewBuiltin("file_exists", func(thread *starlark.Thread, fn *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
		var path string
		if err := starlark.UnpackPositionalArgs(fn.Name(), args, kwargs, 1, &path); err != nil {
			return nil, err
		}
		if sess == nil {
			return starlark.False, nil
		}
		escaped := strings.ReplaceAll(path, "'", "''")
		val, err := sess.QueryValue(fmt.Sprintf("SELECT COUNT(*) FROM glob('%s')", escaped))
		if err != nil {
			return starlark.False, nil
		}
		return starlark.Bool(val != "0"), nil
	})
}

// md5Builtin hashes a string.
// Usage: h = md5("some string")
func md5Builtin(sess *duckdb.Session) *starlark.Builtin {
	return starlark.NewBuiltin("md5", func(thread *starlark.Thread, fn *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
		var s string
		if err := starlark.UnpackPositionalArgs(fn.Name(), args, kwargs, 1, &s); err != nil {
			return nil, err
		}
		if sess == nil {
			return starlark.String(""), nil
		}
		escaped := strings.ReplaceAll(s, "'", "''")
		val, err := sess.QueryValue(fmt.Sprintf("SELECT md5('%s')", escaped))
		if err != nil {
			return nil, fmt.Errorf("md5: %w", err)
		}
		return starlark.String(val), nil
	})
}

// sha256Builtin hashes a string.
// Usage: h = sha256("some string")
func sha256Builtin(sess *duckdb.Session) *starlark.Builtin {
	return starlark.NewBuiltin("sha256", func(thread *starlark.Thread, fn *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
		var s string
		if err := starlark.UnpackPositionalArgs(fn.Name(), args, kwargs, 1, &s); err != nil {
			return nil, err
		}
		if sess == nil {
			return starlark.String(""), nil
		}
		escaped := strings.ReplaceAll(s, "'", "''")
		val, err := sess.QueryValue(fmt.Sprintf("SELECT sha256('%s')::VARCHAR", escaped))
		if err != nil {
			return nil, fmt.Errorf("sha256: %w", err)
		}
		return starlark.String(val), nil
	})
}

// uuidBuiltin generates a UUIDv4.
// Usage: id = uuid()
func uuidBuiltin(sess *duckdb.Session) *starlark.Builtin {
	return starlark.NewBuiltin("uuid", func(thread *starlark.Thread, fn *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
		if err := starlark.UnpackPositionalArgs(fn.Name(), args, kwargs, 0); err != nil {
			return nil, err
		}
		if sess == nil {
			return starlark.String(""), nil
		}
		val, err := sess.QueryValue("SELECT uuid()::VARCHAR")
		if err != nil {
			return nil, fmt.Errorf("uuid: %w", err)
		}
		return starlark.String(val), nil
	})
}

// lookupBuiltin does a key-value lookup against a DuckLake table.
// Usage: known = lookup(table="raw.invoices", key="source_file", value="source_hash", where=["a.pdf", "b.pdf"])
// Returns a Starlark dict. Missing keys are absent. Empty/missing table returns empty dict.
func lookupBuiltin(sess *duckdb.Session) *starlark.Builtin {
	return starlark.NewBuiltin("lookup", func(thread *starlark.Thread, fn *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
		var table, key, value string
		var whereList *starlark.List
		if err := starlark.UnpackArgs(fn.Name(), args, kwargs,
			"table", &table,
			"key", &key,
			"value", &value,
			"where", &whereList,
		); err != nil {
			return nil, err
		}

		if sess == nil || table == "" {
			return starlark.NewDict(0), nil
		}

		// Build WHERE IN clause
		var quoted []string
		iter := whereList.Iterate()
		defer iter.Done()
		var v starlark.Value
		for iter.Next(&v) {
			s, ok := starlark.AsString(v)
			if !ok {
				continue
			}
			quoted = append(quoted, "'"+strings.ReplaceAll(s, "'", "''")+"'")
		}
		if len(quoted) == 0 {
			return starlark.NewDict(0), nil
		}

		escapedKey := duckdb.QuoteIdentifier(key)
		escapedValue := duckdb.QuoteIdentifier(value)
		// Quote table name — may be "schema.table", split and quote each part
		tableParts := strings.Split(table, ".")
		quotedParts := make([]string, len(tableParts))
		for i, p := range tableParts {
			quotedParts[i] = duckdb.QuoteIdentifier(p)
		}
		escapedTable := strings.Join(quotedParts, ".")
		sql := fmt.Sprintf("SELECT %s, %s FROM %s WHERE %s IN (%s)",
			escapedKey, escapedValue, escapedTable, escapedKey, strings.Join(quoted, ", "))

		rows, err := sess.QueryRowsAny(sql)
		if err != nil {
			// Table doesn't exist on first run — return empty dict
			return starlark.NewDict(0), nil
		}

		result := starlark.NewDict(len(rows))
		for _, row := range rows {
			k, ok1 := row[key]
			val, ok2 := row[value]
			if !ok1 || !ok2 {
				continue
			}
			sk, err := goToStarlark(k)
			if err != nil {
				continue
			}
			sv, err := goToStarlark(val)
			if err != nil {
				continue
			}
			result.SetKey(sk, sv)
		}

		return result, nil
	})
}
