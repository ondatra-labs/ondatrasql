// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package httpcassette

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"unicode/utf8"
)

// formatVersion is written to every cassette and checked on load.
const formatVersion = 1

// File is one cassette: the interactions of one lib function in one model,
// in the order they happened. Struct field order is the key order on disk,
// and header maps are written with sorted keys, so a re-recording diffs
// line by line.
type File struct {
	Version int    `json:"version"`
	Model   string `json:"model"`
	Lib     string `json:"lib"`
	// Now is ONDATRA_NOW when the cassette was recorded, empty if unset.
	// Replay names it when a request does not match, since a URL built
	// from time.now() only matches under the same frozen clock.
	Now          string        `json:"now,omitempty"`
	Interactions []Interaction `json:"interactions"`
}

// Interaction is one request and what came back: a response, or the
// transport error (connection refused, timeout) the client saw.
type Interaction struct {
	Request  Request   `json:"request"`
	Response *Response `json:"response,omitempty"`
	Error    string    `json:"error,omitempty"`
}

// Request is a recorded request, credentials replaced by the placeholder.
type Request struct {
	Method       string              `json:"method"`
	URL          string              `json:"url"`
	Headers      map[string][]string `json:"headers,omitempty"`
	BodyEncoding string              `json:"body_encoding,omitempty"`
	Body         json.RawMessage     `json:"body,omitempty"`
}

// Response is a recorded response, credentials replaced by the placeholder.
type Response struct {
	Status       int                 `json:"status"`
	Headers      map[string][]string `json:"headers,omitempty"`
	BodyEncoding string              `json:"body_encoding,omitempty"`
	Body         json.RawMessage     `json:"body,omitempty"`
}

// Body encodings. A body that parses as JSON is stored as JSON (and
// replayed compacted), else as a string if it is valid UTF-8, else as
// base64.
const (
	encJSON   = "json"
	encText   = "text"
	encBase64 = "base64"
)

func encodeBody(b []byte) (string, json.RawMessage) {
	if len(b) == 0 {
		return "", nil
	}
	if json.Valid(b) {
		var buf bytes.Buffer
		if json.Compact(&buf, b) == nil {
			return encJSON, buf.Bytes()
		}
	}
	if utf8.Valid(b) {
		return encText, marshalString(string(b))
	}
	return encBase64, marshalString(base64.StdEncoding.EncodeToString(b))
}

func decodeBody(enc string, raw json.RawMessage) ([]byte, error) {
	switch enc {
	case "":
		return nil, nil
	case encJSON:
		var buf bytes.Buffer
		if err := json.Compact(&buf, raw); err != nil {
			return nil, err
		}
		return buf.Bytes(), nil
	case encText, encBase64:
		var s string
		if err := json.Unmarshal(raw, &s); err != nil {
			return nil, err
		}
		if enc == encText {
			return []byte(s), nil
		}
		return base64.StdEncoding.DecodeString(s)
	default:
		return nil, fmt.Errorf("unknown body_encoding %q", enc)
	}
}

// marshalString is json.Marshal for a string without HTML escaping, so
// <, > and & stay readable in the file.
func marshalString(s string) json.RawMessage {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(s) // encoding a string cannot fail
	return bytes.TrimRight(buf.Bytes(), "\n")
}

// marshalFile renders f pretty-printed with two-space indent.
func marshalFile(f *File) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", "  ")
	if err := enc.Encode(f); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func readFile(path string) (*File, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var f File
	if err := json.Unmarshal(data, &f); err != nil {
		return nil, fmt.Errorf("parse cassette %s: %w", path, err)
	}
	if f.Version != formatVersion {
		return nil, fmt.Errorf("cassette %s: version %d, this ondatrasql reads version %d (re-record it)", path, f.Version, formatVersion)
	}
	return &f, nil
}

// writeFile replaces path atomically: a run killed mid-write leaves the
// previous complete cassette, not half a JSON document.
func writeFile(path string, data []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".cassette-*")
	if err != nil {
		return err
	}
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()           // already failing; the write error is the one to report
		_ = os.Remove(tmp.Name()) // best-effort cleanup of the temp file
		return err
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmp.Name()) // best-effort cleanup of the temp file
		return err
	}
	if err := os.Chmod(tmp.Name(), 0o644); err != nil {
		_ = os.Remove(tmp.Name()) // best-effort cleanup of the temp file
		return err
	}
	return os.Rename(tmp.Name(), path)
}
