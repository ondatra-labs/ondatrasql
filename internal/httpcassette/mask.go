// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package httpcassette

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"strings"
)

// MaskPrefix starts every string maskJSON writes in place of a value.
const MaskPrefix = "masked-"

// MaskString is the placeholder for a masked string: MaskPrefix and the
// first 8 hex digits of its SHA-256. Equal values get equal placeholders,
// so a masked value that joins two responses (an id, a user name used as
// a key) still joins on replay.
func MaskString(s string) string {
	sum := sha256.Sum256([]byte(s))
	return MaskPrefix + hex.EncodeToString(sum[:4])
}

// maskJSON rewrites a JSON document with the value of every key in mask
// (lower case; matched at any depth, without regard to case) replaced:
// a string by MaskString, a number by 0, a boolean by false; null stays
// null. An object or array under a masked key keeps its shape, with each
// value in it replaced the same way. Everything else is copied as
// copyJSON copies it.
func maskJSON(b []byte, mask map[string]bool) ([]byte, error) {
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.UseNumber()
	var out bytes.Buffer
	if err := copyMasked(dec, &out, mask, false); err != nil {
		return nil, err
	}
	if _, err := dec.Token(); err != io.EOF {
		return nil, fmt.Errorf("trailing data after JSON value")
	}
	return out.Bytes(), nil
}

// copyMasked copies one JSON value from dec to out; masking says the
// value is under a masked key.
func copyMasked(dec *json.Decoder, out *bytes.Buffer, mask map[string]bool, masking bool) error {
	tok, err := dec.Token()
	if err != nil {
		return err
	}
	switch t := tok.(type) {
	case json.Delim:
		switch t {
		case '{':
			out.WriteByte('{')
			first := true
			for dec.More() {
				kt, err := dec.Token()
				if err != nil {
					return err
				}
				k, _ := kt.(string)
				if !first {
					out.WriteByte(',')
				}
				first = false
				out.Write(marshalString(k))
				out.WriteByte(':')
				if err := copyMasked(dec, out, mask, masking || mask[strings.ToLower(k)]); err != nil {
					return err
				}
			}
			if _, err := dec.Token(); err != nil {
				return err
			}
			out.WriteByte('}')
		case '[':
			out.WriteByte('[')
			first := true
			for dec.More() {
				if !first {
					out.WriteByte(',')
				}
				first = false
				if err := copyMasked(dec, out, mask, masking); err != nil {
					return err
				}
			}
			if _, err := dec.Token(); err != nil {
				return err
			}
			out.WriteByte(']')
		}
	case string:
		if masking {
			t = MaskString(t)
		}
		out.Write(marshalString(t))
	case json.Number:
		if masking {
			out.WriteByte('0')
		} else {
			out.WriteString(t.String())
		}
	case bool:
		if t && !masking {
			out.WriteString("true")
		} else {
			out.WriteString("false")
		}
	case nil:
		out.WriteString("null")
	}
	return nil
}
