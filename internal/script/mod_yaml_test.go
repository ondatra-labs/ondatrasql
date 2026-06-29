// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package script

import (
	"context"
	"testing"
	"time"
)

func runYAML(t *testing.T, code string) {
	t.Helper()
	rt := NewRuntime(nil, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := rt.Run(ctx, "test", code); err != nil {
		t.Fatal(err)
	}
}

func TestYamlDecodeScalarsAndNesting(t *testing.T) {
	t.Parallel()
	runYAML(t, `
doc = yaml.decode("title: Hello\ncount: 3\nratio: 1.5\nactive: true\nempty: null\n")
if doc["title"] != "Hello":
    fail("title: " + str(doc))
if doc["count"] != 3:
    fail("count: " + str(doc["count"]))
if doc["ratio"] != 1.5:
    fail("ratio: " + str(doc["ratio"]))
if doc["active"] != True:
    fail("active: " + str(doc["active"]))
if doc["empty"] != None:
    fail("empty: " + str(doc["empty"]))
`)
}

// TestYamlDecodeFrontmatter mirrors the shape of a real Markdown frontmatter
// block: nested mapping, a list, and a scalar — exactly what hand-parsing
// chokes on.
func TestYamlDecodeFrontmatter(t *testing.T) {
	t.Parallel()
	runYAML(t, `
fm = yaml.decode("""title: Session note
tags:
  - auth
  - infra
metadata:
  type: project
  weight: 7
""")
if fm["title"] != "Session note":
    fail("title: " + str(fm))
if type(fm["tags"]) != "list" or len(fm["tags"]) != 2 or fm["tags"][0] != "auth":
    fail("tags: " + str(fm["tags"]))
if fm["metadata"]["type"] != "project":
    fail("metadata.type: " + str(fm["metadata"]))
if fm["metadata"]["weight"] != 7:
    fail("metadata.weight: " + str(fm["metadata"]["weight"]))
`)
}

func TestYamlDecodeEmptyIsNone(t *testing.T) {
	t.Parallel()
	runYAML(t, `
if yaml.decode("") != None:
    fail("empty document should decode to None")
`)
}

func TestYamlEncodeRoundTrip(t *testing.T) {
	t.Parallel()
	runYAML(t, `
s = yaml.encode({"title": "X", "tags": ["a", "b"], "n": 2})
back = yaml.decode(s)
if back["title"] != "X" or back["tags"][1] != "b" or back["n"] != 2:
    fail("round-trip failed: " + str(back))
`)
}

func TestYamlModuleAvailable(t *testing.T) {
	t.Parallel()
	runYAML(t, `
if not yaml or not yaml.decode or not yaml.encode:
    fail("yaml module not available")
`)
}

func TestYamlDecodeMalformedErrors(t *testing.T) {
	t.Parallel()
	rt := NewRuntime(nil, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	// Unterminated flow sequence — goccy/go-yaml must surface a clear error,
	// not panic or silently return partial data.
	if _, err := rt.Run(ctx, "test", `yaml.decode("a: [1, 2")`); err == nil {
		t.Fatal("expected error on malformed YAML")
	}
}

func TestYamlDecodeTopLevelScalarAndList(t *testing.T) {
	t.Parallel()
	runYAML(t, `
if yaml.decode("42") != 42:
    fail("top-level scalar")
seq = yaml.decode("- a\n- b\n")
if type(seq) != "list" or seq[1] != "b":
    fail("top-level sequence: " + str(seq))
`)
}

func TestYamlEncodeNonDict(t *testing.T) {
	t.Parallel()
	runYAML(t, `
# encode accepts non-dict values (unlike xml.encode), round-trips through decode
if yaml.decode(yaml.encode([1, 2, 3]))[2] != 3:
    fail("list round-trip")
if yaml.decode(yaml.encode("hello")) != "hello":
    fail("scalar round-trip")
`)
}
