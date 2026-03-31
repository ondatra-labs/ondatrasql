// OndatraSQL - A data pipeline framework for DuckDB + DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package script

import (
	"net/http"
	"testing"

	"go.starlark.net/starlark"
)

func TestHTTPResponseToStarlark(t *testing.T) {
	t.Parallel()
	resp := &HTTPResponse{
		StatusCode: 200,
		Body:       []byte(`{"ok":true}`),
		Headers: http.Header{
			"Content-Type": []string{"application/json"},
			"X-Multi":      []string{"a", "b"},
		},
		JSON:  map[string]interface{}{"ok": true},
		Links: map[string]string{"next": "https://example.com/page2"},
	}

	result := httpResponseToStarlark(resp)

	// status_code
	statusVal, err := result.Attr("status_code")
	if err != nil {
		t.Fatal("status_code missing")
	}
	statusInt, _ := starlark.AsInt32(statusVal)
	if statusInt != 200 {
		t.Errorf("status_code = %v, want 200", statusVal)
	}

	// text
	textVal, err := result.Attr("text")
	if err != nil {
		t.Fatal("text missing")
	}
	text, _ := starlark.AsString(textVal)
	if text != `{"ok":true}` {
		t.Errorf("text = %v", textVal)
	}

	// ok
	okVal, err := result.Attr("ok")
	if err != nil {
		t.Fatal("ok missing")
	}
	if okVal != starlark.True {
		t.Errorf("ok = %v, want True", okVal)
	}

	// JSON
	jsonVal, err := result.Attr("json")
	if err != nil {
		t.Fatal("json missing")
	}
	jsonDict, ok := jsonVal.(*starlark.Dict)
	if !ok {
		t.Fatalf("json is %T, want *starlark.Dict", jsonVal)
	}
	okJsonVal, found, _ := jsonDict.Get(starlark.String("ok"))
	if !found || okJsonVal != starlark.True {
		t.Errorf("json.ok = %v, want true", okJsonVal)
	}

	// Headers
	headersVal, err := result.Attr("headers")
	if err != nil {
		t.Fatal("headers missing")
	}
	headersDict := headersVal.(*starlark.Dict)

	// Single header value
	ct, found, _ := headersDict.Get(starlark.String("Content-Type"))
	if !found {
		t.Fatal("Content-Type not found")
	}
	if s, _ := starlark.AsString(ct); s != "application/json" {
		t.Errorf("Content-Type = %v", ct)
	}

	// Multi value header
	multi, found, _ := headersDict.Get(starlark.String("X-Multi"))
	if !found {
		t.Fatal("X-Multi not found")
	}
	multiList, isList := multi.(*starlark.List)
	if !isList || multiList.Len() != 2 {
		t.Errorf("X-Multi = %v, want list of 2", multi)
	}

	// Links
	links, found, _ := headersDict.Get(starlark.String("_links"))
	if !found {
		t.Fatal("_links missing")
	}
	linksDict := links.(*starlark.Dict)
	next, found, _ := linksDict.Get(starlark.String("next"))
	if !found {
		t.Fatal("_links.next missing")
	}
	if s, _ := starlark.AsString(next); s != "https://example.com/page2" {
		t.Errorf("_links.next = %v", next)
	}
}

func TestHTTPResponseToStarlarkNoJSON(t *testing.T) {
	t.Parallel()
	resp := &HTTPResponse{
		StatusCode: 204,
		Body:       []byte{},
		Headers:    http.Header{},
	}

	result := httpResponseToStarlark(resp)
	jsonVal, err := result.Attr("json")
	if err != nil {
		t.Fatal("json attr should always exist")
	}
	if jsonVal != starlark.None {
		t.Errorf("json should be None for nil JSON, got %s", jsonVal.Type())
	}
}

func TestHTTPResponseNotOk(t *testing.T) {
	t.Parallel()
	resp := &HTTPResponse{
		StatusCode: 404,
		Body:       []byte("not found"),
		Headers:    http.Header{},
	}

	result := httpResponseToStarlark(resp)
	okVal, err := result.Attr("ok")
	if err != nil {
		t.Fatal("ok missing")
	}
	if okVal != starlark.False {
		t.Errorf("ok = %v, want False for 404", okVal)
	}
}
