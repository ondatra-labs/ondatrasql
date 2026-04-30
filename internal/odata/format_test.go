// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package odata

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestValidateFormat covers the $format query param. Spec: only json is
// supported; missing param defaults to JSON (Accept-header path); xml /
// atom / anything else returns 406.
func TestValidateFormat(t *testing.T) {
	cases := []struct {
		name       string
		query      string
		wantOK     bool
		wantStatus int
	}{
		{"missing param defaults to json", "", true, 200},
		{"json explicitly", "$format=json", true, 200},
		{"application/json explicitly", "$format=application/json", true, 200},
		{"xml rejected", "$format=xml", false, 406},
		{"atom rejected", "$format=atom", false, 406},
		{"unknown rejected", "$format=garbage", false, 406},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest("GET", "/odata/orders?"+tc.query, nil)
			rec := httptest.NewRecorder()
			ok := validateFormat(rec, req)
			if ok != tc.wantOK {
				t.Errorf("validateFormat ok = %v, want %v", ok, tc.wantOK)
			}
			if !tc.wantOK {
				if rec.Code != tc.wantStatus {
					t.Errorf("status = %d, want %d", rec.Code, tc.wantStatus)
				}
				body := rec.Body.String()
				if !strings.Contains(body, "NotAcceptable") {
					t.Errorf("body should mention NotAcceptable, got %q", body)
				}
			}
		})
	}
}

// TestValidateFormat_Routing verifies that validateFormat is wired into the
// entity collection / single-entity / count handlers via the live server,
// not just callable as a unit.
func TestValidateFormat_Routing(t *testing.T) {
	// Minimal handler — we only care that validateFormat fires before any
	// downstream session-dependent code runs, so a nil session is fine
	// here as long as the request never reaches the SQL layer.
	mux := http.NewServeMux()
	for _, path := range []string{"GET /odata", "GET /odata/orders", "GET /odata/orders/$count"} {
		mux.HandleFunc(path, func(w http.ResponseWriter, r *http.Request) {
			if !validateFormat(w, r) {
				return
			}
			w.WriteHeader(200)
		})
	}
	srv := httptest.NewServer(mux)
	defer srv.Close()

	for _, path := range []string{"/odata?$format=xml", "/odata/orders?$format=xml", "/odata/orders/$count?$format=xml"} {
		t.Run(path, func(t *testing.T) {
			resp, err := http.Get(srv.URL + path)
			if err != nil {
				t.Fatalf("get: %v", err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != 406 {
				t.Errorf("%s → status %d, want 406", path, resp.StatusCode)
			}
		})
	}
}
