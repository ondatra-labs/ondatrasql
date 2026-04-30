// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package odata

import (
	"testing"
)

// TestStripBaseURL covers all forms the $entity handler may receive in $id:
// absolute matching baseURL, absolute with a foreign URL, and relative
// shorthand (path-only). Foreign URLs must return "" so the handler can
// refuse to dereference URLs we didn't issue.
func TestStripBaseURL(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		idURL   string
		baseURL string
		want    string
	}{
		{
			name:    "absolute matching base",
			idURL:   "https://api.example.com/odata/Orders(123)",
			baseURL: "https://api.example.com",
			want:    "Orders(123)",
		},
		{
			name:    "absolute base with trailing slash",
			idURL:   "https://api.example.com/odata/Orders(123)",
			baseURL: "https://api.example.com/",
			want:    "Orders(123)",
		},
		{
			name:    "absolute foreign URL refused",
			idURL:   "https://other.host/odata/Orders(123)",
			baseURL: "https://api.example.com",
			want:    "",
		},
		{
			name:    "relative path with leading slash",
			idURL:   "/odata/Orders(123)",
			baseURL: "https://api.example.com",
			want:    "Orders(123)",
		},
		{
			name:    "relative path without leading slash",
			idURL:   "Orders(123)",
			baseURL: "https://api.example.com",
			want:    "Orders(123)",
		},
		{
			name:    "string-keyed entity preserved",
			idURL:   "https://api.example.com/odata/Products('sku-1')",
			baseURL: "https://api.example.com",
			want:    "Products('sku-1')",
		},
		{
			name:    "OData-escaped quote preserved through strip",
			idURL:   "https://api.example.com/odata/Products('O''Brien')",
			baseURL: "https://api.example.com",
			want:    "Products('O''Brien')",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := stripBaseURL(tc.idURL, tc.baseURL)
			if got != tc.want {
				t.Errorf("stripBaseURL(%q, %q) = %q, want %q", tc.idURL, tc.baseURL, got, tc.want)
			}
		})
	}
}
