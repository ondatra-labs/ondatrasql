package libregistry

import (
	"strings"
	"testing"
)

func TestAsyncValidation_MissingSubmitParam(t *testing.T) {
	t.Parallel()
	code := `
API = {
    "base_url": "https://api.example.com",
    "fetch": {
        "args": ["query"],
        "async": True,
    },
}

def submit():
    pass

def check(job_ref):
    pass

def fetch_result(result_ref, page):
    pass
`
	_, err := parseLibFile("async_bad", "lib/async_bad.star", code)
	if err == nil {
		t.Fatal("expected error for submit() missing 'query' param")
	}
	if !strings.Contains(err.Error(), "submit()") && !strings.Contains(err.Error(), "query") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestAsyncValidation_MissingCheckJobRef(t *testing.T) {
	t.Parallel()
	code := `
API = {
    "base_url": "https://api.example.com",
    "fetch": {
        "async": True,
    },
}

def submit():
    pass

def check():
    pass

def fetch_result(result_ref, page):
    pass
`
	_, err := parseLibFile("async_check", "lib/async_check.star", code)
	if err == nil {
		t.Fatal("expected error for check() missing 'job_ref' param")
	}
	if !strings.Contains(err.Error(), "job_ref") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestAsyncValidation_MissingFetchResultPage(t *testing.T) {
	t.Parallel()
	code := `
API = {
    "base_url": "https://api.example.com",
    "fetch": {
        "async": True,
    },
}

def submit():
    pass

def check(job_ref):
    pass

def fetch_result(result_ref):
    pass
`
	_, err := parseLibFile("async_page", "lib/async_page.star", code)
	if err == nil {
		t.Fatal("expected error for fetch_result() missing 'page' param")
	}
	if !strings.Contains(err.Error(), "page") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestAsyncValidation_ValidAsync(t *testing.T) {
	t.Parallel()
	code := `
API = {
    "base_url": "https://api.example.com",
    "fetch": {
        "args": ["query"],
        "async": True,
    },
}

def submit(query="", columns=[], is_backfill=True):
    pass

def check(job_ref):
    pass

def fetch_result(result_ref, page):
    pass
`
	lf, err := parseLibFile("async_ok", "lib/async_ok.star", code)
	if err != nil {
		t.Fatalf("expected valid async blueprint, got: %v", err)
	}
	if lf.FetchMode != "async" {
		t.Errorf("FetchMode = %q, want async", lf.FetchMode)
	}
}
