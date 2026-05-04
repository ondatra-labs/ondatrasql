package main

// good: has schema_version
type ProjectStats struct {
	SchemaVersion int    `json:"schema_version"`
	Name          string `json:"name"`
}

// good: SchemaVersion field name without explicit tag still counts
type GoodInfo struct {
	SchemaVersion int
	Foo           string `json:"foo"`
}

// bad: missing SchemaVersion
type BadResult struct { // want `lacks a SchemaVersion field`
	Name string `json:"name"`
}

// bad: missing SchemaVersion (Description suffix)
type ApiDescription struct { // want `lacks a SchemaVersion field`
	Endpoint string `json:"endpoint"`
}

// out of scope: convention name but no JSON tags
type InternalStats struct {
	Foo string
	Bar int
}

// out of scope: name doesn't match convention
type Helper struct {
	Foo string `json:"foo"`
}

// bypass: explicitly opted out
//
//lintcheck:nojsonversion intentionally unversioned: legacy alias kept
//for backwards compat with v0.30 consumers; new types use the convention.
type LegacyReport struct {
	Foo string `json:"foo"`
}

// bypass without reason: marker is present but the comment has no
// trailing text, so the analyzer treats it as if the marker were
// missing — guards against drive-by silence-the-lint suppressions.
//
//lintcheck:nojsonversion
type BareBypassReport struct { // want `lacks a SchemaVersion field`
	Foo string `json:"foo"`
}

// bypass embedded in prose: marker appears mid-comment after other
// text and must NOT suppress the diagnostic (R6 finding —
// strings.Contains based check let prose-citations silently bypass).
//
// see issue tracker for //lintcheck:nojsonversion guidance
type ProseCitationReport struct { // want `lacks a SchemaVersion field`
	Foo string `json:"foo"`
}
