// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

// Package backfill handles SQL hash calculation and backfill detection.
package backfill

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// Hash calculates a SHA256 hash of the SQL query.
// The hash is normalized by removing comments and extra whitespace.
// Used for AST caching where only the code body matters.
func Hash(sql string) string {
	normalized := normalize(sql)
	h := sha256.Sum256([]byte(normalized))
	return hex.EncodeToString(h[:])
}

// ModelDirectives contains directive values that affect execution semantics.
// Changes to these directives should trigger a backfill, so they are included
// in the model hash alongside the SQL/script body.
type ModelDirectives struct {
	Kind               string
	UniqueKey          string
	GroupKey           string // tracked kind: group_key for content-hash dedup
	PartitionedBy      []string
	Incremental        string
	IncrementalInitial string
	Fetch              bool   // @fetch — flips strict-fetch validator + lib-call relationship rule
	Push               string // @push — flips strict-push validator + sink wiring
}

// ModelHash calculates a hash that includes both the code body and semantic
// directives. Changing @kind, @unique_key, @partitioned_by, @incremental,
// @incremental_initial, @fetch or @push triggers a backfill because the hash
// changes — which forces validators to fire on the next run instead of being
// silently bypassed by the skip path.
//
// Config content is deliberately NOT folded in here. It is tracked as its own
// commit field (CommitInfo.ConfigHash) so the run-type query can tell
// "sql changed" apart from "config changed" instead of blaming the model,
// and so a model is only rebuilt for the config it actually uses.
// See BuildConfigIndex.
func ModelHash(sql string, d ModelDirectives) string {
	normalized := normalize(sql)

	// Append directive values in a deterministic format.
	// Only directives that affect how data is written are included.
	var b strings.Builder
	b.WriteString(normalized)
	b.WriteString("\x00kind=")
	b.WriteString(d.Kind)
	b.WriteString("\x00unique_key=")
	b.WriteString(d.UniqueKey)
	b.WriteString("\x00group_key=")
	b.WriteString(d.GroupKey)
	b.WriteString("\x00partitioned_by=")
	b.WriteString(strings.Join(d.PartitionedBy, ","))
	b.WriteString("\x00incremental=")
	b.WriteString(d.Incremental)
	b.WriteString("\x00incremental_initial=")
	b.WriteString(d.IncrementalInitial)
	b.WriteString("\x00fetch=")
	if d.Fetch {
		b.WriteString("1")
	}
	b.WriteString("\x00push=")
	b.WriteString(d.Push)

	h := sha256.Sum256([]byte(b.String()))
	return hex.EncodeToString(h[:])
}

// normalize removes comments and normalizes whitespace for consistent hashing.
// SQL bodies are lowercased so identifier casing doesn't bust the hash.
func normalize(sql string) string {
	return normalizeText(sql, true)
}

// normalizeText strips `--` line comments and `/* ... */` block comments, and
// collapses whitespace runs to a single space. When lower is true, text is
// lowercased as well.
//
// All three transformations apply ONLY outside 'string literals' and "quoted
// identifiers"; quoted regions are copied through byte for byte. That matters
// in both directions:
//
//   - Inside a literal, whitespace and line breaks are data. Collapsing them
//     would make 'a\nb' and 'a b' hash equal.
//   - Inside a literal, case is data. Lowercasing would make 'Active' and
//     'active' hash equal — so a model whose only change is a filter value
//     would not rebuild.
//
// Both are the missed-rebuild direction, which is the failure this package
// exists to prevent.
//
// Config files are normalized with lower=false. Unlike a SQL body, config
// carries case-significant text outside literals too (endpoints, paths,
// environment names), and folding that case would let two genuinely different
// configs hash equal.
func normalizeText(sql string, lower bool) string {
	var out strings.Builder
	out.Grow(len(sql))

	var q quoteState
	pendingSpace := false

	for i := 0; i < len(sql); i++ {
		c := sql[i]

		if q.quoted() {
			out.WriteByte(c)
			switch {
			case c == '\'' && !q.inIdent:
				q.inString = !q.inString
			case c == '"' && !q.inString:
				q.inIdent = !q.inIdent
			}
			continue
		}

		// Outside quotes: a `--` runs to end of line.
		if c == '-' && i+1 < len(sql) && sql[i+1] == '-' {
			for i < len(sql) && sql[i] != '\n' {
				i++
			}
			pendingSpace = true
			continue
		}

		// Outside quotes: a `/* ... */` block runs to its terminator. Leaving
		// these in would do more than keep a comment in the hash — a block
		// comment ahead of a CREATE MACRO stops the statement being recognized
		// at all, demoting it to the global bucket so one comment edit rebuilds
		// every model.
		if c == '/' && i+1 < len(sql) && sql[i+1] == '*' {
			i += 2
			for i+1 < len(sql) && !(sql[i] == '*' && sql[i+1] == '/') {
				i++
			}
			i++ // land on the '/'; the loop's i++ steps past it
			pendingSpace = true
			continue
		}

		if c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\v' || c == '\f' {
			pendingSpace = true
			continue
		}

		if pendingSpace {
			if out.Len() > 0 {
				out.WriteByte(' ')
			}
			pendingSpace = false
		}

		switch {
		case c == '\'':
			q.inString = true
		case c == '"':
			q.inIdent = true
		case lower && c >= 'A' && c <= 'Z':
			c += 'a' - 'A'
		}
		out.WriteByte(c)
	}

	return out.String()
}

// quoteState tracks whether a scan is inside a 'string literal' or a
// "quoted identifier". Both can contain `--`, and both can span lines.
type quoteState struct {
	inString bool // inside '...'
	inIdent  bool // inside "..."
}

func (q quoteState) quoted() bool { return q.inString || q.inIdent }

// hashExcludedConfigFiles lists config files whose content is deliberately
// kept out of the config hash.
//
// Only state.sql qualifies. It configures the operational-state backend (push
// queue, fetch staging, OAuth tokens) and is not part of the model execution
// session at all — state.Open runs it against its own :memory: DuckDB session.
// Nothing in it can change what a model computes.
//
// Everything else stays in. In particular, secrets.sql and catalog.sql are
// NOT excluded, despite sounding like pure plumbing:
//
//   - Credentials do not live in these files. cmd/ondatrasql loads .env into
//     the process environment and loadConfigSQL runs os.ExpandEnv over every
//     config file, so secret *values* are ${VAR} references. Rotating a
//     credential edits .env, which is not hashed either way.
//   - What these files do hold is topology — a secret's ENDPOINT, REGION,
//     SCOPE, HOST, DATABASE, PROVIDER; a catalog's DATA_PATH. Editing any of
//     those changes which bytes a model reads while its SQL stays identical.
//     Skipping the rebuild there would serve stale data silently.
var hashExcludedConfigFiles = map[string]bool{
	"state.sql": true,
}

// configFile is one hashable config file: its path relative to the config
// directory, and its normalized content.
type configFile struct {
	rel  string
	body string
}

// readConfigFiles returns the hashable .sql files under configDir and its
// subdirectories (config/macros/, config/variables/), sorted by path for
// determinism. Files on hashExcludedConfigFiles are skipped.
//
// Contents are normalized (comments and whitespace stripped, case kept) so
// that reformatting or re-commenting config does not rebuild the lake.
//
// Returns (nil, nil) when configDir does not exist or holds no hashable .sql
// files — that is a project with no config, not a failure.
//
// Any other problem is returned as an error rather than degraded into an empty
// file list. A partial read would hash as "no config", which silently turns
// config-change tracking off for every model in the project: exactly the
// failure this package exists to prevent, and invisible when it happens.
func readConfigFiles(configDir string) ([]configFile, error) {
	var paths []string
	if err := filepath.WalkDir(configDir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			// A directory we cannot descend into may hide .sql files; a file
			// we cannot stat may be one. Config directories are small and
			// hand-maintained, so failing loudly beats hashing a subset.
			return fmt.Errorf("walk %s: %w", path, err)
		}
		if !d.IsDir() && strings.HasSuffix(d.Name(), ".sql") {
			rel, relErr := filepath.Rel(configDir, path)
			if relErr != nil {
				return fmt.Errorf("relative path for %s: %w", path, relErr)
			}
			if hashExcludedConfigFiles[rel] {
				return nil
			}
			paths = append(paths, rel)
		}
		return nil
	}); err != nil {
		// A missing config directory is a project without config, not an
		// error. errors.Is, not os.IsNotExist: the walk error is wrapped, and
		// os.IsNotExist does not unwrap.
		if errors.Is(err, fs.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	if len(paths) == 0 {
		return nil, nil
	}
	sort.Strings(paths)

	files := make([]configFile, 0, len(paths))
	for _, rel := range paths {
		content, err := os.ReadFile(filepath.Join(configDir, rel))
		if err != nil {
			return nil, fmt.Errorf("read config/%s: %w", rel, err)
		}
		files = append(files, configFile{rel: rel, body: normalizeText(string(content), false)})
	}
	return files, nil
}
