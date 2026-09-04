// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package backfill

import (
	"crypto/sha256"
	"encoding/hex"
	"path/filepath"
	"sort"
	"strings"
)

// ConfigIndex decomposes a project's config/ directory into individually
// addressable statements so a model can be hashed against only the config it
// actually uses.
//
// Without this, one hash covered every config file and every model shared it:
// editing a single macro — or bumping a thread count — rebuilt the whole lake.
//
// Every statement in config/ lands in one of three buckets:
//
//   - fragment — a named definition a model can reference: CREATE MACRO /
//     SET VARIABLE (in config/macros/** and config/variables/**), and
//     ATTACH ... AS <alias> / CREATE VIEW <name> (in config/sources.sql).
//     A model picks it up when the name appears as an identifier anywhere in
//     the model's reference text.
//   - inert — provably cannot change what any query returns: a SET or RESET of
//     a setting on inertDuckDBSettings, and CREATE SCHEMA. Contributes nothing
//     to any hash.
//   - global — everything else. Session-wide config can change any model's
//     result, so this bucket applies to every model. This is the default: a
//     statement is only scoped or dropped when it is positively recognized.
//
// Classification is per statement, not per file, so one unrecognized statement
// in a macro file sends only itself to the global bucket instead of dragging
// the file's macros along with it.
//
// Two files decompose no further than "global", by design:
//
//   - catalog.sql — its ATTACH carries SNAPSHOT_TIME / SNAPSHOT_VERSION (time
//     travel for the whole lake), DATA_PATH and METADATA_PATH. Treating its
//     alias as a fragment would scope it to models that spell the catalog
//     name, and models address targets as schema.table under USE.
//   - secrets.sql — DuckDB secrets bind to a query by SCOPE, a path prefix,
//     never by name. A model never mentions the secret it uses, so there is
//     nothing to match on.
//
// Name matching is deliberately lexical and over-inclusive: a macro named
// `total` matches a column named `total` and pulls the fragment in. Extra
// dependencies cost a needless rebuild; missing ones serve stale data, so the
// scan errs toward the former. The known limit is a Starlark model that
// assembles a macro name at runtime through string concatenation — the name
// never appears literally in the script, so the reference is not seen.
type ConfigIndex struct {
	global    string            // hash of the always-applies bucket ("" if empty)
	fragments map[string]string // lowercased name -> hash of its definition(s)
	refs      map[string][]string
}

// BuildConfigIndex reads configDir and returns an index of its statements.
//
// Returns (nil, nil) when there is nothing hashable — a missing directory, no
// .sql files, or only files on the exclusion list (see
// hashExcludedConfigFiles). A nil index hashes every model to "", i.e. config
// never forces a rebuild, which is correct for a project without config.
//
// Returns an error if config/ exists but cannot be read. Callers must not
// treat that as "no config": doing so silently disables config-change
// tracking for every model.
func BuildConfigIndex(configDir string) (*ConfigIndex, error) {
	files, err := readConfigFiles(configDir)
	if err != nil {
		return nil, err
	}
	if len(files) == 0 {
		return nil, nil
	}

	var globalBuf strings.Builder
	addGlobal := func(rel, body string) {
		globalBuf.WriteString(rel)
		globalBuf.WriteByte(0)
		globalBuf.WriteString(body)
		globalBuf.WriteByte(0)
	}

	// Split once, and note whether config widens how unqualified names resolve.
	type parsedFile struct {
		rel   string
		stmts []string
	}
	parsed := make([]parsedFile, 0, len(files))
	scopeSources := true
	for _, f := range files {
		sts := splitStatements(f.body)
		parsed = append(parsed, parsedFile{rel: f.rel, stmts: sts})
		for _, stmt := range sts {
			if widensUnqualifiedNames(stmt) {
				scopeSources = false
			}
		}
	}

	// Statement text per fragment name, accumulated in sorted-path order.
	stmts := make(map[string][]string)
	for _, f := range parsed {
		for _, stmt := range f.stmts {
			switch class, name := classifyStatement(f.rel, stmt, scopeSources); class {
			case stmtInert:
				// Contributes to nothing.
			case stmtFragment:
				// Prefix the file: a definition moved to another file may
				// stop being loaded at all (the runtime reads config/macros/
				// but not config/macros.sql). Hashing the statement alone
				// would call that "unchanged" and let callers skip while the
				// macro has vanished from the session.
				stmts[name] = append(stmts[name], f.rel+"\x00"+stmt)
			default:
				addGlobal(f.rel, stmt)
			}
		}
	}

	ix := &ConfigIndex{
		fragments: make(map[string]string, len(stmts)),
		refs:      make(map[string][]string, len(stmts)),
	}
	if globalBuf.Len() > 0 {
		ix.global = hashString(globalBuf.String())
	}
	for name, sts := range stmts {
		ix.fragments[name] = hashString(strings.Join(sts, "\x00"))
	}
	// A macro may call another macro. Resolve those edges now so HashFor only
	// has to walk them. Scan the statement bodies only — the file prefixes are
	// there for hashing, and feeding them to the identifier scan would make
	// path segments look like macro references.
	for name, sts := range stmts {
		ix.refs[name] = ix.directRefs(strings.Join(stripFilePrefixes(sts), " "), name)
	}

	if ix.global == "" && len(ix.fragments) == 0 {
		return nil, nil
	}
	return ix, nil
}

// HashFor returns the config hash that applies to a model whose reference text
// is refText — the model body plus every directive value that can name a
// config macro. Returns "" when no config applies, matching the "no config"
// sentinel that execute/batch_run_type.sql compares against.
func (ix *ConfigIndex) HashFor(refText string) string {
	if ix == nil {
		return ""
	}

	// Transitive closure over macro-calls-macro edges.
	seen := make(map[string]bool)
	queue := ix.directRefs(refText, "")
	for len(queue) > 0 {
		name := queue[len(queue)-1]
		queue = queue[:len(queue)-1]
		if seen[name] {
			continue
		}
		seen[name] = true
		queue = append(queue, ix.refs[name]...)
	}

	if ix.global == "" && len(seen) == 0 {
		return ""
	}

	names := make([]string, 0, len(seen))
	for name := range seen {
		names = append(names, name)
	}
	sort.Strings(names)

	h := sha256.New()
	h.Write([]byte("global\x00" + ix.global + "\x00"))
	for _, name := range names {
		h.Write([]byte(name))
		h.Write([]byte{0})
		h.Write([]byte(ix.fragments[name]))
		h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil))
}

// directRefs returns the fragment names appearing as identifiers in text,
// excluding self (a macro definition always contains its own name).
func (ix *ConfigIndex) directRefs(text, self string) []string {
	var found []string
	seen := make(map[string]bool)
	for _, word := range identifiers(text) {
		if word == self || seen[word] {
			continue
		}
		if _, ok := ix.fragments[word]; ok {
			seen[word] = true
			found = append(found, word)
		}
	}
	return found
}

// stmtClass says how a config statement participates in model hashes.
type stmtClass int

const (
	// stmtGlobal applies to every model. The default for anything not
	// positively recognized.
	stmtGlobal stmtClass = iota
	// stmtInert cannot change any query result and is dropped entirely.
	stmtInert
	// stmtFragment is a named definition, scoped to models referencing it.
	stmtFragment
)

// classifyStatement decides which bucket one config statement belongs to,
// given the config-relative path of the file it came from.
//
// scopeSources is false when config widens how unqualified names resolve (see
// widensUnqualifiedNames), in which case ATTACH aliases and view names cannot
// be scoped by name and fall through to the global bucket.
func classifyStatement(rel, stmt string, scopeSources bool) (stmtClass, string) {
	if name := definedName(stmt); name != "" && definesInFile(rel) {
		return stmtFragment, name
	}
	if scopeSources {
		if name := sourceName(stmt); name != "" && isSourcesFile(rel) {
			return stmtFragment, name
		}
	}
	if isInertStatement(stmt) {
		return stmtInert, ""
	}
	return stmtGlobal, ""
}

// definesInFile reports whether macro and variable definitions in this file
// may be scoped per model. Restricted to the directories the runtime actually
// loads macros and variables from, so a stray CREATE MACRO elsewhere stays
// global rather than being quietly scoped.
func definesInFile(rel string) bool {
	rel = filepath.ToSlash(rel)
	if rel == "macros.sql" || rel == "variables.sql" {
		return true
	}
	return strings.HasPrefix(rel, "macros/") || strings.HasPrefix(rel, "variables/")
}

// isSourcesFile reports whether ATTACH aliases and view names in this file may
// be scoped per model. Only config/sources.sql qualifies — catalog.sql's
// ATTACH is the lake itself and must stay global.
func isSourcesFile(rel string) bool {
	return filepath.ToSlash(rel) == "sources.sql"
}

// widensUnqualifiedNames reports whether a statement makes unqualified names in
// model SQL resolve somewhere the model never spells:
//
//	USE <catalog|schema>
//	SET|RESET [GLOBAL|SESSION|LOCAL] search_path | schema
//
// Scoping a source by its alias assumes a model that depends on it names it.
// With a search path pointing into an attached catalog, `SELECT * FROM orders`
// reads `warehouse.orders` without the word "warehouse" appearing anywhere, so
// repointing that ATTACH would skip exactly the models it affects. The same
// goes for a view reachable unqualified through its schema.
//
// That the search-path statement is itself global does not help: what breaks
// the scoping is its presence, not a change to it.
func widensUnqualifiedNames(stmt string) bool {
	words := strings.Fields(stmt)
	if len(words) == 0 {
		return false
	}
	switch strings.ToUpper(words[0]) {
	case "USE":
		return true
	case "SET", "RESET":
		i := 1
		if i < len(words) {
			switch strings.ToUpper(words[i]) {
			case "GLOBAL", "SESSION", "LOCAL":
				i++
			}
		}
		if i >= len(words) {
			return false
		}
		switch cleanIdentifier(words[i]) {
		case "search_path", "schema":
			return true
		}
	}
	return false
}

// isInertStatement reports whether a statement provably cannot change what any
// query returns:
//
//	SET|RESET [GLOBAL|SESSION|LOCAL] <setting>   for settings on the allowlist
//	CREATE SCHEMA [IF NOT EXISTS] <name>         a namespace, not data
func isInertStatement(stmt string) bool {
	words := strings.Fields(stmt)
	if len(words) < 2 {
		return false
	}
	switch strings.ToUpper(words[0]) {
	case "SET", "RESET":
		i := 1
		switch strings.ToUpper(words[i]) {
		case "GLOBAL", "SESSION", "LOCAL":
			i++
		case "VARIABLE":
			// A user variable, not a DuckDB setting. Never inert.
			return false
		}
		if i >= len(words) {
			return false
		}
		return inertDuckDBSettings[cleanIdentifier(words[i])]
	case "CREATE":
		i := 1
		if strings.ToUpper(words[i]) == "SCHEMA" {
			return true
		}
		return false
	}
	return false
}

// sourceName returns the lowercased name that a sources.sql statement defines,
// or "" if it defines nothing referenceable:
//
//	ATTACH '...' AS <alias> (...)
//	CREATE [OR REPLACE] [TEMP|TEMPORARY] VIEW [IF NOT EXISTS] <name> AS ...
func sourceName(stmt string) string {
	words := strings.Fields(stmt)
	if len(words) < 3 {
		return ""
	}

	switch strings.ToUpper(words[0]) {
	case "ATTACH":
		// Look for AS only after the quoted connection string. That string can
		// contain whitespace and even a bare AS token, and scanning the whole
		// statement would then name the fragment after a path segment — so a
		// model using the real alias would not match it and would skip a
		// genuine change.
		rest := afterQuotedPath(stmt)
		if rest == "" {
			return ""
		}
		tail := strings.Fields(rest)
		for i := 0; i+1 < len(tail); i++ {
			if strings.ToUpper(tail[i]) == "AS" {
				return cleanIdentifier(tail[i+1])
			}
		}
		return ""
	case "CREATE":
		i := 1
		if strings.ToUpper(words[i]) == "OR" && i+1 < len(words) && strings.ToUpper(words[i+1]) == "REPLACE" {
			i += 2
		}
		if i < len(words) {
			switch strings.ToUpper(words[i]) {
			case "TEMP", "TEMPORARY":
				i++
			}
		}
		if i >= len(words) || strings.ToUpper(words[i]) != "VIEW" {
			return ""
		}
		i++
		if i+2 < len(words) && strings.ToUpper(words[i]) == "IF" &&
			strings.ToUpper(words[i+1]) == "NOT" && strings.ToUpper(words[i+2]) == "EXISTS" {
			i += 3
		}
		if i >= len(words) {
			return ""
		}
		return cleanIdentifier(words[i])
	}
	return ""
}

// splitStatements splits on `;` that sit outside 'string literals' and
// "quoted identifiers". Empty statements are dropped. Input is expected to be
// normalized already, so `--` comments are gone.
//
// Both quote kinds are honoured: a `;` inside a quoted identifier would
// otherwise split one statement into fragments that classify as global,
// silently demoting a scopable source or view to a whole-project rebuild.
func splitStatements(body string) []string {
	var out []string
	var q quoteState
	start := 0
	for i := 0; i < len(body); i++ {
		switch {
		case body[i] == '\'' && !q.inIdent:
			// A doubled '' escape toggles twice and nets out correctly.
			q.inString = !q.inString
		case body[i] == '"' && !q.inString:
			q.inIdent = !q.inIdent
		case body[i] == ';' && !q.quoted():
			if stmt := strings.TrimSpace(body[start:i]); stmt != "" {
				out = append(out, stmt)
			}
			start = i + 1
		}
	}
	if stmt := strings.TrimSpace(body[start:]); stmt != "" {
		out = append(out, stmt)
	}
	return out
}

// definedName returns the lowercased name a statement defines, or "" if the
// statement isn't a recognized macro or variable definition.
//
// Recognized:
//
//	CREATE [OR REPLACE] [TEMP|TEMPORARY] MACRO|FUNCTION <name> ( ...
//	SET VARIABLE <name> = ...
func definedName(stmt string) string {
	words := strings.Fields(stmt)
	if len(words) < 3 {
		return ""
	}

	switch strings.ToUpper(words[0]) {
	case "CREATE":
		i := 1
		if strings.ToUpper(words[i]) == "OR" && i+1 < len(words) && strings.ToUpper(words[i+1]) == "REPLACE" {
			i += 2
		}
		if i < len(words) {
			switch strings.ToUpper(words[i]) {
			case "TEMP", "TEMPORARY":
				i++
			}
		}
		if i+1 >= len(words) {
			return ""
		}
		switch strings.ToUpper(words[i]) {
		case "MACRO", "FUNCTION":
			return cleanIdentifier(words[i+1])
		}
	case "SET":
		if strings.ToUpper(words[1]) == "VARIABLE" {
			return cleanIdentifier(words[2])
		}
	}
	return ""
}

// afterQuotedPath returns the part of a statement following its first
// single-quoted literal, honouring ” escapes. Returns "" when there is no
// terminated literal.
func afterQuotedPath(stmt string) string {
	i := strings.IndexByte(stmt, '\'')
	if i < 0 {
		return ""
	}
	for j := i + 1; j < len(stmt); j++ {
		if stmt[j] != '\'' {
			continue
		}
		if j+1 < len(stmt) && stmt[j+1] == '\'' {
			j++ // '' escape: consume both and keep scanning
			continue
		}
		return stmt[j+1:]
	}
	return ""
}

// cleanIdentifier trims the argument list, assignment and quoting off a name
// token, then returns the last dotted segment lowercased. Returns "" if what's
// left isn't a plain identifier.
func cleanIdentifier(tok string) string {
	if i := strings.IndexAny(tok, "(="); i >= 0 {
		tok = tok[:i]
	}
	if i := strings.LastIndex(tok, "."); i >= 0 {
		tok = tok[i+1:]
	}
	tok = strings.Trim(tok, `"`)
	tok = strings.ToLower(strings.TrimSpace(tok))
	if tok == "" || !isIdentStart(tok[0]) {
		return ""
	}
	for i := 1; i < len(tok); i++ {
		if !isIdentPart(tok[i]) {
			return ""
		}
	}
	return tok
}

// identifiers extracts every identifier-shaped word from text, lowercased.
// Quoting is ignored on purpose: a variable name inside getvariable('x') must
// be found just like a macro name in a call position.
func identifiers(text string) []string {
	var out []string
	for i := 0; i < len(text); {
		if !isIdentStart(text[i]) {
			i++
			continue
		}
		j := i + 1
		for j < len(text) && isIdentPart(text[j]) {
			j++
		}
		out = append(out, strings.ToLower(text[i:j]))
		i = j
	}
	return out
}

func isIdentStart(c byte) bool {
	return c == '_' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
}

func isIdentPart(c byte) bool {
	return isIdentStart(c) || (c >= '0' && c <= '9')
}

// stripFilePrefixes removes the "<rel>\x00" prefix that BuildConfigIndex adds
// to each fragment statement for hashing.
func stripFilePrefixes(stmts []string) []string {
	out := make([]string, 0, len(stmts))
	for _, stmt := range stmts {
		if i := strings.IndexByte(stmt, 0); i >= 0 {
			stmt = stmt[i+1:]
		}
		out = append(out, stmt)
	}
	return out
}

func hashString(s string) string {
	h := sha256.Sum256([]byte(s))
	return hex.EncodeToString(h[:])
}
