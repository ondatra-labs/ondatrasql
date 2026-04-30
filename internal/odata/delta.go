// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package odata

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"

	duckdb "github.com/ondatra-labs/ondatrasql/internal/duckdb"
)

// Query options that prevent the runtime from emitting an @odata.deltaLink
// at all, and that are rejected on incoming $deltatoken requests. The
// limitation is documented: a deltaLink is meaningful only against the
// unfiltered table; once $filter / $apply / etc. enter the picture, the
// "what changed for me" answer is not derivable from `table_changes()`
// alone, and a future release will need an explicit design.
var deltaIncompatibleParams = []string{"$filter", "$apply", "$compute", "$search", "$expand"}

// hasDeltaIncompatibleParams reports whether any of the parameters that
// prevent delta tracking are set in the request.
func hasDeltaIncompatibleParams(params url.Values) bool {
	for _, p := range deltaIncompatibleParams {
		if params.Get(p) != "" {
			return true
		}
	}
	return false
}

// emitDeltaLink builds the URL the server appends to a normal collection
// response so the client can come back later and request only what
// changed since this snapshot.
//
// Returns "" (and the caller skips emitting @odata.deltaLink) if the
// query has incompatible options, the entity has no key column, or the
// signing key is missing — i.e. whenever delta tracking is unavailable
// for this response.
func emitDeltaLink(baseURL string, entity EntitySchema, params url.Values, snapshot int64, key []byte) string {
	if len(key) == 0 {
		return ""
	}
	if entity.KeyColumn == "" {
		return ""
	}
	if hasDeltaIncompatibleParams(params) {
		return ""
	}
	tok := DeltaToken{
		Snapshot:   snapshot,
		Entity:     entity.ODataName,
		FilterHash: filterHash(params),
	}
	encoded, err := encodeDeltaToken(tok, key)
	if err != nil {
		return ""
	}
	// The link must round-trip through filterHash on the next call. $select
	// and $orderby are part of the hashed set, so they have to ride along
	// in the URL — otherwise the follow-up request hashes empty and is
	// rejected as DeltaLinkFilterChanged. The other hashed options ($filter,
	// $apply, $compute, $search) are excluded by hasDeltaIncompatibleParams,
	// so they cannot appear here.
	q := url.Values{}
	q.Set("$deltatoken", encoded)
	if v := params.Get("$select"); v != "" {
		q.Set("$select", v)
	}
	if v := params.Get("$orderby"); v != "" {
		q.Set("$orderby", v)
	}
	return fmt.Sprintf("%s/odata/%s?%s",
		strings.TrimSuffix(baseURL, "/"), entity.ODataName, q.Encode())
}

// handleDelta serves a `?$deltatoken=...` request. It verifies the token,
// resolves the snapshot range against the live catalog, runs
// `table_changes()` against the underlying table, and returns the OData
// delta-format response.
//
// Errors classification:
//   - 410 Gone for invalid/tampered tokens, expired snapshots, or filter
//     mismatches — the client should re-issue the original query and start
//     a new delta chain
//   - 400 Bad Request for query options that aren't supported in delta
//     responses ($filter etc.)
//   - 500 Internal Server Error for unexpected DuckDB / runtime failures
func handleDelta(
	w http.ResponseWriter,
	r *http.Request,
	sess *duckdb.Session,
	entity EntitySchema,
	tokenStr string,
	deltaKey []byte,
	baseURL string,
	deltaMu *sync.Mutex,
) {
	// 1. Decode + verify HMAC
	tok, err := decodeDeltaToken(tokenStr, deltaKey)
	if err != nil {
		writeError(w, 410, "DeltaLinkInvalid", err.Error())
		return
	}
	// Token must be for this entity. Catches e.g. clients that copy a
	// deltaLink between sessions and hit the wrong route.
	if tok.Entity != entity.ODataName {
		writeError(w, 410, "DeltaLinkInvalid",
			fmt.Sprintf("token was issued for entity %q, not %q", tok.Entity, entity.ODataName))
		return
	}
	// Filter set must match what the link was issued with.
	params := r.URL.Query()
	if filterHash(params) != tok.FilterHash {
		writeError(w, 410, "DeltaLinkFilterChanged",
			"query options have changed since this delta link was issued; re-issue the original query and use the new delta link")
		return
	}
	// $filter etc. on the delta call itself are also rejected — we can't
	// honor them against table_changes() in a way that's consistent with
	// the original query.
	if hasDeltaIncompatibleParams(params) {
		writeError(w, 400, "BadRequest",
			"$filter, $apply, $compute, $search, and $expand are not supported on delta requests in this release")
		return
	}

	// 2. Resolve current snapshot.
	if err := sess.RefreshSnapshot(); err != nil {
		writeError(w, 500, "InternalError", fmt.Sprintf("refresh snapshot: %v", err))
		return
	}
	current, err := sess.GetCurrentSnapshot()
	if err != nil {
		writeError(w, 500, "InternalError", fmt.Sprintf("read snapshot: %v", err))
		return
	}

	// 3. If nothing has happened since the token was issued, return an
	// empty delta with a refreshed link pointing at the same snapshot.
	// This is the common case for clients polling for changes — they
	// just get an empty `value` and a fresh link.
	if current <= tok.Snapshot {
		writeDeltaResponse(w, baseURL, entity, nil, deltaKey, current, params)
		return
	}

	// 4. Run table_changes() against the entity's underlying table.
	//
	// table_changes() takes a bare table name and resolves it through
	// search_path; we set search_path to the entity's schema for the
	// duration of the call and reset afterwards. The pattern matches
	// what the sink delta path does in execute/sink_delta.go.
	//
	// The mutex serialises the SET+SELECT+RESET sequence across
	// concurrent OData requests sharing this server's session — without
	// it, two delta handlers could race and execute their SELECT against
	// the wrong search_path (cross-schema data leak in the worst case).
	// table_changes() does not accept a qualified `schema.table` name
	// (verified empirically), so dropping the SET isn't an option.
	deltaMu.Lock()
	changes, err := runTableChanges(sess, entity, tok.Snapshot+1, current)
	deltaMu.Unlock()
	if err != nil {
		writeError(w, 500, "InternalError", fmt.Sprintf("table_changes: %v", err))
		return
	}

	// 5. Format response with @removed for deletes.
	writeDeltaResponse(w, baseURL, entity, changes, deltaKey, current, params)
}

// runTableChanges queries DuckLake's table_changes() function for the
// entity's table between the given snapshot range, returning rows with
// their full column set plus a `change_type` column. update_preimage rows
// are filtered out at the SQL level — the client only cares about
// post-state for inserts/updates and the key columns for deletes.
func runTableChanges(sess *duckdb.Session, entity EntitySchema, fromSnap, toSnap int64) ([]map[string]any, error) {
	// Set search_path so table_changes() can resolve the bare table name.
	// Catalog alias must come first so DuckLake's table_changes() can find
	// the table; the rest of the path preserves the default behavior for
	// any nested macro calls.
	originalPath := sess.DefaultSearchPath()
	if err := sess.Exec(fmt.Sprintf("SET search_path = '%s.%s,%s'",
		escapeSQL(sess.CatalogAlias()), escapeSQL(entity.Schema), escapeSQL(originalPath))); err != nil {
		return nil, fmt.Errorf("set search_path: %w", err)
	}
	defer func() {
		// Best-effort reset; a failure here only affects subsequent queries
		// on this session, which is acceptable in the request-scoped
		// HTTP handler context.
		_ = sess.Exec(fmt.Sprintf("SET search_path = '%s'", escapeSQL(originalPath)))
	}()

	sql := fmt.Sprintf(
		"SELECT * FROM table_changes('%s', %d, %d) WHERE change_type IN ('insert', 'update_postimage', 'delete')",
		escapeSQL(entity.Table), fromSnap, toSnap)
	return sess.QueryRowsAny(sql)
}

// writeDeltaResponse formats an OData delta response. Inserts/updates
// become normal entity rows; deletes become `{"@removed": ..., "@odata.id": ...}`
// entries. A fresh @odata.deltaLink is appended so the client can keep
// polling.
//
// changes may be nil/empty — that produces a valid empty delta with just
// context + deltaLink, which is the response for "nothing has changed".
func writeDeltaResponse(
	w http.ResponseWriter,
	baseURL string,
	entity EntitySchema,
	changes []map[string]any,
	deltaKey []byte,
	currentSnapshot int64,
	params url.Values,
) {
	colTypes := make(map[string]string, len(entity.Columns))
	for _, c := range entity.Columns {
		colTypes[c.Name] = strings.ToUpper(c.Type)
	}

	// $select rides along in the deltaLink URL and is bound by filterHash
	// in the token, so by the time we get here it's the same projection
	// that produced the original collection response. Apply it to the
	// delta rows so clients that asked for {id,name} get {id,name},
	// not {id,name,qty}. nil set means "no projection".
	var selectSet map[string]bool
	if sel := params.Get("$select"); sel != "" {
		selectSet = make(map[string]bool)
		for _, c := range strings.Split(sel, ",") {
			if c = strings.TrimSpace(c); c != "" {
				selectSet[c] = true
			}
		}
	}

	value := make([]map[string]any, 0, len(changes))
	for _, row := range changes {
		ct, _ := row["change_type"].(string)
		// Strip metadata columns and apply $select. The key column for
		// tombstones is added explicitly below (deletes always need an
		// identifier), so we don't preserve it here on its own account.
		// @odata.id is built from the full row, not from `clean`, so it
		// works even if $select excludes the key.
		clean := make(map[string]any, len(row))
		for k, v := range row {
			if k == "change_type" || k == "snapshot_id" {
				continue
			}
			if selectSet != nil && !selectSet[k] {
				continue
			}
			clean[k] = toODataValue(v, colTypes[k])
		}

		switch ct {
		case "insert", "update_postimage":
			if id := formatODataID(baseURL, entity.ODataName, entity, row); id != "" {
				clean["@odata.id"] = id
			}
			value = append(value, clean)
		case "delete":
			// For deletes, OData wants @removed. Carry the @odata.id so
			// the client can match against its local cache; carry the key
			// column so clients without @odata.id matching can use it.
			tomb := map[string]any{
				"@removed": map[string]string{"reason": "deleted"},
			}
			if id := formatODataID(baseURL, entity.ODataName, entity, row); id != "" {
				tomb["@odata.id"] = id
			}
			if entity.KeyColumn != "" {
				if v, ok := row[entity.KeyColumn]; ok {
					tomb[entity.KeyColumn] = toODataValue(v, colTypes[entity.KeyColumn])
				}
			}
			value = append(value, tomb)
		}
	}

	resp := map[string]any{
		"@odata.context": fmt.Sprintf("%s/odata/$metadata#%s/$delta",
			strings.TrimSuffix(baseURL, "/"), entity.ODataName),
		"value": value,
	}
	// Append fresh deltaLink so the client can keep polling. Use the
	// CURRENT snapshot — next call will report changes since this point.
	if link := emitDeltaLink(baseURL, entity, params, currentSnapshot, deltaKey); link != "" {
		resp["@odata.deltaLink"] = link
	}

	data, err := json.Marshal(resp)
	if err != nil {
		writeError(w, 500, "InternalError", fmt.Sprintf("marshal delta response: %v", err))
		return
	}
	w.Header().Set("Content-Type", "application/json;odata.metadata=minimal")
	w.Write(data)
}

// loadDeltaKey returns the HMAC signing key for delta links.
//   - If ONDATRA_ODATA_DELTA_KEY is set in env, parse it (hex-encoded).
//   - Otherwise generate a per-process random key and log that
//     deltaLinks won't survive restart.
//
// The first form is the production setup; the second is acceptable for
// development and single-shot test runs.
func loadDeltaKey(envValue string, log func(format string, args ...any)) ([]byte, error) {
	if envValue != "" {
		key, err := parseDeltaKey(envValue)
		if err != nil {
			return nil, fmt.Errorf("ONDATRA_ODATA_DELTA_KEY: %w", err)
		}
		return key, nil
	}
	hex, err := generateDeltaKey()
	if err != nil {
		return nil, err
	}
	if log != nil {
		log("OData: no ONDATRA_ODATA_DELTA_KEY set; generated ephemeral delta-link key (existing @odata.deltaLink URLs will be invalidated on next restart)")
	}
	key, _ := parseDeltaKey(hex)
	return key, nil
}

// escapeSQL is a local copy used by runTableChanges to avoid pulling in
// the execute package for one helper. Mirrors execute/materialize.go.
func escapeSQL(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}

// _ stops the linter complaining about unused `strconv` until pagination
// arrives.
var _ = strconv.Atoi
