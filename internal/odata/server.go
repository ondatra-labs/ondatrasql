// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package odata

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"

	"github.com/ondatra-labs/ondatrasql/internal/duckdb"
)

// NewServer creates an HTTP handler for OData endpoints.
func NewServer(sess *duckdb.Session, schemas []EntitySchema, baseURL string) http.Handler {
	// Normalize once at the boundary so every helper that builds URLs by
	// concatenating `baseURL + "/odata/..."` produces the same canonical
	// form. Without this, an operator-configured trailing slash leaks into
	// emitted @odata.id values (`https://api//odata/...`), and stripBaseURL
	// (which trims) refuses to dereference them.
	baseURL = strings.TrimSuffix(baseURL, "/")

	entityMap := make(map[string]EntitySchema, len(schemas))
	for _, s := range schemas {
		entityMap[s.ODataName] = s
	}

	// Delta link signing key. Persistent if ONDATRA_ODATA_DELTA_KEY is
	// set; per-process random otherwise (with a startup log line so the
	// operator knows existing deltaLinks won't survive a restart).
	deltaKey, deltaKeyErr := loadDeltaKey(os.Getenv("ONDATRA_ODATA_DELTA_KEY"), func(f string, a ...any) {
		fmt.Fprintf(os.Stderr, f+"\n", a...)
	})
	if deltaKeyErr != nil {
		// Fail soft: server starts, deltaLinks just won't be emitted.
		// The error is reported to stderr.
		fmt.Fprintf(os.Stderr, "OData: failed to initialise delta key (%v); @odata.deltaLink will be omitted\n", deltaKeyErr)
		deltaKey = nil
	}

	// Delta-handler serializer. The delta path mutates the session-wide
	// `search_path` to make DuckLake's `table_changes()` resolve a bare
	// table name — see runTableChanges in delta.go. table_changes() does
	// NOT accept a qualified name (verified: errors with "Table with name
	// schema.table does not exist"), so we cannot drop the SET. The
	// session's per-statement mutex doesn't span the SET+SELECT+RESET
	// sequence, so two concurrent delta handlers could clobber each
	// other's search_path mid-query (cross-schema data leak in the worst
	// case). This mutex serialises only the delta path; normal collection
	// queries are unaffected and remain concurrent.
	var deltaMu sync.Mutex

	mux := http.NewServeMux()

	// Service document
	mux.HandleFunc("GET /odata", func(w http.ResponseWriter, r *http.Request) {
		if !validateFormat(w, r) {
			return
		}
		data, err := FormatServiceDocument(baseURL, schemas)
		if err != nil {
			writeError(w, 500, "InternalError", err.Error())
			return
		}
		w.Header().Set("Content-Type", "application/json;odata.metadata=minimal")
		w.Write(data)
	})

	// $metadata — XML, $format does not apply.
	mux.HandleFunc("GET /odata/$metadata", func(w http.ResponseWriter, r *http.Request) {
		data, err := GenerateMetadata(schemas)
		if err != nil {
			writeError(w, 500, "InternalError", err.Error())
			return
		}
		w.Header().Set("Content-Type", "application/xml")
		w.Write(data)
	})

	// Entity $count: /odata/{entity}/$count
	mux.HandleFunc("GET /odata/{entity}/$count", func(w http.ResponseWriter, r *http.Request) {
		if !validateFormat(w, r) {
			return
		}
		entityName := r.PathValue("entity")
		handleCount(w, r, sess, entityMap, entityName)
	})

	// Entity collection: /odata/{entity}
	mux.HandleFunc("GET /odata/{entity}", func(w http.ResponseWriter, r *http.Request) {
		if !validateFormat(w, r) {
			return
		}
		entityName := r.PathValue("entity")

		// Single entity by key: /odata/entity(key)
		if strings.Contains(entityName, "(") {
			handleSingleEntity(w, r, sess, entityMap, entityName, baseURL)
			return
		}

		entity, ok := entityMap[entityName]
		if !ok {
			writeError(w, 404, "NotFound", fmt.Sprintf("Entity set '%s' not found", entityName))
			return
		}

		// $deltatoken: serve a delta-format response from DuckLake's
		// table_changes() between the snapshot pinned in the token and
		// the current snapshot. Routed before any normal-query handling.
		// Serialised via deltaMu — see comment on the mutex declaration.
		if dt := r.URL.Query().Get("$deltatoken"); dt != "" {
			handleDelta(w, r, sess, entity, dt, deltaKey, baseURL, &deltaMu)
			return
		}

		// Capture a single DuckLake snapshot at the start of the request
		// and pin every base-table reference to it. This makes data,
		// $count, $expand, and @odata.deltaLink all read from the same
		// version — no skew if the pipeline commits between intermediate
		// queries. Falls back to 0 (no pinning) if the snapshot read fails;
		// the response is still correct, just potentially showing skew
		// under heavy concurrent writes.
		var snapshot int64
		if err := sess.RefreshSnapshot(); err == nil {
			snapshot, _ = sess.GetCurrentSnapshot()
		}

		// If $expand is active, ensure FK columns are in the query
		queryParams := r.URL.Query()
		expandFKsToHide := map[string]bool{}
		if expand := queryParams.Get("$expand"); expand != "" && queryParams.Get("$select") != "" {
			// Parse $select into a set of column names
			selectCols := make(map[string]bool)
			for _, col := range strings.Split(queryParams.Get("$select"), ",") {
				selectCols[strings.TrimSpace(col)] = true
			}
			for _, expName := range strings.Split(expand, ",") {
				expName = strings.TrimSpace(expName)
				for _, nav := range entity.NavProperties {
					if nav.Name == expName {
						if !selectCols[nav.SourceColumn] {
							queryParams.Set("$select", queryParams.Get("$select")+","+nav.SourceColumn)
							expandFKsToHide[nav.SourceColumn] = true
						}
						break
					}
				}
			}
		}

		sql, err := BuildQuery(entity, queryParams, snapshot)
		if err != nil {
			writeError(w, 400, "BadRequest", err.Error())
			return
		}

		rows, err := sess.QueryRowsAny(sql)
		if err != nil {
			// Classify DuckDB errors: column/type/syntax errors are client errors
			errMsg := err.Error()
			if strings.Contains(errMsg, "not found") || strings.Contains(errMsg, "does not exist") ||
				strings.Contains(errMsg, "Referenced column") || strings.Contains(errMsg, "Binder Error") ||
				strings.Contains(errMsg, "Conversion Error") || strings.Contains(errMsg, "Parser Error") {
				writeError(w, 400, "BadRequest", errMsg)
			} else {
				writeError(w, 500, "InternalError", errMsg)
			}
			return
		}

		// Check if $count=true
		var count *int
		if r.URL.Query().Get("$count") == "true" {
			if queryParams.Get("$apply") != "" {
				// For $apply, count from unpaginated apply query
				applyCountParams := make(url.Values)
				for k, v := range queryParams {
					if k != "$top" && k != "$skip" && k != "$orderby" {
						applyCountParams[k] = v
					}
				}
				countSQL, err := BuildQuery(entity, applyCountParams, snapshot)
				if err == nil {
					countSQL = fmt.Sprintf("SELECT COUNT(*) FROM (%s) AS _cnt", countSQL)
					if val, err := sess.QueryValue(countSQL); err == nil {
						var c int
						fmt.Sscanf(val, "%d", &c)
						count = &c
					}
				}
				// No fallback — if count query fails, omit @odata.count
			} else if queryParams.Get("$compute") != "" {
				// $compute aliases may be referenced in $filter — use BuildQuery subquery for count
				countParams := make(url.Values)
				for k, v := range queryParams {
					if k != "$top" && k != "$skip" && k != "$orderby" {
						countParams[k] = v
					}
				}
				if countSQL, err := BuildQuery(entity, countParams, snapshot); err == nil {
					countSQL = fmt.Sprintf("SELECT COUNT(*) FROM (%s) AS _cnt", countSQL)
					if val, err := sess.QueryValue(countSQL); err == nil {
						var c int
						fmt.Sscanf(val, "%d", &c)
						count = &c
					}
				}
			} else {
				countSQL, err := BuildCountQuery(entity, queryParams, snapshot)
				if err == nil {
					if val, err := sess.QueryValue(countSQL); err == nil {
						var c int
						fmt.Sscanf(val, "%d", &c)
						count = &c
					}
				}
			}
		}

		// $expand — inline related entities
		if expand := r.URL.Query().Get("$expand"); expand != "" {
			expandNames := strings.Split(expand, ",")
			for _, expName := range expandNames {
				expName = strings.TrimSpace(expName)
				var nav *NavigationProperty
				for idx := range entity.NavProperties {
					if entity.NavProperties[idx].Name == expName {
						nav = &entity.NavProperties[idx]
						break
					}
				}
				if nav == nil {
					writeError(w, 400, "BadRequest", fmt.Sprintf("unknown navigation property: %s", expName))
					return
				}

				// Find target entity schema
				targetEntity, ok := entityMap[nav.TargetEntity]
				if !ok {
					writeError(w, 500, "InternalError", fmt.Sprintf("navigation target %s not found", nav.TargetEntity))
					return
				}

				// FK column is guaranteed to be in rows (injected above if $select excluded it)

				// For each row, fetch related entities
				for i, row := range rows {
					keyVal := row[nav.SourceColumn]
					if keyVal == nil {
						if nav.IsCollection {
							rows[i][expName] = []map[string]any{}
						} else {
							rows[i][expName] = nil
						}
						continue
					}

					var filterVal string
					switch v := keyVal.(type) {
					case string:
						filterVal = "'" + strings.ReplaceAll(v, "'", "''") + "'"
					case int, int64, int32, float64, float32:
						filterVal = fmt.Sprintf("%v", v)
					case bool:
						filterVal = fmt.Sprintf("%t", v)
					default:
						// Safety: quote unknown types as strings to prevent injection
						filterVal = "'" + strings.ReplaceAll(fmt.Sprintf("%v", v), "'", "''") + "'"
					}

					// Pin the inner SELECT to the same snapshot the parent
					// query used so $expand rows can't drift from the
					// parent rows (e.g. seeing a new related row that
					// wasn't there when the parent was read).
					relSQL := fmt.Sprintf("SELECT * FROM %s WHERE %s = %s",
						qualifiedTableSQL(targetEntity.Schema, targetEntity.Table, snapshot),
						quoteIdent(nav.TargetColumn), filterVal)

					relRows, err := sess.QueryRowsAny(relSQL)
					if err != nil {
						rows[i][expName] = []map[string]any{}
						continue
					}

					// Pre-convert with target entity's column types
					targetColTypes := make(map[string]string, len(targetEntity.Columns))
					for _, c := range targetEntity.Columns {
						targetColTypes[c.Name] = strings.ToUpper(c.Type)
					}
					for ri, relRow := range relRows {
						converted := make(map[string]any, len(relRow))
						for k, v := range relRow {
							converted[k] = convertODataValue(v, targetColTypes[k])
						}
						relRows[ri] = converted
					}

					if nav.IsCollection {
						rows[i][expName] = relRows
					} else if len(relRows) > 0 {
						rows[i][expName] = relRows[0]
					} else {
						rows[i][expName] = nil
					}
				}
			}
			// Remove auto-injected FK columns from response
			for fk := range expandFKsToHide {
				for ri := range rows {
					delete(rows[ri], fk)
				}
			}
		}

		// Build the delta link before serializing. Use the SAME snapshot
		// we pinned the data query to — that way the deltaLink token's
		// snapshot matches what the client actually saw, eliminating the
		// "client thinks they've seen up to N+1 but data was at N" skew
		// we'd have if we re-read the snapshot here. Skipped when query
		// has incompatible options or no delta key is configured.
		var deltaLink string
		if deltaKey != nil && snapshot > 0 {
			deltaLink = emitDeltaLink(baseURL, entity, queryParams, snapshot, deltaKey)
		}

		data, err := FormatResponse(baseURL, entityName, rows, count, entity)
		if err != nil {
			writeError(w, 500, "InternalError", err.Error())
			return
		}

		// Splice @odata.deltaLink into the JSON before the trailing '}'.
		// Cheaper than rebuilding the whole response struct, and avoids
		// changing FormatResponse's signature (which is exported).
		if deltaLink != "" {
			data = appendODataAnnotation(data, "@odata.deltaLink", deltaLink)
		}

		w.Header().Set("Content-Type", "application/json;odata.metadata=minimal")
		w.Write(data)
	})

	// $entity — dereference a canonical entity-id URL (the @odata.id we emit)
	// back to the entity. Spec: GET /odata/$entity?$id=<url>.
	mux.HandleFunc("GET /odata/$entity", func(w http.ResponseWriter, r *http.Request) {
		if !validateFormat(w, r) {
			return
		}
		idURL := r.URL.Query().Get("$id")
		if idURL == "" {
			writeError(w, 400, "BadRequest", "$entity requires $id query parameter")
			return
		}
		// $id may be absolute or relative. We only dereference IDs that
		// point at our own service — refusing foreign URLs prevents the
		// server from being used as a redirector for arbitrary HTTP fetches.
		entityPath := stripBaseURL(idURL, baseURL)
		if entityPath == "" {
			writeError(w, 400, "BadRequest",
				fmt.Sprintf("$id %q is not a valid entity URL on this service", idURL))
			return
		}
		// entityPath is now like "Orders(123)" — same shape handleSingleEntity
		// already handles for the in-path case GET /odata/Orders(123).
		if !strings.Contains(entityPath, "(") {
			writeError(w, 400, "BadRequest",
				fmt.Sprintf("$id %q does not address a single entity (missing key)", idURL))
			return
		}
		handleSingleEntity(w, r, sess, entityMap, entityPath, baseURL)
	})

	// $batch — JSON batch format (OData v4.01)
	mux.HandleFunc("POST /odata/$batch", func(w http.ResponseWriter, r *http.Request) {
		handleBatch(w, r, mux)
	})

	return odataVersion(mux)
}

// odataVersion wraps a handler to set the OData-Version header on all responses.
func odataVersion(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("OData-Version", "4.0")
		next.ServeHTTP(w, r)
	})
}

// appendODataAnnotation splices a top-level "key": "value" annotation
// into a JSON object's serialized bytes, just before the closing brace.
// Used to attach @odata.deltaLink without rebuilding the response struct
// (which would mean changing FormatResponse's exported signature).
//
// Returns the original bytes if the input doesn't end with `}` (defensive
// against unexpected serialization shapes).
func appendODataAnnotation(data []byte, key, value string) []byte {
	if len(data) == 0 || data[len(data)-1] != '}' {
		return data
	}
	encodedValue, _ := json.Marshal(value)
	prefix := []byte(`,"` + key + `":`)
	out := make([]byte, 0, len(data)+len(prefix)+len(encodedValue))
	out = append(out, data[:len(data)-1]...)
	out = append(out, prefix...)
	out = append(out, encodedValue...)
	out = append(out, '}')
	return out
}

// stripBaseURL returns the entity-path portion of an entity-id URL if it
// belongs to this service (matches our baseURL), or "" otherwise. Used by
// the $entity handler to refuse to dereference foreign URLs and to extract
// the `Orders(123)` segment from a full canonical URL.
//
// Accepts both absolute (`https://api/odata/Orders(123)`) and relative
// (`Orders(123)`, `/odata/Orders(123)`) forms.
func stripBaseURL(idURL, baseURL string) string {
	// Relative form — already an entity path or /odata/-prefixed path.
	if !strings.Contains(idURL, "://") {
		path := idURL
		path = strings.TrimPrefix(path, "/")
		path = strings.TrimPrefix(path, "odata/")
		return path
	}
	// Absolute form — must match our baseURL.
	prefix := strings.TrimSuffix(baseURL, "/") + "/odata/"
	if !strings.HasPrefix(idURL, prefix) {
		return ""
	}
	return strings.TrimPrefix(idURL, prefix)
}

// validateFormat enforces the $format query option. The OData spec lets
// clients select the response format via either the Accept header or the
// $format param. We only serve JSON, so anything other than $format=json
// is rejected with 406. Returns true if the request is acceptable; false
// (after writing the error) if it should be aborted.
func validateFormat(w http.ResponseWriter, r *http.Request) bool {
	format := r.URL.Query().Get("$format")
	if format == "" || format == "json" || format == "application/json" {
		return true
	}
	writeError(w, 406, "NotAcceptable",
		fmt.Sprintf("$format=%q is not supported; only json", format))
	return false
}

func handleCount(w http.ResponseWriter, r *http.Request, sess *duckdb.Session, entityMap map[string]EntitySchema, entityName string) {
	entity, ok := entityMap[entityName]
	if !ok {
		writeError(w, 404, "NotFound", fmt.Sprintf("Entity set '%s' not found", entityName))
		return
	}

	// Pin to a snapshot so $count is consistent if the same client also
	// queried the data — both reads see the same DuckLake version.
	var snapshot int64
	if err := sess.RefreshSnapshot(); err == nil {
		snapshot, _ = sess.GetCurrentSnapshot()
	}

	params := r.URL.Query()
	var countSQL string

	if params.Get("$compute") != "" || params.Get("$apply") != "" {
		// Use BuildQuery subquery for $compute/$apply — they extend the column set
		countParams := make(url.Values)
		for k, v := range params {
			if k != "$top" && k != "$skip" && k != "$orderby" {
				countParams[k] = v
			}
		}
		baseSQL, err := BuildQuery(entity, countParams, snapshot)
		if err != nil {
			writeError(w, 400, "BadRequest", err.Error())
			return
		}
		countSQL = fmt.Sprintf("SELECT COUNT(*) FROM (%s) AS _cnt", baseSQL)
	} else {
		var err error
		countSQL, err = BuildCountQuery(entity, params, snapshot)
		if err != nil {
			writeError(w, 400, "BadRequest", err.Error())
			return
		}
	}

	val, err := sess.QueryValue(countSQL)
	if err != nil {
		writeError(w, 500, "InternalError", err.Error())
		return
	}

	w.Header().Set("Content-Type", "text/plain")
	w.Write([]byte(val))
}

// batchRequest is a single request in a JSON batch.
type batchRequest struct {
	ID     string            `json:"id"`
	Method string            `json:"method"`
	URL    string            `json:"url"`
	Headers map[string]string `json:"headers,omitempty"`
}

// batchResponse is a single response in a JSON batch.
type batchResponse struct {
	ID      string            `json:"id"`
	Status  int               `json:"status"`
	Headers map[string]string `json:"headers,omitempty"`
	Body    json.RawMessage   `json:"body,omitempty"`
}

// handleBatch processes a JSON batch request (OData v4.01 format).
func handleBatch(w http.ResponseWriter, r *http.Request, handler http.Handler) {
	// Parse batch request
	var batch struct {
		Requests []batchRequest `json:"requests"`
	}
	if err := json.NewDecoder(r.Body).Decode(&batch); err != nil {
		writeError(w, 400, "BadRequest", fmt.Sprintf("invalid batch JSON: %v", err))
		return
	}

	if len(batch.Requests) == 0 {
		writeError(w, 400, "BadRequest", "empty batch request")
		return
	}

	// Execute each request
	responses := make([]batchResponse, len(batch.Requests))
	for i, req := range batch.Requests {
		// Build internal HTTP request
		innerURL := req.URL
		if !strings.HasPrefix(innerURL, "/") {
			innerURL = "/odata/" + innerURL
		}
		innerReq, err := http.NewRequest(req.Method, innerURL, nil)
		if err != nil {
			responses[i] = batchResponse{
				ID:     req.ID,
				Status: 400,
				Body:   json.RawMessage(`{"error":{"code":"BadRequest","message":"invalid request URL"}}`),
			}
			continue
		}
		// Copy query params from URL
		if u, err := url.Parse(innerURL); err == nil {
			innerReq.URL = u
		}

		// Capture response
		rec := &responseRecorder{headers: make(http.Header), status: 200}
		handler.ServeHTTP(rec, innerReq)

		// Ensure body is valid JSON; wrap plain text preserving status semantics
		bodyBytes := rec.body.Bytes()
		if !json.Valid(bodyBytes) {
			code := "ServerError"
			if rec.status >= 400 && rec.status < 500 {
				code = "ClientError"
			}
			bodyBytes, _ = json.Marshal(map[string]any{
				"error": map[string]string{"code": code, "message": strings.TrimSpace(string(bodyBytes))},
			})
		}
		responses[i] = batchResponse{
			ID:      req.ID,
			Status:  rec.status,
			Headers: map[string]string{"Content-Type": "application/json"},
			Body:    json.RawMessage(bodyBytes),
		}
	}

	// Return batch response
	resp := struct {
		Responses []batchResponse `json:"responses"`
	}{Responses: responses}

	data, err := json.Marshal(resp)
	if err != nil {
		writeError(w, 500, "InternalError", fmt.Sprintf("batch response marshal: %v", err))
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(200)
	w.Write(data)
}

// responseRecorder captures an HTTP response for batch processing.
type responseRecorder struct {
	headers http.Header
	status  int
	body    bytes.Buffer
}

func (r *responseRecorder) Header() http.Header         { return r.headers }
func (r *responseRecorder) WriteHeader(status int)       { r.status = status }
func (r *responseRecorder) Write(b []byte) (int, error)  { return r.body.Write(b) }

func handleSingleEntity(w http.ResponseWriter, r *http.Request, sess *duckdb.Session, entityMap map[string]EntitySchema, raw, baseURL string) {
	// Parse "entityName(keyValue)"
	idx := strings.Index(raw, "(")
	if idx < 0 || !strings.HasSuffix(raw, ")") {
		writeError(w, 400, "BadRequest", "Invalid entity-by-key syntax; expected entity(key)")
		return
	}
	entityName := raw[:idx]
	keyValue := raw[idx+1 : len(raw)-1]

	entity, ok := entityMap[entityName]
	if !ok {
		writeError(w, 404, "NotFound", fmt.Sprintf("Entity set '%s' not found", entityName))
		return
	}
	if entity.KeyColumn == "" {
		writeError(w, 400, "BadRequest", fmt.Sprintf("Entity '%s' has no key column; @expose requires a key column", entityName))
		return
	}

	// Pin to a snapshot — single-entity reads are usually independent
	// enough that pinning isn't strictly required, but it keeps the
	// behaviour consistent with the collection handler ($entity-via-id
	// dereferencing produces identical reads to the original
	// `value`-array entries).
	var snapshot int64
	if err := sess.RefreshSnapshot(); err == nil {
		snapshot, _ = sess.GetCurrentSnapshot()
	}

	sql, err := BuildSingleEntityQuery(entity, keyValue, snapshot)
	if err != nil {
		writeError(w, 400, "BadRequest", err.Error())
		return
	}

	rows, err := sess.QueryRowsAny(sql)
	if err != nil {
		writeError(w, 500, "InternalError", err.Error())
		return
	}
	if len(rows) == 0 {
		writeError(w, 404, "NotFound", "Entity not found")
		return
	}

	data, err := FormatSingleEntityResponse(baseURL, entityName, rows[0], entity)
	if err != nil {
		writeError(w, 500, "InternalError", err.Error())
		return
	}

	w.Header().Set("Content-Type", "application/json;odata.metadata=minimal")
	w.Write(data)
}

// writeError writes an OData-compliant JSON error response.
func writeError(w http.ResponseWriter, status int, code, message string) {
	resp := map[string]interface{}{
		"error": map[string]string{
			"code":    code,
			"message": message,
		},
	}
	data, _ := json.Marshal(resp)
	w.Header().Set("Content-Type", "application/json;odata.metadata=minimal")
	w.WriteHeader(status)
	w.Write(data)
}
