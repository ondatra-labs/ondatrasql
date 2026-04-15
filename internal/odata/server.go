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
	"strings"

	"github.com/ondatra-labs/ondatrasql/internal/duckdb"
)

// NewServer creates an HTTP handler for OData endpoints.
func NewServer(sess *duckdb.Session, schemas []EntitySchema, baseURL string) http.Handler {
	entityMap := make(map[string]EntitySchema, len(schemas))
	for _, s := range schemas {
		entityMap[s.ODataName] = s
	}

	mux := http.NewServeMux()

	// Service document
	mux.HandleFunc("GET /odata", func(w http.ResponseWriter, r *http.Request) {
		data, err := FormatServiceDocument(baseURL, schemas)
		if err != nil {
			writeError(w, 500, "InternalError", err.Error())
			return
		}
		w.Header().Set("Content-Type", "application/json;odata.metadata=minimal")
		w.Write(data)
	})

	// $metadata
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
		entityName := r.PathValue("entity")
		handleCount(w, r, sess, entityMap, entityName)
	})

	// Entity collection: /odata/{entity}
	mux.HandleFunc("GET /odata/{entity}", func(w http.ResponseWriter, r *http.Request) {
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

		sql, err := BuildQuery(entity, queryParams)
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
				countSQL, err := BuildQuery(entity, applyCountParams)
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
				if countSQL, err := BuildQuery(entity, countParams); err == nil {
					countSQL = fmt.Sprintf("SELECT COUNT(*) FROM (%s) AS _cnt", countSQL)
					if val, err := sess.QueryValue(countSQL); err == nil {
						var c int
						fmt.Sscanf(val, "%d", &c)
						count = &c
					}
				}
			} else {
				countSQL, err := BuildCountQuery(entity, queryParams)
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
					default:
						filterVal = fmt.Sprintf("%v", v)
					}

					relSQL := fmt.Sprintf("SELECT * FROM %s.%s WHERE %s = %s",
						quoteIdent(targetEntity.Schema), quoteIdent(targetEntity.Table),
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

		data, err := FormatResponse(baseURL, entityName, rows, count, entity)
		if err != nil {
			writeError(w, 500, "InternalError", err.Error())
			return
		}

		w.Header().Set("Content-Type", "application/json;odata.metadata=minimal")
		w.Write(data)
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

func handleCount(w http.ResponseWriter, r *http.Request, sess *duckdb.Session, entityMap map[string]EntitySchema, entityName string) {
	entity, ok := entityMap[entityName]
	if !ok {
		writeError(w, 404, "NotFound", fmt.Sprintf("Entity set '%s' not found", entityName))
		return
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
		baseSQL, err := BuildQuery(entity, countParams)
		if err != nil {
			writeError(w, 400, "BadRequest", err.Error())
			return
		}
		countSQL = fmt.Sprintf("SELECT COUNT(*) FROM (%s) AS _cnt", baseSQL)
	} else {
		var err error
		countSQL, err = BuildCountQuery(entity, params)
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
		writeError(w, 400, "BadRequest", fmt.Sprintf("Entity '%s' has no key column; use @expose with a key column or query the collection with $filter", entityName))
		return
	}

	sql, err := BuildSingleEntityQuery(entity, keyValue)
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
