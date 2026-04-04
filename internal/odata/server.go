// OndatraSQL - You don't need a data stack anymore
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package odata

import (
	"encoding/json"
	"fmt"
	"net/http"
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

		// Detect entity-by-key pattern like "mart_revenue(1)" and return 501
		if strings.Contains(entityName, "(") {
			writeError(w, 501, "NotImplemented", "Entity-by-key access is not supported; use collection queries with $filter instead")
			return
		}

		entity, ok := entityMap[entityName]
		if !ok {
			writeError(w, 404, "NotFound", fmt.Sprintf("Entity set '%s' not found", entityName))
			return
		}

		sql, err := BuildQuery(entity, r.URL.Query())
		if err != nil {
			writeError(w, 400, "BadRequest", err.Error())
			return
		}

		rows, err := sess.QueryRowsAny(sql)
		if err != nil {
			writeError(w, 500, "InternalError", err.Error())
			return
		}

		// Check if $count=true
		var count *int
		if r.URL.Query().Get("$count") == "true" {
			countSQL, err := BuildCountQuery(entity, r.URL.Query())
			if err == nil {
				if val, err := sess.QueryValue(countSQL); err == nil {
					var c int
					fmt.Sscanf(val, "%d", &c)
					count = &c
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

	sql, err := BuildCountQuery(entity, r.URL.Query())
	if err != nil {
		writeError(w, 400, "BadRequest", err.Error())
		return
	}

	val, err := sess.QueryValue(sql)
	if err != nil {
		writeError(w, 500, "InternalError", err.Error())
		return
	}

	w.Header().Set("Content-Type", "text/plain")
	w.Write([]byte(val))
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
