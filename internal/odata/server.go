// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package odata

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"mime"
	"net/http"
	"net/url"
	"os"
	"runtime"
	"strconv"
	"strings"

	"github.com/ondatra-labs/ondatrasql/internal/duckdb"
)

// requestDB bundles a single read-pool conn with the writer session for
// the duration of one OData request. Created at handler entry, released
// when the handler returns. Mirrors the `*duckdb.Session` method names the
// OData code calls so the call sites don't change shape.
//
// All Query / Exec methods route to the bound pool conn; accessor methods
// (CatalogAlias, DefaultSearchPath) pass through to the session because
// those are session-level config, not per-conn state.
type requestDB struct {
	ctx  context.Context
	conn *sql.Conn
	sess *duckdb.Session
}

func (r *requestDB) QueryRowsAny(sqlStr string) ([]map[string]any, error) {
	return duckdb.QueryRowsAnyOnConn(r.ctx, r.conn, sqlStr)
}

func (r *requestDB) QueryValue(sqlStr string) (string, error) {
	return duckdb.QueryValueOnConn(r.ctx, r.conn, sqlStr)
}

func (r *requestDB) QueryRowsMap(sqlStr string) ([]map[string]string, error) {
	return duckdb.QueryRowsMapOnConn(r.ctx, r.conn, sqlStr)
}

func (r *requestDB) Exec(sqlStr string) error {
	return duckdb.ExecOnConn(r.ctx, r.conn, sqlStr)
}

// RefreshSnapshot is a no-op on the pool path. The writer session uses a
// `curr_snapshot` session variable that lives on its own conn; pool conns
// don't share it. GetCurrentSnapshot reads `current_snapshot()` directly.
func (r *requestDB) RefreshSnapshot() error { return nil }

func (r *requestDB) GetCurrentSnapshot() (int64, error) {
	return duckdb.CurrentSnapshotOnConn(r.ctx, r.conn)
}

func (r *requestDB) DefaultSearchPath() string { return r.sess.DefaultSearchPath() }
func (r *requestDB) CatalogAlias() string      { return r.sess.CatalogAlias() }

// defaultPoolSize is min(GOMAXPROCS, 8). The 8 cap exists so 64-vCPU CI
// runners don't auto-spawn 64 DuckDB conns; operators that want more can
// set ONDATRA_ODATA_POOL_SIZE explicitly.
func defaultPoolSize() int {
	n := runtime.GOMAXPROCS(0)
	if n > 8 {
		n = 8
	}
	if n < 1 {
		n = 1
	}
	return n
}

// loadPoolSize resolves the OData read-pool size from
// ONDATRA_ODATA_POOL_SIZE: the env value if set (parsed as a positive
// integer), or defaultPoolSize() if unset. Returns an error when the env
// is set but unparseable or non-positive — operators get a clear failure
// rather than a silent fallback (mirrors loadDeltaMaxAge fail-closed).
func loadPoolSize(envValue string) (int, error) {
	envValue = strings.TrimSpace(envValue)
	if envValue == "" {
		return defaultPoolSize(), nil
	}
	n, err := strconv.Atoi(envValue)
	if err != nil || n <= 0 {
		return 0, fmt.Errorf("ONDATRA_ODATA_POOL_SIZE=%q: must be a positive integer", envValue)
	}
	return n, nil
}

// defaultPageSize is the server-side cap on rows per response when no
// ONDATRA_ODATA_PAGE_SIZE is set. Picked to balance two concerns:
//   - small enough that one request doesn't ship a 100MB JSON payload to
//     an unsuspecting BI client
//   - large enough that small collections don't pay the round-trip cost
//     of @odata.nextLink follow-ups for no benefit
const defaultPageSize = 10000

// loadPageSize resolves the server-side page size from
// ONDATRA_ODATA_PAGE_SIZE. Same semantics as loadPoolSize: empty falls
// back to the default; invalid values fail-closed.
func loadPageSize(envValue string) (int, error) {
	envValue = strings.TrimSpace(envValue)
	if envValue == "" {
		return defaultPageSize, nil
	}
	n, err := strconv.Atoi(envValue)
	if err != nil || n <= 0 {
		return 0, fmt.Errorf("ONDATRA_ODATA_PAGE_SIZE=%q: must be a positive integer", envValue)
	}
	return n, nil
}

// acquireDB checks out a read conn for the request. Returns a release fn
// that the handler must call (typically `defer release()`). On failure
// writes a 503 error and returns nil/nil/err so the caller can return
// early.
func acquireDB(ctx context.Context, sess *duckdb.Session, w http.ResponseWriter) (*requestDB, func(), bool) {
	pool := sess.ReadPool()
	if pool == nil {
		writeError(w, 500, "InternalError", "OData read pool not initialised")
		return nil, nil, false
	}
	c, err := pool.Acquire(ctx)
	if err != nil {
		// Context cancelled (client disconnected) → no point writing a body
		// the client won't read. Pool closed → 503 because we cannot serve.
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil, nil, false
		}
		writeError(w, 503, "ServiceUnavailable", fmt.Sprintf("acquire read conn: %v", err))
		return nil, nil, false
	}
	db := &requestDB{ctx: ctx, conn: c, sess: sess}
	release := func() { pool.Release(c) }
	return db, release, true
}

// NewServer creates an HTTP handler for OData endpoints.
//
// If the session has no read pool yet, NewServer initializes one with a
// default size of min(GOMAXPROCS, 8). Operators that want to override the
// size should call sess.InitReadPool(N) themselves before calling
// NewServer — see cmd/ondatrasql/odata_cmd.go for the env-driven config.
//
// Returns an error only when ONDATRA_ODATA_DELTA_MAX_AGE is set to an
// unparseable value: that's an explicit operator request for token
// expiry, and silently disabling it would deny the security guarantee
// they configured. Other failures (delta-key parse, read-pool init)
// degrade gracefully — server starts, deltaLinks may be omitted, etc.
func NewServer(sess *duckdb.Session, schemas []EntitySchema, baseURL string) (http.Handler, error) {
	// Normalize once at the boundary so every helper that builds URLs by
	// concatenating `baseURL + "/odata/..."` produces the same canonical
	// form. Without this, an operator-configured trailing slash leaks into
	// emitted @odata.id values (`https://api//odata/...`), and stripBaseURL
	// (which trims) refuses to dereference them.
	baseURL = strings.TrimSuffix(baseURL, "/")

	// Resolve pool size from env (fail-closed on bad value, mirrors
	// MAX_AGE handling). Always read the env even if a pool is already
	// initialised, so a typoed env still surfaces an error at server-
	// construction time.
	poolSize, err := loadPoolSize(os.Getenv("ONDATRA_ODATA_POOL_SIZE"))
	if err != nil {
		return nil, err
	}
	pageSize, err := loadPageSize(os.Getenv("ONDATRA_ODATA_PAGE_SIZE"))
	if err != nil {
		return nil, err
	}
	if sess.ReadPool() == nil {
		if initErr := sess.InitReadPool(poolSize); initErr != nil {
			// Fail soft on session-level errors (e.g. catalog not
			// initialised in a test that skipped InitWithCatalog) — log
			// and return a handler that 500s every request rather than
			// crashing at boot.
			fmt.Fprintf(os.Stderr, "OData: failed to initialise read pool (%v); every request will return 500\n", initErr)
		}
	}

	entityMap := make(map[string]EntitySchema, len(schemas))
	for _, s := range schemas {
		entityMap[s.ODataName] = s
	}

	// Delta link signing keyset. Persistent if ONDATRA_ODATA_DELTA_KEY is
	// set; per-process random otherwise (with a startup log line so the
	// operator knows existing deltaLinks won't survive a restart). The
	// env supports comma-separated `kid:hex` pairs for zero-downtime
	// rotation — see parseKeyset.
	deltaKeyset, deltaKeyErr := loadDeltaKeyset(os.Getenv("ONDATRA_ODATA_DELTA_KEY"), func(f string, a ...any) {
		fmt.Fprintf(os.Stderr, f+"\n", a...)
	})
	if deltaKeyErr != nil {
		// Fail soft: server starts, deltaLinks just won't be emitted.
		// The error is reported to stderr.
		fmt.Fprintf(os.Stderr, "OData: failed to initialise delta key (%v); @odata.deltaLink will be omitted\n", deltaKeyErr)
		deltaKeyset = nil
	}

	// Optional max-age: tokens older than this are rejected as expired.
	// Unset = no expiry check (default). Configurable expiry guards against
	// long-lived deltaLinks pinning ancient snapshots and producing huge
	// table_changes() result sets.
	//
	// Fail closed on parse errors: if the operator typed
	// ONDATRA_ODATA_DELTA_MAX_AGE=garbage they wanted expiry; silently
	// running with no expiry is a security regression.
	deltaMaxAge, maxAgeErr := loadDeltaMaxAge(os.Getenv("ONDATRA_ODATA_DELTA_MAX_AGE"))
	if maxAgeErr != nil {
		return nil, maxAgeErr
	}

	// v0.27.0: the previous `deltaMu` mutex is gone. Each request now gets
	// its own conn from the read pool, so the delta handler's
	// `SET search_path = 'lake.<schema>,...'` mutation is per-conn — no
	// cross-handler bleed possible. The pool itself enforces a hard
	// concurrency cap (size = ONDATRA_ODATA_POOL_SIZE) so the writer's conn
	// stays untouched by readers.

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
		db, release, ok := acquireDB(r.Context(), sess, w)
		if !ok {
			return
		}
		defer release()
		entityName := r.PathValue("entity")
		handleCount(w, r, db, entityMap, entityName)
	})

	// Positional access: /odata/{entity}/{N} where N is a non-negative
	// integer. Per OData Part 2 §4.10 this is the canonical $index form
	// (zero-based ordinal in the default key ordering). Matches AFTER
	// /$count above; mux dispatches the literal "$count" first.
	mux.HandleFunc("GET /odata/{entity}/{ordinal}", func(w http.ResponseWriter, r *http.Request) {
		if !validateFormat(w, r) {
			return
		}
		entityName := r.PathValue("entity")
		ordinal := r.PathValue("ordinal")
		n, err := strconv.Atoi(ordinal)
		if err != nil || n < 0 {
			// Not a valid ordinal — treat as 404 since /Entity/<garbage>
			// doesn't match any defined OData segment.
			writeError(w, 404, "NotFound", fmt.Sprintf("path segment %q is not a valid ordinal under /odata/%s/", ordinal, entityName))
			return
		}
		db, release, ok := acquireDB(r.Context(), sess, w)
		if !ok {
			return
		}
		defer release()
		handleEntityByOrdinal(w, r, db, entityMap, entityName, n, baseURL)
	})

	// Entity collection: /odata/{entity}
	mux.HandleFunc("GET /odata/{entity}", func(w http.ResponseWriter, r *http.Request) {
		if !validateFormat(w, r) {
			return
		}
		db, release, ok := acquireDB(r.Context(), sess, w)
		if !ok {
			return
		}
		defer release()

		entityName := r.PathValue("entity")

		// $crossjoin: /odata/$crossjoin(EntitySet1,EntitySet2,...). Routed
		// before the (key) check below — `$crossjoin(A,B)` also has parens
		// and would otherwise be misrouted to handleSingleEntity.
		if strings.HasPrefix(entityName, "$crossjoin(") && strings.HasSuffix(entityName, ")") {
			handleCrossJoin(w, r, db, entityMap, entityName, baseURL)
			return
		}

		// Single entity by key: /odata/entity(key)
		if strings.Contains(entityName, "(") {
			handleSingleEntity(w, r, db, entityMap, entityName, baseURL)
			return
		}

		entity, eok := entityMap[entityName]
		if !eok {
			writeError(w, 404, "NotFound", fmt.Sprintf("Entity set '%s' not found", entityName))
			return
		}

		// $deltatoken: serve a delta-format response from DuckLake's
		// table_changes() between the snapshot pinned in the token and
		// the current snapshot. Routed before any normal-query handling.
		// Each request has its own conn (search_path mutation is per-conn),
		// so no mutex is needed here.
		if dt := r.URL.Query().Get("$deltatoken"); dt != "" {
			handleDelta(w, r, db, entity, dt, deltaKeyset, deltaMaxAge, baseURL)
			return
		}

		// $skiptoken: server-driven paging. If present, decode and use the
		// verified offset (overrides $skip). Status-code split per Part 1
		// §11.4: 410 Gone for tokens whose underlying snapshot is no
		// longer addressable (expired) or whose query shape no longer
		// matches what the client originally issued (filter-changed).
		// 400 Bad Request for tampered, malformed, or wrong-entity tokens
		// — those are client errors, not "resource gone".
		queryParams := r.URL.Query()
		var skipTokenOffset int64
		if st := queryParams.Get("$skiptoken"); st != "" {
			tok, err := decodeSkipToken(st, deltaKeyset, deltaMaxAge)
			if err != nil {
				if errors.Is(err, errSkipTokenExpired) {
					writeError(w, 410, "SkipTokenExpired", err.Error())
					return
				}
				writeError(w, 400, "SkipTokenInvalid", err.Error())
				return
			}
			if tok.Entity != entity.ODataName {
				writeError(w, 400, "SkipTokenInvalid",
					fmt.Sprintf("token was issued for entity %q, not %q", tok.Entity, entity.ODataName))
				return
			}
			if skipTokenFilterHash(queryParams) != tok.FilterHash {
				writeError(w, 410, "SkipTokenFilterChanged",
					"query options have changed since this nextLink was issued; re-issue the original query")
				return
			}
			skipTokenOffset = tok.Offset
			// Strip $skiptoken so BuildQuery doesn't see it.
			queryParams.Del("$skiptoken")
		}

		// Capture a single DuckLake snapshot at the start of the request
		// and pin every base-table reference to it. This makes data,
		// $count, $expand, and @odata.deltaLink all read from the same
		// version — no skew if the pipeline commits between intermediate
		// queries. Falls back to 0 (no pinning) if the snapshot read fails;
		// the response is still correct, just potentially showing skew
		// under heavy concurrent writes.
		var snapshot int64
		if err := db.RefreshSnapshot(); err == nil {
			snapshot, _ = db.GetCurrentSnapshot()
		}

		// Server-side page-size enforcement. Compute effective LIMIT/OFFSET:
		//   - serverLimit = min(clientTop, pageSize); pageSize cap applies
		//     when client didn't set $top or set it above the cap
		//   - clientCapApplied tracks whether we honored a smaller client
		//     $top; we never emit @odata.nextLink in that case
		// The query fetches serverLimit+1 rows so we can detect "more
		// available" without a separate COUNT(*).
		//
		// Validate $top / $skip up front. We override these before
		// BuildQuery sees them, so we have to reject malformed values
		// here — otherwise BuildQuery's own parse-and-reject path is
		// bypassed and bad input silently becomes 0 / pageSize.
		clientTop := -1
		if t := queryParams.Get("$top"); t != "" {
			n, err := strconv.Atoi(t)
			if err != nil || n < 0 {
				writeError(w, 400, "BadRequest", fmt.Sprintf("invalid $top: %q (must be a non-negative integer)", t))
				return
			}
			clientTop = n
		}
		clientSkip := 0
		if s := queryParams.Get("$skip"); s != "" {
			n, err := strconv.Atoi(s)
			if err != nil || n < 0 {
				writeError(w, 400, "BadRequest", fmt.Sprintf("invalid $skip: %q (must be a non-negative integer)", s))
				return
			}
			clientSkip = n
		}
		serverLimit := pageSize
		clientCapApplied := false
		if clientTop >= 0 && clientTop <= pageSize {
			serverLimit = clientTop
			clientCapApplied = true
		}
		// Effective offset = $skip + skiptoken offset. The token's offset
		// already accounts for previous pages; client $skip on a follow-up
		// would shift it further.
		effectiveOffset := skipTokenOffset + int64(clientSkip)
		queryParams.Set("$top", strconv.Itoa(serverLimit+1))
		queryParams.Set("$skip", strconv.FormatInt(effectiveOffset, 10))

		// If $expand is active, ensure FK columns are in the query
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

		rows, err := db.QueryRowsAny(sql)
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

		// We fetched serverLimit+1 rows to detect "more available". Trim
		// to serverLimit and remember whether more existed.
		hasMore := len(rows) > serverLimit
		if hasMore {
			rows = rows[:serverLimit]
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
					if val, err := db.QueryValue(countSQL); err == nil {
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
					if val, err := db.QueryValue(countSQL); err == nil {
						var c int
						fmt.Sscanf(val, "%d", &c)
						count = &c
					}
				}
			} else {
				countSQL, err := BuildCountQuery(entity, queryParams, snapshot)
				if err == nil {
					if val, err := db.QueryValue(countSQL); err == nil {
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

					relRows, err := db.QueryRowsAny(relSQL)
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
		//
		// deltaLink is suppressed for paginated responses: a deltaLink
		// describes "everything from snapshot N onward" while a nextLink
		// describes "the rest of this current page chain" — emitting both
		// invites confusion about which the client should poll.
		var deltaLink string
		if deltaKeyset != nil && snapshot > 0 && !hasMore && skipTokenOffset == 0 {
			// Use the original request params (without the $top/$skip
			// overrides we applied for paging) so the deltaLink's
			// filter_hash matches what the client originally sent.
			deltaLink = emitDeltaLink(baseURL, entity, r.URL.Query(), snapshot, deltaKeyset)
		}

		// Build the next link for server-driven paging. Emit only when we
		// hit the server's pageSize cap (not the client's $top), and when
		// a signing keyset is configured. Re-uses the existing keyset.
		var nextLink string
		if hasMore && !clientCapApplied && deltaKeyset != nil {
			// Use original params (not the $top/$skip override) so the
			// emitted nextLink carries forward the user's actual options.
			origParams := r.URL.Query()
			origParams.Del("$top")
			origParams.Del("$skip")
			origParams.Del("$skiptoken")
			nextLink = emitNextLink(baseURL, entity, origParams,
				effectiveOffset+int64(serverLimit), deltaKeyset)
		}

		data, err := FormatResponse(baseURL, entityName, rows, count, entity)
		if err != nil {
			writeError(w, 500, "InternalError", err.Error())
			return
		}

		// Splice @odata.deltaLink and @odata.nextLink into the JSON before
		// the trailing '}'. Cheaper than rebuilding the whole response
		// struct, and avoids changing FormatResponse's signature.
		if nextLink != "" {
			data = appendODataAnnotation(data, "@odata.nextLink", nextLink)
		}
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
		db, release, ok := acquireDB(r.Context(), sess, w)
		if !ok {
			return
		}
		defer release()
		handleSingleEntity(w, r, db, entityMap, entityPath, baseURL)
	})

	// $batch — JSON batch format (OData v4.01)
	mux.HandleFunc("POST /odata/$batch", func(w http.ResponseWriter, r *http.Request) {
		handleBatch(w, r, mux)
	})

	return odataVersion(mux), nil
}

// odataVersion wraps a handler to set the OData-Version header on all
// responses. We advertise 4.01 because the implementation emits v4.01-only
// features (`$compute`, `$apply`, JSON `$batch`, `@removed`, `$index`,
// `$crossjoin`); claiming only 4.0 while emitting v4.01 confuses strict
// clients. Long-form annotations (`@odata.context` etc.) remain valid in
// v4.01 per JSON Format §4.5 and match what Microsoft Graph / SAP Gateway
// emit, so we keep them.
func odataVersion(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("OData-Version", "4.01")
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
// $format param. We only serve JSON, so anything other than json (with
// any optional metadata-level parameter) is rejected with 406.
//
// The spec accepts forms like `application/json;odata.metadata=minimal`
// — we strip the parameter via mime.ParseMediaType before comparing.
// Bare `json` is also conformant (Part 2 §5.1.5).
func validateFormat(w http.ResponseWriter, r *http.Request) bool {
	format := r.URL.Query().Get("$format")
	if format == "" || format == "json" {
		return true
	}
	// Strip parameters (`;odata.metadata=...`, `;charset=...`).
	mediaType, _, err := mime.ParseMediaType(format)
	if err == nil && mediaType == "application/json" {
		return true
	}
	writeError(w, 406, "NotAcceptable",
		fmt.Sprintf("$format=%q is not supported; only json (or application/json with optional metadata-level parameter)", format))
	return false
}

func handleCount(w http.ResponseWriter, r *http.Request, db *requestDB, entityMap map[string]EntitySchema, entityName string) {
	entity, ok := entityMap[entityName]
	if !ok {
		writeError(w, 404, "NotFound", fmt.Sprintf("Entity set '%s' not found", entityName))
		return
	}

	// Pin to a snapshot so $count is consistent if the same client also
	// queried the data — both reads see the same DuckLake version.
	var snapshot int64
	if err := db.RefreshSnapshot(); err == nil {
		snapshot, _ = db.GetCurrentSnapshot()
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

	val, err := db.QueryValue(countSQL)
	if err != nil {
		writeError(w, 500, "InternalError", err.Error())
		return
	}

	w.Header().Set("Content-Type", "text/plain")
	w.Write([]byte(val))
}

// batchRequest is a single request in a JSON batch (OData v4.01).
//
// AtomicityGroup and DependsOn are spec fields per JSON Format §19.1.4-5.
// We reject AtomicityGroup explicitly because it requires write-side
// transactional semantics (and we are read-only). DependsOn is a no-op
// for read-only batches — sub-requests don't share state — so we accept
// the field but don't reorder.
type batchRequest struct {
	ID              string            `json:"id"`
	Method          string            `json:"method"`
	URL             string            `json:"url"`
	Headers         map[string]string `json:"headers,omitempty"`
	AtomicityGroup  string            `json:"atomicityGroup,omitempty"`
	DependsOn       []string          `json:"dependsOn,omitempty"`
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
		// AtomicityGroup requires transactional rollback across writes
		// (JSON Format §19.1.4). We're read-only so atomicity is
		// meaningless — reject with 501 to flag the mismatch rather than
		// silently process as if the group didn't exist.
		if req.AtomicityGroup != "" {
			responses[i] = batchResponse{
				ID:     req.ID,
				Status: 501,
				Body:   json.RawMessage(`{"error":{"code":"NotImplemented","message":"atomicityGroup is not supported on this read-only server"}}`),
			}
			continue
		}

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
		// Copy per-request headers (Accept, Prefer, etc.) onto the
		// inner request so handlers see what the client asked for.
		// JSON Format §19.1.3 — these MUST be honored.
		for k, v := range req.Headers {
			innerReq.Header.Set(k, v)
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

func handleSingleEntity(w http.ResponseWriter, r *http.Request, db *requestDB, entityMap map[string]EntitySchema, raw, baseURL string) {
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
	if err := db.RefreshSnapshot(); err == nil {
		snapshot, _ = db.GetCurrentSnapshot()
	}

	// Per-key access only; positional access uses /Entity/N (handled
	// by handleEntityByOrdinal). The earlier `Entity($index=N)` parens
	// form was non-spec — Part 2 §4.10 is path-segment ordinal — and
	// has been replaced before v0.28.0 ships.
	sql, err := BuildSingleEntityQuery(entity, keyValue, snapshot)
	if err != nil {
		writeError(w, 400, "BadRequest", err.Error())
		return
	}

	rows, err := db.QueryRowsAny(sql)
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

// handleEntityByOrdinal serves /odata/{entity}/{N} — positional access
// in the default key ordering. Sugar over `ORDER BY key LIMIT 1 OFFSET N`
// with snapshot pinning. Returns 404 if N is past the last row.
func handleEntityByOrdinal(w http.ResponseWriter, r *http.Request, db *requestDB, entityMap map[string]EntitySchema, entityName string, ordinal int, baseURL string) {
	entity, ok := entityMap[entityName]
	if !ok {
		writeError(w, 404, "NotFound", fmt.Sprintf("Entity set '%s' not found", entityName))
		return
	}
	if entity.KeyColumn == "" {
		writeError(w, 400, "BadRequest", fmt.Sprintf("Entity '%s' has no key column; positional access requires @expose <key>", entityName))
		return
	}

	var snapshot int64
	if err := db.RefreshSnapshot(); err == nil {
		snapshot, _ = db.GetCurrentSnapshot()
	}

	sql := fmt.Sprintf("SELECT * FROM %s ORDER BY %s LIMIT 1 OFFSET %d",
		qualifiedTableSQL(entity.Schema, entity.Table, snapshot),
		quoteIdent(entity.KeyColumn), ordinal)

	rows, err := db.QueryRowsAny(sql)
	if err != nil {
		writeError(w, 500, "InternalError", err.Error())
		return
	}
	if len(rows) == 0 {
		writeError(w, 404, "NotFound", fmt.Sprintf("ordinal %d is past the end of %s", ordinal, entityName))
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

// handleCrossJoin serves /odata/$crossjoin(A,B,...). Generates a SQL
// CROSS JOIN, pins it to the request snapshot, and returns rows nested
// per source entity. The whole reason for this endpoint is that
// `@expose` only marks tables available — it doesn't define joins; this
// route lets clients (BI tools) do their own joins via $filter without
// requiring the operator to predefine a JOIN-containing model.
func handleCrossJoin(w http.ResponseWriter, r *http.Request, db *requestDB, entityMap map[string]EntitySchema, raw, baseURL string) {
	// Parse "$crossjoin(A,B,C)" → ["A","B","C"].
	inner := strings.TrimSuffix(strings.TrimPrefix(raw, "$crossjoin("), ")")
	if inner == "" {
		writeError(w, 400, "BadRequest", "$crossjoin requires entity-set names")
		return
	}
	names := strings.Split(inner, ",")
	entities := make([]EntitySchema, 0, len(names))
	for _, n := range names {
		n = strings.TrimSpace(n)
		if n == "" {
			writeError(w, 400, "BadRequest", "$crossjoin: empty entity-set name")
			return
		}
		e, ok := entityMap[n]
		if !ok {
			writeError(w, 404, "NotFound", fmt.Sprintf("Entity set '%s' not found", n))
			return
		}
		entities = append(entities, e)
	}
	if len(entities) < 2 {
		writeError(w, 400, "BadRequest", "$crossjoin requires at least 2 entity sets")
		return
	}

	// Snapshot pin — same as collection handler.
	var snapshot int64
	if err := db.RefreshSnapshot(); err == nil {
		snapshot, _ = db.GetCurrentSnapshot()
	}

	params := r.URL.Query()
	sql, err := BuildCrossJoinQuery(entities, params, snapshot)
	if err != nil {
		writeError(w, 400, "BadRequest", err.Error())
		return
	}

	rows, err := db.QueryRowsAny(sql)
	if err != nil {
		errMsg := err.Error()
		if strings.Contains(errMsg, "not found") || strings.Contains(errMsg, "does not exist") ||
			strings.Contains(errMsg, "Binder Error") || strings.Contains(errMsg, "Conversion Error") ||
			strings.Contains(errMsg, "Parser Error") {
			writeError(w, 400, "BadRequest", errMsg)
		} else {
			writeError(w, 500, "InternalError", errMsg)
		}
		return
	}

	// Nest rows by entity. Columns are aliased "<EntitySet>__<col>" by
	// BuildCrossJoinQuery; split on "__" to demux. Column names cannot
	// contain "__" in OData (entity names use single underscores).
	colTypes := make(map[string]map[string]string, len(entities))
	for _, e := range entities {
		ct := make(map[string]string, len(e.Columns))
		for _, c := range e.Columns {
			ct[c.Name] = strings.ToUpper(c.Type)
		}
		colTypes[e.ODataName] = ct
	}
	value := make([]map[string]any, 0, len(rows))
	for _, row := range rows {
		nested := make(map[string]any, len(entities))
		for _, e := range entities {
			sub := make(map[string]any)
			prefix := e.ODataName + "__"
			for k, v := range row {
				if strings.HasPrefix(k, prefix) {
					colName := k[len(prefix):]
					sub[colName] = toODataValue(v, colTypes[e.ODataName][colName])
				}
			}
			nested[e.ODataName] = sub
		}
		value = append(value, nested)
	}

	resp := map[string]any{
		"@odata.context": fmt.Sprintf("%s/odata/$metadata#Collection(Edm.ComplexType)", baseURL),
		"value":          value,
	}
	if r.URL.Query().Get("$count") == "true" {
		// COUNT(*) over the same FROM/WHERE — strip $top/$skip from a
		// copy of params so it counts the full filtered cross product.
		countParams := make(url.Values)
		for k, v := range params {
			if k != "$top" && k != "$skip" {
				countParams[k] = v
			}
		}
		baseSQL, err := BuildCrossJoinQuery(entities, countParams, snapshot)
		if err == nil {
			countSQL := fmt.Sprintf("SELECT COUNT(*) FROM (%s) AS _cnt", baseSQL)
			if val, err := db.QueryValue(countSQL); err == nil {
				var c int
				fmt.Sscanf(val, "%d", &c)
				resp["@odata.count"] = c
			}
		}
	}

	data, err := json.Marshal(resp)
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
