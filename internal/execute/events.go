// OndatraSQL - A data pipeline framework for DuckDB + DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package execute

import (
	"bytes"
	"context"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	duckdbdriver "github.com/duckdb/duckdb-go/v2"
	"github.com/ondatra-labs/ondatrasql/internal/backfill"
	"github.com/ondatra-labs/ondatrasql/internal/parser"
	"github.com/ondatra-labs/ondatrasql/internal/script"
	sqltpl "github.com/ondatra-labs/ondatrasql/internal/sql"
)

// httpClient is a shared client with timeouts for daemon communication.
var httpClient = &http.Client{Timeout: 30 * time.Second}

// runEvents flushes buffered events from the daemon's Badger store into DuckLake.
//
// Flow:
// 1. CREATE TABLE IF NOT EXISTS (from model column definitions)
// 2. Check daemon is running via admin health endpoint
// 3. Recover: check inflight claims against ack table, ack already-committed ones
// 4. Loop: claim → temp table → transactional INSERT + ack record → daemon ack
// 5. Commit pipeline metadata
func (r *Runner) runEvents(ctx context.Context, model *parser.Model, result *Result, start time.Time) (*Result, error) {
	result.RunType = "flush"
	result.RunReason = "event flush"

	// Step 1: Ensure the target table exists
	stepStart := time.Now()
	if err := r.ensureSchemaExists(model.Target); err != nil {
		return nil, fmt.Errorf("ensure schema: %w", err)
	}
	createSQL := buildEventsCreateSQL(model)
	if err := r.sess.Exec(createSQL); err != nil {
		r.trace(result, "create_table", stepStart, "error")
		return nil, fmt.Errorf("create events table: %w", err)
	}
	r.trace(result, "create_table", stepStart, "ok")

	// Step 2: Check daemon is running
	stepStart = time.Now()
	if !isDaemonRunning(ctx, r.adminPort) {
		r.trace(result, "check_daemon", stepStart, "error")
		return nil, fmt.Errorf("event daemon is not running. Start it with: COLLECT_PORT=8080 ondatrasql daemon")
	}
	r.trace(result, "check_daemon", stepStart, "ok")

	// Step 3: Recover inflight claims from previous crashed runs.
	// Check each against the ack table — already-committed claims are acked in
	// the daemon (discarded). Uncommitted claims are left for normal recovery.
	// This MUST succeed before flushing — otherwise we risk duplicates.
	stepStart = time.Now()
	schema, table := formatTarget(model.Target)
	if err := script.EnsureAckTable(r.sess); err != nil {
		r.trace(result, "recover_inflight", stepStart, "error")
		return nil, fmt.Errorf("ensure ack table: %w", err)
	}
	if err := r.recoverInflightEvents(ctx, schema, table); err != nil {
		r.trace(result, "recover_inflight", stepStart, "error")
		return nil, fmt.Errorf("inflight recovery: %w", err)
	}
	r.trace(result, "recover_inflight", stepStart, "ok")

	// Step 4: Flush loop — claim events, insert via temp table, ack
	var totalRows int64

	for {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		// Claim batch
		stepStart = time.Now()
		limit := r.claimLimit
		if limit <= 0 {
			limit = 10000
		}
		claimResp, err := claimEvents(ctx, r.adminPort, schema, table, limit)
		if err != nil {
			r.trace(result, "claim", stepStart, "error")
			return nil, fmt.Errorf("claim events: %w", err)
		}
		r.trace(result, "claim", stepStart, "ok")

		if len(claimResp.Events) == 0 {
			break
		}

		// Insert into temp table via Appender
		stepStart = time.Now()
		tmpTable := "tmp_evt_" + sanitizeTarget(model.Target)
		r.sess.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", tmpTable))

		insertErr := r.insertEventsToTemp(model, claimResp.Events, tmpTable)
		if insertErr != nil {
			r.trace(result, "insert_temp", stepStart, "error")
			nackEvents(ctx, r.adminPort, schema, table, claimResp.ClaimID)
			return nil, fmt.Errorf("insert events to temp: %w", insertErr)
		}
		r.trace(result, "insert_temp", stepStart, "ok")

		// Transactional INSERT INTO target + ack record (atomic)
		stepStart = time.Now()
		ackSQL := script.AckSQL(claimResp.ClaimID, model.Target, int64(len(claimResp.Events)))
		txnSQL := fmt.Sprintf("BEGIN;\n%s;\nINSERT INTO %s SELECT * FROM %s;\nCOMMIT",
			ackSQL, model.Target, tmpTable)

		if err := r.sess.Exec(txnSQL); err != nil {
			r.trace(result, "commit_batch", stepStart, "error")
			nackEvents(ctx, r.adminPort, schema, table, claimResp.ClaimID)
			r.sess.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", tmpTable))
			return nil, fmt.Errorf("commit events batch: %w", err)
		}
		r.trace(result, "commit_batch", stepStart, "ok")

		// Daemon ack — data + ack record are committed. If we crash here,
		// recoverInflightEvents() will find the ack record and ack the daemon.
		stepStart = time.Now()
		daemonAckErr := ackEvents(ctx, r.adminPort, schema, table, claimResp.ClaimID)
		if daemonAckErr != nil {
			daemonAckErr = ackEvents(ctx, r.adminPort, schema, table, claimResp.ClaimID)
		}
		if daemonAckErr != nil {
			r.trace(result, "ack", stepStart, "error")
			result.Warnings = append(result.Warnings, fmt.Sprintf("daemon ack failed for claim %s (data committed, will be deduped on recovery): %v", claimResp.ClaimID, daemonAckErr))
		} else {
			r.trace(result, "ack", stepStart, "ok")
			// Daemon acked — crash window closed, delete ack record.
			script.DeleteAck(r.sess, claimResp.ClaimID)
		}

		r.sess.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", tmpTable))
		totalRows += int64(len(claimResp.Events))
	}

	result.RowsAffected = totalRows

	// Step 5: Commit metadata
	if totalRows > 0 {
		stepStart = time.Now()
		sqlHash := backfill.ModelHash(model.SQL, backfill.ModelDirectives{Kind: model.Kind})
		_, err := r.materializeEvents(model, sqlHash, result.RunType, result, start)
		if err != nil {
			r.trace(result, "commit", stepStart, "error")
			return nil, fmt.Errorf("commit metadata: %w", err)
		}
		r.trace(result, "commit", stepStart, "ok")
	}

	result.Duration = time.Since(start)
	return result, nil
}

// recoverInflightEvents checks inflight claims in the daemon against the DuckDB
// ack table. Claims that were already committed (crash between DuckDB commit and
// daemon ack) are acked in the daemon. Uncommitted claims are left in inflight
// for the daemon's normal recovery (recoverOldInflight during Claim).
func (r *Runner) recoverInflightEvents(ctx context.Context, schema, table string) error {
	claimIDs, err := getInflightClaims(ctx, r.adminPort, schema, table)
	if err != nil {
		return fmt.Errorf("get inflight claims: %w", err)
	}

	var errs []string
	for _, claimID := range claimIDs {
		alreadyAcked, ackErr := script.IsAcked(r.sess, claimID)
		if ackErr != nil {
			errs = append(errs, fmt.Sprintf("check ack for %s: %v", claimID, ackErr))
			continue // Can't determine — leave for normal recovery
		}
		if alreadyAcked {
			// Already committed to DuckDB — ack in daemon to discard
			if err := ackEvents(ctx, r.adminPort, schema, table, claimID); err != nil {
				errs = append(errs, fmt.Sprintf("daemon ack for committed claim %s: %v", claimID, err))
			}
		}
		// Uncommitted claims: left in inflight. The daemon's Claim() will
		// recover them (recoverOldInflight) after the 10-minute threshold.
	}

	if len(errs) > 0 {
		return fmt.Errorf("inflight recovery: %s", strings.Join(errs, "; "))
	}
	return nil
}

// buildEventsCreateSQL generates CREATE TABLE IF NOT EXISTS from column definitions.
func buildEventsCreateSQL(model *parser.Model) string {
	var cols []string
	for _, c := range model.Columns {
		def := c.Name + " " + c.Type
		if c.NotNull {
			def += " NOT NULL"
		}
		cols = append(cols, def)
	}
	return fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s)", model.Target, strings.Join(cols, ", "))
}

// insertEventsToTemp inserts events into a temp table using the DuckDB Appender API.
func (r *Runner) insertEventsToTemp(model *parser.Model, events []map[string]any, tmpTable string) error {
	if len(events) == 0 {
		return nil
	}

	// Build column definitions for temp table
	var colDefs []string
	colNames := make([]string, len(model.Columns))
	for i, c := range model.Columns {
		colNames[i] = c.Name
		colDefs = append(colDefs, c.Name+" "+c.Type)
	}

	createSQL := fmt.Sprintf("CREATE TEMP TABLE %s (%s)", tmpTable, strings.Join(colDefs, ", "))
	if err := r.sess.Exec(createSQL); err != nil {
		return fmt.Errorf("create temp table: %w", err)
	}

	return r.sess.RawConn(func(conn any) error {
		driverConn, ok := conn.(driver.Conn)
		if !ok {
			return fmt.Errorf("unexpected conn type %T", conn)
		}

		appender, err := duckdbdriver.NewAppenderFromConn(driverConn, "", tmpTable)
		if err != nil {
			return fmt.Errorf("create appender: %w", err)
		}

		for _, event := range events {
			row := make([]driver.Value, len(colNames))
			for i, colName := range colNames {
				v, ok := event[colName]
				if !ok || v == nil {
					row[i] = nil
				} else {
					row[i] = toDriverValue(v)
				}
			}
			if err := appender.AppendRow(row...); err != nil {
				appender.Close()
				return fmt.Errorf("append row: %w", err)
			}
		}

		if err := appender.Close(); err != nil {
			return fmt.Errorf("flush appender: %w", err)
		}
		return nil
	})
}

// toDriverValue converts a JSON-decoded Go value to a driver.Value for the Appender.
func toDriverValue(v any) driver.Value {
	switch val := v.(type) {
	case string:
		// Try parsing as timestamp (Appender requires time.Time for TIMESTAMPTZ)
		if t, err := time.Parse(time.RFC3339Nano, val); err == nil {
			return t
		}
		if t, err := time.Parse(time.RFC3339, val); err == nil {
			return t
		}
		return val
	case float64:
		// JSON numbers are float64; pass integers as int64
		if val == float64(int64(val)) {
			return int64(val)
		}
		return val
	case bool:
		return val
	case nil:
		return nil
	default:
		// Complex types (maps, slices) → JSON string
		b, err := json.Marshal(val)
		if err != nil {
			return nil
		}
		return string(b)
	}
}

// formatSQLValue converts a Go value to a SQL literal.
// Used by tests; the main insert path uses the Appender API.
func formatSQLValue(v any) string {
	switch val := v.(type) {
	case string:
		return "'" + strings.ReplaceAll(val, "'", "''") + "'"
	case float64:
		if val == float64(int64(val)) {
			return fmt.Sprintf("%d", int64(val))
		}
		return fmt.Sprintf("%g", val)
	case bool:
		if val {
			return "true"
		}
		return "false"
	case nil:
		return "NULL"
	default:
		b, err := json.Marshal(val)
		if err != nil {
			return "NULL"
		}
		return "'" + strings.ReplaceAll(string(b), "'", "''") + "'"
	}
}

// materializeEvents commits pipeline metadata for an events flush.
func (r *Runner) materializeEvents(model *parser.Model, sqlHash, runType string, result *Result, start time.Time) (int64, error) {
	endTime := time.Now()
	steps := ConvertTraceToSteps(result.Trace)
	info := backfill.CommitInfo{
		Model:      model.Target,
		SQLHash:    sqlHash,
		RunType:    runType,
		DagRunID:   r.dagRunID,
		Depends:    []string{},
		Kind:       model.Kind,
		SourceFile: model.FilePath,
		StartTime:  start.UTC().Format(time.RFC3339),
		EndTime:    endTime.UTC().Format(time.RFC3339),
		DurationMs: endTime.Sub(start).Milliseconds(),
		DuckDBVersion: r.sess.GetVersion(),
		Steps:      steps,
		GitCommit:  r.gitInfo.Commit,
		GitBranch:  r.gitInfo.Branch,
		GitRepoURL: r.gitInfo.RepoURL,
	}
	jsonBytes, err := json.Marshal(info)
	if err != nil {
		return 0, fmt.Errorf("marshal commit metadata: %w", err)
	}
	extraInfo := string(jsonBytes)

	registrySQL := fmt.Sprintf("DELETE FROM _ondatra_registry WHERE target = '%s';\nINSERT INTO _ondatra_registry VALUES ('%s', '%s', current_timestamp)",
		escapeSQL(model.Target), escapeSQL(model.Target), escapeSQL(model.Kind))

	txnSQL := sqltpl.MustFormat("execute/commit.sql", registrySQL, model.Target, escapeSQL(extraInfo))

	return result.RowsAffected, r.sess.Exec(txnSQL)
}

// claimResponse is the JSON response from the daemon's claim endpoint.
type claimResponse struct {
	ClaimID string           `json:"claim_id"`
	Events  []map[string]any `json:"events"`
}

// inflightResponse is the JSON response from the daemon's inflight endpoint.
type inflightResponse struct {
	ClaimIDs []string `json:"claim_ids"`
}

// isDaemonRunning checks if the event daemon is reachable.
func isDaemonRunning(ctx context.Context, adminPort string) bool {
	req, err := http.NewRequestWithContext(ctx, "GET", "http://127.0.0.1:"+adminPort+"/health", nil)
	if err != nil {
		return false
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return false
	}
	resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}

// claimEvents requests a batch of events from the daemon.
func claimEvents(ctx context.Context, adminPort, schema, table string, limit int) (*claimResponse, error) {
	url := fmt.Sprintf("http://127.0.0.1:%s/flush/%s/%s/claim?limit=%d", adminPort, schema, table, limit)
	req, err := http.NewRequestWithContext(ctx, "POST", url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("claim returned %d: %s", resp.StatusCode, string(body))
	}

	var result claimResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decode claim response: %w", err)
	}
	return &result, nil
}

// getInflightClaims returns inflight claim IDs from the daemon.
func getInflightClaims(ctx context.Context, adminPort, schema, table string) ([]string, error) {
	url := fmt.Sprintf("http://127.0.0.1:%s/flush/%s/%s/inflight", adminPort, schema, table)
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("inflight returned %d: %s", resp.StatusCode, string(body))
	}

	var result inflightResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decode inflight response: %w", err)
	}
	return result.ClaimIDs, nil
}

// ackEvents confirms successful insertion.
func ackEvents(ctx context.Context, adminPort, schema, table, claimID string) error {
	url := fmt.Sprintf("http://127.0.0.1:%s/flush/%s/%s/ack", adminPort, schema, table)
	body, _ := json.Marshal(map[string]string{"claim_id": claimID}) //nolint:errcheck // map[string]string cannot fail
	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewBuffer(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := httpClient.Do(req)
	if err != nil {
		return err
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("ack returned %d", resp.StatusCode)
	}
	return nil
}

// nackEvents returns events to the queue after a failed insertion.
// Best-effort: errors are ignored since the main operation already failed.
func nackEvents(ctx context.Context, adminPort, schema, table, claimID string) {
	url := fmt.Sprintf("http://127.0.0.1:%s/flush/%s/%s/nack", adminPort, schema, table)
	body, _ := json.Marshal(map[string]string{"claim_id": claimID}) //nolint:errcheck // map[string]string cannot fail
	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewBuffer(body))
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := httpClient.Do(req)
	if err != nil {
		return
	}
	resp.Body.Close()
}

// formatTarget splits a schema.table target into schema and table parts.
func formatTarget(target string) (string, string) {
	parts := strings.SplitN(target, ".", 2)
	if len(parts) == 2 {
		return parts[0], parts[1]
	}
	return "main", target
}

// sanitizeTarget converts a target name to a safe identifier for temp tables.
func sanitizeTarget(s string) string {
	var b strings.Builder
	for _, c := range s {
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_' {
			b.WriteRune(c)
		} else {
			b.WriteByte('_')
		}
	}
	return b.String()
}
