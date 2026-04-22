// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package execute

import (
	"context"
	"crypto/md5"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/ondatra-labs/ondatrasql/internal/collect"
	"github.com/ondatra-labs/ondatrasql/internal/duckdb"
	"github.com/ondatra-labs/ondatrasql/internal/libregistry"
	"github.com/ondatra-labs/ondatrasql/internal/parser"
	"github.com/ondatra-labs/ondatrasql/internal/script"
)

// sinkExecutor handles outbound sync after materialization.
type sinkExecutor struct {
	runner             *Runner
	model              *parser.Model
	sinkLib            *libregistry.LibFunc
	result             *Result
	rl                 *rateLimiter
	sinkEvents         []collect.SyncEvent // delta from createSinkDelta (nil for table/skip)
	postCommitSnapshot int64               // for reading current state from DuckLake
}

// executeSink runs the outbound sync pipeline after materialization.
// It receives pre-computed SyncEvents (rowid + op + snapshot), batches them,
// reads row data from DuckLake at push time, and calls push() per batch.
func (r *Runner) executeSink(ctx context.Context, model *parser.Model, result *Result, sinkEvents []collect.SyncEvent, postCommitSnapshot int64) error {
	if model.Sink == "" {
		return nil
	}

	if r.libRegistry == nil {
		return fmt.Errorf("sink %q: lib registry not set (call SetLibRegistry before running sink models)", model.Sink)
	}
	sinkLib := r.libRegistry.Get(model.Sink)
	if sinkLib == nil {
		return fmt.Errorf("sink %q not found in lib/", model.Sink)
	}
	if sinkLib.SinkConfig == nil {
		return fmt.Errorf("sink %q has no SINK dict", model.Sink)
	}

	cfg := sinkLib.SinkConfig

	// Defensive guardrail -- should have been caught by ValidateModelSinkCompat at startup
	if err := ValidateModelSinkCompat([]*parser.Model{model}, r.libRegistry); err != nil {
		return err
	}

	// Build rate limiter
	var rl *rateLimiter
	if cfg.RateLimit != nil {
		var err error
		rl, err = newRateLimiter(cfg.RateLimit.Requests, cfg.RateLimit.Per)
		if err != nil {
			return fmt.Errorf("rate limiter: %w", err)
		}
	}

	se := &sinkExecutor{
		runner:             r,
		model:              model,
		sinkLib:            sinkLib,
		result:             result,
		sinkEvents:         sinkEvents,
		postCommitSnapshot: postCommitSnapshot,
		rl:                 rl,
	}

	return se.run(ctx)
}

func (se *sinkExecutor) run(ctx context.Context) error {
	cfg := se.sinkLib.SinkConfig

	// Open SyncStore (Badger-backed, stores only SyncEvent not full rows)
	store, err := se.openSyncStore()
	if err != nil {
		return fmt.Errorf("open sync store: %w", err)
	}
	defer store.Close()

	target := "sync:" + se.model.Target

	// Check for RECENT inflight claims (younger than inflightMaxAge or with heartbeat).
	// Old inflight recovery is handled atomically inside Claim() — no separate step needed.
	hasInflight, inflightErr := store.HasRecentInflight(target)
	if inflightErr != nil {
		return fmt.Errorf("check recent inflight: %w", inflightErr)
	}
	if hasInflight {
		if len(se.sinkEvents) > 0 {
			if err := se.queueDelta(store, target, cfg.BatchMode, true); err != nil {
				return err
			}
			se.writeSyncLogEntry(se.model.Target, "*", "queued",
				fmt.Sprintf("blocked by active inflight, %d events queued", len(se.sinkEvents)))
		} else if se.result.RunType == "backfill" && len(se.sinkEvents) == 0 {
			// Empty backfill with active inflight: the backfill produced 0
			// events but there's still active work. ClearAllAndWrite with
			// nil events clears everything (backfill supersedes).
			if err := store.ClearAllAndWrite(target, nil); err != nil {
				return fmt.Errorf("clear state for empty backfill: %w", err)
			}
			if err := se.callFinalize(ctx, 0, 0); err != nil {
				return fmt.Errorf("finalize (empty backfill, inflight): %w", err)
			}
			se.writeSyncLogEntry(se.model.Target, "*", "batch_complete",
				"0 rows (empty backfill, state cleared)")
			return nil
		} else {
			se.writeSyncLogEntry(se.model.Target, "*", "blocked",
				"blocked by active inflight, no new delta")
		}
		return fmt.Errorf("sink %s: active or recent inflight claims exist. "+
			"Will be processed when inflight resolves", se.model.Sink)
	}

	// --- No inflight: process delta and/or backlog ---

	if len(se.sinkEvents) > 0 {
		if err := se.queueDelta(store, target, cfg.BatchMode, false); err != nil {
			return err
		}
	} else if se.result.RunType == "backfill" {
		// Empty backfill: finalize first, then clear stale backlog.
		if err := se.callFinalize(ctx, 0, 0); err != nil {
			return fmt.Errorf("finalize (empty backfill): %w", err)
		}
		if err := store.ClearAll(target); err != nil {
			return fmt.Errorf("clear stale backlog for empty backfill: %w", err)
		}
		return nil
	} else {
		hasPending, pendErr := store.HasPendingEvents(target)
		if pendErr != nil {
			return fmt.Errorf("check pending events: %w", pendErr)
		}
		if !hasPending {
			return nil
		}
	}

	// Process loop: claim batches from Badger until empty.
	// This handles both fresh delta and backlog from previous failed runs.
	batchSize := cfg.BatchSize
	if batchSize <= 0 {
		batchSize = 1
	}

	maxConcurrent := cfg.MaxConcurrent
	if maxConcurrent <= 0 {
		maxConcurrent = 1
	}
	// atomic/async with max_concurrent > 1 is rejected at startup by ValidateModelSinkCompat

	var (
		mu         sync.Mutex
		syncErrors []string
		succeeded  int64
		failed     int64
	)

	sem := make(chan struct{}, maxConcurrent)
	var wg sync.WaitGroup
	batchNum := 0

	for {
		if ctx.Err() != nil {
			break
		}

		// Claim next batch from Badger
		claimID, claimed, err := store.Claim(target, batchSize)
		if err != nil {
			mu.Lock()
			syncErrors = append(syncErrors, fmt.Sprintf("claim: %v", err))
			mu.Unlock()
			break
		}
		if len(claimed) == 0 {
			break // no more events
		}

		batchNum++
		sem <- struct{}{}
		wg.Add(1)

		go func(num int, events []collect.SyncEvent, cid string) {
			defer func() { <-sem; wg.Done() }()

			// Rate limit
			if se.rl != nil {
				if err := se.rl.Wait(ctx); err != nil {
					nackErr := store.Nack(cid)
					mu.Lock()
					if nackErr != nil {
						syncErrors = append(syncErrors, fmt.Sprintf("rate limit cancelled and nack failed: %v; nack: %v", err, nackErr))
					} else {
						syncErrors = append(syncErrors, fmt.Sprintf("rate limit cancelled: %v", err))
					}
					failed += int64(len(events))
					mu.Unlock()
					return
				}
			}

			batchResult := se.executeBatch(ctx, events, cid, num, store)

			mu.Lock()
			succeeded += batchResult.ok
			failed += batchResult.failed
			if batchResult.err != nil {
				syncErrors = append(syncErrors, batchResult.err.Error())
			}
			se.result.Warnings = append(se.result.Warnings, batchResult.warnings...)
			mu.Unlock()
		}(batchNum, claimed, claimID)
	}

	wg.Wait()

	// Context cancellation is a real error -- backlog may remain unprocessed
	if ctx.Err() != nil {
		mu.Lock()
		syncErrors = append(syncErrors, fmt.Sprintf("cancelled: %v (unprocessed backlog may remain)", ctx.Err()))
		mu.Unlock()
	}

	// Call finalize() only when all rows succeeded (no retryable failures pending).
	// Partial failures get finalize on the next run after retries complete.
	if succeeded > 0 && failed == 0 && ctx.Err() == nil {
		if err := se.callFinalize(ctx, succeeded, failed); err != nil {
			syncErrors = append(syncErrors, fmt.Sprintf("finalize: %v", err))
		}
	}

	// Write sync log (always, even on total failure)
	se.writeSyncLog(succeeded, failed, syncErrors)

	// Add sync stats to result
	se.result.SyncSucceeded = succeeded
	se.result.SyncFailed = failed

	if len(syncErrors) > 0 {
		if failed > 0 {
			se.result.Warnings = append(se.result.Warnings,
				fmt.Sprintf("sink %s: %d succeeded, %d failed", se.model.Sink, succeeded, failed))
			return fmt.Errorf("sink %s: %d of %d rows failed", se.model.Sink, failed, succeeded+failed)
		}
		// Run-level error without row failures (cancellation, finalize, etc.)
		se.result.Warnings = append(se.result.Warnings,
			fmt.Sprintf("sink %s: %s", se.model.Sink, syncErrors[0]))
		return fmt.Errorf("sink %s: %s", se.model.Sink, syncErrors[0])
	}

	return nil
}

// callFinalize calls the optional finalize() function in the sink after all batches.
// Sinks can define finalize(succeeded, failed) for cleanup, commits, or notifications.
// If finalize() is not defined, this is a no-op.
func (se *sinkExecutor) callFinalize(ctx context.Context, succeeded, failed int64) error {
	rt := script.NewRuntime(se.runner.sess, nil, se.runner.projectDir)
	return rt.RunSinkFinalize(ctx, se.model.Sink, succeeded, failed, httpConfigFromLib(se.sinkLib.APIConfig, ctx, se.runner.projectDir))
}

// perRowResult holds the outcome of per-row status validation.
type perRowResult struct {
	OK         []collect.SyncEvent // events whose rows got "ok" or "warn:" status
	Failed     []collect.SyncEvent // events whose rows got "error:" status (retryable)
	Rejected   []collect.SyncEvent // events whose rows got "reject:" status (permanent, dead letter)
	FailErrors []string            // error messages for failed rows
	RejectMsgs []string            // rejection reasons (logged to _sync_log)
	WarnMsgs   []string            // warning messages (logged to _sync_log)
}

// classifyPerRowStatus checks per-row status against rows read from DuckLake,
// then maps results back to the original SyncEvents.
// Status keys from push() must be composite: "rowid:change_type"
// (e.g. "42:insert", "42:update_postimage"). This ensures that update_preimage
// and update_postimage for the same rowid can have independent statuses.
func (se *sinkExecutor) classifyPerRowStatus(perRow map[string]string, rows []map[string]any, events []collect.SyncEvent) (*perRowResult, error) {
	// Validate completeness: every row must have a composite key status.
	// Push must return status keyed by "rowid:change_type".
	var missing []string
	for _, row := range rows {
		key := formatCompositeKey(row["__ondatra_rowid"], row["__ondatra_change_type"])
		if _, ok := perRow[key]; !ok {
			missing = append(missing, key)
		}
	}
	if len(missing) > 0 {
		return nil, fmt.Errorf("push: per-row status missing keys for %d rows (first: %s) — keys must be \"rowid:change_type\"", len(missing), missing[0])
	}

	// Build composite key → status map from rows.
	rowStatus := make(map[string]string, len(rows))
	for _, row := range rows {
		key := formatCompositeKey(row["__ondatra_rowid"], row["__ondatra_change_type"])
		rowStatus[key] = perRow[key]
	}

	// Classify events based on their composite key's status
	result := &perRowResult{}
	for _, event := range events {
		eventKey := fmt.Sprintf("%d:%s", event.RowID, event.ChangeType)
		status, ok := rowStatus[eventKey]
		if !ok {
			// Event's row wasn't in the read set. For deletes this means the
			// snapshot expired — classify as rejected. For all other change types
			// the row was removed by a later materialization — ack as no-op.
			if event.ChangeType == "delete" {
				result.Rejected = append(result.Rejected, event)
				result.RejectMsgs = append(result.RejectMsgs, fmt.Sprintf("row %d: delete row not found (snapshot may have expired)", event.RowID))
				continue
			}
			result.OK = append(result.OK, event) // no-op ack
			continue
		}
		switch {
		case strings.HasPrefix(status, "ok"):
			result.OK = append(result.OK, event)
		case strings.HasPrefix(status, "warn:"):
			result.OK = append(result.OK, event)
			result.WarnMsgs = append(result.WarnMsgs, fmt.Sprintf("row %d: %s", event.RowID, status))
		case strings.HasPrefix(status, "reject:"):
			result.Rejected = append(result.Rejected, event)
			result.RejectMsgs = append(result.RejectMsgs, fmt.Sprintf("row %d: %s", event.RowID, status))
		default:
			result.Failed = append(result.Failed, event)
			result.FailErrors = append(result.FailErrors, fmt.Sprintf("row %d: %s", event.RowID, status))
		}
	}
	return result, nil
}

// formatCompositeKey builds "rowid:change_type" from row fields.
func formatCompositeKey(rowid, changeType any) string {
	return formatRowID(rowid) + ":" + fmt.Sprintf("%v", changeType)
}

// readRowsByEvents reads actual row data from DuckLake for a batch of SyncEvents.
// Current-state rows (insert, update_postimage) are read from the live table.
// Historical rows (delete, update_preimage) are read from the snapshot before the
// change, so push() receives the row data as it was before modification.
// Each row gets __ondatra_rowid and __ondatra_change_type for the Starlark push().
func (se *sinkExecutor) readRowsByEvents(events []collect.SyncEvent) ([]map[string]any, error) {
	target := quoteTarget(se.model.Target)

	// Separate events into current-state (insert, update_postimage) and
	// historical (delete, update_preimage). Same RowID can appear in both
	// (update produces preimage + postimage), so we track change_type per entry.
	type currentEvent struct {
		rid        string
		changeType string
	}
	var currentEvents []currentEvent
	historicalBySnap := make(map[int64][]historicalEvent)

	for _, e := range events {
		rid := fmt.Sprintf("%d", e.RowID)
		if e.ChangeType == "delete" || e.ChangeType == "update_preimage" {
			if e.Snapshot <= 0 {
				return nil, fmt.Errorf("%s event for rowid %d has invalid snapshot %d", e.ChangeType, e.RowID, e.Snapshot)
			}
			historicalBySnap[e.Snapshot-1] = append(historicalBySnap[e.Snapshot-1], historicalEvent{
				rid:        rid,
				changeType: e.ChangeType,
			})
		} else {
			currentEvents = append(currentEvents, currentEvent{rid: rid, changeType: e.ChangeType})
		}
	}

	var allRows []map[string]any

	// Read current-state rows (insert, update_postimage)
	if len(currentEvents) > 0 {
		rids := make([]string, len(currentEvents))
		ridToType := make(map[string]string, len(currentEvents))
		for i, ce := range currentEvents {
			rids[i] = ce.rid
			ridToType[ce.rid] = ce.changeType
		}
		sql := fmt.Sprintf("SELECT *, rowid AS __ondatra_rowid FROM %s WHERE rowid IN (%s)",
			target, strings.Join(rids, ","))
		rows, err := se.runner.sess.QueryRowsAny(sql)
		if err != nil {
			return nil, fmt.Errorf("read rows: %w", err)
		}
		for _, row := range rows {
			ridStr := fmt.Sprintf("%d", toInt64(row["__ondatra_rowid"]))
			row["__ondatra_change_type"] = ridToType[ridStr]
			allRows = append(allRows, row)
		}
	}

	// Read historical rows (delete, update_preimage) from pre-change snapshots
	for snap, hevents := range historicalBySnap {
		rids := make([]string, len(hevents))
		for i, he := range hevents {
			rids[i] = he.rid
		}
		sql := fmt.Sprintf("SELECT *, rowid AS __ondatra_rowid FROM %s AT (VERSION => %d) WHERE rowid IN (%s)",
			target, snap, strings.Join(rids, ","))
		rows, err := se.runner.sess.QueryRowsAny(sql)
		if err != nil {
			return nil, fmt.Errorf("read historical rows at snapshot %d: %w", snap, err)
		}
		// Build rowid → change_type for this snapshot's events
		ridToType := make(map[string]string, len(hevents))
		for _, he := range hevents {
			ridToType[he.rid] = he.changeType
		}
		for _, row := range rows {
			ridStr := fmt.Sprintf("%d", toInt64(row["__ondatra_rowid"]))
			row["__ondatra_change_type"] = ridToType[ridStr]
			allRows = append(allRows, row)
		}
	}

	return allRows, nil
}

// historicalEvent pairs a rowid string with its change_type for snapshot reads.
type historicalEvent struct {
	rid        string
	changeType string
}

// queueDelta writes new sink events to Badger. Strategy is kind-agnostic:
//   - async: WriteBatch only (preserve job_ref for polling resume)
//   - backfill: ClearAllAndWrite (full replace, supersedes everything incl. inflight)
//   - incremental + inflight: WriteBatch (preserve active claims, queue for later)
//   - incremental + no inflight: merge + ClearAllAndWrite (dedup old backlog)
func (se *sinkExecutor) queueDelta(store *collect.SyncStore, target string, batchMode string, hasInflight bool) error {
	if batchMode == "async" {
		if err := store.WriteBatch(target, se.sinkEvents); err != nil {
			return fmt.Errorf("queue delta for async (preserving job_ref): %w", err)
		}
		return nil
	}

	// Backfill is "full replace" — supersedes everything including active
	// inflight claims. The active worker's Ack will fail silently (keys
	// gone), and _sync_acked provides the safety net against double-push.
	if se.result.RunType == "backfill" {
		if err := store.ClearAllAndWrite(target, se.sinkEvents); err != nil {
			return fmt.Errorf("replace all state (backfill): %w", err)
		}
		return nil
	}

	// Incremental with active inflight: WriteBatch only — don't destroy
	// the active worker's claim state. Events queue for later processing.
	if hasInflight {
		if err := store.WriteBatch(target, se.sinkEvents); err != nil {
			return fmt.Errorf("queue delta (preserving inflight): %w", err)
		}
		return nil
	}

	// Incremental, no inflight: merge new delta with existing backlog
	merged, err := se.mergeBacklogWithDelta(store, target, se.sinkEvents)
	if err != nil {
		return fmt.Errorf("merge backlog: %w", err)
	}
	if err := store.ClearAllAndWrite(target, merged); err != nil {
		return fmt.Errorf("replace all state (merged): %w", err)
	}
	return nil
}

// ackAll records the push success in DuckLake (_sync_acked), then acks Badger.
// If Badger ack fails, _sync_acked ensures next run skips already-pushed rows.
func (se *sinkExecutor) ackAll(store *collect.SyncStore, claimID string, batchNum int, events []collect.SyncEvent) batchOutcome {
	target := "sync:" + se.model.Target

	// Step 1: Record in DuckLake (survives Badger failures)
	if err := writeSyncAck(se.runner.sess, claimID, target, int64(len(events))); err != nil {
		// DuckLake write failed -- still ack Badger (push DID succeed)
		se.result.Warnings = append(se.result.Warnings,
			fmt.Sprintf("_sync_acked write failed (push succeeded): %v", err))
	}

	// Step 2: Ack Badger
	if err := store.Ack(claimID); err != nil {
		// Badger ack failed but _sync_acked recorded it.
		// Next run: isSyncAcked → skip re-push → retry ack.
		return batchOutcome{
			ok:     int64(len(events)),
			failed: 0,
			err:    fmt.Errorf("batch %d: badger ack failed but push was recorded in _sync_acked: %w", batchNum, err),
		}
	}

	// Step 3: Cleanup _sync_acked (Badger confirmed, no longer needed)
	deleteSyncAck(se.runner.sess, claimID)

	return allOK(events)
}

// nackAll wraps store.Nack and returns allFailed outcome.
func nackAll(store *collect.SyncStore, claimID string, batchNum int, events []collect.SyncEvent, pushErr error) batchOutcome {
	if nackErr := store.Nack(claimID); nackErr != nil {
		return batchOutcome{
			ok:     0,
			failed: int64(len(events)),
			err:    fmt.Errorf("batch %d: nack failed: nack: %v, push: %w", batchNum, nackErr, pushErr),
		}
	}
	return allFailed(events, pushErr)
}

// batchOutcome holds per-batch stats for accurate reporting.
type batchOutcome struct {
	ok       int64
	failed   int64
	err      error
	warnings []string // collected in goroutine, merged under mutex
}

func allFailed(events []collect.SyncEvent, err error) batchOutcome {
	return batchOutcome{ok: 0, failed: int64(len(events)), err: err}
}

func allOK(events []collect.SyncEvent) batchOutcome {
	return batchOutcome{ok: int64(len(events)), failed: 0, err: nil}
}

// executeBatch reads rows from DuckLake for claimed events, runs push, and handles ack/nack.
func (se *sinkExecutor) executeBatch(ctx context.Context, events []collect.SyncEvent, claimID string, batchNum int, store *collect.SyncStore) batchOutcome {
	cfg := se.sinkLib.SinkConfig
	target := "sync:" + se.model.Target

	// Check if this claim was already pushed (crash recovery: Badger ack
	// failed but _sync_acked recorded the success). Skip re-push, just ack.
	if isSyncAcked(se.runner.sess, claimID) {
		if err := store.Ack(claimID); err != nil {
			return batchOutcome{ok: int64(len(events)), failed: 0,
				err: fmt.Errorf("batch %d: re-ack after crash recovery: %w", batchNum, err)}
		}
		deleteSyncAck(se.runner.sess, claimID)
		return allOK(events)
	}

	// Read actual row data from DuckLake for this batch of events
	rows, readErr := se.readRowsByEvents(events)
	if readErr != nil {
		return nackAll(store, claimID, batchNum, events,
			fmt.Errorf("read rows from DuckLake: %w", readErr))
	}

	// For async: check if there's a saved job_ref from a previous crashed run.
	// Verify the event fingerprint matches the current batch to prevent stale resume.
	if cfg.BatchMode == "async" {
		savedRef, savedHash, loadErr := store.LoadJobRef(target)
		if loadErr != nil {
			return nackAll(store, claimID, batchNum, events,
				fmt.Errorf("load job ref: %w (cannot determine if async job already exists)", loadErr))
		}
		if savedRef != nil {
			currentHash := batchEventHash(events)
			if savedHash == currentHash {
				// Same batch -- resume polling
				return se.pollAsyncJob(ctx, savedRef, rows, events, claimID, batchNum, store, target)
			}
			// Stale job_ref from a different batch -- must delete before new push
			if delErr := store.DeleteJobRef(target); delErr != nil {
				return nackAll(store, claimID, batchNum, events,
					fmt.Errorf("delete stale job_ref: %w (cannot start new push with stale ref)", delErr))
			}
		}
	}

	rt := script.NewRuntime(se.runner.sess, nil, se.runner.projectDir)
	sinkResult, err := rt.RunSink(ctx, se.model.Sink, rows, batchNum, httpConfigFromLib(se.sinkLib.APIConfig, ctx, se.runner.projectDir))

	switch cfg.BatchMode {
	case "sync":
		if err != nil {
			return nackAll(store, claimID, batchNum, events, fmt.Errorf("push: %w", err))
		}
		// sync mode REQUIRES a per-row status dict. None, list, or missing
		// return is a blueprint error -- nack to prevent silent data loss.
		if sinkResult.PerRow == nil {
			return nackAll(store, claimID, batchNum, events,
				fmt.Errorf("push: sync mode requires per-row status dict return, got nil (push must return {\"rowid:change_type\": \"ok\"|\"error: ...\"})"))
		}
		if len(sinkResult.PerRow) == 0 {
			return nackAll(store, claimID, batchNum, events, fmt.Errorf("push: empty per-row status dict"))
		}
		classified, classErr := se.classifyPerRowStatus(sinkResult.PerRow, rows, events)
		if classErr != nil {
			return nackAll(store, claimID, batchNum, events, classErr)
		}

		// Log warnings to _sync_log (rows were delivered, just FYI)
		for _, w := range classified.WarnMsgs {
			se.writeSyncLogEntry(se.model.Target, "*", "warning", w)
		}

		// Log rejections to _sync_log (permanent failures, never retried)
		for _, r := range classified.RejectMsgs {
			se.writeSyncLogEntry(se.model.Target, "*", "rejected", r)
		}

		// Determine what to requeue: only retryable failures, not rejected
		if len(classified.Failed) > 0 || len(classified.Rejected) > 0 {
			// Requeue only retryable failures. Rejected events are acked (removed).
			if err := store.AckAndRequeue(claimID, target, classified.Failed, false); err != nil {
				return nackAll(store, claimID, batchNum, events,
					fmt.Errorf("ack-and-requeue: %w", err))
			}
			return batchOutcome{
				ok:     int64(len(classified.OK)) + int64(len(classified.Rejected)),
				failed: int64(len(classified.Failed)),
				err:    fmt.Errorf("push batch %d: %d ok, %d failed, %d rejected", batchNum, len(classified.OK), len(classified.Failed), len(classified.Rejected)),
			}
		}
		return se.ackAll(store, claimID, batchNum, events)

	case "atomic":
		if err != nil {
			return nackAll(store, claimID, batchNum, events, fmt.Errorf("push: %w", err))
		}
		// atomic push must return None. A non-nil return (e.g. per-row dict)
		// suggests the blueprint was written for sync mode but configured as
		// atomic. Warn but still ack -- the push itself succeeded.
		outcome := se.ackAll(store, claimID, batchNum, events)
		if sinkResult.PerRow != nil || sinkResult.RawReturn != nil {
			outcome.warnings = append(outcome.warnings,
				fmt.Sprintf("sink %s: atomic push returned a value (expected None). Check if batch_mode should be \"sync\" instead", se.model.Sink))
		}
		return outcome

	case "async":
		if err != nil {
			return nackAll(store, claimID, batchNum, events, fmt.Errorf("push: %w", err))
		}
		if sinkResult.RawReturn == nil {
			return nackAll(store, claimID, batchNum, events, fmt.Errorf("push: must return job reference dict"))
		}
		// Persist job_ref in Badger so polling can resume after crash
		if err := store.SaveJobRef(target, sinkResult.RawReturn, batchEventHash(events)); err != nil {
			return nackAll(store, claimID, batchNum, events, fmt.Errorf("save job ref: %w", err))
		}
		return se.pollAsyncJob(ctx, sinkResult.RawReturn, rows, events, claimID, batchNum, store, target)

	default:
		return nackAll(store, claimID, batchNum, events, fmt.Errorf("unknown batch_mode %q", cfg.BatchMode))
	}
}

// pollAsyncJob runs the polling loop for an async batch mode job.
// Used both for fresh push results and for resuming after crash (saved job_ref).
func (se *sinkExecutor) pollAsyncJob(ctx context.Context, jobRef map[string]any, rows []map[string]any, events []collect.SyncEvent, claimID string, batchNum int, store *collect.SyncStore, target string) batchOutcome {
	cfg := se.sinkLib.SinkConfig

	pollInterval, _ := parseDuration(cfg.PollInterval)
	if pollInterval == 0 {
		pollInterval = 30 * time.Second
	}
	pollTimeout, _ := parseDuration(cfg.PollTimeout)
	if pollTimeout == 0 {
		pollTimeout = 1 * time.Hour
	}

	// Heartbeat TTL: 3x poll interval. The heartbeat is refreshed every
	// iteration, so 3x gives margin for slow poll() calls. Using poll_timeout
	// (default 1h) would block crash recovery for far too long.
	heartbeatTTL := 3 * pollInterval

	deadline := time.Now().Add(pollTimeout)
	for time.Now().Before(deadline) {
		// Update heartbeat so RecoverOldInflight won't reclaim this batch
		if err := store.TouchClaim(claimID, heartbeatTTL); err != nil {
			return nackAll(store, claimID, batchNum, events,
				fmt.Errorf("heartbeat failed (claim may be recovered by another run): %w", err))
		}

		pollRt := script.NewRuntime(se.runner.sess, nil, se.runner.projectDir)
		done, perRow, pollErr := pollRt.RunSinkPoll(ctx, se.model.Sink, jobRef, httpConfigFromLib(se.sinkLib.APIConfig, ctx, se.runner.projectDir))
		if pollErr != nil {
			return nackAll(store, claimID, batchNum, events, fmt.Errorf("poll: %w", pollErr))
		}
		if done {
			if perRow == nil {
				return nackAll(store, claimID, batchNum, events,
					fmt.Errorf("poll: done=True but per_row missing"))
			}
			classified, classErr := se.classifyPerRowStatus(perRow, rows, events)
			if classErr != nil {
				return nackAll(store, claimID, batchNum, events, fmt.Errorf("poll: %w", classErr))
			}
			for _, w := range classified.WarnMsgs {
				se.writeSyncLogEntry(se.model.Target, "*", "warning", w)
			}
			for _, r := range classified.RejectMsgs {
				se.writeSyncLogEntry(se.model.Target, "*", "rejected", r)
			}
			if len(classified.Failed) > 0 || len(classified.Rejected) > 0 {
				if err := store.AckAndRequeue(claimID, target, classified.Failed, true); err != nil {
					return nackAll(store, claimID, batchNum, events,
						fmt.Errorf("ack-and-requeue: %w", err))
				}
				return batchOutcome{
					ok:     int64(len(classified.OK)) + int64(len(classified.Rejected)),
					failed: int64(len(classified.Failed)),
					err:    fmt.Errorf("poll batch %d: %d ok, %d failed, %d rejected", batchNum, len(classified.OK), len(classified.Failed), len(classified.Rejected)),
				}
			}
			// Atomically: ack claim + delete job_ref (no failures to requeue)
			if err := store.AckAndRequeue(claimID, target, nil, true); err != nil {
				return batchOutcome{ok: 0, failed: int64(len(events)),
					err: fmt.Errorf("batch %d: ack+delete job_ref: %w", batchNum, err)}
			}
			return allOK(events)
		}

		// Sleep after polling (not before) so crash recovery polls immediately
		select {
		case <-ctx.Done():
			return nackAll(store, claimID, batchNum, events, ctx.Err())
		case <-time.After(pollInterval):
		}
	}
	return nackAll(store, claimID, batchNum, events, fmt.Errorf("poll: timeout after %s", pollTimeout))
}

// httpConfigFromLib converts libregistry.APIConfig to script.APIHTTPConfig.
func httpConfigFromLib(apiCfg *libregistry.APIConfig, ctx context.Context, projectDir string) *script.APIHTTPConfig {
	if apiCfg == nil {
		return nil
	}
	return &script.APIHTTPConfig{
		BaseURL:    apiCfg.BaseURL,
		Headers:    apiCfg.Headers,
		Timeout:    apiCfg.Timeout,
		Retry:      apiCfg.Retry,
		Backoff:    apiCfg.Backoff,
		Auth:       apiCfg.Auth,
		ProjectDir: projectDir,
		Ctx:        ctx,
	}
}

// openSyncStore opens the SyncStore (Badger) for outbound sync tracking.
func (se *sinkExecutor) openSyncStore() (*collect.SyncStore, error) {
	dir := filepath.Join(se.runner.projectDir, ".ondatra", "sync")
	return collect.OpenSyncStore(dir)
}

// writeSyncLog writes aggregate sync stats to _sync_log in DuckLake.
// Best-effort observability -- failures are logged as warnings, not hard errors.
// Badger handles durability; _sync_log is for queryable monitoring.
// ensureSyncLogTable creates _sync_log if it doesn't exist.
func (se *sinkExecutor) ensureSyncLogTable() {
	se.runner.sess.Exec(`CREATE TABLE IF NOT EXISTS _sync_log (
		target VARCHAR NOT NULL,
		sync_key VARCHAR NOT NULL,
		status VARCHAR NOT NULL,
		error_message VARCHAR,
		operation VARCHAR,
		batch_mode VARCHAR,
		synced_at TIMESTAMPTZ DEFAULT current_timestamp,
		dag_run_id VARCHAR
	)`)
}

func (se *sinkExecutor) writeSyncLog(succeeded, failed int64, syncErrors []string) {
	se.ensureSyncLogTable()

	if failed == 0 && len(syncErrors) == 0 {
		se.writeSyncLogEntry(se.model.Target, "*", "batch_complete",
			fmt.Sprintf("%d succeeded", succeeded))
	} else if failed == 0 && len(syncErrors) > 0 {
		// Run errors without row-level failures (e.g. cancellation, ack/jobref errors)
		se.writeSyncLogEntry(se.model.Target, "*", "error",
			fmt.Sprintf("%d succeeded, run error: %s", succeeded, syncErrors[0]))
	} else {
		errorSummary := fmt.Sprintf("%d succeeded, %d failed", succeeded, failed)
		if len(syncErrors) > 0 {
			// Include first error for context
			errorSummary += ": " + syncErrors[0]
		}
		var status string
		if succeeded == 0 {
			status = "rejected"
		} else {
			status = "partial_failure"
		}
		se.writeSyncLogEntry(se.model.Target, "*", status, errorSummary)
	}
}

// writeSyncLogEntry writes a single entry to _sync_log, creating the table if needed.
func (se *sinkExecutor) writeSyncLogEntry(target, syncKey, status, msg string) {
	se.ensureSyncLogTable()
	esc := func(s string) string { return strings.ReplaceAll(s, "'", "''") }
	sql := fmt.Sprintf(`INSERT INTO _sync_log (target, sync_key, status, error_message, operation, batch_mode, dag_run_id)
		VALUES ('%s', '%s', '%s', '%s', 'push', '%s', '%s')`,
		esc(target), esc(syncKey), esc(status), esc(msg),
		esc(se.sinkLib.SinkConfig.BatchMode), esc(se.runner.dagRunID))
	if err := se.runner.sess.Exec(sql); err != nil {
		se.result.Warnings = append(se.result.Warnings,
			fmt.Sprintf("_sync_log: failed to write entry: %v", err))
	}
}

// mergeBacklogWithDelta reads ALL existing events (both pending and inflight) from
// Badger, merges them with new delta events. Dedup by (RowID, ChangeType): if an
// event with the same identity appears in both backlog and delta, the delta version
// wins. This preserves both update_preimage and update_postimage for the same RowID.
// Kind-agnostic — relies on DuckLake's stable row lineage for identity.
//
// Reads both evt: and inflight: because the caller uses ClearAllAndWrite which
// clears everything. Without reading inflight events, they would be silently lost.
func (se *sinkExecutor) mergeBacklogWithDelta(store *collect.SyncStore, target string, delta []collect.SyncEvent) ([]collect.SyncEvent, error) {
	backlog, err := store.ReadAllEvents(target)
	if err != nil {
		return nil, fmt.Errorf("read all events: %w", err)
	}
	if len(backlog) == 0 {
		return delta, nil
	}

	type eventKey struct {
		RowID      int64
		ChangeType string
	}
	deltaKeys := make(map[eventKey]bool, len(delta))
	for _, e := range delta {
		deltaKeys[eventKey{e.RowID, e.ChangeType}] = true
	}

	var result []collect.SyncEvent
	for _, e := range backlog {
		if !deltaKeys[eventKey{e.RowID, e.ChangeType}] {
			result = append(result, e)
		}
	}
	result = append(result, delta...)
	return result, nil
}

// formatRowID converts a rowid value to string matching Starlark's str() output.
// After DuckLake read, rowid may be int64 or float64. Starlark str(0.0) = "0.0",
// but Go fmt.Sprintf("%v", float64(0)) = "0". This function ensures consistency.
func formatRowID(v any) string {
	switch val := v.(type) {
	case float64:
		// Match Starlark: str(float) always includes decimal point
		if val == float64(int64(val)) {
			return fmt.Sprintf("%d.0", int64(val))
		}
		return fmt.Sprintf("%g", val)
	case int64:
		return fmt.Sprintf("%d", val)
	case int:
		return fmt.Sprintf("%d", val)
	default:
		return fmt.Sprintf("%v", v)
	}
}

// batchEventHash computes a content fingerprint of a batch of SyncEvents.
// Used to verify that a saved async job_ref matches the current claimed batch.
// Deterministic: sorts by RowID before hashing.
func batchEventHash(events []collect.SyncEvent) string {
	ids := make([]string, len(events))
	for i, e := range events {
		ids[i] = fmt.Sprintf("%d:%s:%d", e.RowID, e.ChangeType, e.Snapshot)
	}
	sort.Strings(ids)
	return fmt.Sprintf("%x", md5.Sum([]byte(strings.Join(ids, ","))))
}

// quoteTarget quotes a schema.table target for safe SQL usage.
func quoteTarget(target string) string {
	parts := strings.SplitN(target, ".", 2)
	if len(parts) == 2 {
		return duckdb.QuoteIdentifier(parts[0]) + "." + duckdb.QuoteIdentifier(parts[1])
	}
	return duckdb.QuoteIdentifier(target)
}


