// OndatraSQL - A data pipeline framework for DuckDB + DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package script

import (
	"fmt"
	"sync/atomic"

	"github.com/ondatra-labs/ondatrasql/internal/collect"
	dbsess "github.com/ondatra-labs/ondatrasql/internal/duckdb"
)

// badgerCollector writes save() rows to Badger for durable buffering,
// then claims and materializes them into a DuckDB temp table.
type badgerCollector struct {
	target   string
	sess     *dbsess.Session
	store    *collect.Store
	rowCount atomic.Int64
}

// newBadgerCollector opens (or creates) a Badger store at ingestDir.
// Handles crash recovery: inflight claims from a previous crashed run are checked
// against the DuckDB ack table. Already-committed claims are discarded (acked in
// Badger). Uncommitted claims are recovered back to evt: for re-processing.
// Pending evt: events are NOT purged — they are valid unprocessed data.
func newBadgerCollector(target, ingestDir string, sess *dbsess.Session) (*badgerCollector, error) {
	store, err := collect.Open(ingestDir)
	if err != nil {
		return nil, fmt.Errorf("open ingest store: %w", err)
	}

	// Find inflight claims from previous runs and selectively recover or discard.
	inflightClaims, err := store.FindInflightClaims(target)
	if err != nil {
		store.Close()
		return nil, fmt.Errorf("find inflight claims: %w", err)
	}
	for _, claimID := range inflightClaims {
		// Check if this claim was already committed to DuckDB (crash between
		// DuckDB commit and Badger ack). If so, discard from Badger.
		alreadyAcked, ackErr := IsAcked(sess, claimID)
		if ackErr != nil {
			// Cannot determine ack status — recover to evt: as the safe default.
			// Events may be re-processed (at-least-once), but no data is lost.
			if nackErr := store.Nack(claimID); nackErr != nil {
				store.Close()
				return nil, fmt.Errorf("nack claim %s after ack lookup failure: %w (lookup: %v)", claimID, nackErr, ackErr)
			}
			continue
		}
		if alreadyAcked {
			if err := store.Ack(claimID); err != nil {
				store.Close()
				return nil, fmt.Errorf("ack already-committed claim %s: %w", claimID, err)
			}
		} else {
			if err := store.Nack(claimID); err != nil {
				store.Close()
				return nil, fmt.Errorf("recover inflight claim %s: %w", claimID, err)
			}
		}
	}

	return &badgerCollector{
		target: target,
		sess:   sess,
		store:  store,
	}, nil
}

// add writes a single row to Badger.
func (bc *badgerCollector) add(row map[string]interface{}) error {
	if err := bc.store.Write(bc.target, row); err != nil {
		return fmt.Errorf("badger write: %w", err)
	}
	bc.rowCount.Add(1)
	return nil
}

// count returns the number of rows written in this run.
func (bc *badgerCollector) count() int {
	return int(bc.rowCount.Load())
}

// createTempTable claims all events from Badger (both pre-existing and newly
// written) and materializes them into a DuckDB temp table via saveCollector.
// Already-committed claims are handled at startup (newBadgerCollector), so
// all events here are guaranteed to be unprocessed.
// Returns the temp table name, row count, claim IDs for ack/nack, and any error.
func (bc *badgerCollector) createTempTable() (string, int64, []string, error) {
	// Claim all events in batches — includes both pre-existing (recovered)
	// events and newly written events from the current run.
	var allEvents []map[string]any
	var claimIDs []string

	for {
		claimID, events, err := bc.store.Claim(bc.target, 1000)
		if err != nil {
			// Nack any already-claimed batches
			for _, id := range claimIDs {
				bc.store.Nack(id)
			}
			return "", 0, nil, fmt.Errorf("claim events: %w", err)
		}
		if len(events) == 0 {
			break
		}
		claimIDs = append(claimIDs, claimID)
		allEvents = append(allEvents, events...)
	}

	if len(allEvents) == 0 {
		return "", 0, nil, nil
	}

	// Build a saveCollector from claimed events and create the temp table
	sc := &saveCollector{
		target: bc.target,
		sess:   bc.sess,
	}
	for _, event := range allEvents {
		sc.add(event)
	}

	tmpTable, err := sc.createTempTable()
	if err != nil {
		// Nack all claims so events can be retried
		for _, id := range claimIDs {
			bc.store.Nack(id)
		}
		return "", 0, nil, fmt.Errorf("create temp table from claims: %w", err)
	}

	return tmpTable, int64(len(allEvents)), claimIDs, nil
}

// ack acknowledges all claim IDs (events successfully committed to DuckDB).
func (bc *badgerCollector) ack(claimIDs []string) error {
	for _, id := range claimIDs {
		if err := bc.store.Ack(id); err != nil {
			return fmt.Errorf("ack claim %s: %w", id, err)
		}
	}
	return nil
}

// nack returns all claimed events back to the queue (DuckDB commit failed).
func (bc *badgerCollector) nack(claimIDs []string) error {
	for _, id := range claimIDs {
		if err := bc.store.Nack(id); err != nil {
			return fmt.Errorf("nack claim %s: %w", id, err)
		}
	}
	return nil
}

// close closes the Badger store.
func (bc *badgerCollector) close() error {
	if bc.store != nil {
		return bc.store.Close()
	}
	return nil
}
