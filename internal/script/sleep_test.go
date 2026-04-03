// OndatraSQL - You don't need a data stack anymore
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

//go:build integration

package script

import (
	"context"
	"testing"
	"time"
)

func TestSleepCancelledByContext(t *testing.T) {
	t.Parallel()
	rt := NewRuntime(sharedSess, nil)

	ctx, cancel := context.WithCancel(context.Background())
	// Cancel after 100ms — sleep(60) should abort almost immediately
	go func() {
		time.Sleep(100 * time.Millisecond)
		cancel()
	}()

	start := time.Now()
	_, err := rt.Run(ctx, "test", `sleep(60)`)
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected error from cancelled context")
	}
	if elapsed > 2*time.Second {
		t.Errorf("sleep cancellation took %v, expected <2s", elapsed)
	}
}
