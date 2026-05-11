package main

import (
	"context"

	"github.com/orneryd/nornicdb/pkg/lifecycle"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
)

// workersAdapter wraps *nornicdb.DB so the embed-queue lifecycle
// participates in the lifecycle.Component drain order.
//
// The embed queue is started by nornicdb.Open(...) — the adapter does
// NOT need to call any Start function; Start blocks on <-ctx.Done() so
// its only job is to land StopEmbedQueue() at the correct ordinal in
// the reverse-iteration drain (OBS-08: workers stop AFTER bolt+http but
// BEFORE pprof+telemetry).
//
// This is the W-3 fix for the existing main.go anti-pattern that
// stopped workers FIRST, opposite of OBS-08.
type workersAdapter struct {
	db *nornicdb.DB
}

// Compile-time interface assertion.
var _ lifecycle.Component = (*workersAdapter)(nil)

// Name returns "embed-workers" for supervisor logging.
func (a *workersAdapter) Name() string { return "embed-workers" }

// Start blocks until ctx cancellation. The embed queue is already
// running (started by nornicdb.Open) — this Start is a sentinel block
// that defers shutdown to the supervised drain.
func (a *workersAdapter) Start(ctx context.Context) error {
	<-ctx.Done()
	return nil
}

// Shutdown drains and stops the embed queue. StopEmbedQueue is itself
// idempotent — safe to call even if the queue was never started or has
// already stopped.
func (a *workersAdapter) Shutdown(ctx context.Context) error {
	a.db.StopEmbedQueue()
	return nil
}
