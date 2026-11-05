package storage

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// The transaction's context-taking bulk reads (label population, whole-graph
// population, edge-type population and stored adjacency) are admitted through
// the transaction's own lock, so the caller's context is their lifetime: a
// request already waiting for that lock ends when its context ends instead of
// waiting for the current holder. The bounded return below is measured before
// the holder releases, the holder keeps its hold, the canceled request leaves no
// hold behind, and the same transaction still answers a healthy caller
// afterwards.
//
// The admission observer pins each wait to the actual tx.mu and records the
// context the read passed to admission, so this covers each native public
// method's own admission rather than an earlier lock.
type transactionBulkReadCase struct {
	name string
	// items is the population a healthy read publishes for the fixture.
	items int
	run   func(ctx context.Context, tx *BadgerTransaction, hub NodeID) (any, error)
}

func transactionBulkReadCases() []transactionBulkReadCase {
	return []transactionBulkReadCase{
		{
			name:  "ReadEdges",
			items: 2,
			run: func(ctx context.Context, tx *BadgerTransaction, hub NodeID) (any, error) {
				return tx.ReadEdges(ctx, TransactionEdgeRead{Kind: TransactionReadOutgoing, Node: hub})
			},
		},
		{
			name:  "AllEdges",
			items: 2,
			run: func(ctx context.Context, tx *BadgerTransaction, hub NodeID) (any, error) {
				return tx.AllEdges(ctx)
			},
		},
		{
			name:  "GetEdgesByType",
			items: 2,
			run: func(ctx context.Context, tx *BadgerTransaction, hub NodeID) (any, error) {
				return tx.GetEdgesByType(ctx, "R")
			},
		},
		{
			name:  "GetNodesByLabel",
			items: 1,
			run: func(ctx context.Context, tx *BadgerTransaction, hub NodeID) (any, error) {
				return tx.GetNodesByLabel(ctx, "Hub")
			},
		},
		{
			name:  "StreamNodesByLabelProjected",
			items: 1,
			run: func(ctx context.Context, tx *BadgerTransaction, hub NodeID) (any, error) {
				visited := 0
				err := tx.StreamNodesByLabelProjected(ctx, "Hub", nil, func(*Node) error {
					visited++
					return nil
				})
				return visited, err
			},
		},
		{
			// The whole-node stream's Label is an index hint, not a filter:
			// callers test membership, so this stream visits the fixture.
			name:  "StreamNodesWithOptions",
			items: 3,
			run: func(ctx context.Context, tx *BadgerTransaction, hub NodeID) (any, error) {
				visited := 0
				err := tx.StreamNodesWithOptions(ctx, StreamNodesOptions{Label: "Hub"}, func(*Node) error {
					visited++
					return nil
				})
				return visited, err
			},
		},
		{
			name:  "AllNodes",
			items: 3,
			run: func(ctx context.Context, tx *BadgerTransaction, hub NodeID) (any, error) {
				return tx.AllNodes(ctx)
			},
		},
		{
			name:  "StreamEdgesByType",
			items: 2,
			run: func(ctx context.Context, tx *BadgerTransaction, hub NodeID) (any, error) {
				visited := 0
				err := tx.StreamEdgesByType(ctx, "R", func(*Edge) error {
					visited++
					return nil
				})
				return visited, err
			},
		},
	}
}

// countBulkReadItems reports how many records a bulk read published.
func countBulkReadItems(t *testing.T, value any) int {
	t.Helper()
	switch published := value.(type) {
	case nil:
		return 0
	case []*Node:
		return len(published)
	case []*Edge:
		return len(published)
	case int:
		return published
	default:
		t.Fatalf("unexpected bulk read result %T", value)
		return -1
	}
}

func TestBulkTransactionReadsCancelWaitingForTransactionLock(t *testing.T) {
	engine := newWriteBehindTestEngine(t, time.Hour)
	hub := seedIncidentContextGraph(t, engine, 2)

	for _, read := range transactionBulkReadCases() {
		t.Run(read.name, func(t *testing.T) {
			tx, err := engine.BeginTransaction(t.Context())
			require.NoError(t, err)
			t.Cleanup(func() { _ = tx.Rollback() })

			healthy, err := read.run(t.Context(), tx, hub)
			require.NoError(t, err, "healthy control materializes the read")
			require.Equal(t, read.items, countBulkReadItems(t, healthy))

			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			waited := make(chan context.Context, 1)
			restore := setAdmissionWaitObserver(func(owner any, observed context.Context) {
				if owner != &tx.mu {
					return
				}
				select {
				case waited <- observed:
				default:
				}
			})
			t.Cleanup(restore)

			tx.mu.Lock()
			released := false
			unlock := func() {
				if released {
					return
				}
				released = true
				tx.mu.Unlock()
			}
			type outcome struct {
				value any
				err   error
			}
			done := make(chan outcome, 1)
			joined := make(chan struct{})
			t.Cleanup(func() {
				cancel()
				unlock()
				select {
				case <-joined:
				case <-time.After(10 * time.Second):
					t.Error("the bulk read goroutine did not join after its admission was released")
				}
			})
			go func() {
				defer close(joined)
				value, err := read.run(ctx, tx, hub)
				done <- outcome{value: value, err: err}
			}()

			var admission context.Context
			select {
			case admission = <-waited:
			case <-time.After(10 * time.Second):
				t.Fatal("the bulk read never reached the transaction's held lock")
			}

			cancel()
			started := time.Now()
			var out outcome
			select {
			case out = <-done:
			case <-time.After(100 * time.Millisecond):
				unlock()
				select {
				case out = <-done:
				case <-time.After(10 * time.Second):
					t.Fatal("the bulk read did not return after its admission was released")
				}
				t.Fatalf("a canceled bulk read waited %v for the lock holder instead of returning within 100ms before the holder released it", time.Since(started))
			}
			elapsed := time.Since(started)
			<-joined
			require.Less(t, elapsed, 100*time.Millisecond,
				"a canceled bulk read returns without waiting for the lock holder")
			require.ErrorIs(t, out.err, context.Canceled)
			require.Zero(t, countBulkReadItems(t, out.value), "a canceled bulk read publishes no partial result")
			require.True(t, admission == ctx, "the bulk read dropped the caller context at admission")

			// The holder released nothing above: the canceled request returned
			// while the lock was still held, and it left no hold of its own
			// behind.
			unlock()
			require.True(t, tx.mu.TryLock(), "the canceled read left no hold on the transaction")
			tx.mu.Unlock()

			retried, err := read.run(t.Context(), tx, hub)
			require.NoError(t, err, "the canceled read leaves the transaction healthy")
			require.Equal(t, read.items, countBulkReadItems(t, retried))
		})
	}
}

// The transaction's own state keeps its precedence at admission: a transaction
// whose client released it, or that is already closed, reports
// ErrTransactionClosed even for a canceled request, and reports it without
// waiting for another holder.
func TestBulkTransactionReadsKeepTransactionStateErrorPrecedence(t *testing.T) {
	engine := newWriteBehindTestEngine(t, time.Hour)
	hub := seedIncidentContextGraph(t, engine, 2)

	for _, read := range transactionBulkReadCases() {
		t.Run(read.name, func(t *testing.T) {
			closed, err := engine.BeginTransaction(t.Context())
			require.NoError(t, err)
			require.NoError(t, closed.Rollback())

			value, err := read.run(t.Context(), closed, hub)
			require.ErrorIs(t, err, ErrTransactionClosed, "a closed transaction reports its closure, not an empty result")
			require.Zero(t, countBulkReadItems(t, value))

			detached, err := engine.BeginTransaction(t.Context())
			require.NoError(t, err)
			t.Cleanup(func() { _ = detached.Rollback() })
			detached.clientDetached.Store(true)

			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			detached.mu.Lock()
			released := false
			unlock := func() {
				if released {
					return
				}
				released = true
				detached.mu.Unlock()
			}
			type outcome struct {
				value any
				err   error
			}
			done := make(chan outcome, 1)
			joined := make(chan struct{})
			t.Cleanup(func() {
				cancel()
				unlock()
				select {
				case <-joined:
				case <-time.After(10 * time.Second):
					t.Error("the released client's read goroutine did not join after its admission was released")
				}
			})
			go func() {
				defer close(joined)
				value, err := read.run(ctx, detached, hub)
				done <- outcome{value: value, err: err}
			}()

			cancel()
			started := time.Now()
			var out outcome
			select {
			case out = <-done:
			case <-time.After(100 * time.Millisecond):
				unlock()
				select {
				case out = <-done:
				case <-time.After(10 * time.Second):
					t.Fatal("the released client's read did not return after its admission was released")
				}
				t.Fatalf("a released client's canceled bulk read waited %v for the lock holder instead of reporting its closure within 100ms before the holder released it", time.Since(started))
			}
			elapsed := time.Since(started)
			<-joined
			require.Less(t, elapsed, 100*time.Millisecond,
				"a released client's bulk read reports its closure without waiting for the lock holder")
			require.ErrorIs(t, out.err, ErrTransactionClosed, "a released client reports its closure ahead of the request's cancellation")
			require.NotErrorIs(t, out.err, context.Canceled)
			require.Zero(t, countBulkReadItems(t, out.value))

			unlock()
			require.True(t, detached.mu.TryLock(), "the released client's read left no hold on the transaction")
			detached.mu.Unlock()
		})
	}
}
