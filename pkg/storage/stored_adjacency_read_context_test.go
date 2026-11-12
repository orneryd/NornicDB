package storage

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// seedIncidentContextGraph creates ctx:hub with count outgoing relationships,
// ctx:hub-[ctx:eN:R]->ctx:leafN.
func seedIncidentContextGraph(t testing.TB, engine *BadgerEngine, count int) NodeID {
	t.Helper()
	const hub NodeID = "ctx:hub"
	_, err := engine.CreateNode(context.Background(), &Node{ID: hub, Labels: []string{"Hub"}})
	require.NoError(t, err)
	for i := range count {
		leaf := NodeID(fmt.Sprintf("ctx:leaf%d", i))
		_, err = engine.CreateNode(context.Background(), &Node{ID: leaf, Labels: []string{"Leaf"}})
		require.NoError(t, err)
		require.NoError(t, engine.CreateEdge(context.Background(), &Edge{
			ID:        EdgeID(fmt.Sprintf("ctx:e%d", i)),
			StartNode: hub,
			EndNode:   leaf,
			Type:      "R",
		}))
	}
	return hub
}

// bufferedIncidentScopeWrite leaves one acknowledged write pending in the
// write-behind buffer for the ctx namespace, so a read of that namespace cannot
// be answered by a completed cache result: it must capture a view across the
// write-behind retirement boundary (captureReadView).
func bufferedIncidentScopeWrite(t testing.TB, engine *BadgerEngine) {
	t.Helper()
	tx, err := engine.BeginTransaction(context.Background())
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	require.NoError(t, tx.SetImplicit(true))
	require.NoError(t, tx.SetNamespace("ctx"))
	_, err = tx.CreateNode(context.Background(), &Node{ID: "ctx:pending", Labels: []string{"Hub"}})
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
}

// A stored adjacency read uses the caller's context at the store's own read
// admission, not only while filtering what that admission returned. The
// write-behind retirement boundary held here is the admission captureReadView
// acquires; a read that substituted a background context for the caller waits
// for it and then enumerates anyway. A canceled read returns the caller's error
// and no partial list, and the same transaction stays healthy.
func TestStoredAdjacencyReadObservesCancellationAtHeldReadAdmission(t *testing.T) {
	engine := newWriteBehindTestEngine(t, time.Hour)
	require.NotNil(t, engine.writeBehind)
	hub := seedIncidentContextGraph(t, engine, 2)
	bufferedIncidentScopeWrite(t, engine)

	tx, err := engine.BeginTransaction(t.Context())
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })

	healthy, err := tx.ReadStoredAdjacentEdges(t.Context(), AdjacentEdgeRead{Node: hub})
	require.NoError(t, err)
	require.Len(t, healthy, 2)

	engine.writeBehind.mu.Lock()
	released := false
	release := func() {
		if released {
			return
		}
		released = true
		engine.writeBehind.mu.Unlock()
	}
	t.Cleanup(release)

	// A canceled caller must not wait for the held admission. The unaffected
	// control below runs last, after this admission is released: a concurrent
	// read would hold the transaction mutex while it waited here.
	ctx, cancel := context.WithCancel(context.Background())
	type outcome struct {
		edges []*Edge
		err   error
	}
	done := make(chan outcome, 1)
	joined := make(chan struct{})
	go func() {
		defer close(joined)
		edges, err := tx.ReadStoredAdjacentEdges(ctx, AdjacentEdgeRead{Node: hub})
		done <- outcome{edges: edges, err: err}
	}()
	t.Cleanup(func() {
		cancel()
		release()
		select {
		case <-joined:
		case <-time.After(10 * time.Second):
			t.Error("the stored adjacency read goroutine did not join after its admission was released")
		}
	})

	cancel()
	var out outcome
	select {
	case out = <-done:
	case <-time.After(500 * time.Millisecond):
		release()
		select {
		case out = <-done:
		case <-time.After(10 * time.Second):
			t.Fatal("the stored adjacency read did not return after its admission was released")
		}
		t.Fatal("the stored adjacency read ignored the caller's cancellation: it waited for the held read admission and ended only when the test released it")
	}
	release()
	<-joined
	require.ErrorIs(t, out.err, context.Canceled)
	require.Nil(t, out.edges, "a canceled read publishes no partial relationship list")

	healthy, err = tx.ReadStoredAdjacentEdges(t.Context(), AdjacentEdgeRead{Node: hub})
	require.NoError(t, err, "the canceled read leaves the transaction healthy")
	require.Len(t, healthy, 2)
}
