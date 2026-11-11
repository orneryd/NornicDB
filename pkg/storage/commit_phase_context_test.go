package storage

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestAcceptedPreparedCountAdmissionKeepsDetachedCompletion(t *testing.T) {
	b := newTestEngine(t)
	for _, id := range []NodeID{"test:phase-a", "test:phase-b"} {
		_, err := b.CreateNode(t.Context(), &Node{ID: id, Labels: []string{"Phase"}})
		require.NoError(t, err)
	}
	tx, err := b.BeginTransaction(t.Context())
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })
	require.NoError(t, tx.CreateEdge(t.Context(), &Edge{ID: "test:phase-edge", StartNode: "test:phase-a", EndNode: "test:phase-b", Type: "LINK"}))
	requirePositionalChangesPending(t, tx)
	caller, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	prepared, err := tx.PrepareEntityCommit(caller, EntityCommitRequest{LogicalID: tx.ID})
	require.NoError(t, err)
	t.Cleanup(prepared.Abort)
	waiting := make(chan context.Context, 1)
	t.Cleanup(setAdmissionWaitObserver(func(owner any, ctx context.Context) {
		if owner == &b.edgeTypeCountWriteMu {
			select {
			case waiting <- ctx:
			default:
			}
		}
	}))
	b.edgeTypeCountWriteMu.Lock()
	var once sync.Once
	unlock := func() { once.Do(b.edgeTypeCountWriteMu.Unlock) }
	done := make(chan error, 1)
	joined := make(chan struct{})
	var callbacks atomic.Int32
	t.Cleanup(func() {
		cancel()
		unlock()
		select {
		case <-joined:
		case <-time.After(5 * time.Second):
			t.Error("accepted publication worker did not join")
		}
	})
	go func() { defer close(joined); done <- prepared.Commit(func() { callbacks.Add(1) }) }()
	var completion context.Context
	select {
	case completion = <-waiting:
	case <-time.After(5 * time.Second):
		t.Fatal("accepted command did not reach held count publication admission")
	}
	cancel()
	require.ErrorIs(t, caller.Err(), context.Canceled)
	require.NotNil(t, completion)
	require.Nil(t, completion.Done())
	require.NoError(t, completion.Err(), "accepted work must not retain caller or engine-stop cancellation")
	unlock()
	select {
	case err = <-done:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("accepted publication did not complete")
	}
	select {
	case <-joined:
	case <-time.After(5 * time.Second):
		t.Fatal("accepted worker did not join after result")
	}
	edge, err := b.GetStoredEdge(t.Context(), "test:phase-edge")
	require.NoError(t, err)
	require.Equal(t, EdgeID("test:phase-edge"), edge.ID)
	require.EqualValues(t, 1, callbacks.Load())
}
