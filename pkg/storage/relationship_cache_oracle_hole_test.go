package storage

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// The oracle hole models an earlier ordinary commit delayed at its physical
// write. The later update is physically durable and has revoked its cache scope,
// but cannot publish yet. All payload observations use exported storage reads.
func TestRelationshipCacheRejectedCallbackPreservesInvalidation(t *testing.T) {
	e := NewMemoryEngine()
	t.Cleanup(func() { require.NoError(t, e.Close()) })
	b := e.BadgerEngine
	for _, id := range []NodeID{"test:old-callback-a", "test:old-callback-b"} {
		_, err := b.CreateNode(&Node{ID: id})
		require.NoError(t, err)
	}
	edge := &Edge{ID: "test:old-callback-e", StartNode: "test:old-callback-a", EndNode: "test:old-callback-b", Type: "R", Properties: map[string]any{"v": int64(0)}}
	require.NoError(t, b.CreateEdge(edge))
	_, err := b.GetOutgoingEdges(edge.StartNode)
	require.NoError(t, err)
	entered := [2]chan struct{}{make(chan struct{}), make(chan struct{})}
	resume := [2]chan struct{}{make(chan struct{}), make(chan struct{})}
	var once [2]sync.Once
	release := func(i int) { once[i].Do(func() { close(resume[i]) }) }
	var phase atomic.Int32
	restore := setCommitTailHook(func() {
		i := int(phase.Add(1)) - 1
		if i < 2 {
			close(entered[i])
			<-resume[i]
		}
	})
	defer restore()
	defer release(0)
	defer release(1)
	wait := func(ch <-chan struct{}) {
		t.Helper()
		select {
		case <-ch:
		case <-time.After(10 * time.Second):
			t.Fatal("callback did not reach its post-publication pause")
		}
	}
	old, newer := copyEdge(edge), copyEdge(edge)
	old.Properties["v"], newer.Properties["v"] = int64(1), int64(2)
	oldDone, newDone := make(chan error, 1), make(chan error, 1)
	go func() { oldDone <- b.UpdateEdge(old) }()
	wait(entered[0])
	hole, err := b.db.oracle.assign()
	require.NoError(t, err)
	var finishOnce sync.Once
	finish := func() { finishOnce.Do(func() { b.db.oracle.finish(hole) }) }
	defer finish()
	go func() { newDone <- b.UpdateEdge(newer) }()
	require.Eventually(t, func() bool { return b.nodeCacheCommitTs.Load() > hole }, 10*time.Second, time.Millisecond)
	require.Less(t, b.db.oracle.published.Load(), hole)
	// This rejected callback must not discard the newer write's tombstone.
	release(0)
	require.NoError(t, <-oldDone)
	rows, err := b.GetOutgoingEdges(edge.StartNode)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, int64(1), rows[0].Properties["v"])
	finish()
	wait(entered[1])
	// The durable point-read kernel proves that the new value is visible.
	point, err := b.GetEdge(edge.ID)
	require.NoError(t, err)
	require.Equal(t, int64(2), point.Properties["v"])
	for _, read := range []struct {
		name string
		fn   func(NodeID) ([]*Edge, error)
		id   NodeID
	}{{"outgoing", b.GetOutgoingEdges, edge.StartNode}, {"incoming", b.GetIncomingEdges, edge.EndNode}} {
		rows, err = read.fn(read.id)
		require.NoError(t, err)
		require.Len(t, rows, 1)
		if rows[0].Properties["v"] != int64(2) {
			t.Errorf("%s served old relationship v=%v after durable point read returned v=2", read.name, rows[0].Properties["v"])
		}
	}
	release(1)
	require.NoError(t, <-newDone)
}
