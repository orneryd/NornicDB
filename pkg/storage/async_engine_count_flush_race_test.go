package storage

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type blockingBulkCreateEngine struct {
	Engine

	updateNodeStarted chan struct{}
	allowUpdateNode   chan struct{}
}

func (b *blockingBulkCreateEngine) UpdateNode(node *Node) error {
	select {
	case <-b.updateNodeStarted:
		// already signaled
	default:
		close(b.updateNodeStarted)
	}
	<-b.allowUpdateNode
	return b.Engine.UpdateNode(node)
}

func TestAsyncEngine_NodeCount_BlocksDuringFlush(t *testing.T) {
	base := NewMemoryEngine()
	t.Cleanup(func() { _ = base.Close() })

	inner := &blockingBulkCreateEngine{
		Engine:                base,
		updateNodeStarted:      make(chan struct{}),
		allowUpdateNode:        make(chan struct{}),
	}

	ae := NewAsyncEngine(inner, &AsyncEngineConfig{
		FlushInterval:    time.Hour, // manual flush only
		MaxNodeCacheSize: 0,
		MaxEdgeCacheSize: 0,
	})
	t.Cleanup(func() { _ = ae.Close() })

	// Queue some nodes for flush.
	_, err := ae.CreateNode(&Node{ID: "nornic:node-1", Labels: []string{"N"}})
	require.NoError(t, err)
	_, err = ae.CreateNode(&Node{ID: "nornic:node-2", Labels: []string{"N"}})
	require.NoError(t, err)

	flushDone := make(chan error, 1)
	go func() { flushDone <- ae.Flush() }()

	// Wait until flush reaches the underlying UpdateNode call (still holding flushMu.Lock()).
	select {
	case <-inner.updateNodeStarted:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("flush did not reach UpdateNode")
	}

	// NodeCount should block while flush holds flushMu.Lock().
	countDone := make(chan int64, 1)
	go func() {
		n, _ := ae.NodeCount()
		countDone <- n
	}()

	select {
	case <-countDone:
		t.Fatal("NodeCount returned while flush was in progress (should block)")
	case <-time.After(50 * time.Millisecond):
		// expected: still blocked
	}

	// Allow flush to proceed and finish.
	close(inner.allowUpdateNode)
	require.NoError(t, <-flushDone)

	// Now NodeCount should complete and match expected value.
	select {
	case got := <-countDone:
		require.Equal(t, int64(2), got)
	case <-time.After(500 * time.Millisecond):
		t.Fatal("NodeCount did not return after flush completed")
	}
}
