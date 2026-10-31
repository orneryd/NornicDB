package replication

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A physical receipt must acknowledge committed data independently of a slow listener.
func TestTransactionWirePhysicalOutcomeDoesNotWaitForListeners(t *testing.T) {
	engine, adapter := wireFixture(t, false)
	entered, release := make(chan struct{}), make(chan struct{})
	var unblock sync.Once
	t.Cleanup(func() { unblock.Do(func() { close(release) }) })
	var calls atomic.Int64
	engine.OnNodeCreated(func(*storage.Node) { close(entered); <-release })
	engine.OnNodeCreated(func(*storage.Node) { calls.Add(1) })
	command := wireCommand(t, "causal-listener", storage.Operation{Type: storage.OpCreateNode, Node: wireNode("causal", "committed")})
	returned := make(chan error, 1)
	go func() { returned <- adapter.ApplyCommand(command) }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("listener delivery was not reached")
	}
	select {
	case err := <-returned:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("physical-only ApplyCommand did not return")
	}
	node, err := engine.GetStoredNode("wire:causal")
	require.NoError(t, err)
	require.Equal(t, "committed", node.Properties["value"])
	countAtReturn := calls.Load()
	unblock.Do(func() { close(release) })
	require.Eventually(t, func() bool { return calls.Load() == 1 }, 5*time.Second, time.Millisecond)
	t.Logf("CAUSAL physical_returned=true entity_visible=true callbacks_at_return=%d callbacks_after_delivery=%d", countAtReturn, calls.Load())
	require.Zero(t, countAtReturn, "physical completion must not wait for the blocked listener")
}
