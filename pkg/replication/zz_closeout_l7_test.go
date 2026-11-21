package replication

// Close-out lane L7 (scratch branch only; test-side, not product code).
// L-H7-015: TestPrototypeTransactionWireAtomicAndReceipt failed under -race at
// transaction_wire_prototype_test.go:111 with expected 1, actual 8: the event
// count read after the corrected command (beforeEvents) was 1, and 8 after the
// duplicate command. This diagnostic runs the same steps and records the event
// count right after each ApplyCommand returns and again once it has stopped
// changing, to tell late delivery of the first command's 8 storage callbacks
// from a duplicate delivery by the second (receipted) command.

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func l7015Settled(events *atomic.Int64) (int64, time.Duration) {
	start := time.Now()
	last, stable := events.Load(), time.Now()
	for time.Since(start) < 10*time.Second {
		time.Sleep(10 * time.Millisecond)
		if current := events.Load(); current != last {
			last, stable = current, time.Now()
			continue
		}
		if time.Since(stable) >= 500*time.Millisecond {
			break
		}
	}
	return last, stable.Sub(start)
}

func TestL7015EventDeliveryAfterApply(t *testing.T) {
	b, a := wireFixture(t, false)
	for _, id := range []string{"a", "b", "gone"} {
		_, err := b.CreateNode(t.Context(), wireNode(id, "old"))
		require.NoError(t, err)
	}
	require.NoError(t, b.CreateEdge(t.Context(), wireEdge("old", "a", "b", "old")))
	var events atomic.Int64
	b.OnNodeCreated(func(*storage.Node) { events.Add(1) })
	b.OnNodeUpdated(func(*storage.Node) { events.Add(1) })
	b.OnNodeDeleted(func(storage.NodeID) { events.Add(1) })
	b.OnEdgeCreated(func(*storage.Edge) { events.Add(1) })
	b.OnEdgeUpdated(func(*storage.Edge) { events.Add(1) })
	b.OnEdgeDeleted(func(storage.EdgeID) { events.Add(1) })
	ops := []storage.Operation{
		{Type: storage.OpUpdateNode, Node: wireNode("a", "updated")}, {Type: storage.OpCreateNode, Node: wireNode("new", "created")},
		{Type: storage.OpUpdateEdge, Edge: wireEdge("old", "a", "b", "updated")}, {Type: storage.OpDeleteEdge, EdgeID: storage.EdgeID("wire:old")},
		{Type: storage.OpCreateEdge, Edge: wireEdge("new-edge", "a", "new", "created")}, {Type: storage.OpUpdateEdge, Edge: wireEdge("new-edge", "a", "new", "final")},
		{Type: storage.OpDeleteNode, NodeID: storage.NodeID("wire:gone")}, {Type: storage.OpCreateNode, Node: wireNode("gone", "recreated")},
	}
	invalid := append(append([]storage.Operation(nil), ops...), storage.Operation{Type: storage.OpCreateEdge, Edge: wireEdge("invalid", "a", "missing", "invalid")})
	require.Error(t, applyQueued(t, a, wireCommand(t, "atomic", invalid...)))
	failedSettled, _ := l7015Settled(&events)
	command := wireCommand(t, "atomic", ops...)
	require.NoError(t, applyQueued(t, a, command))
	firstImmediate := events.Load()
	firstSettled, firstLag := l7015Settled(&events)
	require.NoError(t, applyQueued(t, a, command))
	duplicateImmediate := events.Load()
	duplicateSettled, _ := l7015Settled(&events)
	t.Logf("L7_015 events: failed_command=%d first_apply_immediate=%d first_apply_settled=%d (last change %s after return) duplicate_immediate=%d duplicate_settled=%d",
		failedSettled, firstImmediate, firstSettled, firstLag, duplicateImmediate, duplicateSettled)
	require.Zero(t, failedSettled, "the failed command delivers nothing")
	require.Equal(t, firstSettled, duplicateSettled, "the duplicate (receipted) command delivers no further events")
	require.Equal(t, firstImmediate, firstSettled, "every event of the first command is delivered before ApplyCommand returns")
}
