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
	"context"
	"runtime"
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

// L-H7-015 minimal reproduction of the step: drainCommitNotifications
// (pkg/storage/badger_commit_notifications.go) returns at once when another
// goroutine is already delivering, so the committing ApplyCommand returns
// before its own notifications are delivered. Here an earlier CreateNode's
// listener is held inside delivery while the command applies.
//   blocked: expected FAIL (0 events at return, 8 once the drainer is released)
//   control: no drainer in progress, expected PASS (8 at return)
func TestL7015ConcurrentDrainerDefersDelivery(t *testing.T) {
	for _, blocked := range []bool{false, true} {
		t.Run(map[bool]string{false: "control", true: "blocked"}[blocked], func(t *testing.T) {
			b, a := wireFixture(t, false)
			for _, id := range []string{"a", "b", "gone"} {
				_, err := b.CreateNode(t.Context(), wireNode(id, "old"))
				require.NoError(t, err)
			}
			require.NoError(t, b.CreateEdge(t.Context(), wireEdge("old", "a", "b", "old")))
			var events atomic.Int64
			entered, release := make(chan struct{}), make(chan struct{})
			b.OnNodeCreated(func(n *storage.Node) {
				if n.ID == storage.NodeID("wire:blocker") {
					close(entered)
					<-release
					return
				}
				events.Add(1)
			})
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
			creatorDone := make(chan error, 1)
			if blocked {
				go func() {
					_, err := b.CreateNode(context.Background(), wireNode("blocker", "x"))
					creatorDone <- err
				}()
				select {
				case <-entered:
				case <-time.After(20 * time.Second):
					t.Fatal("the blocker's listener was never called")
				}
			} else {
				close(release)
			}
			applied := make(chan error, 1)
			go func() { applied <- applyQueued(t, a, wireCommand(t, "atomic", ops...)) }()
			select {
			case err := <-applied:
				require.NoError(t, err)
			case <-time.After(20 * time.Second):
				close(release)
				t.Fatal("ApplyCommand waited for the drainer (did not return within 20 s)")
			}
			atReturn := events.Load()
			if blocked {
				close(release)
				require.NoError(t, <-creatorDone)
			}
			settled, lag := l7015Settled(&events)
			t.Logf("L7_015 drainer_in_progress=%v events_at_ApplyCommand_return=%d settled=%d (last change %s)", blocked, atReturn, settled, lag)
			require.Equal(t, settled, atReturn, "every event of the command is delivered before ApplyCommand returns")
		})
	}
}

// L-H7-015 natural case: which goroutine delivers the command's events. Same
// steps as the original test, no listener is held. Logs the delivering stack
// whenever an event had not been delivered when ApplyCommand returned.
func TestL7015NaturalDeliveryGoroutine(t *testing.T) {
	b, a := wireFixture(t, false)
	for _, id := range []string{"a", "b", "gone"} {
		_, err := b.CreateNode(t.Context(), wireNode(id, "old"))
		require.NoError(t, err)
	}
	require.NoError(t, b.CreateEdge(t.Context(), wireEdge("old", "a", "b", "old")))
	var events atomic.Int64
	stacks := make(chan string, 64)
	record := func() {
		buf := make([]byte, 16<<10)
		stacks <- string(buf[:runtime.Stack(buf, false)])
		events.Add(1)
	}
	b.OnNodeCreated(func(*storage.Node) { record() })
	b.OnNodeUpdated(func(*storage.Node) { record() })
	b.OnNodeDeleted(func(storage.NodeID) { record() })
	b.OnEdgeCreated(func(*storage.Edge) { record() })
	b.OnEdgeUpdated(func(*storage.Edge) { record() })
	b.OnEdgeDeleted(func(storage.EdgeID) { record() })
	ops := []storage.Operation{
		{Type: storage.OpUpdateNode, Node: wireNode("a", "updated")}, {Type: storage.OpCreateNode, Node: wireNode("new", "created")},
		{Type: storage.OpUpdateEdge, Edge: wireEdge("old", "a", "b", "updated")}, {Type: storage.OpDeleteEdge, EdgeID: storage.EdgeID("wire:old")},
		{Type: storage.OpCreateEdge, Edge: wireEdge("new-edge", "a", "new", "created")}, {Type: storage.OpUpdateEdge, Edge: wireEdge("new-edge", "a", "new", "final")},
		{Type: storage.OpDeleteNode, NodeID: storage.NodeID("wire:gone")}, {Type: storage.OpCreateNode, Node: wireNode("gone", "recreated")},
	}
	self := make([]byte, 64)
	self = self[:runtime.Stack(self, false)]
	require.NoError(t, applyQueued(t, a, wireCommand(t, "atomic", ops...)))
	atReturn := events.Load()
	settled, _ := l7015Settled(&events)
	first := <-stacks
	late := atReturn != settled
	t.Logf("L7_015N late=%v at_return=%d settled=%d caller=%.20q deliverer=%.20q", late, atReturn, settled, self, first)
	if late {
		t.Logf("L7_015N LATE STACK\n%s", first)
	}
}
