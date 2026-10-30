package replication

// Author: #1667 implementation lane (Codex).

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func admissionCandidateIDs(t *testing.T, b *storage.BadgerEngine) ([]storage.NodeID, []storage.EdgeID) {
	t.Helper()
	nodes, err := b.GetStoredNodesByLabel("ContextCandidate")
	require.NoError(t, err)
	edges, err := b.AllEdges()
	require.NoError(t, err)
	var ns []storage.NodeID
	var es []storage.EdgeID
	for _, n := range nodes {
		ns = append(ns, n.ID)
	}
	for _, e := range edges {
		es = append(es, e.ID)
	}
	return ns, es
}

func TestForwardEntityPreparationCancellation(t *testing.T) {
	for _, kind := range []storage.OperationType{storage.OpCreateNode, storage.OpCreateEdge, storage.OpDeleteNode, storage.OpDeleteEdge} {
		for _, phase := range []string{"homogeneous", "collection"} {
			t.Run(fmt.Sprintf("%s/%s", kind, phase), func(t *testing.T) {
				b, adapter, r, _ := admissionReplicatorFixture(t, ModeStandalone, context.Background())
				engine := NewReplicatedEngine(b, r, time.Second)
				_, err := b.CreateNode(&storage.Node{ID: "test:left"})
				require.NoError(t, err)
				_, err = b.CreateNode(&storage.Node{ID: "test:right"})
				require.NoError(t, err)
				ops := make([]storage.Operation, 20)
				for i := range ops {
					n := &storage.Node{ID: storage.NodeID(fmt.Sprintf("test:forward%d", i)), Labels: []string{"ContextCandidate"}}
					e := &storage.Edge{ID: storage.EdgeID(fmt.Sprintf("test:forward%d", i)), StartNode: "test:left", EndNode: "test:right", Type: "CONTEXT"}
					ops[i] = storage.Operation{Type: kind, Node: n, NodeID: n.ID, Edge: e, EdgeID: e.ID}
					if kind == storage.OpDeleteNode {
						_, err = b.CreateNode(n)
						require.NoError(t, err)
					}
					if kind == storage.OpDeleteEdge {
						require.NoError(t, b.CreateEdge(e))
					}
				}
				ns, es := admissionCandidateIDs(t, b)
				before, err := adapter.GetWALPosition()
				require.NoError(t, err)
				parent, cancel := context.WithCancel(context.Background())
				defer cancel()
				after := 3 // Entry check, then the second homogeneity iteration.
				if phase == "collection" {
					after = len(ops) + 3 // Entry + all homogeneity + second conversion.
				}
				ctx := &preparationContext{Context: parent, owner: "ForwardEntityChanges", after: after, cancel: cancel}
				err = engine.ForwardEntityChanges(ctx, ops, storage.CommitModeImplicit)
				t.Logf("kind=%s phase=%s observations=%d result=%v", kind, phase, ctx.hits, err)
				require.GreaterOrEqual(t, ctx.hits, after)
				require.ErrorIs(t, err, context.Canceled)
				gotNodes, gotEdges := admissionCandidateIDs(t, b)
				require.ElementsMatch(t, ns, gotNodes)
				require.ElementsMatch(t, es, gotEdges)
				position, err := adapter.GetWALPosition()
				require.NoError(t, err)
				require.Equal(t, before, position)
			})
		}
	}
}

func TestManagedReplicationRequestCancellation(t *testing.T) {
	for _, bulk := range []bool{false, true} {
		t.Run(fmt.Sprintf("bulk=%v", bulk), func(t *testing.T) {
			b, adapter, r, barrier := admissionReplicatorFixture(t, ModeStandalone, context.Background())
			engine := NewReplicatedEngine(adapter.engine, r, 3*time.Second)
			manager, err := multidb.NewDatabaseManager(context.Background(), engine, nil)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, manager.Close()) })
			managed, err := manager.GetStorage("nornic")
			require.NoError(t, err)
			parent, cancel := context.WithCancel(context.Background())
			defer cancel()
			require.NoError(t, adapter.FlushWAL(), "catalog setup must finish before the cancellation baseline")
			before, err := adapter.GetWALPosition()
			require.NoError(t, err)
			barrier.armed.Store(true)
			result := make(chan error, 1)
			go func() {
				if !bulk {
					_, err := storage.CreateNodeWithContext(parent, managed, &storage.Node{ID: "managed", Labels: []string{"ContextCandidate"}})
					result <- err
					return
				}
				nodes := make([]*storage.Node, 20)
				for i := range nodes {
					nodes[i] = &storage.Node{ID: storage.NodeID(fmt.Sprintf("managed%d", i)), Labels: []string{"ContextCandidate"}}
				}
				result <- storage.BulkCreateNodesWithContext(parent, managed, nodes)
			}()
			select {
			case <-barrier.entered:
			case <-time.After(5 * time.Second):
				close(barrier.resume)
				t.Fatal("managed public write did not reach native preparation")
			}
			cancel()
			close(barrier.resume)
			select {
			case err = <-result:
			case <-time.After(5 * time.Second):
				t.Fatal("managed write did not settle")
			}
			nodes, readErr := b.GetStoredNodesByLabel("ContextCandidate")
			require.NoError(t, readErr)
			after, readErr := adapter.GetWALPosition()
			require.NoError(t, readErr)
			t.Logf("bulk=%v managed=%v candidates=%d WAL=%d->%d", bulk, err, len(nodes), before, after)
			require.ErrorIs(t, err, context.Canceled)
			require.Empty(t, nodes)
			require.Equal(t, before, after)
		})
	}
}

func TestForwardEntityRequestCancellation(t *testing.T) {
	for _, bulk := range []bool{false, true} {
		t.Run(fmt.Sprintf("bulk=%v", bulk), func(t *testing.T) {
			b, adapter, r, barrier := admissionReplicatorFixture(t, ModeStandalone, context.Background())
			engine := NewReplicatedEngine(b, r, 3*time.Second)
			parent, cancel := context.WithCancel(context.Background())
			defer cancel()
			count, mode := 1, storage.CommitModeDirect
			if bulk {
				count, mode = 20, storage.CommitModeImplicit
			}
			ops := make([]storage.Operation, count)
			for i := range ops {
				ops[i] = storage.Operation{Type: storage.OpCreateNode, Node: &storage.Node{ID: storage.NodeID(fmt.Sprintf("test:forward%d", i)), Labels: []string{"ContextCandidate"}}}
			}
			before, err := adapter.GetWALPosition()
			require.NoError(t, err)
			barrier.armed.Store(true)
			result := make(chan error, 1)
			go func() { result <- engine.ForwardEntityChanges(parent, ops, mode) }()
			select {
			case <-barrier.entered:
			case <-time.After(5 * time.Second):
				close(barrier.resume)
				t.Fatal("forwarded public write did not reach native preparation")
			}
			cancel()
			close(barrier.resume)
			select {
			case err = <-result:
			case <-time.After(5 * time.Second):
				t.Fatal("forwarded write did not settle")
			}
			nodes, readErr := b.GetStoredNodesByLabel("ContextCandidate")
			require.NoError(t, readErr)
			after, readErr := adapter.GetWALPosition()
			require.NoError(t, readErr)
			t.Logf("bulk=%v forward=%v candidates=%d WAL=%d->%d", bulk, err, len(nodes), before, after)
			require.ErrorIs(t, err, context.Canceled)
			require.Empty(t, nodes)
			require.Equal(t, before, after)
		})
	}
}

func TestManagedAcceptedRequestWaitCancellation(t *testing.T) {
	b, adapter, r, _ := admissionReplicatorFixture(t, ModeStandalone, context.Background())
	manager, err := multidb.NewDatabaseManager(context.Background(), NewReplicatedEngine(adapter.engine, r, 3*time.Second), nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, manager.Close()) })
	managed, err := manager.GetStorage("nornic")
	require.NoError(t, err)
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	resume := func() { once.Do(func() { close(release) }) }
	defer resume()
	b.OnNodeCreated(func(n *storage.Node) {
		if n.ID == "nornic:accepted" {
			close(entered)
			<-release
		}
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() {
		_, err := storage.CreateNodeWithContext(ctx, managed, &storage.Node{ID: "accepted"})
		result <- err
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("managed command did not publish before the native listener")
	}
	cancel()
	prompt := true
	select {
	case err = <-result:
	case <-time.After(time.Second):
		prompt = false
		resume()
		select {
		case err = <-result:
		case <-time.After(5 * time.Second):
			t.Fatal("managed command did not finish after listener release")
		}
	}
	t.Logf("cancelled wait returned before listener release=%v result=%v", prompt, err)
	require.True(t, prompt, "request wait is cancellable after acceptance")
	var accepted *storage.AcceptedCommitError
	require.ErrorAs(t, err, &accepted, "accepted work must not masquerade as a rejected command")
	require.Equal(t, storage.CommitRecoveryPending, accepted.State)
	require.ErrorIs(t, err, context.Canceled)
	node, err := b.GetStoredNode("nornic:accepted")
	require.NoError(t, err, "cancelling the waiter must not undo accepted publication")
	require.Equal(t, storage.NodeID("nornic:accepted"), node.ID)
	resume()
}

// Same native authority, different transparent outer pointers: consume the
// retained frame once. A serialized peer must still prepare its own frame.
func TestManagedPreparedFrameThroughTransparentWrappers(t *testing.T) {
	b, adapter, r, _ := admissionReplicatorFixture(t, ModeStandalone, context.Background())
	manager, err := multidb.NewDatabaseManager(context.Background(), NewReplicatedEngine(b, r, 3*time.Second), nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, manager.Close()) })
	managed, err := manager.GetStorage("nornic")
	require.NoError(t, err)
	nodes := make([]*storage.Node, 20)
	for i := range nodes {
		nodes[i] = &storage.Node{ID: storage.NodeID(fmt.Sprintf("retained%d", i)), Labels: []string{"ContextCandidate"}}
	}
	require.NoError(t, adapter.FlushWAL(), "catalog setup is a separate accepted command")
	before, err := adapter.GetWALPosition()
	require.NoError(t, err)
	result := make(chan error, 1)
	go func() { result <- storage.BulkCreateNodesWithContext(context.Background(), managed, nodes) }()
	select {
	case err = <-result:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("same native authority attempted to prepare an already retained frame")
	}
	stored, err := b.GetStoredNodesByLabel("ContextCandidate")
	require.NoError(t, err)
	require.Len(t, stored, len(nodes))
	after, err := adapter.GetWALPosition()
	require.NoError(t, err)
	require.Equal(t, before+1, after, "one accepted command owns one replay entry")
}
