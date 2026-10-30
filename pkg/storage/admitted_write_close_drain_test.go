package storage

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Author: NOR-288 lane (Claude), 2026-10-30. Close ordering after the entity
// authority waits took the engine owner's work context: a write admitted
// before Close and waiting on a held entity authority still completes during
// Close's drain, Close returns only after it, and its effect is durable. The
// owner's context is cancelled only after the drain, so the wait is not cut.
func TestAdmittedWriteCompletesDuringCloseDrain(t *testing.T) {
	for _, tc := range []struct {
		name   string
		edge   bool
		mutate func(*BadgerEngine, string) error
	}{
		{name: "delete_node", mutate: func(b *BadgerEngine, id string) error { return b.DeleteNode(NodeID(id)) }},
		{name: "delete_edge", edge: true, mutate: func(b *BadgerEngine, id string) error { return b.DeleteEdge(EdgeID(id)) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			b, err := NewBadgerEngine(dir)
			require.NoError(t, err)
			require.NoError(t, b.BulkCreateNodes([]*Node{{ID: "test:a"}, {ID: "test:b"}}))
			require.NoError(t, b.BulkCreateEdges([]*Edge{{ID: "test:r", StartNode: "test:a", EndNode: "test:b", Type: "R"}}))
			target := "test:a"
			if tc.edge {
				target = "test:r"
			}
			// A prepared commit holds the target's entity authority.
			tx, err := b.BeginTransaction()
			require.NoError(t, err)
			if tc.edge {
				edge, err := tx.GetStoredEdge(EdgeID(target))
				require.NoError(t, err)
				edge.Properties = map[string]any{"prepared": true}
				require.NoError(t, tx.UpdateEdge(edge))
			} else {
				node, err := tx.GetStoredNode(NodeID(target))
				require.NoError(t, err)
				node.Properties = map[string]any{"prepared": true}
				require.NoError(t, tx.UpdateGraphNode(node, ClientNodeEdits))
			}
			prepared, err := tx.PrepareEntityCommit(context.Background(), EntityCommitRequest{LogicalID: "drain-holder"})
			require.NoError(t, err)
			var abortOnce sync.Once
			abort := func() { abortOnce.Do(prepared.Abort) }
			t.Cleanup(abort)
			waiting := make(chan struct{})
			var waitOnce sync.Once
			restore := SetUniqueKeyLockWaitHookForTesting(func() { waitOnce.Do(func() { close(waiting) }) })
			t.Cleanup(restore)
			// The admitted write waits on the held authority.
			written := make(chan error, 1)
			go func() { written <- tc.mutate(b, target) }()
			select {
			case <-waiting:
			case err := <-written:
				t.Fatalf("write did not wait on the held authority: %v", err)
			case <-time.After(5 * time.Second):
				t.Fatal("write never waited on the held authority")
			}
			// Close begins: writers are refused and the drain waits for both.
			closed := make(chan error, 1)
			go func() { closed <- b.Close() }()
			deadline := time.After(5 * time.Second)
			for {
				b.mu.RLock()
				closing := b.closing
				b.mu.RUnlock()
				if closing {
					break
				}
				select {
				case <-deadline:
					t.Fatal("Close did not begin refusing writers")
				default:
					runtime.Gosched()
				}
			}
			select {
			case err := <-closed:
				t.Fatalf("Close returned before the admitted write finished: %v", err)
			case err := <-written:
				t.Fatalf("admitted write ended before its authority was released: %v", err)
			case <-time.After(100 * time.Millisecond):
			}
			// The holder settles; the admitted write completes during the drain.
			abort()
			select {
			case err := <-written:
				require.NoError(t, err, "an admitted write completes during Close's drain")
				require.False(t, errors.Is(err, context.Canceled))
			case <-time.After(10 * time.Second):
				t.Fatal("admitted write did not complete during the drain")
			}
			select {
			case err := <-closed:
				require.NoError(t, err)
			case <-time.After(10 * time.Second):
				t.Fatal("Close did not return after the drain")
			}
			reopened, err := NewBadgerEngine(dir)
			require.NoError(t, err)
			t.Cleanup(func() { _ = reopened.Close() })
			if tc.edge {
				_, err = reopened.GetStoredEdge(EdgeID(target))
			} else {
				_, err = reopened.GetStoredNode(NodeID(target))
			}
			require.ErrorIs(t, err, ErrNotFound, "the drained write is durable")
		})
	}
}

// The unique-value lock waits (CreateNode, UpdateNode, CreateEdge, UpdateEdge,
// transaction commit publication) obey the same Close ordering: an admitted
// write waiting on a held unique value completes during the drain and is
// durable.
func TestAdmittedUniqueValueWriteCompletesDuringCloseDrain(t *testing.T) {
	for _, tc := range []struct {
		name  string
		write func(*BadgerEngine) error
		check func(*BadgerEngine) error
	}{
		{name: "create_node", write: func(b *BadgerEngine) error {
			_, err := b.CreateNode(&Node{ID: "test:n", Labels: []string{"Doc"}, Properties: map[string]any{"uid": "v"}})
			return err
		}, check: func(b *BadgerEngine) error { _, err := b.GetStoredNode("test:n"); return err }},
		{name: "update_node", write: func(b *BadgerEngine) error {
			return b.UpdateNode(&Node{ID: "test:a", Labels: []string{"Doc"}, Properties: map[string]any{"uid": "v"}})
		}, check: func(b *BadgerEngine) error {
			n, err := b.GetStoredNode("test:a")
			if err == nil && n.Properties["uid"] != "v" {
				err = errors.New("update not durable")
			}
			return err
		}},
		{name: "create_edge", write: func(b *BadgerEngine) error {
			return b.CreateEdge(&Edge{ID: "test:new", StartNode: "test:a", EndNode: "test:b", Type: "LINK", Properties: map[string]any{"uid": "v"}})
		}, check: func(b *BadgerEngine) error { _, err := b.GetStoredEdge("test:new"); return err }},
		{name: "update_edge", write: func(b *BadgerEngine) error {
			return b.UpdateEdge(&Edge{ID: "test:r", StartNode: "test:a", EndNode: "test:b", Type: "LINK", Properties: map[string]any{"uid": "v"}})
		}, check: func(b *BadgerEngine) error {
			e, err := b.GetStoredEdge("test:r")
			if err == nil && e.Properties["uid"] != "v" {
				err = errors.New("update not durable")
			}
			return err
		}},
		{name: "commit", write: func(b *BadgerEngine) error {
			tx, err := b.BeginTransaction()
			if err != nil {
				return err
			}
			if _, err := tx.CreateNode(&Node{ID: "test:u", Labels: []string{"Doc"}, Properties: map[string]any{"uid": "v"}}); err != nil {
				return err
			}
			return tx.Commit()
		}, check: func(b *BadgerEngine) error { _, err := b.GetStoredNode("test:u"); return err }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			b, err := NewBadgerEngine(dir)
			require.NoError(t, err)
			schema := b.GetSchemaForNamespace("test")
			require.NoError(t, schema.AddConstraint(Constraint{Name: "doc_uid", Type: ConstraintUnique, EntityType: ConstraintEntityNode, Label: "Doc", Properties: []string{"uid"}}))
			require.NoError(t, schema.AddConstraint(Constraint{Name: "link_uid", Type: ConstraintUnique, EntityType: ConstraintEntityRelationship, Label: "LINK", Properties: []string{"uid"}}))
			require.NoError(t, b.BulkCreateNodes([]*Node{{ID: "test:a"}, {ID: "test:b"}}))
			require.NoError(t, b.BulkCreateEdges([]*Edge{{ID: "test:r", StartNode: "test:a", EndNode: "test:b", Type: "LINK", Properties: map[string]any{"uid": "old"}}}))
			// Another owner holds the unique value the write needs.
			release, err := schema.acquireUniqueConstraintCommitLocks(context.Background(), "drain-holder", append(
				schema.appendConstraintValueKeys(nil, []*Node{{ID: "test:holder", Labels: []string{"Doc"}, Properties: map[string]any{"uid": "v"}}}, nil),
				schema.appendConstraintValueKeys(nil, nil, []*Edge{{ID: "test:holder", StartNode: "test:a", EndNode: "test:b", Type: "LINK", Properties: map[string]any{"uid": "v"}}})...))
			require.NoError(t, err)
			var releaseOnce sync.Once
			t.Cleanup(func() { releaseOnce.Do(release) })
			waiting := make(chan struct{})
			var waitOnce sync.Once
			restore := SetUniqueKeyLockWaitHookForTesting(func() { waitOnce.Do(func() { close(waiting) }) })
			t.Cleanup(restore)
			written := make(chan error, 1)
			go func() { written <- tc.write(b) }()
			select {
			case <-waiting:
			case err := <-written:
				t.Fatalf("write did not wait on the held unique value: %v", err)
			case <-time.After(5 * time.Second):
				t.Fatal("write never waited on the held unique value")
			}
			closed := make(chan error, 1)
			go func() { closed <- b.Close() }()
			deadline := time.After(5 * time.Second)
			for {
				b.mu.RLock()
				closing := b.closing
				b.mu.RUnlock()
				if closing {
					break
				}
				select {
				case <-deadline:
					t.Fatal("Close did not begin refusing writers")
				default:
					runtime.Gosched()
				}
			}
			select {
			case err := <-closed:
				t.Fatalf("Close returned before the admitted write finished: %v", err)
			case err := <-written:
				t.Fatalf("admitted write ended before the value was released: %v", err)
			case <-time.After(100 * time.Millisecond):
			}
			releaseOnce.Do(release)
			select {
			case err := <-written:
				require.NoError(t, err, "an admitted write completes during Close's drain")
			case <-time.After(10 * time.Second):
				t.Fatal("admitted write did not complete during the drain")
			}
			select {
			case err := <-closed:
				require.NoError(t, err)
			case <-time.After(10 * time.Second):
				t.Fatal("Close did not return after the drain")
			}
			reopened, err := NewBadgerEngine(dir)
			require.NoError(t, err)
			t.Cleanup(func() { _ = reopened.Close() })
			require.NoError(t, tc.check(reopened), "the drained write is durable")
		})
	}
}

// A suppression a delete ended is retired by its batch write (the enrolled
// helper, or Close's own drain when Close comes first), whose authority and
// namespace waits take the engine owner's context: Close finishes it and the
// reopened store keeps neither the node nor its suppression.
func TestCloseFinishesEndedSuppressionsThroughDrain(t *testing.T) {
	dir := t.TempDir()
	b, err := NewBadgerEngine(dir)
	require.NoError(t, err)
	_, err = b.CreateNode(&Node{ID: "test:c", Labels: []string{"Doc"}})
	require.NoError(t, err)
	require.NoError(t, b.SetManualSuppression(StoredEntityNodeKind, "test:c", true))
	require.True(t, b.HasManualSuppressions())
	require.NoError(t, b.DeleteNode("test:c"))
	closed := make(chan error, 1)
	go func() { closed <- b.Close() }()
	select {
	case err := <-closed:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("Close did not finish the ended suppression")
	}
	reopened, err := NewBadgerEngine(dir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopened.Close() })
	require.False(t, reopened.HasManualSuppressions(), "the ended suppression was retired")
	_, err = reopened.GetStoredNode("test:c")
	require.ErrorIs(t, err, ErrNotFound)
}
