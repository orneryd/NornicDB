package storage

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestNativeEntityHelpersRespectPreparedAuthority(t *testing.T) {
	cases := []struct {
		name   string
		edge   bool
		mutate func(*BadgerEngine, string) error
	}{
		{name: "delete_node", mutate: func(b *BadgerEngine, id string) error { return b.DeleteNode(NodeID(id)) }},
		{name: "delete_edge", edge: true, mutate: func(b *BadgerEngine, id string) error { return b.DeleteEdge(EdgeID(id)) }},
		{name: "sidecar", mutate: func(b *BadgerEngine, id string) error {
			node, err := b.GetNode(NodeID(id))
			if err != nil {
				return err
			}
			node.ChunkEmbeddings = [][]float32{{7, 8}}
			node.EmbedMeta = map[string]any{"chunk_count": 1}
			return b.UpdateNodeEmbeddingSidecar(node)
		}},
		{name: "range", mutate: func(b *BadgerEngine, id string) error {
			return b.replaceSeparateEmbeddingChunks(NodeID(id), [][]float32{{7, 8}}, nil)
		}},
		{name: "manual_node", mutate: func(b *BadgerEngine, id string) error {
			return b.SetManualSuppression(StoredEntityNodeKind, string(id), true)
		}},
		{name: "manual_edge", edge: true, mutate: func(b *BadgerEngine, id string) error {
			return b.SetManualSuppression(StoredEntityEdgeKind, string(id), true)
		}},
		{name: "deindex_node", mutate: func(b *BadgerEngine, id string) error { _, err := b.EnqueueDeindexIfSuppressed(id, false); return err }},
		{name: "deindex_edge", edge: true, mutate: func(b *BadgerEngine, id string) error { _, err := b.EnqueueDeindexIfSuppressed(id, true); return err }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b := newTestEngine(t)
			require.NoError(t, b.BulkCreateNodes([]*Node{{ID: "test:a", Labels: []string{"Doc"}, Properties: map[string]any{"text": "same"}}, {ID: "test:b"}, {ID: "test:c", Labels: []string{"Doc"}, Properties: map[string]any{"text": "same"}}, {ID: "test:d"}}))
			require.NoError(t, b.BulkCreateEdges([]*Edge{{ID: "test:r", StartNode: "test:a", EndNode: "test:b", Type: "R"}, {ID: "test:s", StartNode: "test:c", EndNode: "test:d", Type: "R"}}))
			if tc.name == "deindex_node" || tc.name == "deindex_edge" {
				b.SetDecayEnabled(true)
			}
			target, disjoint := "test:a", "test:c"
			if tc.edge {
				target, disjoint = "test:r", "test:s"
			}
			tx, err := b.BeginTransaction()
			require.NoError(t, err)
			if tc.edge {
				edge, err := tx.GetStoredEdge(EdgeID(target))
				require.NoError(t, err)
				edge.Properties = map[string]any{"prepared": true}
				require.NoError(t, tx.UpdateEdge(EdgeUpdateRequest{After: edge}))
			} else {
				node, err := tx.GetStoredNode(NodeID(target))
				require.NoError(t, err)
				node.Properties["prepared"] = true
				require.NoError(t, tx.UpdateGraphNode(NodeUpdateRequest{After: node, Policy: ClientNodeEdits}))
			}
			prepared, err := tx.PrepareEntityCommit(context.Background(), EntityCommitRequest{LogicalID: "helper-guard"})
			require.NoError(t, err)
			var abortOnce sync.Once
			abort := func() { abortOnce.Do(prepared.Abort) }
			waitEntered := make(chan struct{})
			var waitOnce sync.Once
			restore := SetUniqueKeyLockWaitHookForTesting(func() { waitOnce.Do(func() { close(waitEntered) }) })
			done := make(chan error, 1)
			t.Cleanup(func() {
				abort()
				restore()
				select {
				case <-done:
				case <-time.After(5 * time.Second):
					t.Error("helper worker did not finish")
				}
			})
			go func() { done <- tc.mutate(b, target) }()
			select {
			case <-waitEntered:
			case err := <-done:
				done <- err
				t.Fatalf("helper bypassed prepared authority: %v", err)
			case <-time.After(5 * time.Second):
				t.Fatal("helper did not enroll its real authority wait")
			}
			require.NoError(t, tc.mutate(b, disjoint), "disjoint entity proceeds while prepared target is held")
			if tc.edge {
				edge, err := b.GetStoredEdge(EdgeID(target))
				require.NoError(t, err)
				require.NotContains(t, edge.Properties, "prepared")
				require.False(t, b.manuallySuppressed(manualSuppressionEdge, target))
			} else {
				node, err := b.GetStoredNode(NodeID(target))
				require.NoError(t, err)
				require.NotContains(t, node.Properties, "prepared")
				require.Empty(t, node.ChunkEmbeddings)
				require.False(t, b.manuallySuppressed(manualSuppressionNode, target))
			}
			abort()
			select {
			case err := <-done:
				done <- err
				require.NoError(t, err)
			case <-time.After(5 * time.Second):
				t.Fatal("helper did not complete after definite discard")
			}
		})
	}
}

func TestAcceptedPhysicalFailureLetsAuthorityWaiterYieldRepairCut(t *testing.T) {
	b := newTestEngine(t)
	_, err := b.CreateNode(&Node{ID: "test:a", Properties: map[string]any{"value": "before"}})
	require.NoError(t, err)
	failure := errors.New("accepted physical preparation failed")
	var reject atomic.Bool
	require.NoError(t, b.InitializeStoredEntityObserver("test", func(StoredEntitySnapshot) (StoredEntityObserver, error) {
		return func(StoredEntityPublication) (func(), error) {
			if reject.Load() {
				return nil, failure
			}
			return func() {}, nil
		}, nil
	}, nil))
	tx, err := b.BeginTransaction()
	require.NoError(t, err)
	node, err := tx.GetStoredNode("test:a")
	require.NoError(t, err)
	node.Properties["value"] = "prepared"
	require.NoError(t, tx.UpdateGraphNode(NodeUpdateRequest{After: node, Policy: ClientNodeEdits}))
	prepared, err := tx.PrepareEntityCommit(context.Background(), EntityCommitRequest{LogicalID: "accepted-repair"})
	require.NoError(t, err)
	releaseAccepted, err := prepared.RetainAcceptedAuthority(nil)
	require.NoError(t, err)
	enrolled := make(chan struct{})
	var once sync.Once
	restore := SetUniqueKeyLockWaitHookForTesting(func() { once.Do(func() { close(enrolled) }) })
	writerDone := make(chan error, 1)
	cutDone := make(chan error, 1)
	cutStarted := false
	t.Cleanup(func() {
		prepared.Abort()
		releaseAccepted()
		restore()
		select {
		case <-writerDone:
		case <-time.After(5 * time.Second):
			t.Error("waiting writer did not stop")
		}
		if cutStarted {
			select {
			case <-cutDone:
			case <-time.After(5 * time.Second):
				t.Error("initializer did not stop")
			}
		}
	})
	go func() { writerDone <- b.DeleteNode("test:a") }()
	select {
	case <-enrolled:
	case <-time.After(5 * time.Second):
		t.Fatal("writer did not wait for accepted authority")
	}
	var snapshotValue any
	cutStarted = true
	go func() {
		cutDone <- b.InitializeStoredEntityObserver("test", func(snapshot StoredEntitySnapshot) (StoredEntityObserver, error) {
			err := snapshot.ForEach(func(entry StoredEntitySnapshotEntry) error {
				change, err := entry.Read()
				if err != nil {
					return err
				}
				if change.AfterNode != nil && change.AfterNode.ID == "test:a" {
					snapshotValue = change.AfterNode.Properties["value"]
				}
				return nil
			})
			return func(StoredEntityPublication) (func(), error) { return func() {}, nil }, err
		}, nil)
	}()
	deadline := time.After(5 * time.Second)
	for b.writeBarrier.TryRLock() {
		b.writeBarrier.RUnlock()
		select {
		case <-deadline:
			t.Fatal("repair initializer did not queue its lifecycle cut")
		default:
			runtime.Gosched()
		}
	}
	reject.Store(true)
	require.ErrorIs(t, prepared.Commit(nil), failure, "native failure releases its original frame but accepted ownership remains")
	prepared.MarkRecoveryPending()
	select {
	case err := <-writerDone:
		writerDone <- err
		require.ErrorIs(t, err, ErrConflict)
	case <-time.After(5 * time.Second):
		t.Fatal("recovery-pending owner did not wake writer to drop lifecycle lease")
	}
	select {
	case err := <-cutDone:
		cutDone <- err
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("repair cut remained behind failed command's waiter")
	}
	require.Equal(t, "before", snapshotValue, "failed native attempt made no stored change")
	_, err = b.CreateNode(&Node{ID: "test:disjoint"})
	require.NoError(t, err)
	require.ErrorIs(t, b.UpdateNode(NodeUpdateRequest{After: &Node{ID: "test:a", Properties: map[string]any{"value": "blocked"}}}), ErrConflict, "pending accepted authority still protects target")
	releaseAccepted()
	require.NoError(t, b.UpdateNode(NodeUpdateRequest{After: &Node{ID: "test:a", Properties: map[string]any{"value": "after-discard"}}}))
}
