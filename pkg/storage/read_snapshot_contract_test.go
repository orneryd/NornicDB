// Author: Codex implementation lane, 2026-10-27 (NOR-148 S16-S25).
package storage

import (
	"context"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

func TestReadSnapshotOriginalReaderContracts(t *testing.T) {
	for _, reader := range []SnapshotReader{SnapshotAllNodes, SnapshotAllEdges} {
		t.Run(map[SnapshotReader]string{SnapshotAllNodes: "materialized_nodes", SnapshotAllEdges: "materialized_edges"}[reader], func(t *testing.T) {
			b := newTestEngine(t)
			for _, id := range []NodeID{"selected:a", "other:z"} {
				_, err := b.CreateNode(&Node{ID: id})
				require.NoError(t, err)
			}
			require.NoError(t, b.CreateEdge(&Edge{ID: "other:e", StartNode: "other:z", EndNode: "other:z", Type: "R"}))
			scoped := NewNamespacedEngine(b, "selected")
			snapshot, err := OpenReadSnapshot(WithSnapshotReader(context.Background(), reader), scoped)
			require.NoError(t, err)
			defer snapshot.Close()
			captured := snapshot.(QueryReadSnapshot)
			before, valid := captured.CapturedRevision()
			require.True(t, valid)
			// The existing raw-storage corruption fixture changes physical storage
			// without a graph notification. It cannot be certified by graph count alone.
			key := edgeKey("other:e")
			if reader == SnapshotAllNodes {
				key = nodeKey("other:z")
			}
			require.NoError(t, b.withUpdate(updateRequest{}, func(txn *badger.Txn) error { return txn.Set(key, []byte{0xff}) }))
			current, valid := captured.CurrentRevision()
			require.True(t, valid)
			require.NotEqual(t, before, current)
			require.Equal(t, before.GraphMutationVersion(), current.GraphMutationVersion())
			visit := func(view QueryReadSnapshot) error {
				if reader == SnapshotAllNodes {
					return view.IterateVisibleNodes(context.Background(), func(*Node) bool { return true })
				}
				return view.IterateAllEdges(context.Background(), func(*Edge) error { return nil })
			}
			require.NoError(t, visit(captured), "the already admitted view is unchanged")
			next, err := OpenReadSnapshot(WithSnapshotReader(context.Background(), reader), scoped)
			require.NoError(t, err)
			defer next.Close()
			require.Error(t, visit(next.(QueryReadSnapshot)), "materialized errors precede namespace filtering")
		})
	}
}

func TestReadSnapshotSingleCommitCoverageUnits(t *testing.T) {
	b := newTestEngine(t)
	capture := func() SnapshotReadRevision {
		view, err := OpenReadSnapshot(WithSnapshotReader(context.Background(), SnapshotAllNodes), b)
		require.NoError(t, err)
		defer view.Close()
		revision, valid := view.(QueryReadSnapshot).CapturedRevision()
		require.True(t, valid)
		return revision
	}
	before := capture()
	require.NoError(t, b.BulkCreateNodes([]*Node{{ID: "test:a"}, {ID: "test:b"}}))
	require.NoError(t, b.withUpdate(updateRequest{}, func(txn *badger.Txn) error { return txn.Set([]byte("unrelated-fixture-metadata"), []byte("value")) }))
	after := capture()
	require.False(t, after.SingleReaderCommitAfter(before), "a multi-operation graph commit plus a raw commit is not one closed publication")
	_, err := b.CreateNode(&Node{ID: "test:c"})
	require.NoError(t, err)
	allocated := capture()
	// The ID counters a create persists are committed in its reader
	// publication: one step, whose rows event coverage proves.
	require.True(t, allocated.SingleReaderCommitAfter(after), "a create and its ID-counter commit are one publication")
	node, err := b.GetNode("test:c")
	require.NoError(t, err)
	node.Properties = map[string]any{"text": "changed"}
	// Allocate this property's dictionary token before the measured interval.
	require.NoError(t, b.UpdateNode(NodeUpdateRequest{After: node}))
	beforeUpdate := capture()
	node.Properties["text"] = "changed again"
	require.NoError(t, b.UpdateNode(NodeUpdateRequest{After: node}))
	final := capture()
	require.True(t, final.SingleReaderCommitAfter(beforeUpdate), "one existing-ID update can be maintained with independent event coverage")
	b.MarkNodeEmbedded(node.ID)
	retired := capture()
	// A pending-marker retirement keeps release's classification: a
	// reader-relevant commit of its own, never folded into a neighbour.
	require.False(t, retired.SameReaderView(final), "a pending-marker retirement is a reader-relevant commit")
	require.True(t, retired.SingleReaderCommitAfter(final), "it is exactly one reader-relevant commit after the update")
}

func TestReadSnapshotPendingMarkerNeutrality(t *testing.T) {
	b := newTestEngine(t)
	_, err := b.CreateNode(&Node{ID: "test:a", Labels: []string{"Doc"}, Properties: map[string]any{"text": "hotel", "other": int64(7)}, NamedEmbeddings: map[string][]float32{"title": {1, 2}}, EmbedMeta: map[string]any{"model": "fixture"}})
	require.NoError(t, err)
	require.NoError(t, b.CreateEdge(&Edge{ID: "test:r", StartNode: "test:a", EndNode: "test:a", Type: "LINK", Properties: map[string]any{"text": "hotel"}}))
	b.AddToPendingEmbeddings("test:a")
	nodesBefore, err := b.AllNodes()
	require.NoError(t, err)
	edgesBefore, err := b.AllEdges()
	require.NoError(t, err)
	image := func() map[string]string {
		rows := map[string]string{}
		require.NoError(t, b.withView(func(txn *badger.Txn) error {
			it := txn.NewIterator(badger.DefaultIteratorOptions)
			defer it.Close()
			for it.Rewind(); it.Valid(); it.Next() {
				value, err := it.Item().ValueCopy(nil)
				if err != nil {
					return err
				}
				rows[string(it.Item().KeyCopy(nil))] = string(value)
			}
			return nil
		}))
		return rows
	}
	before := image()
	_, pending := before[string(pendingEmbedKey("test:a"))]
	require.True(t, pending, "the marker must exist before removal")
	b.MarkNodeEmbedded("test:a")
	after := image()
	delete(before, string(pendingEmbedKey("test:a")))
	require.Equal(t, before, after, "only the pending-marker key may change: no body, label, property, visibility, access/decay, vector or metadata key")
	nodesAfter, err := b.AllNodes()
	require.NoError(t, err)
	require.Equal(t, nodesBefore, nodesAfter, "public node hydration, including embedding fields, must be unchanged")
	edgesAfter, err := b.AllEdges()
	require.NoError(t, err)
	require.Equal(t, edgesBefore, edgesAfter)
}

func TestReadSnapshotPopulationDoesNotRetainBodyCache(t *testing.T) {
	b := newTestEngine(t)
	_, err := b.CreateNode(&Node{ID: "test:a", Labels: []string{"Doc"}, Properties: map[string]any{"text": "hotel"}})
	require.NoError(t, err)
	before := len(b.nodeBodyCache)
	view, err := OpenReadSnapshot(WithSnapshotReader(context.Background(), SnapshotAllNodes), b)
	require.NoError(t, err)
	var rows []*Node
	require.NoError(t, view.(QueryReadSnapshot).IterateVisibleNodes(context.Background(), func(node *Node) bool { rows = append(rows, node); return true }))
	require.Len(t, rows, 1)
	require.Equal(t, "hotel", rows[0].Properties["text"])
	require.Equal(t, before, len(b.nodeBodyCache), "a captured population must not create a second retained body copy")
	view.Close()
	require.Equal(t, before, len(b.nodeBodyCache))
}
