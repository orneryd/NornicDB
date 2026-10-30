package storage

import (
	"context"
	"github.com/dgraph-io/badger/v4"
	"github.com/orneryd/nornicdb/pkg/knowledgepolicy"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestDirectDeleteKernelPhysicalCompletion(t *testing.T) {
	b := newWriteBehindTestEngine(t, time.Hour)
	bufferedCreate(t, b, &Node{ID: "test:delete", Labels: []string{"L"}})
	bufferedWrite(t, b, func(tx *BadgerTransaction) error {
		return tx.CreateEdge(&Edge{ID: "test:loop", StartNode: "test:delete", EndNode: "test:delete", Type: "SELF"})
	})
	require.Positive(t, b.writeBehind.PendingOps())
	require.NoError(t, b.DeleteNode("test:delete"))
	// Physical storage is checked before Flush, including older acknowledged work.
	require.NoError(t, b.withView(func(txn *badger.Txn) error {
		_, e := txn.Get(nodeKey("test:delete"))
		require.ErrorIs(t, e, badger.ErrKeyNotFound)
		_, e = txn.Get(edgeKey("test:loop"))
		require.ErrorIs(t, e, badger.ErrKeyNotFound)
		return nil
	}))
	require.Zero(t, b.writeBehind.PendingOps(), "direct deletion was physically committed, not acknowledged into the buffer")
	nodes, e := b.NodeCount()
	require.NoError(t, e)
	require.Zero(t, nodes)
	edges, e := b.EdgeCount()
	require.NoError(t, e)
	require.Zero(t, edges)
	require.NoError(t, b.FlushWriteBehind())
	_, err := b.GetNode("test:delete")
	require.ErrorIs(t, err, ErrNotFound)
}
func TestDirectDeleteKernelCatalogAndRollback(t *testing.T) {
	for _, commit := range []bool{false, true} {
		t.Run(map[bool]string{false: "rollback", true: "commit"}[commit], func(t *testing.T) {
			b := newTestEngine(t)
			id := NodeID("test:catalog")
			_, err := b.CreateNode(&Node{ID: id, Labels: []string{"L"}})
			require.NoError(t, err)
			original := &knowledgepolicy.AccessMetaEntry{TargetID: string(id), TargetScope: knowledgepolicy.ScopeNode, HasAccessState: true, LastMutatedAt: 123, HasIndexKeys: true, IndexKeys: [][]byte{{1, 2}}, Deindexed: true}
			require.NoError(t, b.PutAccessMeta(string(id), original))
			originalResult, e := b.ReadAccessMeta(knowledgepolicy.AccessMetaReadRequest{Mode: knowledgepolicy.AccessMetaReadPoint, EntityID: string(id)})
			require.NoError(t, e)
			originalEntry := originalResult.Entry
			if commit {
				require.NoError(t, b.DeleteNode(id))
			} else {
				tx, e := b.BeginTransaction()
				require.NoError(t, e)
				require.NoError(t, tx.DeleteGraphNode(id, CascadeEdges))
				require.NoError(t, tx.Rollback())
			}
			metaResult, e := b.ReadAccessMeta(knowledgepolicy.AccessMetaReadRequest{Mode: knowledgepolicy.AccessMetaReadPoint, EntityID: string(id)})
			require.NoError(t, e)
			meta := metaResult.Entry
			require.NotNil(t, meta)
			require.Equal(t, int64(123), meta.LastMutatedAt)
			catalog, e := b.GetIndexEntryCatalog(string(id))
			require.NoError(t, e)
			if commit {
				require.Nil(t, catalog)
				require.False(t, meta.HasIndexKeys)
				require.False(t, meta.Deindexed)
			} else {
				require.NotNil(t, catalog)
				require.True(t, meta.HasIndexKeys)
				require.Equal(t, originalEntry.IndexKeys, meta.IndexKeys)
			}
		})
	}
}

func TestNodeDeletionKernelSelfLoopAndStagedCatalog(t *testing.T) {
	for _, staged := range []bool{false, true} {
		t.Run(map[bool]string{false: "native", true: "staged"}[staged], func(t *testing.T) {
			b := newTestEngine(t)
			id := NodeID("test:self")
			_, err := b.CreateNode(&Node{ID: id, Labels: []string{"L"}})
			require.NoError(t, err)
			require.NoError(t, b.CreateEdge(&Edge{ID: "test:loop", StartNode: id, EndNode: id, Type: "SELF"}))
			var result nodeDeletionResult
			if staged {
				tx, e := b.BeginTransaction()
				require.NoError(t, e)
				defer tx.Rollback()
				_, e = accessMetaInTxn(tx.staged(), string(id), accessMetaWrite, &knowledgepolicy.AccessMetaEntry{TargetID: string(id), TargetScope: knowledgepolicy.ScopeNode, HasAccessState: true, LastMutatedAt: 456, HasIndexKeys: true, IndexKeys: [][]byte{{1, 2}}, Deindexed: true})
				require.NoError(t, e)
				tx.mu.Lock()
				result, err = b.executeNodeDeletion(context.Background(), tx.staged(), id, &Node{ID: id, Labels: []string{"L"}})
				tx.mu.Unlock()
				require.NoError(t, err)
				meta, e := accessMetaInTxn(tx.staged(), string(id), accessMetaRead, nil)
				require.NoError(t, e)
				require.Equal(t, int64(456), meta.LastMutatedAt)
				require.False(t, meta.HasIndexKeys)
				require.Empty(t, meta.IndexKeys)
				require.False(t, meta.Deindexed)
				require.NoError(t, tx.Rollback())
				_, e = b.GetNode(id)
				require.NoError(t, e)
				_, e = b.GetEdge("test:loop")
				require.NoError(t, e)
			} else {
				require.NoError(t, b.withUpdate(func(raw *badger.Txn) error {
					result, err = b.executeNodeDeletion(context.Background(), raw, id, nil)
					return err
				}))
			}
			require.Equal(t, []EdgeID{"test:loop"}, result.ids)
			require.Len(t, result.edges, 1)
			require.Equal(t, id, result.edges[0].StartNode)
			require.Equal(t, id, result.edges[0].EndNode)
		})
	}
}
