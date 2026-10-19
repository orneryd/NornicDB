package storage

import (
	"fmt"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/orneryd/nornicdb/pkg/knowledgepolicy"
	"github.com/stretchr/testify/require"
)

func TestNamespaceDropRetiresEntitySideState(t *testing.T) {
	for _, buffered := range []bool{false, true} {
		t.Run(fmt.Sprintf("buffered=%v", buffered), func(t *testing.T) {
			dir := t.TempDir()
			engine, err := NewBadgerEngineWithOptions(BadgerOptions{DataDir: dir, LowMemory: true, WriteBehind: buffered, WriteBehindInterval: time.Hour})
			require.NoError(t, err)
			closed := false
			defer func() {
				if !closed {
					require.NoError(t, engine.Close())
				}
			}()
			acc := knowledgepolicy.NewAccessAccumulator(true, 0)
			engine.SetAccessAccumulator(acc)
			var targetKeys [][]byte
			for _, namespace := range []string{"side_a", "side_b"} {
				seed, endpoint, edge := NodeID(namespace+":seed"), NodeID(namespace+":end"), EdgeID(namespace+":edge")
				for _, id := range []NodeID{seed, endpoint} {
					_, err := engine.CreateNode(&Node{ID: id, Labels: []string{"Side"}})
					require.NoError(t, err)
				}
				require.NoError(t, engine.CreateEdge(&Edge{ID: edge, StartNode: seed, EndNode: endpoint, Type: "SIDE"}))
				_, err := engine.GetNode(seed)
				require.NoError(t, err)
				_, err = engine.GetEdge(edge)
				require.NoError(t, err)
				_, err = engine.GetOutgoingEdges(seed)
				require.NoError(t, err)
				meta := &knowledgepolicy.AccessMetaEntry{TargetID: string(seed), TargetScope: knowledgepolicy.ScopeNode, Overflow: map[string]any{"custom": int64(7)}}
				meta.Fixed.AccessCount = 42
				require.NoError(t, engine.PutAccessMeta(string(seed), meta))
				cat, err := engine.GetIndexEntryCatalog(string(seed))
				require.NoError(t, err)
				require.NotNil(t, cat)
				require.NoError(t, engine.WriteIndexTombstones(cat.IndexKeys))
				work := &DeindexWorkItem{WorkItemID: "deindex:" + string(seed), TargetID: string(seed), TargetScope: "NODE", Status: "pending"}
				require.NoError(t, engine.PutDeindexWorkItem(work))
				require.NoError(t, engine.SetNodeManuallySuppressed(seed, true))
				require.NoError(t, engine.SetEdgeManuallySuppressed(edge, true))
				acc.IncrementAccess(string(seed))
				acc.IncrementCustom(string(seed), "custom", 2)
				if namespace == "side_a" {
					targetKeys = append(targetKeys, manualSuppressionKey(manualSuppressionNode, string(seed)), manualSuppressionKey(manualSuppressionEdge, string(edge)), accessMetaKey(string(seed)), deindexWorkItemKey(work.WorkItemID))
					for _, key := range cat.IndexKeys {
						targetKeys = append(targetKeys, indexTombstoneKey(key))
					}
				}
			}
			_, _, err = engine.DeleteByPrefix("side_a:")
			require.NoError(t, err)
			require.NoError(t, engine.db.View(func(tx *badger.Txn) error {
				for _, key := range targetKeys {
					_, err := tx.Get(key)
					require.ErrorIs(t, err, badger.ErrKeyNotFound, "retired side key %x", key)
				}
				return nil
			}))
			require.False(t, engine.manuallySuppressed(manualSuppressionNode, "side_a:seed"))
			require.False(t, engine.manuallySuppressed(manualSuppressionEdge, "side_a:edge"))
			require.True(t, engine.manuallySuppressed(manualSuppressionNode, "side_b:seed"))
			require.True(t, engine.manuallySuppressed(manualSuppressionEdge, "side_b:edge"))
			require.Zero(t, acc.ReadThrough("side_a:seed", "accessCount", 0))
			require.Equal(t, int64(1), acc.ReadThrough("side_b:seed", "accessCount", 0))
			for _, id := range []NodeID{"side_a:seed", "side_a:end"} {
				_, err := engine.CreateNode(&Node{ID: id, Labels: []string{"Side"}})
				require.NoError(t, err)
			}
			require.NoError(t, engine.CreateEdge(&Edge{ID: "side_a:edge", StartNode: "side_a:seed", EndNode: "side_a:end", Type: "SIDE"}))
			_, err = engine.GetNode("side_a:seed")
			require.NoError(t, err)
			freshEdges, err := engine.GetOutgoingEdges("side_a:seed")
			require.NoError(t, err)
			require.Len(t, freshEdges, 1)
			require.NoError(t, engine.Close())
			closed = true
			reopened, err := NewBadgerEngineWithOptions(BadgerOptions{DataDir: dir, LowMemory: true})
			require.NoError(t, err)
			defer reopened.Close()
			_, err = reopened.GetNode("side_a:seed")
			require.NoError(t, err)
			meta, err := reopened.GetAccessMeta("side_a:seed")
			require.NoError(t, err)
			require.Nil(t, meta)
			require.False(t, reopened.manuallySuppressed(manualSuppressionEdge, "side_a:edge"))
			require.True(t, reopened.manuallySuppressed(manualSuppressionEdge, "side_b:edge"))
		})
	}
}
