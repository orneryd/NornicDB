package storage

import (
	"context"
	"fmt"
	"runtime"
	"strings"
	"testing"
	"unsafe"
	"weak"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestRetiredStorageStringsCollectible(t *testing.T) {
	cases := map[string]func(string) any{
		"pin": func(key string) any {
			owner := &NodeConfig{PinList: []string{key}}
			require.True(t, owner.RemoveFromPin(key))
			return owner
		},
		"deny": func(key string) any {
			owner := &NodeConfig{DenyList: []string{key}}
			require.True(t, owner.RemoveFromDeny(key))
			return owner
		},
		"range_index": func(key string) any {
			owner := &RangeIndex{entries: []rangeEntry{{nodeID: NodeID(key), value: 1}}}
			require.True(t, owner.deleteEntryLocked(NodeID(key), 1))
			return owner
		},
		"pending_index": func(key string) any {
			owner := newPendingNodeIndex()
			owner.tracked["L"] = map[string]struct{}{"v": {}}
			owner.add(&Node{ID: "test:keep", Labels: []string{"L"}, Properties: map[string]any{"v": 1}})
			node := &Node{ID: NodeID(key), Labels: []string{"L"}, Properties: map[string]any{"v": 1}}
			owner.add(node)
			owner.remove(node)
			return owner
		},
		"snapshot_node_order": func(key string) any {
			owner := &BadgerTransaction{}
			owner.cacheSnapshotPrefixNodeByIDLocked(nil, &Node{ID: NodeID(key)})
			require.Contains(t, owner.snapshotPrefixNodeByID, NodeID(key))
			for i := 0; i < maxSnapshotPrefixNodeCacheNodes; i++ {
				owner.cacheSnapshotPrefixNodeByIDLocked(nil, &Node{ID: NodeID(fmt.Sprintf("test:keep%d", i))})
			}
			require.NotContains(t, owner.snapshotPrefixNodeByID, NodeID(key))
			return owner
		},
		"snapshot_edge_order": func(key string) any {
			owner := &BadgerTransaction{}
			owner.cacheSnapshotEdgeByIDLockedWithContext(context.Background(), &Edge{ID: EdgeID(key), StartNode: "test:a", EndNode: "test:b", Type: "R"})
			require.Contains(t, owner.snapshotEdgeByID, EdgeID(key))
			for i := 0; i < maxSnapshotEdgeCacheEntries; i++ {
				owner.cacheSnapshotEdgeByIDLockedWithContext(context.Background(), &Edge{ID: EdgeID(fmt.Sprintf("test:keep%d", i)), StartNode: "test:a", EndNode: "test:b", Type: "R"})
			}
			require.NotContains(t, owner.snapshotEdgeByID, EdgeID(key))
			return owner
		},
	}
	for name, retire := range cases {
		t.Run(name, func(t *testing.T) {
			owner, reference := func() (any, weak.Pointer[byte]) {
				key := strings.Repeat("retired", 64)
				return retire(key), weak.Make(unsafe.StringData(key))
			}()
			runtime.GC()
			require.Nil(t, reference.Value(), "retired string storage must be collectible")
			runtime.KeepAlive(owner)
		})
	}
}

func TestRetiredWALBatchPayloadCollectible(t *testing.T) {
	wasEnabled := config.IsWALEnabled()
	config.EnableWAL()
	t.Cleanup(func() {
		if !wasEnabled {
			config.DisableWAL()
		}
	})
	for _, commit := range []bool{false, true} {
		t.Run(fmt.Sprintf("commit=%t", commit), func(t *testing.T) {
			wal, err := NewWAL(t.TempDir(), nil)
			require.NoError(t, err)
			t.Cleanup(func() { _ = wal.Close() })
			owner := wal.NewBatch()
			reference := func() weak.Pointer[byte] {
				require.NoError(t, owner.AppendNode(OpCreateNode, &Node{ID: "test:retired", Properties: map[string]any{"payload": strings.Repeat("x", 1024)}}))
				ref := weak.Make(&owner.pending[0].Data[0])
				if commit {
					require.NoError(t, owner.Commit())
				} else {
					owner.Rollback()
				}
				return ref
			}()
			require.Zero(t, owner.Len())
			runtime.GC()
			require.Nil(t, reference.Value(), "completed or discarded WAL payload must be collectible")
			runtime.KeepAlive(owner)
		})
	}
}

type retiredSnapshotSink struct{ Engine }

func (*retiredSnapshotSink) BulkCreateNodes([]*Node) error { return nil }
func (*retiredSnapshotSink) BulkCreateEdges([]*Edge) error { return nil }

func TestRetiredRecoverySnapshotBatchesCollectible(t *testing.T) {
	owner := &recoverySnapshotVisitor{engine: &retiredSnapshotSink{}}
	node, edge := func() (weak.Pointer[Node], weak.Pointer[Edge]) {
		n := &Node{ID: "test:retired"}
		e := &Edge{ID: "test:retired", StartNode: "test:a", EndNode: "test:b"}
		owner.nodes, owner.edges = []*Node{n}, []*Edge{e}
		nref, eref := weak.Make(n), weak.Make(e)
		require.NoError(t, owner.Flush())
		return nref, eref
	}()
	runtime.GC()
	require.Nil(t, node.Value())
	require.Nil(t, edge.Value())
	runtime.KeepAlive(owner)
}
