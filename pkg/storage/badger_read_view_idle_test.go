package storage

import (
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

// Committed work belongs to the capture boundary even when overlay selection
// can be skipped. Exercise both physical transactions and build-long views.
func TestReadViewIdleCaptureRunsCommittedWork(t *testing.T) {
	for _, mode := range []string{"disabled", "idle", "retired", "other-namespace"} {
		for _, longLived := range []bool{false, true} {
			name := mode + "/strict"
			if longLived {
				name = mode + "/long-lived"
			}
			t.Run(name, func(t *testing.T) {
				b := newWriteBehindTestEngine(t, time.Hour)
				b.writeBehind.Close()
				_, err := b.CreateNode(&Node{ID: "test:holder", Labels: []string{"L"}})
				require.NoError(t, err)
				if mode == "disabled" {
					w := b.writeBehind
					b.writeBehind = nil
					t.Cleanup(func() { b.writeBehind = w })
				} else if mode == "retired" {
					bufferedCreate(t, b, &Node{ID: "test:retired"})
					require.NoError(t, b.FlushWriteBehind())
				} else if mode == "other-namespace" {
					tx, err := b.BeginTransaction()
					require.NoError(t, err)
					require.NoError(t, tx.SetImplicit(true))
					require.NoError(t, tx.SetNamespace("other"))
					_, err = tx.CreateNode(&Node{ID: "other:pending"})
					require.NoError(t, err)
					require.NoError(t, tx.Commit())
					require.Positive(t, b.writeBehind.PendingOps())
				}
				_, idle := b.writeBehind.idleReadEpoch("test")
				require.True(t, idle)
				b.adjCacheStoreOutgoing(b.adjCacheGen.current(), "test:holder", []EdgeID{"test:r"}, b.db.oracle.published.Load())
				calls := 0
				var ids []EdgeID
				var hit bool
				view, err := b.captureReadView(func(w *WriteBehindBuffer) {
					calls++
					require.Nil(t, w, "only overlay selection is skipped")
					ids, hit = b.adjCacheLoadOutgoing("test:holder")
				}, readViewOptions{scope: "test", longLived: longLived})
				require.NoError(t, err)
				defer view.Close()
				require.Equal(t, 1, calls)
				require.True(t, hit)
				require.Equal(t, []EdgeID{"test:r"}, ids)
				require.NoError(t, view.withView(func(txn *badger.Txn) error {
					_, err := txn.Get(nodeKey("test:holder"))
					return err
				}))
			})
		}
	}
}

func TestReadViewEmptyOverlaySelectors(t *testing.T) {
	var w *WriteBehindBuffer
	require.Equal(t, bufferedEntityState{}, w.entityStateLocked(bufferedNodeEntity, "test:n"))
	nodes, nodeIDs := w.labelOverlayLocked("L")
	require.Nil(t, nodes)
	require.Nil(t, nodeIDs)
	nodes, nodeIDs = w.allNodesOverlayLocked()
	require.Nil(t, nodes)
	require.Nil(t, nodeIDs)
	edges, edgeIDs := w.typeOverlayLocked("R")
	require.Nil(t, edges)
	require.Nil(t, edgeIDs)
	edges, edgeIDs = w.allEdgesOverlayLocked()
	require.Nil(t, edges)
	require.Nil(t, edgeIDs)
	edges, suppressed, deleted := w.edgeAdjacencyOverlayLocked("test:n", true)
	require.Nil(t, edges)
	require.Nil(t, suppressed)
	require.False(t, deleted)
	edges, suppressed, sourceDeleted, targetDeleted := w.edgesBetweenOverlayLocked("test:a", "test:b")
	require.Nil(t, edges)
	require.Nil(t, suppressed)
	require.False(t, sourceDeleted)
	require.False(t, targetDeleted)
	require.Nil(t, w.keyCandidatesLocked("L", "p", int64(1)))
	w.visitEntityOverlayLocked(bufferedNodeEntity, func(*CommitBuffer, func(string)) {
		t.Fatal("empty overlay must not enumerate candidates")
	}, func(string, bufferedEntityState) { t.Fatal("empty overlay must not visit entities") })
	require.Zero(t, testing.AllocsPerRun(100, func() {
		w.entityStateLocked(bufferedNodeEntity, "test:n")
		w.labelOverlayLocked("L")
		w.allNodesOverlayLocked()
		w.typeOverlayLocked("R")
		w.allEdgesOverlayLocked()
		w.edgeAdjacencyOverlayLocked("test:n", true)
		w.edgesBetweenOverlayLocked("test:a", "test:b")
		w.keyCandidatesLocked("L", "p", int64(1))
	}))
}

// A cold body cache forces adjacency's mixed committed-ID/overlay callback,
// rather than satisfying the whole traversal through the idle cached path.
func TestReadViewIdleAdjacencyCommittedIDs(t *testing.T) {
	for _, retired := range []bool{false, true} {
		name := "idle"
		if retired {
			name = "retired"
		}
		t.Run(name, func(t *testing.T) {
			b := newWriteBehindTestEngine(t, time.Hour)
			b.writeBehind.Close()
			for _, id := range []NodeID{"test:a", "test:b"} {
				_, err := b.CreateNode(&Node{ID: id})
				require.NoError(t, err)
			}
			edge := &Edge{ID: "test:r", StartNode: "test:a", EndNode: "test:b", Type: "R"}
			if retired {
				bufferedWrite(t, b, func(tx *BadgerTransaction) error { return tx.CreateEdge(edge) })
				require.NoError(t, b.FlushWriteBehind())
			} else {
				require.NoError(t, b.CreateEdge(edge))
			}
			_, idle := b.writeBehind.idleReadEpoch("test")
			require.True(t, idle)
			for _, read := range []func() ([]*Edge, error){
				func() ([]*Edge, error) { return b.GetOutgoingEdges("test:a") },
				func() ([]*Edge, error) { return b.GetIncomingEdges("test:b") },
				func() ([]*Edge, error) {
					out, in, err := readAdjacency(b, adjacencyBoth, "test:a", "", nil, visibleRead)
					require.Empty(t, in)
					return out, err
				},
			} {
				b.adjCacheStoreOutgoing(b.adjCacheGen.current(), "test:a", []EdgeID{edge.ID}, b.db.oracle.published.Load())
				b.adjCacheStoreIncoming(b.adjCacheGen.current(), "test:b", []EdgeID{edge.ID}, b.db.oracle.published.Load())
				b.edgeCacheMu.Lock()
				delete(b.edgeCache, edge.ID)
				b.edgeCacheMu.Unlock()
				got, err := read()
				require.NoError(t, err)
				require.Len(t, got, 1)
				require.Equal(t, edge.ID, got[0].ID)
			}
		})
	}
}
