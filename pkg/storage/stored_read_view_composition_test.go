package storage

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Stored and visible reads share the capture and cache owners. Warming one
// mode must not change the other, including while another key is buffered.
func TestStoredReadViewHiddenAdjacency(t *testing.T) {
	for _, phase := range []string{"idle", "busy", "retired"} {
		t.Run(phase, func(t *testing.T) {
			b := newWriteBehindTestEngine(t, time.Hour)
			b.writeBehind.Close()
			setupHiddenGraph(t, b)
			if phase != "idle" {
				bufferedCreate(t, b, &Node{ID: "test:unrelated"})
				if phase == "retired" {
					require.NoError(t, b.FlushWriteBehind())
				}
			}
			b.cacheInvalidateEdges()
			for _, temperature := range []string{"cold", "warm"} {
				t.Run(temperature, func(t *testing.T) {
					_, err := b.GetNode("test:hiddenPeer")
					require.ErrorIs(t, err, ErrNotFound)
					node, err := b.GetStoredNode("test:hiddenPeer")
					require.NoError(t, err)
					require.Equal(t, NodeID("test:hiddenPeer"), node.ID)
					_, err = b.GetNode("test:hiddenPeer")
					require.ErrorIs(t, err, ErrNotFound)
					ids := edgeIDLister(t)
					require.Equal(t, []string{"test:e1", "test:e4"}, ids(b.GetStoredIncomingEdges("test:peer1")))
					require.Empty(t, ids(b.GetIncomingEdges("test:peer1")))
					require.Equal(t, []string{"test:e1", "test:e3"}, ids(b.GetStoredOutgoingEdges("test:hub")))
					require.Equal(t, []string{"test:e1", "test:e4"}, ids(b.GetStoredEdgesByType("SUPPRESSED")))
					require.Empty(t, ids(b.GetEdgesByType("SUPPRESSED")))
					_, err = b.GetEdge("test:e1")
					require.ErrorIs(t, err, ErrNotFound)
					_, err = b.GetStoredEdge("test:e1")
					require.NoError(t, err)
					nodes, err := b.GetStoredNodesByLabel("Peer")
					require.NoError(t, err)
					require.Len(t, nodes, 3)
					nodes, err = b.GetNodesByLabel("Peer")
					require.NoError(t, err)
					require.Len(t, nodes, 2)
					var edges []*Edge
					require.NoError(t, b.StreamStoredEdgesByType(context.Background(), "SUPPRESSED", func(edge *Edge) error {
						edges = append(edges, edge)
						return nil
					}))
					require.Equal(t, []string{"test:e1", "test:e4"}, ids(edges, nil))
				})
			}
		})
	}
}

// The read-view lane's captured equality-index candidates must retain the
// integrity lane's stored-body policy, even after all generations retire.
func TestStoredReadViewHiddenKeyHolder(t *testing.T) {
	for _, phase := range []string{"idle", "busy", "retired"} {
		t.Run(phase, func(t *testing.T) {
			b := newWriteBehindTestEngine(t, time.Hour)
			b.writeBehind.Close()
			b.SetDecayEnabled(true)
			sm := b.GetSchemaForNamespace("test")
			require.NoError(t, sm.AddConstraint(factKey))
			idx, ok := sm.ConstraintPropertyIndex(factKey.Name)
			require.True(t, ok)
			require.NoError(t, sm.BackfillCompositeIndex(idx.Name, nil))
			holder := factNode("holder", 1, "name", 1)
			require.NoError(t, bufferedTry(t, b, holder))
			require.NoError(t, b.FlushWriteBehind())
			require.NoError(t, b.SetNodeManuallySuppressed(holder.ID, true))
			if phase != "idle" {
				bufferedCreate(t, b, &Node{ID: "test:unrelated"})
				if phase == "retired" {
					require.NoError(t, b.FlushWriteBehind())
				}
			}
			_, err := b.GetNode(holder.ID)
			require.ErrorIs(t, err, ErrNotFound)
			_, err = b.GetStoredNode(holder.ID)
			require.NoError(t, err)
			require.Error(t, bufferedTry(t, b, factNode("duplicate", 1, "name", 1)),
				"a hidden physical holder must still enforce the filled NODE KEY")
		})
	}
}

// Transaction integrity reads must use the same captured adjacency boundary
// as public traversals, rather than rebuilding an independent buffer/disk view.
func TestStoredReadViewTransactionIncidenceAcrossReplay(t *testing.T) {
	b := newWriteBehindTestEngine(t, time.Hour)
	b.writeBehind.Close()
	for _, id := range []NodeID{"test:a", "test:b"} {
		_, err := b.CreateNode(&Node{ID: id})
		require.NoError(t, err)
	}
	bufferedCreateEdge(t, b, &Edge{ID: "test:r", StartNode: "test:a", EndNode: "test:b", Type: "LINK"})
	count := b.GapProbeAcrossReplay(t, func() (int64, error) {
		tx, err := b.BeginTransaction()
		if err != nil {
			return 0, err
		}
		defer tx.Rollback()
		edges, err := tx.IncidentEdges("test:a")
		return int64(len(edges)), err
	})
	require.EqualValues(t, 1, count)
}
