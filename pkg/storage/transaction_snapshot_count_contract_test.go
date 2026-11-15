package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// B10 native regression. A transaction begun before a peer commits a
// relationship creation or deletion answers every relationship read from its
// pinned snapshot: the point read, both adjacency scans and the type-index
// listing agree with each other and with the data it held at BEGIN. The derived
// count accessors must then report that the committed counters no longer count
// those rows, because the counters hold the peer's committed change and a count
// answered from them disagreed with those reads.

// b10PeerCommitsRelationship applies the peer's committed change.
type b10PeerCommitsRelationship func(t *testing.T, engine *BadgerEngine, edgeID EdgeID)

// b10RelationshipPeerHistory seeds one relationship in namespace test, opens a
// transaction pinned before the peer's commit, and applies that commit.
func b10RelationshipPeerHistory(t *testing.T, peer b10PeerCommitsRelationship) (*BadgerEngine, *BadgerTransaction, EdgeID) {
	t.Helper()
	engine := newIsolatedBadgerEngine(t)
	createEdgeEndpoints(t, engine)
	const edgeID EdgeID = "test:e1"
	require.NoError(t, engine.CreateEdge(t.Context(), &Edge{
		ID: edgeID, StartNode: "test:a", EndNode: "test:b", Type: "REL",
		Properties: map[string]interface{}{"x": "begin"},
	}))
	tx := beginPinnedTransaction(t, engine)
	t.Cleanup(func() { _ = tx.Rollback() })
	peer(t, engine, edgeID)
	return engine, tx, edgeID
}

// TestPinnedTransactionRelationshipReadsAgreeAfterPeerCommit is the read
// contract B10 reports: the point read, adjacency scans and type listing of one
// pinned transaction answering from the same snapshot.
func TestPinnedTransactionRelationshipReadsAgreeAfterPeerCommit(t *testing.T) {
	const peerEdgeID EdgeID = "test:e2"
	for _, scenario := range []struct {
		name string
		peer b10PeerCommitsRelationship
		// probe is the relationship the peer's commit created or deleted and
		// the transaction therefore answers from its snapshot.
		probe EdgeID
		// wantPresent is whether the transaction still reads probe.
		wantPresent bool
	}{
		{
			name: "peer creates a relationship after begin",
			peer: func(t *testing.T, engine *BadgerEngine, _ EdgeID) {
				t.Helper()
				require.NoError(t, engine.CreateEdge(t.Context(), &Edge{
					ID: peerEdgeID, StartNode: "test:a", EndNode: "test:b", Type: "REL",
					Properties: map[string]interface{}{"x": "peer"},
				}))
			},
			probe:       peerEdgeID,
			wantPresent: false,
		},
		{
			name: "peer deletes the relationship after begin",
			peer: func(t *testing.T, engine *BadgerEngine, edgeID EdgeID) {
				t.Helper()
				require.NoError(t, engine.DeleteEdge(t.Context(), edgeID))
			},
			probe:       "test:e1",
			wantPresent: true,
		},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			engine, tx, edgeID := b10RelationshipPeerHistory(t, scenario.peer)
			ctx := t.Context()

			// Sensitivity control: the peer's change is committed, so a reader
			// outside the transaction answers it. The snapshot reads below
			// therefore do not agree by accident on unchanged storage.
			if scenario.wantPresent {
				_, err := engine.GetEdge(ctx, edgeID)
				require.ErrorIs(t, err, ErrNotFound, "the peer's deletion must be committed")
			} else {
				_, err := engine.GetEdge(ctx, scenario.probe)
				require.NoError(t, err, "the peer's creation must be committed")
			}

			point, err := tx.GetEdge(ctx, scenario.probe)
			if scenario.wantPresent {
				require.NoError(t, err, "a peer deletion is invisible to the pinned snapshot")
				require.Equal(t, scenario.probe, point.ID)
			} else {
				require.ErrorIs(t, err, ErrNotFound, "a peer creation is invisible to the pinned snapshot")
			}

			outgoing, err := tx.ReadEdges(ctx, AdjacentEdgeRead{Direction: AdjacentOutgoing, Node: "test:a"})
			require.NoError(t, err)
			incoming, err := tx.ReadEdges(ctx, AdjacentEdgeRead{Direction: AdjacentIncoming, Node: "test:b"})
			require.NoError(t, err)
			byType, err := tx.GetEdgesByType(ctx, "REL")
			require.NoError(t, err)
			all, err := tx.AllEdges(ctx)
			require.NoError(t, err)

			// The snapshot holds exactly the relationship committed before BEGIN,
			// whichever way the peer changed committed storage afterwards.
			for _, listing := range []struct {
				name  string
				edges []*Edge
			}{
				{"outgoing adjacency", outgoing},
				{"incoming adjacency", incoming},
				{"type listing", byType},
				{"all relationships", all},
			} {
				require.Len(t, listing.edges, 1, "%s must answer from the pinned snapshot", listing.name)
				require.Equal(t, edgeID, listing.edges[0].ID, listing.name)
				require.NotEqual(t, peerEdgeID, listing.edges[0].ID, listing.name)
			}
		})
	}
}

// TestPinnedTransactionCountersDeclineAfterPeerCommit is the count half of B10:
// once the peer's committed relationship change moves the namespace past the
// transaction's begin boundary, the committed counters no longer count the rows
// the transaction reads, and storage says so instead of answering with them.
func TestPinnedTransactionCountersDeclineAfterPeerCommit(t *testing.T) {
	engine, tx, _ := b10RelationshipPeerHistory(t, func(t *testing.T, engine *BadgerEngine, _ EdgeID) {
		t.Helper()
		require.NoError(t, engine.CreateEdge(t.Context(), &Edge{
			ID: "test:e2", StartNode: "test:a", EndNode: "test:b", Type: "REL",
		}))
	})
	ctx := t.Context()

	// The peer's commit moved the namespace past the transaction's begin
	// boundary, so the committed counters hold a relationship these reads do not
	// return: neither accessor may answer from them.
	delta, ok := tx.PendingEdgeTypeCountDelta("test", "REL")
	require.False(t, ok, "the peer's committed relationship moved the namespace past begin")
	require.Zero(t, delta)
	count, described, err := tx.PendingCountStoreDelta(ctx, CountStoreReadRequest{
		Kind: CountStoreRelationshipsByType, Namespace: "test", Types: []string{"REL"},
	})
	require.NoError(t, err)
	require.False(t, described, "the count-store read must decline the committed counter")
	require.Zero(t, count.Count)

	// The rows are still the transaction's answer, and its own pending write
	// merges on top of them without admitting the peer's relationship.
	require.NoError(t, tx.CreateEdge(ctx, &Edge{
		ID: "test:e3", StartNode: "test:a", EndNode: "test:b", Type: "REL",
	}))
	edges, err := tx.GetEdgesByType(ctx, "REL")
	require.NoError(t, err)
	ids := make([]EdgeID, 0, len(edges))
	for _, edge := range edges {
		ids = append(ids, edge.ID)
	}
	require.ElementsMatch(t, []EdgeID{"test:e1", "test:e3"}, ids,
		"the snapshot relationship plus the transaction's own write, and not the peer's")

	// The same counters still describe a transaction begun after the peer's
	// commit: the decline is about the stale snapshot, not a disabled counter.
	fresh, err := engine.BeginTransaction(ctx, OrdinaryBegin)
	require.NoError(t, err)
	t.Cleanup(func() { _ = fresh.Rollback() })
	require.NoError(t, fresh.SetNamespace("test"))
	_, described, err = fresh.PendingCountStoreDelta(ctx, CountStoreReadRequest{
		Kind: CountStoreRelationshipsByType, Namespace: "test", Types: []string{"REL"},
	})
	require.NoError(t, err)
	require.True(t, described, "a transaction begun after the commit is counted by the counters")
}
