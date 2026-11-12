package storage

import (
	"context"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

// A transaction lists relationship headers as ReadEdges lists relationships at
// its snapshot: adjacency values give
// the type and other end, a peer's later change doesn't show, the
// transaction's own writes and deletes are merged, and an entry without a
// value or a head newer than the read version reads the record.
func TestTransactionEdgeHeaders(t *testing.T) {
	eng := newTestEngine(t)
	for _, id := range []NodeID{"test:a", "test:b", "test:c", "test:d"} {
		_, err := eng.CreateNode(t.Context(), &Node{ID: id, Labels: []string{"N"}})
		require.NoError(t, err)
	}
	w := map[string]any{"w": int64(1)}
	require.NoError(t, eng.CreateEdge(t.Context(), &Edge{ID: "test:e1", StartNode: "test:a", EndNode: "test:b", Type: "K", Properties: w}))
	require.NoError(t, eng.CreateEdge(t.Context(), &Edge{ID: "test:e2", StartNode: "test:a", EndNode: "test:c", Type: "K", Properties: w}))
	require.NoError(t, eng.CreateEdge(t.Context(), &Edge{ID: "test:e3", StartNode: "test:a", EndNode: "test:d", Type: "K", Properties: w}))
	require.NoError(t, eng.CreateEdge(t.Context(), &Edge{ID: "test:e4", StartNode: "test:b", EndNode: "test:c", Type: "K", Properties: w}))

	// e3's entry predates the values.
	aNum, _ := eng.idDict.lookupNodeNumID("test:a")
	e3Num, _ := eng.idDict.lookupEdgeNumID("test:e3")
	require.NoError(t, eng.withUpdate(context.Background(), updateRequest{}, func(txn *badger.Txn) error {
		return txn.Set(outgoingIndexKey(aNum, e3Num), []byte{})
	}))

	tx, err := eng.BeginTransaction(t.Context())
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	require.NoError(t, tx.SetNamespace("test"))

	// A peer retypes e1 after the snapshot.
	require.NoError(t, eng.UpdateEdge(t.Context(), EdgeUpdateRequest{After: &Edge{ID: "test:e1", StartNode: "test:a", EndNode: "test:b", Type: "K2", Properties: w}}))

	require.NoError(t, tx.CreateEdge(t.Context(), &Edge{ID: "test:e5", StartNode: "test:a", EndNode: "test:b", Type: "L"}))
	require.NoError(t, tx.DeleteEdge(t.Context(), "test:e2"))

	same := func(nodeID NodeID) {
		t.Helper()
		headers, err := tx.ReadEdges(context.Background(), AdjacentEdgeRead{Direction: AdjacentOutgoing, Node: nodeID, Headers: true})
		require.NoError(t, err)
		full, err := tx.ReadEdges(context.Background(), AdjacentEdgeRead{Node: nodeID})
		require.NoError(t, err)
		require.Equal(t, headerSummary(full), headerSummary(headers), nodeID)
		headers, err = tx.ReadEdges(context.Background(), AdjacentEdgeRead{Direction: AdjacentIncoming, Node: nodeID, Headers: true})
		require.NoError(t, err)
		full, err = tx.ReadEdges(context.Background(), AdjacentEdgeRead{Direction: AdjacentIncoming, Node: nodeID})
		require.NoError(t, err)
		require.Equal(t, headerSummary(full), headerSummary(headers), nodeID)
	}
	headers, err := tx.ReadEdges(context.Background(), AdjacentEdgeRead{Direction: AdjacentOutgoing, Node: "test:a", Headers: true})
	require.NoError(t, err)
	require.Equal(t, []string{"test:e1:K:test:a->test:b", "test:e3:K:test:a->test:d", "test:e5:L:test:a->test:b"}, headerSummary(headers))
	for _, header := range headers {
		if header.ID == "test:e1" {
			require.Nil(t, header.Properties, "e1 comes from its adjacency value")
		}
	}
	// The second listing uses the cached adjacency and, after the full read, the
	// cached records.
	same("test:a")
	same("test:a")
	same("test:b")
	same("test:c")

	// A read version older than a relationship's head reads its record.
	e4Head := MVCCHead{}
	require.NoError(t, eng.withView(func(txn *badger.Txn) error {
		var err error
		e4Head, err = eng.loadEdgeMVCCHeadInTxn(txn, "test:e4")
		return err
	}))
	tx.snapshotEdgeByID = nil
	tx.snapshotOutgoingAdjacency.clear()
	readTS := tx.readTS
	tx.readTS = e4Head.Version
	require.NoError(t, eng.UpdateEdge(t.Context(), EdgeUpdateRequest{After: &Edge{ID: "test:e4", StartNode: "test:b", EndNode: "test:c", Type: "K", Properties: map[string]any{"w": int64(2)}}}))
	tx2, err := eng.BeginTransaction(t.Context())
	require.NoError(t, err)
	defer func() { _ = tx2.Rollback() }()
	require.NoError(t, tx2.SetNamespace("test"))
	tx2.readTS = e4Head.Version
	headers, err = tx2.ReadEdges(context.Background(), AdjacentEdgeRead{Direction: AdjacentOutgoing, Node: "test:b", Headers: true})
	require.NoError(t, err)
	require.Len(t, headers, 1)
	require.Equal(t, int64(1), headers[0].Properties["w"], "the record at the read version")
	tx.readTS = readTS

	// Decay filtering makes the records decide visibility, a zero read version
	// answers from the current engine view, and a finished transaction errors.
	eng.SetDecayEnabled(true)
	decayedHeaders, err := tx.ReadEdges(context.Background(), AdjacentEdgeRead{Direction: AdjacentOutgoing, Node: "test:a", Headers: true})
	require.NoError(t, err)
	decayedFull, err := tx.ReadEdges(context.Background(), AdjacentEdgeRead{Direction: AdjacentOutgoing, Node: "test:a"})
	require.NoError(t, err)
	require.Equal(t, headerSummary(decayedFull), headerSummary(decayedHeaders))
	eng.SetDecayEnabled(false)
	tx.readTS = MVCCVersion{}
	current, err := tx.ReadEdges(context.Background(), AdjacentEdgeRead{Direction: AdjacentOutgoing, Node: "test:a", Headers: true})
	require.NoError(t, err)
	require.NotEmpty(t, current)
	tx.readTS = readTS
	_, err = tx.ReadEdges(context.Background(), AdjacentEdgeRead{Direction: AdjacentOutgoing, Node: "other:a", Headers: true})
	require.Error(t, err)
	require.NoError(t, tx.Rollback())
	_, err = tx.ReadEdges(context.Background(), AdjacentEdgeRead{Direction: AdjacentIncoming, Node: "test:a", Headers: true})
	require.Error(t, err)
}
