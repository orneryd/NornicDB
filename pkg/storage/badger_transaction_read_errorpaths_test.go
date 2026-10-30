package storage

import (
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

func TestBadgerTransaction_ReadEdgeAndAdjacency_ErrorPaths(t *testing.T) {
	engine := newTestEngine(t)

	// Seed a minimal graph for read helpers.
	_, err := engine.CreateNode(&Node{ID: "test:a", Labels: []string{"N"}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&Node{ID: "test:b", Labels: []string{"N"}})
	require.NoError(t, err)
	require.NoError(t, engine.CreateEdge(&Edge{ID: "test:e1", StartNode: "test:a", EndNode: "test:b", Type: "REL"}))

	tx, err := engine.BeginTransaction()
	require.NoError(t, err)

	// pinNamespaceFromIDLocked failure through public read methods.
	_, err = tx.GetOutgoingEdges("unprefixed")
	require.Error(t, err)
	require.Contains(t, err.Error(), "ID must be prefixed with namespace")

	_, err = tx.GetIncomingEdges("also-unprefixed")
	require.Error(t, err)
	require.Contains(t, err.Error(), "ID must be prefixed with namespace")

	_, err = tx.GetEdgesBetween("test:a", "missingprefix")
	require.Error(t, err)
	require.Contains(t, err.Error(), "ID must be prefixed with namespace")

	require.NoError(t, tx.Rollback())

	// ensureLifecycleActiveLocked early-return path for closed tx.
	_, err = tx.GetOutgoingEdges("test:a")
	require.ErrorIs(t, err, ErrTransactionClosed)
	_, err = tx.GetIncomingEdges("test:b")
	require.ErrorIs(t, err, ErrTransactionClosed)
	_, err = tx.GetEdgesBetween("test:a", "test:b")
	require.ErrorIs(t, err, ErrTransactionClosed)
}

func TestBadgerTransaction_GetCommittedEdgeLocked_PrimaryDecodeError(t *testing.T) {
	engine := newTestEngine(t)

	badEdgeID := EdgeID("test:bad-edge")
	require.NoError(t, engine.withUpdate(func(txn *badger.Txn) error {
		return txn.Set(edgeKey(badEdgeID), []byte("not-a-valid-edge-encoding"))
	}))

	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	tx.readTS = MVCCVersion{}

	_, err = tx.getCommittedEdgeLocked(badEdgeID)
	require.Error(t, err)
	require.Contains(t, err.Error(), "unexpected format")
}

func TestBadgerTransaction_GetNodesByLabelLocked_ReadTSBranches(t *testing.T) {
	engine := newTestEngine(t)
	_, err := engine.CreateNode(&Node{ID: "test:n1", Labels: []string{"Person"}})
	require.NoError(t, err)

	txNoPin, err := engine.BeginTransaction()
	require.NoError(t, err)
	nodes, err := txNoPin.getNodesByLabelLocked("Person")
	require.NoError(t, err)
	require.NotEmpty(t, nodes)
	require.NoError(t, txNoPin.Rollback())

	txPinned, err := engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, txPinned.SetNamespace("test"))
	nodes, err = txPinned.getNodesByLabelLocked("Person")
	require.NoError(t, err)
	require.NotEmpty(t, nodes)
	require.NoError(t, txPinned.Rollback())
}

func TestBadgerTransaction_GetCommittedEdgeLocked_VisibleAtDecodeErrorPath(t *testing.T) {
	engine := newTestEngine(t)

	_, err := engine.CreateNode(&Node{ID: "test:a", Labels: []string{"N"}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&Node{ID: "test:b", Labels: []string{"N"}})
	require.NoError(t, err)
	require.NoError(t, engine.CreateEdge(&Edge{ID: "test:e1", StartNode: "test:a", EndNode: "test:b", Type: "REL"}))

	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetNamespace("test"))
	defer func() { _ = tx.Rollback() }()

	// Existing tests cover ErrNotVisibleAtSnapshot->ErrNotFound conversion.
	// Corrupt the edge MVCC head so visible-at returns a different error.
	require.NoError(t, engine.withUpdate(func(txn *badger.Txn) error {
		headKey := engine.mvccEdgeHeadKeyStringLookup("test:e1")
		if headKey == nil {
			return ErrNotFound
		}
		return txn.Set(headKey, []byte("corrupt-head"))
	}))

	// The original physical snapshot predates the injected corruption.
	_, err = tx.getCommittedEdgeLocked("test:e1")
	require.NoError(t, err)

	corruptReader, err := engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, corruptReader.SetNamespace("test"))
	defer func() { _ = corruptReader.Rollback() }()
	_, err = corruptReader.getCommittedEdgeLocked("test:e1")
	require.Error(t, err)
	require.NotErrorIs(t, err, ErrNotFound)
}
